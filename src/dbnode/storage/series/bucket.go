// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package series

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	errMoreThanOneStreamAfterMerge = errors.New("buffer has more than one stream after merge")
	errNoAvailableBuckets          = errors.New("[invariant violated] buffer has no available buckets")
	timeZero                       time.Time
)

type databaseBufferDrainFn func(b block.DatabaseBlock)

type dbBucket struct {
	opts                  Options
	start                 time.Time
	realtimeEncoders      []inOrderEncoder
	outOfOrderEncoders    []inOrderEncoder
	bootstrapped          []block.DatabaseBlock
	cached                block.DatabaseBlock
	lastReadUnixNanos     int64
	lastWriteUnixNanos    int64
	lastSnapshotUnixNanos int64
}

type inOrderEncoder struct {
	encoder     encoding.Encoder
	lastWriteAt time.Time
}

func (b *dbBucket) resetTo(
	start time.Time,
	opts Options,
) {
	// Close the old context if we're resetting for use
	b.finalize()

	b.opts = opts
	bopts := b.opts.DatabaseBlockOptions()
	b.start = start

	encoder := bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())
	b.realtimeEncoders = append(b.realtimeEncoders, inOrderEncoder{
		encoder: encoder,
	})
	encoder = bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())
	b.outOfOrderEncoders = append(b.outOfOrderEncoders, inOrderEncoder{
		encoder: encoder,
	})

	b.bootstrapped = nil
	b.cached = nil
	atomic.StoreInt64(&b.lastReadUnixNanos, 0)
	atomic.StoreInt64(&b.lastWriteUnixNanos, 0)
	atomic.StoreInt64(&b.lastSnapshotUnixNanos, 0)
}

func (b *dbBucket) finalize() {
	b.resetEncoders()
	b.resetBootstrapped()
}

func (b *dbBucket) empty() bool {
	for _, block := range b.bootstrapped {
		if block.Len() > 0 {
			return false
		}
	}
	for _, elem := range b.realtimeEncoders {
		if elem.encoder != nil && elem.encoder.NumEncoded() > 0 {
			return false
		}
	}
	for _, elem := range b.outOfOrderEncoders {
		if elem.encoder != nil && elem.encoder.NumEncoded() > 0 {
			return false
		}
	}
	return true
}

func (b *dbBucket) bootstrap(
	bl block.DatabaseBlock,
) {
	b.bootstrapped = append(b.bootstrapped, bl)
}

func (b *dbBucket) write(
	// `now` represents the time the metric came in and not the
	// time of the metric itself.
	now time.Time,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	datapoint := ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}

	// Find the correct encoder to write to
	idx := -1
	for i := range b.realtimeEncoders {
		lastWriteAt := b.realtimeEncoders[i].lastWriteAt
		if timestamp.Equal(lastWriteAt) {
			last, err := b.realtimeEncoders[i].encoder.LastEncoded()
			if err != nil {
				return err
			}
			if last.Value == value {
				// No-op since matches the current value
				// TODO(r): in the future we could return some metadata that
				// this result was a no-op and hence does not need to be written
				// to the commit log, otherwise high frequency write volumes
				// that are using M3DB as a cache-like index of things seen
				// in a time window will still cause a flood of disk/CPU resource
				// usage writing values to the commit log, even if the memory
				// profile is lean as a side effect of this write being a no-op.
				return nil
			}
			continue
		}

		if timestamp.After(lastWriteAt) {
			idx = i
			break
		}
	}

	// Upsert/last-write-wins semantics.
	// NB(r): We push datapoints with the same timestamp but differing
	// value into a new encoder later in the stack of in order encoders
	// since an encoder is immutable.
	// The encoders pushed later will surface their values first.
	if idx != -1 {
		return b.writeToEncoderIndex(idx, datapoint, unit, annotation)
	}

	// Need a new encoder, we didn't find an encoder to write to
	b.opts.Stats().IncCreatedEncoders()
	bopts := b.opts.DatabaseBlockOptions()
	blockSize := b.opts.RetentionOptions().BlockSize()
	blockAllocSize := bopts.DatabaseBlockAllocSize()

	encoder := bopts.EncoderPool().Get()
	encoder.Reset(timestamp.Truncate(blockSize), blockAllocSize)

	b.realtimeEncoders = append(b.realtimeEncoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: timestamp,
	})

	idx = len(b.realtimeEncoders) - 1
	err := b.writeToEncoderIndex(idx, datapoint, unit, annotation)
	if err != nil {
		encoder.Close()
		b.realtimeEncoders = b.realtimeEncoders[:idx]
		return err
	}

	b.setLastWrite(now)
	return nil
}

func (b *dbBucket) writeToEncoderIndex(
	idx int,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation []byte,
) error {
	err := b.realtimeEncoders[idx].encoder.Encode(datapoint, unit, annotation)
	if err != nil {
		return err
	}

	b.realtimeEncoders[idx].lastWriteAt = datapoint.Timestamp
	return nil
}

func (b *dbBucket) streams(ctx context.Context) []xio.BlockReader {
	streams := make([]xio.BlockReader, 0, len(b.bootstrapped)+len(b.realtimeEncoders))

	if b.cached != nil {
		if s, err := b.cached.Stream(ctx); err == nil && s.IsNotEmpty() {
			streams = append(streams, s)
		}
	}
	for i := range b.bootstrapped {
		if b.bootstrapped[i].Len() == 0 {
			continue
		}
		if s, err := b.bootstrapped[i].Stream(ctx); err == nil && s.IsNotEmpty() {
			// NB(r): block stream method will register the stream closer already
			streams = append(streams, s)
		}
	}
	for i := range b.realtimeEncoders {
		start := b.start
		if s := b.realtimeEncoders[i].encoder.Stream(); s != nil {
			br := xio.BlockReader{
				SegmentReader: s,
				Start:         start,
				BlockSize:     b.opts.RetentionOptions().BlockSize(),
			}
			ctx.RegisterFinalizer(s)
			streams = append(streams, br)
		}
	}
	for i := range b.outOfOrderEncoders {
		start := b.start
		if s := b.outOfOrderEncoders[i].encoder.Stream(); s != nil {
			br := xio.BlockReader{
				SegmentReader: s,
				Start:         start,
				BlockSize:     b.opts.RetentionOptions().BlockSize(),
			}
			ctx.RegisterFinalizer(s)
			streams = append(streams, br)
		}
	}

	return streams
}

func (b *dbBucket) streamsLen() int {
	length := 0
	for i := range b.bootstrapped {
		length += b.bootstrapped[i].Len()
	}
	for i := range b.realtimeEncoders {
		length += b.realtimeEncoders[i].encoder.Len()
	}
	for i := range b.outOfOrderEncoders {
		length += b.outOfOrderEncoders[i].encoder.Len()
	}
	return length
}

func (b *dbBucket) mergeStreams(ctx context.Context) (xio.BlockReader, error) {
	if b.empty() {
		return xio.EmptyBlockReader, nil
	}

	// We need to merge all the bootstrapped blocks / encoders into a single stream for
	// the sake of being able to persist it to disk as a single encoded stream.
	_, err := b.merge()
	if err != nil {
		return xio.EmptyBlockReader, err
	}

	// This operation is safe because all of the underlying resources will respect the
	// lifecycle of the context in one way or another. The "bootstrapped blocks" that
	// we stream from will mark their internal context as dependent on that of the passed
	// context, and the Encoder's that we stream from actually perform a data copy and
	// don't share a reference.
	streams := b.streams(ctx)
	if len(streams) != 1 {
		// Should never happen as the call to merge above should result in only a single
		// stream being present.
		return xio.EmptyBlockReader, errMoreThanOneStreamAfterMerge
	}

	// Direct indexing is safe because a bucket not being empty guarantees us at
	// least one stream
	return streams[0], nil
}

func (b *dbBucket) setLastRead(value time.Time) {
	atomic.StoreInt64(&b.lastReadUnixNanos, value.UnixNano())
}

func (b *dbBucket) setLastWrite(value time.Time) {
	atomic.StoreInt64(&b.lastWriteUnixNanos, value.UnixNano())
}

func (b *dbBucket) setLastSnapshot(value time.Time) {
	atomic.StoreInt64(&b.lastSnapshotUnixNanos, value.UnixNano())
}

func (b *dbBucket) lastRead() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
}

func (b *dbBucket) lastWrite() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastWriteUnixNanos))
}

func (b *dbBucket) lastSnapshot() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastSnapshotUnixNanos))
}

func (b *dbBucket) resetEncoders() {
	var zeroed inOrderEncoder
	for i := range b.realtimeEncoders {
		// Register when this bucket resets we close the encoder
		encoder := b.realtimeEncoders[i].encoder
		encoder.Close()
		b.realtimeEncoders[i] = zeroed
	}
	b.realtimeEncoders = b.realtimeEncoders[:0]

	for i := range b.outOfOrderEncoders {
		// Register when this bucket resets we close the encoder
		encoder := b.outOfOrderEncoders[i].encoder
		encoder.Close()
		b.outOfOrderEncoders[i] = zeroed
	}
	b.outOfOrderEncoders = b.outOfOrderEncoders[:0]
}

func (b *dbBucket) resetBootstrapped() {
	for i := range b.bootstrapped {
		bl := b.bootstrapped[i]
		bl.Close()
	}
	b.bootstrapped = nil
}

func (b *dbBucket) needsMerge() bool {
	return !b.empty() && !(b.hasJustSingleBootstrappedBlock() ||
		b.hasJustSingleRealtimeEncoder() || b.hasJustSingleOutOfOrderEncoder())
}

func (b *dbBucket) hasJustSingleRealtimeEncoder() bool {
	return len(b.realtimeEncoders) == 1 && len(b.bootstrapped) == 0 && b.outOfOrderEncodersEmpty()
}

func (b *dbBucket) hasJustSingleOutOfOrderEncoder() bool {
	return len(b.outOfOrderEncoders) == 1 && len(b.bootstrapped) == 0 && b.realtimeEncodersEmpty()
}

func (b *dbBucket) hasJustSingleBootstrappedBlock() bool {
	return b.realtimeEncodersEmpty() && b.outOfOrderEncodersEmpty() && len(b.bootstrapped) == 1
}

func (b *dbBucket) hasJustCachedBlock() bool {
	return b.realtimeEncodersEmpty() && b.outOfOrderEncodersEmpty() &&
		len(b.bootstrapped) == 0 && b.cached != nil

}

func (b *dbBucket) realtimeEncodersEmpty() bool {
	return len(b.realtimeEncoders) == 0 ||
		(len(b.realtimeEncoders) == 1 &&
			b.realtimeEncoders[0].encoder.Len() == 0)
}

func (b *dbBucket) outOfOrderEncodersEmpty() bool {
	return len(b.outOfOrderEncoders) == 0 ||
		(len(b.outOfOrderEncoders) == 1 &&
			b.outOfOrderEncoders[0].encoder.Len() == 0)
}

type mergeResult struct {
	merges int
}

func (b *dbBucket) merge() (mergeResult, error) {
	if !b.needsMerge() {
		// Save unnecessary work
		return mergeResult{}, nil
	}

	merges := 0
	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(b.start, bopts.DatabaseBlockAllocSize())

	// If we have to merge bootstrapped from disk during a merge then this
	// can make ticking very slow, ensure to notify this bug
	if len(b.bootstrapped) > 0 {
		unretrieved := 0
		for i := range b.bootstrapped {
			if !b.bootstrapped[i].IsRetrieved() {
				unretrieved++
			}
		}
		if unretrieved > 0 {
			log := b.opts.InstrumentOptions().Logger()
			log.Warnf("buffer merging %d unretrieved blocks", unretrieved)
		}
	}

	var (
		start   = b.start
		readers = make([]xio.SegmentReader, 0, len(b.realtimeEncoders)+len(b.outOfOrderEncoders)+len(b.bootstrapped))
		streams = make([]xio.SegmentReader, 0, len(b.realtimeEncoders)+len(b.outOfOrderEncoders))
		iter    = b.opts.MultiReaderIteratorPool().Get()
		ctx     = b.opts.ContextPool().Get()
	)
	defer func() {
		iter.Close()
		ctx.Close()
		// NB(r): Only need to close the mutable encoder streams as
		// the context we created for reading the bootstrap blocks
		// when closed will close those streams.
		for _, stream := range streams {
			stream.Finalize()
		}
	}()

	// Rank bootstrapped blocks as data that has appeared before data that
	// arrived locally in the buffer
	for i := range b.bootstrapped {
		block, err := b.bootstrapped[i].Stream(ctx)
		if err == nil && block.SegmentReader != nil {
			merges++
			readers = append(readers, block.SegmentReader)
		}
	}

	for i := range b.realtimeEncoders {
		if s := b.realtimeEncoders[i].encoder.Stream(); s != nil {
			merges++
			readers = append(readers, s)
			streams = append(streams, s)
		}
	}
	for i := range b.outOfOrderEncoders {
		if s := b.outOfOrderEncoders[i].encoder.Stream(); s != nil {
			merges++
			readers = append(readers, s)
			streams = append(streams, s)
		}
	}

	var lastWriteAt time.Time
	iter.Reset(readers, start, b.opts.RetentionOptions().BlockSize())
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			return mergeResult{}, err
		}
		lastWriteAt = dp.Timestamp
	}
	if err := iter.Err(); err != nil {
		return mergeResult{}, err
	}

	b.resetEncoders()
	b.resetBootstrapped()

	b.realtimeEncoders = append(b.realtimeEncoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: lastWriteAt,
	})

	return mergeResult{merges: merges}, nil
}

type discardMergedResult struct {
	block  block.DatabaseBlock
	merges int
}

// TODO use this
func (b *dbBucket) discardMerged() (discardMergedResult, error) {
	if b.hasJustSingleRealtimeEncoder() {
		// Already merged as a single encoder
		encoder := b.realtimeEncoders[0].encoder
		newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		blockSize := b.opts.RetentionOptions().BlockSize()
		newBlock.Reset(b.start, blockSize, encoder.Discard())

		// The single encoder is already discarded, no need to call resetEncoders
		// just remove it from the list of encoders
		b.realtimeEncoders = b.realtimeEncoders[:0]
		b.resetBootstrapped()

		return discardMergedResult{newBlock, 0}, nil
	}

	if b.hasJustSingleBootstrappedBlock() {
		// Already merged just a single bootstrapped block
		existingBlock := b.bootstrapped[0]

		// Need to reset encoders but do not want to finalize the block as we
		// are passing ownership of it to the caller
		b.resetEncoders()
		b.bootstrapped = nil

		return discardMergedResult{existingBlock, 0}, nil
	}

	result, err := b.merge()
	if err != nil {
		b.resetEncoders()
		b.resetBootstrapped()
		return discardMergedResult{}, err
	}

	merged := b.realtimeEncoders[0].encoder

	newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	blockSize := b.opts.RetentionOptions().BlockSize()
	newBlock.Reset(b.start, blockSize, merged.Discard())

	// The merged encoder is already discarded, no need to call resetEncoders
	// just remove it from the list of encoders
	b.realtimeEncoders = b.realtimeEncoders[:0]
	b.resetBootstrapped()

	return discardMergedResult{newBlock, result.merges}, nil
}

func (b *dbBucket) removeCached() {
	b.cached = nil
}

type dbBucketPool struct {
	pool pool.ObjectPool
}

// newdbBucketPool creates a new dbBucketPool
func newdbBucketPool(opts pool.ObjectPoolOptions) *dbBucketPool {
	p := &dbBucketPool{pool: pool.NewObjectPool(opts)}
	p.pool.Init(func() interface{} {
		return &dbBucket{}
	})
	return p
}

func (p *dbBucketPool) Get() *dbBucket {
	return p.pool.Get().(*dbBucket)
}

func (p *dbBucketPool) Put(bucket *dbBucket) {
	p.pool.Put(*bucket)
}
