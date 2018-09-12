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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
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

const (
	lruCacheSize = 2

	// TODO: make sure this is a good pool size or make it customizable
	defaultBufferBucketPoolSize = 16
)

type databaseBuffer interface {
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	Snapshot(ctx context.Context, blockStart time.Time) (xio.SegmentReader, error)

	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) [][]xio.BlockReader

	FetchBlocks(
		ctx context.Context,
		starts []time.Time,
	) []block.FetchBlockResult

	FetchBlocksMetadata(
		ctx context.Context,
		start, end time.Time,
		opts FetchBlocksMetadataOptions,
	) block.FetchBlockMetadataResults

	IsEmpty() bool

	Stats() bufferStats

	// MinMax returns the minimum and maximum blockstarts for the buckets
	// that are contained within the buffer. These ranges exclude buckets
	// that have already been drained (as those buckets are no longer in use.)
	MinMax() (time.Time, time.Time, error)

	Tick() bufferTickResult

	Bootstrap(bl block.DatabaseBlock) error

	Reset(opts Options)
}

type bufferStats struct {
	openBlocks  int
	wiredBlocks int
}

type drainAndResetResult struct {
	mergedOutOfOrderBlocks int
}

type bufferTickResult struct {
	mergedOutOfOrderBlocks int
}

type dbBuffer struct {
	opts           Options
	nowFn          clock.NowFn
	drainFn        databaseBufferDrainFn
	bucketLRUCache [lruCacheSize]*dbBufferBucket
	buckets        map[xtime.UnixNano]*dbBufferBucket
	bucketPool     *dbBufferBucketPool
	blockSize      time.Duration
	bufferPast     time.Duration
	bufferFuture   time.Duration
}

type databaseBufferDrainFn func(b block.DatabaseBlock)

// NB(prateek): databaseBuffer.Reset(...) must be called upon the returned
// object prior to use.
func newDatabaseBuffer(drainFn databaseBufferDrainFn) databaseBuffer {
	b := &dbBuffer{
		drainFn: drainFn,
		buckets: make(map[xtime.UnixNano]*dbBufferBucket),
	}

	return b
}

func (b *dbBuffer) Reset(opts Options) {
	b.opts = opts
	b.nowFn = opts.ClockOptions().NowFn()
	ropts := opts.RetentionOptions()
	b.blockSize = ropts.BlockSize()
	b.bufferPast = ropts.BufferPast()
	b.bufferFuture = ropts.BufferFuture()

	if ropts.NonRealtimeWritesEnabled() {
		bucketPoolOpts := pool.NewObjectPoolOptions().SetSize(defaultBufferBucketPoolSize)
		b.bucketPool = newDBBufferBucketPool(bucketPoolOpts)
	}
}

func (b *dbBuffer) MinMax() (time.Time, time.Time, error) {
	var min, max time.Time
	for i := range b.buckets {
		if (min.IsZero() || b.buckets[i].start.Before(min)) && !b.buckets[i].drained {
			min = b.buckets[i].start
		}
		if max.IsZero() || b.buckets[i].start.After(max) && !b.buckets[i].drained {
			max = b.buckets[i].start
		}
	}

	if min.IsZero() || max.IsZero() {
		// Should never happen
		return time.Time{}, time.Time{}, errNoAvailableBuckets
	}
	return min, max, nil
}

func (b *dbBuffer) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	if !b.opts.RetentionOptions().NonRealtimeWritesEnabled() && !b.isRealtime(timestamp) {
		return m3dberrors.ErrNonRealtimeWriteTimeNotEnabled
	}

	bucket := b.bucketForTime(timestamp)
	return bucket.write(b.nowFn(), timestamp, value, unit, annotation)
}

func (b *dbBuffer) isRealtime(timestamp time.Time) bool {
	now := b.nowFn()
	futureLimit := now.Add(1 * b.bufferFuture)
	pastLimit := now.Add(-1 * b.bufferPast)
	return pastLimit.Before(timestamp) && futureLimit.After(timestamp)
}

func (b *dbBuffer) bucketForTime(t time.Time) *dbBufferBucket {
	blockStart := t.Truncate(b.blockSize)

	// First check LRU cache
	for _, bucket := range b.bucketLRUCache {
		if bucket == nil {
			continue
		}

		if bucket.start.Equal(blockStart) {
			return bucket
		}
	}

	// Then check the map
	key := xtime.ToUnixNano(blockStart)
	if bucket, ok := b.buckets[key]; ok {
		return bucket
	}

	// Doesn't exist, so create it
	bucket := b.bucketPool.Get()
	bucket.resetTo(blockStart, b.opts)
	// Update LRU cache
	b.bucketLRUCache[b.lruBucketIdxInCache()] = bucket
	b.buckets[key] = bucket
	return bucket
}

// lruBucketIdx returns the index of the least recently used bucket in the LRU cache
func (b *dbBuffer) lruBucketIdxInCache() int {
	idx := -1
	var lastReadTime time.Time

	for i, bucket := range b.bucketLRUCache {
		if bucket == nil {
			// An empty slot in the cache is older than any existing bucket
			return i
		}

		curLastRead := bucket.lastRead()
		if idx == -1 || curLastRead.Before(lastReadTime) {
			lastReadTime = curLastRead
			idx = i
		}
	}

	return idx
}

func (b *dbBuffer) bucketKeyForTime(t time.Time) xtime.UnixNano {
	return xtime.ToUnixNano(t.Truncate(b.blockSize))
}

func (b *dbBuffer) removeBucket(key xtime.UnixNano) {
	b.bucketPool.Put(b.buckets[key])
	delete(b.buckets, key)
}

func (b *dbBuffer) IsEmpty() bool {
	for _, bucket := range b.buckets {
		if bucket.canRead() {
			return false
		}
	}

	return true
}

func (b *dbBuffer) Stats() bufferStats {
	var stats bufferStats

	for _, bucket := range b.buckets {
		if !bucket.canRead() {
			continue
		}
		stats.openBlocks++
		stats.wiredBlocks++
	}

	return stats
}

func (b *dbBuffer) Tick() bufferTickResult {
	mergedOutOfOrder := 0
	for key := range b.buckets {
		mergedOutOfOrder += bucketTick(b.nowFn(), b, key.ToTime())
	}

	return bufferTickResult{
		mergedOutOfOrderBlocks: mergedOutOfOrder,
	}
}

func bucketTick(now time.Time, b *dbBuffer, start time.Time) int {
	mergedOutOfOrderBlocks := 0
	bucket := b.bucketForTime(start)

	// Try to merge any out of order encoders to amortize the cost of a drain
	r, err := bucket.merge()
	if err != nil {
		log := b.opts.InstrumentOptions().Logger()
		log.Errorf("buffer merge encode error: %v", err)
	}
	if r.merges > 0 {
		mergedOutOfOrderBlocks++
	}

	if bucket.isStale(now) {
		b.removeBucket(b.bucketKeyForTime(start))
	}

	return mergedOutOfOrderBlocks
}

func (b *dbBuffer) bucketDrain(now time.Time, t time.Time) int {
	mergedOutOfOrderBlocks := 0

	bucket := b.bucketForTime(t)
	// Rotate the buffer to a block, merging if required
	result, err := bucket.discardMerged()
	if err != nil {
		log := b.opts.InstrumentOptions().Logger()
		log.Errorf("buffer merge encode error: %v", err)
	} else {
		if result.merges > 0 {
			mergedOutOfOrderBlocks++
		}
		if !(result.block.Len() > 0) {
			log := b.opts.InstrumentOptions().Logger()
			log.Errorf("buffer drain tried to drain empty stream for bucket: %v",
				bucket.start.String())
		} else {
			// If this block was read mark it as such
			if lastRead := bucket.lastRead(); !lastRead.IsZero() {
				result.block.SetLastReadTime(lastRead)
			}
			if b.drainFn != nil {
				b.drainFn(result.block)
			}
		}
	}

	bucket.drained = true
	bucket.resetNumWrites()

	return mergedOutOfOrderBlocks
}

func (b *dbBuffer) Bootstrap(bl block.DatabaseBlock) error {
	blockStart := bl.StartTime()
	key := b.bucketKeyForTime(blockStart)
	bootstrapped := false

	if bucket, ok := b.buckets[key]; ok {
		if bucket.drained {
			return fmt.Errorf(
				"block at %s cannot be bootstrapped by buffer because its already drained",
				blockStart.String(),
			)
		}
		bucket.bootstrap(bl)
		bootstrapped = true
	}

	if !bootstrapped {
		return fmt.Errorf("block at %s not contained by buffer", blockStart.String())
	}
	return nil
}

// forEachBucketAsc iterates over the buckets in time ascending order
// to read bucket data
func (b *dbBuffer) forEachBucketAsc(
	fn func(*dbBufferBucket),
) {
	for _, bucket := range b.buckets {
		fn(bucket)
	}
}

func (b *dbBuffer) Snapshot(ctx context.Context, blockStart time.Time) (xio.SegmentReader, error) {
	var (
		res xio.SegmentReader
		err error
	)

	for _, bucket := range b.buckets {
		if err != nil {
			// Something already went wrong and we want to return the error to the caller
			// as soon as possible instead of continuing to do work.
			continue
		}

		if !bucket.canRead() {
			continue
		}

		if !blockStart.Equal(bucket.start) {
			continue
		}

		// We need to merge all the bootstrapped blocks / encoders into a single stream for
		// the sake of being able to persist it to disk as a single encoded stream.
		_, err = bucket.merge()
		if err != nil {
			continue
		}

		// This operation is safe because all of the underlying resources will respect the
		// lifecycle of the context in one way or another. The "bootstrapped blocks" that
		// we stream from will mark their internal context as dependent on that of the passed
		// context, and the Encoder's that we stream from actually perform a data copy and
		// don't share a reference.
		streams := bucket.streams(ctx)
		if len(streams) != 1 {
			// Should never happen as the call to merge above should result in only a single
			// stream being present.
			err = errMoreThanOneStreamAfterMerge
			continue
		}

		// Direct indexing is safe because canRead guarantees us at least one stream
		res = streams[0]
	}

	return res, err
}

func (b *dbBuffer) ReadEncoded(ctx context.Context, start, end time.Time) [][]xio.BlockReader {
	// TODO(r): pool these results arrays
	var res [][]xio.BlockReader
	for _, bucket := range b.buckets {
		if !bucket.canRead() {
			continue
		}
		if !start.Before(bucket.start.Add(b.blockSize)) {
			continue
		}
		if !bucket.start.Before(end) {
			continue
		}

		res = append(res, bucket.streams(ctx))

		// NB(r): Store the last read time, should not set this when
		// calling FetchBlocks as a read is differentiated from
		// a FetchBlocks call. One is initiated by an external
		// entity and the other is used for streaming blocks between
		// the storage nodes. This distinction is important as this
		// data is important for use with understanding access patterns, etc.
		bucket.setLastRead(b.nowFn())
	}

	return res
}

func (b *dbBuffer) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	var res []block.FetchBlockResult

	for _, bucket := range b.buckets {
		if !bucket.canRead() {
			continue
		}
		found := false
		// starts have only a few items, linear search should be okay time-wise to
		// avoid allocating a map here.
		for _, start := range starts {
			if start.Equal(bucket.start) {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		streams := bucket.streams(ctx)
		res = append(res, block.NewFetchBlockResult(bucket.start, streams, nil))
	}

	return res
}

func (b *dbBuffer) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	opts FetchBlocksMetadataOptions,
) block.FetchBlockMetadataResults {
	blockSize := b.opts.RetentionOptions().BlockSize()
	res := b.opts.FetchBlockMetadataResultsPool().Get()
	for _, bucket := range b.buckets {
		if !bucket.canRead() {
			continue
		}
		if !start.Before(bucket.start.Add(blockSize)) || !bucket.start.Before(end) {
			continue
		}
		size := int64(bucket.streamsLen())
		// If we have no data in this bucket, return early without appending it to the result.
		if size == 0 {
			continue
		}
		var resultSize int64
		if opts.IncludeSizes {
			resultSize = size
		}
		var resultLastRead time.Time
		if opts.IncludeLastRead {
			resultLastRead = bucket.lastRead()
		}
		// NB(r): Ignore if opts.IncludeChecksum because we avoid
		// calculating checksum since block is open and is being mutated
		res.Add(block.FetchBlockMetadataResult{
			Start:    bucket.start,
			Size:     resultSize,
			LastRead: resultLastRead,
		})
	}

	return res
}

type dbBufferBucket struct {
	opts               Options
	start              time.Time
	encoders           []inOrderEncoder
	bootstrapped       []block.DatabaseBlock
	lastReadUnixNanos  int64
	lastWriteUnixNanos int64
	undrainedWrites    uint64
	drained            bool
}

type inOrderEncoder struct {
	encoder     encoding.Encoder
	lastWriteAt time.Time
}

func (b *dbBufferBucket) resetTo(
	start time.Time,
	opts Options,
) {
	// Close the old context if we're resetting for use
	b.finalize()

	b.opts = opts
	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())

	b.start = start
	b.encoders = append(b.encoders, inOrderEncoder{
		encoder: encoder,
	})
	b.bootstrapped = nil
	atomic.StoreInt64(&b.lastReadUnixNanos, 0)
	atomic.StoreInt64(&b.lastWriteUnixNanos, 0)
	b.drained = false
	b.resetNumWrites()
}

func (b *dbBufferBucket) finalize() {
	b.resetEncoders()
	b.resetBootstrapped()
}

func (b *dbBufferBucket) empty() bool {
	for _, block := range b.bootstrapped {
		if block.Len() > 0 {
			return false
		}
	}
	for _, elem := range b.encoders {
		if elem.encoder != nil && elem.encoder.NumEncoded() > 0 {
			return false
		}
	}
	return true
}

func (b *dbBufferBucket) canRead() bool {
	return !b.drained && !b.empty()
}

func (b *dbBufferBucket) isStale(now time.Time) bool {
	return now.Sub(b.lastWrite()) > b.opts.RetentionOptions().NonRealtimeFlushAfterNoMetricPeriod()
}

func (b *dbBufferBucket) isFull() bool {
	return b.numWrites() >= b.opts.RetentionOptions().NonRealtimeMaxWritesBeforeFlush()
}

func (b *dbBufferBucket) bootstrap(
	bl block.DatabaseBlock,
) {
	b.bootstrapped = append(b.bootstrapped, bl)
}

func (b *dbBufferBucket) write(
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
	for i := range b.encoders {
		lastWriteAt := b.encoders[i].lastWriteAt
		if timestamp.Equal(lastWriteAt) {
			last, err := b.encoders[i].encoder.LastEncoded()
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

	b.encoders = append(b.encoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: timestamp,
	})

	idx = len(b.encoders) - 1
	err := b.writeToEncoderIndex(idx, datapoint, unit, annotation)
	if err != nil {
		encoder.Close()
		b.encoders = b.encoders[:idx]
		return err
	}

	b.setLastWrite(now)
	b.incNumWrites()
	// Required for non-realtime buckets
	b.drained = false
	return nil
}

func (b *dbBufferBucket) writeToEncoderIndex(
	idx int,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation []byte,
) error {
	err := b.encoders[idx].encoder.Encode(datapoint, unit, annotation)
	if err != nil {
		return err
	}

	b.encoders[idx].lastWriteAt = datapoint.Timestamp
	return nil
}

func (b *dbBufferBucket) streams(ctx context.Context) []xio.BlockReader {
	streams := make([]xio.BlockReader, 0, len(b.bootstrapped)+len(b.encoders))

	for i := range b.bootstrapped {
		if b.bootstrapped[i].Len() == 0 {
			continue
		}
		if s, err := b.bootstrapped[i].Stream(ctx); err == nil && s.IsNotEmpty() {
			// NB(r): block stream method will register the stream closer already
			streams = append(streams, s)
		}
	}
	for i := range b.encoders {
		start := b.start
		if s := b.encoders[i].encoder.Stream(); s != nil {
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

func (b *dbBufferBucket) streamsLen() int {
	length := 0
	for i := range b.bootstrapped {
		length += b.bootstrapped[i].Len()
	}
	for i := range b.encoders {
		length += b.encoders[i].encoder.Len()
	}
	return length
}

func (b *dbBufferBucket) setLastRead(value time.Time) {
	atomic.StoreInt64(&b.lastReadUnixNanos, value.UnixNano())
}

func (b *dbBufferBucket) setLastWrite(value time.Time) {
	atomic.StoreInt64(&b.lastWriteUnixNanos, value.UnixNano())
}

func (b *dbBufferBucket) incNumWrites() {
	atomic.AddUint64(&b.undrainedWrites, 1)
}

func (b *dbBufferBucket) resetNumWrites() {
	atomic.StoreUint64(&b.undrainedWrites, uint64(0))
}

func (b *dbBufferBucket) lastRead() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
}

func (b *dbBufferBucket) lastWrite() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastWriteUnixNanos))
}

func (b *dbBufferBucket) numWrites() uint64 {
	return atomic.LoadUint64(&b.undrainedWrites)
}

func (b *dbBufferBucket) resetEncoders() {
	var zeroed inOrderEncoder
	for i := range b.encoders {
		// Register when this bucket resets we close the encoder
		encoder := b.encoders[i].encoder
		encoder.Close()
		b.encoders[i] = zeroed
	}
	b.encoders = b.encoders[:0]
}

func (b *dbBufferBucket) resetBootstrapped() {
	for i := range b.bootstrapped {
		bl := b.bootstrapped[i]
		bl.Close()
	}
	b.bootstrapped = nil
}

func (b *dbBufferBucket) needsMerge() bool {
	return b.canRead() && !(b.hasJustSingleEncoder() || b.hasJustSingleBootstrappedBlock())
}

func (b *dbBufferBucket) hasJustSingleEncoder() bool {
	return len(b.encoders) == 1 && len(b.bootstrapped) == 0
}

func (b *dbBufferBucket) hasJustSingleBootstrappedBlock() bool {
	encodersEmpty := len(b.encoders) == 0 ||
		(len(b.encoders) == 1 &&
			b.encoders[0].encoder.Len() == 0)
	return encodersEmpty && len(b.bootstrapped) == 1
}

type mergeResult struct {
	merges int
}

func (b *dbBufferBucket) merge() (mergeResult, error) {
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
		readers = make([]xio.SegmentReader, 0, len(b.encoders)+len(b.bootstrapped))
		streams = make([]xio.SegmentReader, 0, len(b.encoders))
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

	for i := range b.encoders {
		if s := b.encoders[i].encoder.Stream(); s != nil {
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

	b.encoders = append(b.encoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: lastWriteAt,
	})

	return mergeResult{merges: merges}, nil
}

type discardMergedResult struct {
	block  block.DatabaseBlock
	merges int
}

// TODO
func (b *dbBufferBucket) discardMerged() (discardMergedResult, error) {
	if b.hasJustSingleEncoder() {
		// Already merged as a single encoder
		encoder := b.encoders[0].encoder
		newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		blockSize := b.opts.RetentionOptions().BlockSize()
		newBlock.Reset(b.start, blockSize, encoder.Discard())

		// The single encoder is already discarded, no need to call resetEncoders
		// just remove it from the list of encoders
		b.encoders = b.encoders[:0]
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

	merged := b.encoders[0].encoder

	newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	blockSize := b.opts.RetentionOptions().BlockSize()
	newBlock.Reset(b.start, blockSize, merged.Discard())

	// The merged encoder is already discarded, no need to call resetEncoders
	// just remove it from the list of encoders
	b.encoders = b.encoders[:0]
	b.resetBootstrapped()

	return discardMergedResult{newBlock, result.merges}, nil
}

type dbBufferBucketPool struct {
	pool pool.ObjectPool
}

// newDBBufferBucketPool creates a new dbBufferBucketPool
func newDBBufferBucketPool(opts pool.ObjectPoolOptions) *dbBufferBucketPool {
	p := &dbBufferBucketPool{pool: pool.NewObjectPool(opts)}
	p.pool.Init(func() interface{} {
		return &dbBufferBucket{}
	})
	return p
}

func (p *dbBufferBucketPool) Get() *dbBufferBucket {
	return p.pool.Get().(*dbBufferBucket)
}

func (p *dbBufferBucketPool) Put(bucket *dbBufferBucket) {
	p.pool.Put(*bucket)
}
