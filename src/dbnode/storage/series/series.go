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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/storage/block"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

type bootstrapState int

const (
	bootstrapNotStarted bootstrapState = iota
	bootstrapped

	lruCacheSize = 2

	// TODO: make sure this is a good pool size or make it customizable
	defaultBufferBucketPoolSize = 16
)

var (
	// ErrSeriesAllDatapointsExpired is returned on tick when all datapoints are expired
	ErrSeriesAllDatapointsExpired = errors.New("series datapoints are all expired")

	errSeriesAlreadyBootstrapped = errors.New("series is already bootstrapped")
	errSeriesNotBootstrapped     = errors.New("series is not yet bootstrapped")
	errStreamDidNotExistForBlock = errors.New("stream did not exist for block")
)

type dbSeries struct {
	sync.RWMutex
	opts Options

	// NB(r): One should audit all places that access the
	// series ID before changing ownership semantics (e.g.
	// pooling the ID rather than releasing it to the GC on
	// calling series.Reset()).
	id   ident.ID
	tags ident.Tags

	bs                          bootstrapState
	blockRetriever              QueryableBlockRetriever
	onRetrieveBlock             block.OnRetrieveBlock
	blockOnEvictedFromWiredList block.OnEvictedFromWiredList
	pool                        DatabaseSeriesPool

	bucketLRUCache [lruCacheSize]*dbBucket
	buckets        map[xtime.UnixNano]*dbBucket
	bucketPool     *dbBucketPool
	bufferPast     time.Duration
	bufferFuture   time.Duration
}

// NewDatabaseSeries creates a new database series
func NewDatabaseSeries(id ident.ID, tags ident.Tags, opts Options) DatabaseSeries {
	s := newDatabaseSeries()
	s.Reset(id, tags, nil, nil, nil, opts)
	return s
}

// newPooledDatabaseSeries creates a new pooled database series
func newPooledDatabaseSeries(pool DatabaseSeriesPool) DatabaseSeries {
	series := newDatabaseSeries()
	series.pool = pool
	return series
}

// NB(prateek): dbSeries.Reset(...) must be called upon the returned
// object prior to use.
func newDatabaseSeries() *dbSeries {
	series := &dbSeries{
		bs: bootstrapNotStarted,
	}
	return series
}

func (s *dbSeries) now() time.Time {
	nowFn := s.opts.ClockOptions().NowFn()
	return nowFn()
}

func (s *dbSeries) ID() ident.ID {
	s.RLock()
	id := s.id
	s.RUnlock()
	return id
}

func (s *dbSeries) Tags() ident.Tags {
	s.RLock()
	tags := s.tags
	s.RUnlock()
	return tags
}

func (s *dbSeries) Tick() (TickResult, error) {
	var r TickResult
	s.Lock()
	defer s.Unlock()

	for _, bucket := range s.buckets {
		// Try to merge any out of order encoders to amortize the cost of a drain
		mergeResult, err := bucket.merge()
		if err != nil {
			log := s.opts.InstrumentOptions().Logger()
			log.Errorf("buffer merge encode error: %v", err)
		}

		if mergeResult.merges > 0 {
			r.MergedOutOfOrderBlocks++
		}
	}

	update, err := s.updateBlocksWithLock()
	if err != nil {
		return r, err
	}

	r.TickStatus = update.TickStatus
	r.MadeExpiredBlocks, r.MadeUnwiredBlocks =
		update.madeExpiredBlocks, update.madeUnwiredBlocks

	if update.ActiveBlocks == 0 {
		return r, ErrSeriesAllDatapointsExpired
	}
	return r, nil
}

type updateBlocksResult struct {
	TickStatus
	madeExpiredBlocks int
	madeUnwiredBlocks int
}

func (s *dbSeries) updateBlocksWithLock() (updateBlocksResult, error) {
	var (
		result       updateBlocksResult
		now          = s.now()
		ropts        = s.opts.RetentionOptions()
		retriever    = s.blockRetriever
		cachePolicy  = s.opts.CachePolicy()
		expireCutoff = now.Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
		wiredTimeout = ropts.BlockDataExpiryAfterNotAccessedPeriod()
	)
	for startNano, bucket := range s.buckets {
		start := startNano.ToTime()
		cachedBlock := bucket.cached
		if start.Before(expireCutoff) {
			s.removeBucketAt(start)
			// If we're using the LRU policy and the block was retrieved from disk,
			// then don't close the block because that is the WiredList's
			// responsibility. The block will hang around the WiredList until
			// it is evicted to make room for something else at which point it
			// will be closed.
			//
			// Note that while we don't close the block, we do remove it from the list
			// of blocks. This is so that the series itself can still be expired if this
			// was the last block. The WiredList will still notify the shard/series via
			// the OnEvictedFromWiredList method when it closes the block, but those
			// methods are noops for series/blocks that have already been removed.
			//
			// Also note that while technically the DatabaseBlock protects against double
			// closes, they can be problematic due to pooling. I.E if the following sequence
			// of actions happens:
			// 		1) Tick closes expired block
			// 		2) Block is re-inserted into pool
			// 		3) Block is pulled out of pool and used for critical data
			// 		4) WiredList tries to close the block, not knowing that it has
			// 		   already been closed, and re-opened / re-used leading to
			// 		   unexpected behavior or data loss.
			if cachedBlock != nil {
				if cachePolicy == CacheLRU && cachedBlock.WasRetrievedFromDisk() {
					// Do nothing
				} else {
					cachedBlock.Close()
				}
				result.madeExpiredBlocks++
			}
			continue
		}

		if cachedBlock == nil {
			continue
		}

		result.ActiveBlocks++

		if cachePolicy == CacheAll || retriever == nil {
			// Never unwire
			result.WiredBlocks++
			continue
		}

		if cachePolicy == CacheAllMetadata && !cachedBlock.IsRetrieved() {
			// Already unwired
			result.UnwiredBlocks++
			continue
		}

		// Potentially unwire
		var unwired, shouldUnwire bool
		// IsBlockRetrievable makes sure that the block has been flushed. This
		// prevents us from unwiring blocks that haven't been flushed yet which
		// would cause data loss.
		if retriever.IsBlockRetrievable(start) {
			switch cachePolicy {
			case CacheNone:
				shouldUnwire = true
			case CacheAllMetadata:
				// Apply RecentlyRead logic (CacheAllMetadata is being removed soon)
				fallthrough
			case CacheRecentlyRead:
				sinceLastRead := now.Sub(cachedBlock.LastReadTime())
				shouldUnwire = sinceLastRead >= wiredTimeout
			case CacheLRU:
				// The tick is responsible for managing the lifecycle of blocks that were not
				// read from disk (not retrieved), and the WiredList will manage those that were
				// retrieved from disk.
				shouldUnwire = !cachedBlock.WasRetrievedFromDisk()
			default:
				s.opts.InstrumentOptions().Logger().Fatalf(
					"unhandled cache policy in series tick: %s", cachePolicy)
			}
		}

		if shouldUnwire {
			switch cachePolicy {
			case CacheAllMetadata:
				// Keep the metadata but remove contents

				// NB(r): Each block needs shared ref to the series ID
				// or else each block needs to have a copy of the ID
				id := s.id
				checksum, err := cachedBlock.Checksum()
				if err != nil {
					return result, err
				}
				metadata := block.RetrievableBlockMetadata{
					ID:       id,
					Length:   cachedBlock.Len(),
					Checksum: checksum,
				}
				cachedBlock.ResetRetrievable(start, cachedBlock.BlockSize(), retriever, metadata)
			default:
				// Remove the block and it will be looked up later
				bucket.removeCached()
				cachedBlock.Close()
			}

			unwired = true
			result.madeUnwiredBlocks++
		}

		if unwired {
			result.UnwiredBlocks++
		} else {
			result.WiredBlocks++
			if cachedBlock.HasMergeTarget() {
				result.PendingMergeBlocks++
			}
		}
	}

	bufferStats := s.Stats()
	result.ActiveBlocks += bufferStats.wiredBlocks
	result.WiredBlocks += bufferStats.wiredBlocks
	result.OpenBlocks += bufferStats.openBlocks

	return result, nil
}

func (s *dbSeries) IsEmpty() bool {
	s.RLock()
	defer s.RUnlock()
	for _, bucket := range s.buckets {
		if !bucket.empty() {
			return false
		}
	}

	return true
}

func (s *dbSeries) NumActiveBlocks() int {
	s.RLock()
	value := s.Stats().wiredBlocks
	s.RUnlock()
	return value
}

func (s *dbSeries) IsBootstrapped() bool {
	s.RLock()
	state := s.bs
	s.RUnlock()
	return state == bootstrapped
}

func (s *dbSeries) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	s.Lock()
	defer s.Unlock()

	if !s.opts.RetentionOptions().OutOfOrderWritesEnabled() && !s.isRealtime(timestamp) {
		return m3dberrors.ErrOutOfOrderWriteTimeNotEnabled
	}

	blockSize := s.opts.RetentionOptions().BlockSize()
	blockStart := timestamp.Truncate(blockSize)
	bucket, ok := s.bucketAt(blockStart)
	if !ok {
		bucket = s.newBucketAt(blockStart)
	}
	s.updateBucketLRUCache(bucket)
	return bucket.write(s.now(), timestamp, value, unit, annotation)
}

func (s *dbSeries) ReadEncoded(
	ctx context.Context,
	start, end time.Time,
) ([][]xio.BlockReader, error) {
	s.RLock()
	reader := NewReaderUsingRetriever(s.id, s.blockRetriever, s.onRetrieveBlock, s, s.opts)
	r, err := reader.readersWithBlocksMapAndBuffer(ctx, start, end, s.buckets)
	s.RUnlock()
	return r, err
}

func (s *dbSeries) ReadEncodedBuckets(ctx context.Context, start, end time.Time) [][]xio.BlockReader {
	blockSize := s.opts.RetentionOptions().BlockSize()
	// TODO(r): pool these results arrays
	var res [][]xio.BlockReader
	for _, bucket := range s.buckets {
		if bucket.empty() {
			continue
		}
		if !start.Before(bucket.start.Add(blockSize)) {
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
		bucket.setLastRead(s.now())
	}

	return res
}

func (s *dbSeries) FetchBlocks(
	ctx context.Context,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	s.RLock()
	var buckets map[xtime.UnixNano]*dbBucket
	if s.IsEmpty() {
		buckets = s.buckets
	}
	r, err := Reader{
		opts:       s.opts,
		id:         s.id,
		retriever:  s.blockRetriever,
		onRetrieve: s.onRetrieveBlock,
	}.fetchBlocksWithBlocksMapAndBuffer(ctx, starts, buckets)
	s.RUnlock()
	return r, err
}

func (s *dbSeries) FetchBlocksMetadata(
	ctx context.Context,
	start time.Time,
	end time.Time,
	opts FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResult, error) {
	blockSize := s.opts.RetentionOptions().BlockSize()
	res := s.opts.FetchBlockMetadataResultsPool().Get()

	s.RLock()
	defer s.RUnlock()

	// Iterate over the encoders in the database buffer
	if !s.IsEmpty() {
		bufferResults := s.opts.FetchBlockMetadataResultsPool().Get()
		for _, bucket := range s.buckets {
			if bucket.empty() {
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
			if !opts.IncludeCachedBlocks && bucket.hasJustCachedBlock() {
				// Do not include cached blocks if not specified to, this is
				// to avoid high amounts of duplication if a significant number of
				// blocks are cached in memory when returning blocks metadata
				// from both in-memory and disk structures.
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
			//HERE do we calculate checksum here?
			// NB(r): Ignore if opts.IncludeChecksum because we avoid
			// calculating checksum since block is open and is being mutated
			bufferResults.Add(block.FetchBlockMetadataResult{
				Start:    bucket.start,
				Size:     resultSize,
				LastRead: resultLastRead,
			})
		}

		for _, result := range bufferResults.Results() {
			res.Add(result)
		}
		bufferResults.Close()
	}

	res.Sort()

	// NB(r): Since ID and Tags are garbage collected we can safely
	// return refs.
	tagsIter := s.opts.IdentifierPool().TagsIterator()
	tagsIter.Reset(s.tags)
	return block.NewFetchBlocksMetadataResult(s.id, tagsIter, res), nil
}

func (s *dbSeries) addBlockWithLock(b block.DatabaseBlock) {
	b.SetOnEvictedFromWiredList(s.blockOnEvictedFromWiredList)
	s.putCachedBlock(b)
}

// NB(xichen): we are holding a big lock here to drain the in-memory buffer.
// This could potentially be expensive in that we might accumulate a lot of
// data in memory during bootstrapping. If that becomes a problem, we could
// bootstrap in batches, e.g., drain and reset the buffer, drain the streams,
// then repeat, until len(s.pendingBootstrap) is below a given threshold.
func (s *dbSeries) Bootstrap(bootstrappedBlocks block.DatabaseSeriesBlocks) (BootstrapResult, error) {
	s.Lock()
	defer func() {
		s.bs = bootstrapped
		s.Unlock()
	}()

	var result BootstrapResult
	if s.bs == bootstrapped {
		return result, errSeriesAlreadyBootstrapped
	}

	if bootstrappedBlocks == nil {
		return result, nil
	}

	var (
		multiErr = xerrors.NewMultiError()
	)
	for tNano, block := range bootstrappedBlocks.AllBlocks() {
		t := tNano.ToTime()
		bucket, ok := s.bucketAt(t)
		if !ok {
			bucket = s.newBucketAt(t)
		}
		bucket.bootstrap(block)
		result.NumBlocksMovedToBuffer++
	}

	s.bs = bootstrapped
	return result, multiErr.FinalError()
}

func (s *dbSeries) OnRetrieveBlock(
	id ident.ID,
	tags ident.TagIterator,
	startTime time.Time,
	segment ts.Segment,
) {
	var (
		b    block.DatabaseBlock
		list *block.WiredList
	)
	s.Lock()
	defer func() {
		s.Unlock()
		if b != nil && list != nil {
			// 1) We need to update the WiredList so that blocks that were read from disk
			// can enter the list (OnReadBlock is only called for blocks that
			// were read from memory, regardless of whether the data originated
			// from disk or a buffer rotation.)
			// 2) We must perform this action outside of the lock to prevent deadlock
			// with the WiredList itself when it tries to call OnEvictedFromWiredList
			// on the same series that is trying to perform a blocking update.
			// 3) Doing this outside of the lock is safe because updating the
			// wired list is asynchronous already (Update just puts the block in
			// a channel to be processed later.)
			// 4) We have to perform a blocking update because in this flow, the block
			// is not already in the wired list so we need to make sure that the WiredList
			// takes control of its lifecycle.
			list.BlockingUpdate(b)
		}
	}()

	if !id.Equal(s.id) {
		return
	}

	b = s.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	metadata := block.RetrievableBlockMetadata{
		ID:       s.id,
		Length:   segment.Len(),
		Checksum: digest.SegmentChecksum(segment),
	}
	blockSize := s.opts.RetentionOptions().BlockSize()
	b.ResetRetrievable(startTime, blockSize, s.blockRetriever, metadata)
	// Use s.id instead of id here, because id is finalized by the context whereas
	// we rely on the G.C to reclaim s.id. This is important because the block will
	// hold onto the id ref, and (if the LRU caching policy is enabled) the shard
	// will need it later when the WiredList calls its OnEvictedFromWiredList method.
	// Also note that ResetRetrievable will mark the block as not retrieved from disk,
	// but OnRetrieveBlock will then properly mark it as retrieved from disk so subsequent
	// calls to WasRetrievedFromDisk will return true.
	b.OnRetrieveBlock(s.id, tags, startTime, segment)

	// NB(r): Blocks retrieved have been triggered by a read, so set the last
	// read time as now so caching policies are followed.
	b.SetLastReadTime(s.now())

	// If we retrieved this from disk then we directly emplace it
	s.addBlockWithLock(b)

	list = s.opts.DatabaseBlockOptions().WiredList()
}

// OnReadBlock is only called for blocks that were read from memory, regardless of
// whether the data originated from disk or buffer rotation.
func (s *dbSeries) OnReadBlock(b block.DatabaseBlock) {
	if list := s.opts.DatabaseBlockOptions().WiredList(); list != nil {
		// The WiredList is only responsible for managing the lifecycle of blocks
		// retrieved from disk.
		if b.WasRetrievedFromDisk() {
			// 1) Need to update the WiredList so it knows which blocks have been
			// most recently read.
			// 2) We do a non-blocking update here to prevent deadlock with the
			// WiredList calling OnEvictedFromWiredList on the same series since
			// OnReadBlock is usually called within the context of a read lock
			// on this series.
			// 3) Its safe to do a non-blocking update because the wired list has
			// already been exposed to this block, so even if the wired list drops
			// this update, it will still manage this blocks lifecycle.
			list.NonBlockingUpdate(b)
		}
	}
}

func (s *dbSeries) OnEvictedFromWiredList(id ident.ID, blockStart time.Time) {
	s.Lock()
	defer s.Unlock()

	// Should never happen
	if !id.Equal(s.id) {
		return
	}

	bucket, ok := s.bucketAt(blockStart)
	if block := bucket.cached; ok && block != nil {
		if !block.WasRetrievedFromDisk() {
			// Should never happen - invalid application state could cause data loss
			instrument.EmitInvariantViolationAndGetLogger(
				s.opts.InstrumentOptions()).WithFields(
				xlog.NewField("id", id.String()),
				xlog.NewField("blockStart", blockStart),
			).Errorf("tried to evict block that was not retrieved from disk")
			return
		}

		bucket.removeCached()
	}
}

func (s *dbSeries) newBootstrapBlockError(
	b block.DatabaseBlock,
	err error,
) error {
	msgFmt := "bootstrap series error occurred for %s block at %s: %v"
	renamed := fmt.Errorf(msgFmt, s.id.String(), b.StartTime().String(), err)
	return xerrors.NewRenamedError(err, renamed)
}

func (s *dbSeries) Flush(
	ctx context.Context,
	blockStart time.Time,
	persistFn persist.DataFn,
) (FlushOutcome, error) {
	s.RLock()
	defer s.RUnlock()

	if s.bs != bootstrapped {
		return FlushOutcomeErr, errSeriesNotBootstrapped
	}

	bucket, ok := s.bucketAt(blockStart)
	if !ok {
		return FlushOutcomeErr, errStreamDidNotExistForBlock
	}

	br, err := bucket.mergeStreams(ctx)
	if err != nil {
		return FlushOutcomeErr, err
	}
	if br.IsEmpty() {
		return FlushOutcomeErr, errStreamDidNotExistForBlock
	}
	segment, err := br.Segment()
	if err != nil {
		return FlushOutcomeErr, err
	}

	checksum := digest.SegmentChecksum(segment)

	err = persistFn(s.id, s.tags, segment, checksum)
	if err != nil {
		return FlushOutcomeErr, err
	}

	return FlushOutcomeFlushedToDisk, nil
}

func (s *dbSeries) Snapshot(
	ctx context.Context,
	blockStart time.Time,
	persistFn persist.DataFn,
) error {
	// Need a write lock because the buffer Snapshot method mutates
	// state (by performing a pro-active merge).
	s.Lock()
	defer s.Unlock()

	if s.bs != bootstrapped {
		return errSeriesNotBootstrapped
	}

	var (
		stream xio.SegmentReader
		err    error
	)

	if bucket, ok := s.bucketAt(blockStart); ok {
		stream, err = bucket.mergeStreams(ctx)
		if err != nil {
			return err
		}
		if stream == xio.EmptyBlockReader {
			return nil
		}
	}

	segment, err := stream.Segment()
	if err != nil {
		return err
	}

	return persistFn(s.id, s.tags, segment, digest.SegmentChecksum(segment))
}

func (s *dbSeries) Close() {
	s.Lock()
	defer s.Unlock()

	// See Reset() for why these aren't finalized
	s.id = nil
	s.tags = ident.Tags{}

	switch s.opts.CachePolicy() {
	case CacheLRU:
		// In the CacheLRU case, blocks that were retrieved from disk are owned
		// by the WiredList and should not be closed here. They will eventually
		// be evicted and closed by  the WiredList when it needs to make room
		// for new blocks.
		for _, bucket := range s.buckets {
			if block := bucket.cached; block != nil && !block.WasRetrievedFromDisk() {
				block.Close()
			}
		}
	default:
		s.removeAllCached()
	}

	if s.pool != nil {
		s.pool.Put(s)
	}
}

func (s *dbSeries) Reset(
	id ident.ID,
	tags ident.Tags,
	blockRetriever QueryableBlockRetriever,
	onRetrieveBlock block.OnRetrieveBlock,
	onEvictedFromWiredList block.OnEvictedFromWiredList,
	opts Options,
) {
	s.Lock()
	defer s.Unlock()

	// NB(r): We explicitly do not place this ID back into an
	// existing pool as high frequency users of series IDs such
	// as the commit log need to use the reference without the
	// overhead of ownership tracking. In addition, the blocks
	// themselves have a reference to the ID which is required
	// for the LRU/WiredList caching strategy eviction process.
	// Since the wired list can still have a reference to a
	// DatabaseBlock for which the corresponding DatabaseSeries
	// has been closed, its important that the ID itself is still
	// available because the process of kicking a DatabaseBlock
	// out of the WiredList requires the ID.
	//
	// Since series are purged so infrequently the overhead
	// of not releasing back an ID to a pool is amortized over
	// a long period of time.
	s.id = id
	s.tags = tags

	if s.opts.RetentionOptions().OutOfOrderWritesEnabled() {
		bucketPoolOpts := pool.NewObjectPoolOptions().SetSize(defaultBufferBucketPoolSize)
		s.bucketPool = newdbBucketPool(bucketPoolOpts)
	}
	s.opts = opts
	s.bufferPast = opts.RetentionOptions().BufferPast()
	s.bufferFuture = opts.RetentionOptions().BufferFuture()
	s.removeAllBuckets()
	s.bs = bootstrapNotStarted
	s.blockRetriever = blockRetriever
	s.onRetrieveBlock = onRetrieveBlock
	s.blockOnEvictedFromWiredList = onEvictedFromWiredList
}

// newBucketAt creates a new bucket for a given time and puts it in the
// bucket map
func (s *dbSeries) newBucketAt(t time.Time) *dbBucket {
	bucket := s.bucketPool.Get()
	bucket.resetTo(t, s.opts)
	s.buckets[xtime.ToUnixNano(t)] = bucket

	return bucket
}

func (s *dbSeries) bucketAt(t time.Time) (*dbBucket, bool) {
	// First check LRU cache
	for _, bucket := range s.bucketLRUCache {
		if bucket == nil {
			continue
		}

		if bucket.start.Equal(t) {
			return bucket, true
		}
	}

	// Then check the map
	if bucket, ok := s.buckets[xtime.ToUnixNano(t)]; ok {
		return bucket, true
	}

	return nil, false
}

func (s *dbSeries) updateBucketLRUCache(bucket *dbBucket) {
	s.bucketLRUCache[s.lruBucketIdxInCache()] = bucket
}

// lruBucketIdx returns the index of the least recently used bucket in the LRU cache
func (s *dbSeries) lruBucketIdxInCache() int {
	idx := -1
	var lastReadTime time.Time

	for i, bucket := range s.bucketLRUCache {
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

func (s *dbSeries) isRealtime(timestamp time.Time) bool {
	now := s.now()
	futureLimit := now.Add(1 * s.bufferFuture)
	pastLimit := now.Add(-1 * s.bufferPast)
	return pastLimit.Before(timestamp) && futureLimit.After(timestamp)
}

type bufferStats struct {
	openBlocks  int
	wiredBlocks int
}

type drainAndResetResult struct {
	mergedOutOfOrderBlocks int
}

func (s *dbSeries) removeBucketAt(t time.Time) {
	key := xtime.ToUnixNano(t)
	s.bucketPool.Put(s.buckets[key])
	delete(s.buckets, key)
}

func (s *dbSeries) removeAllBuckets() {
	for tNano := range s.buckets {
		s.removeBucketAt(tNano.ToTime())
	}
}

func (s *dbSeries) removeAllCached() {
	for _, bucket := range s.buckets {
		bucket.removeCached()
	}
}

func (s *dbSeries) Stats() bufferStats {
	var stats bufferStats

	for _, bucket := range s.buckets {
		if bucket.empty() {
			continue
		}
		stats.openBlocks++
		stats.wiredBlocks++
	}

	return stats
}

func (s *dbSeries) MinMax() (time.Time, time.Time, error) {
	var min, max time.Time
	for _, bucket := range s.buckets {
		if min.IsZero() || bucket.start.Before(min) {
			min = bucket.start
		}
		if max.IsZero() || bucket.start.After(max) {
			max = bucket.start
		}
	}

	if min.IsZero() || max.IsZero() {
		// Should never happen
		return time.Time{}, time.Time{}, errNoAvailableBuckets
	}
	return min, max, nil
}

func (s *dbSeries) putCachedBlock(block block.DatabaseBlock) {
	blockStart := block.StartTime()
	bucket, ok := s.bucketAt(blockStart)
	if !ok {
		bucket = s.newBucketAt(blockStart)
	}
	bucket.cached = block
}
