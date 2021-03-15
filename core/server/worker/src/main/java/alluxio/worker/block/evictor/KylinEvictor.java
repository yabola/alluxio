package alluxio.worker.block.evictor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import alluxio.Sessions;
import alluxio.collections.Pair;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

/**
 * Refer to alluxio.worker.block.evictor.LRUEvictor
 **/
public class KylinEvictor extends AbstractEvictor {

    private static final Logger logger = LoggerFactory.getLogger("KylinEvictor");

    public static final HashMap<Long, Integer> cacheLevel = Maps.newHashMap();

    public static final HashSet<String> reserveIds = new HashSet<>();

    private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
    private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
    private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;
    private static final boolean UNUSED_MAP_VALUE = true;
    private static final int HOT_CACHE_LEVEL = 1;
    private static final int DEFAULT_CACHE_LEVEL = 2;
    private static final int UNKNOWN_CACHE_LEVEL = 99;

    protected Map<Long, Boolean> mLRUHotCache =
            Collections.synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
                    LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

    protected Map<Long, Boolean> mLRUDefaultCache =
            Collections.synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
                    LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));


    public KylinEvictor(BlockMetadataEvictorView view, Allocator allocator) {
        super(view, allocator);
        initCache();
    }

    public void initCache() {
        // preload existing blocks loaded by StorageDir to Evictor
        for (StorageTierView tierView : mMetadataView.getTierViews()) {
            for (StorageDirView dirView : tierView.getDirViews()) {
                for (BlockMeta blockMeta : ((StorageDirEvictorView) dirView)
                        .getEvictableBlocks()) { // all blocks with initial view
                    putLRUCache(computCacheLevel(blockMeta.getBlockId()), blockMeta.getBlockId());
                }
            }
        }
        logger.info("mLRUHotCache info :" + mLRUHotCache);
        logger.info("mLRUDefaultCache info :" + mLRUDefaultCache);
    }

    public void updateCache(Long blockId, Integer level) {
        if (level.equals(cacheLevel.get(blockId))) {
            logger.info("Cache {} is uptodate already.", blockId);
            return;
        }
        cacheLevel.put(blockId, level);
        if (mLRUHotCache.containsKey(blockId) || mLRUDefaultCache.containsKey(blockId)) {
            removeLRUCache(UNKNOWN_CACHE_LEVEL, blockId);
            putLRUCache(level, blockId);
        }
    }

    public int computCacheLevel(Long blockId) {
        return cacheLevel.getOrDefault(blockId, DEFAULT_CACHE_LEVEL);
    }

    public void putLRUCache(int level, Long blockId) {
        if (level == HOT_CACHE_LEVEL) {
            mLRUHotCache.put(blockId, UNUSED_MAP_VALUE);
        } else {
            mLRUDefaultCache.put(blockId, UNUSED_MAP_VALUE);
        }
    }

    public void removeLRUCache(int level, Long blockId) {
        switch (level) {
            case HOT_CACHE_LEVEL:
                mLRUHotCache.remove(blockId);
                break;
            case DEFAULT_CACHE_LEVEL:
                mLRUDefaultCache.remove(blockId);
                break;
            case UNKNOWN_CACHE_LEVEL:
                mLRUHotCache.remove(blockId);
                mLRUDefaultCache.remove(blockId);
                break;
            default:
                logger.warn("Not a legal level");
        }
    }


    private Iterator<Long> getBlockIteratorByLevel(int level) {
        if (level == HOT_CACHE_LEVEL) {
            return mLRUHotCache.keySet().iterator();
        } else {
            return mLRUDefaultCache.keySet().iterator();
        }
    }


    @Override
    protected StorageDirEvictorView cascadingEvict(long bytesToBeAvailable,
                                                   BlockStoreLocation location, EvictionPlan plan, Mode mode) {
        location = updateBlockStoreLocation(bytesToBeAvailable, location);

        StorageDirEvictorView candidateDirView = (StorageDirEvictorView)
                EvictorUtils.selectDirWithRequestedSpace(bytesToBeAvailable, location, mMetadataView);
        if (candidateDirView != null) {
            return candidateDirView;
        }

        EvictionDirCandidates dirCandidates = new EvictionDirCandidates();
        for (int i = 2; i >= 1; i--) {
            Iterator<Long> it = getBlockIteratorByLevel(i);
            while (it.hasNext() && dirCandidates.candidateSize() < bytesToBeAvailable) {
                long blockId = it.next();
                try {
                    BlockMeta block = mMetadataView.getBlockMeta(blockId);
                    if (block != null) { // might not present in this view
                        if (block.getBlockLocation().belongsTo(location)) {
                            String tierAlias = block.getParentDir().getParentTier().getTierAlias();
                            int dirIndex = block.getParentDir().getDirIndex();
                            StorageDirView dirView = mMetadataView.getTierView(tierAlias).getDirView(dirIndex);
                            if (dirView != null) {
                                dirCandidates.add((StorageDirEvictorView) dirView, blockId, block.getBlockSize());
                            }
                        }
                    }
                } catch (BlockDoesNotExistException e) {
//                LOG.warn("Remove block {} from evictor cache because {}", blockId, e);
                    it.remove();
                    onRemoveBlockFromIterator(blockId);
                }
            }
        }

        if (mode == Mode.GUARANTEED && dirCandidates.candidateSize() < bytesToBeAvailable) {
            return null;
        }
        candidateDirView = dirCandidates.candidateDir();
        if (candidateDirView == null) {
            return null;
        }
        List<Long> candidateBlocks = dirCandidates.candidateBlocks();
        StorageTierView nextTierView
                = mMetadataView.getNextTier(candidateDirView.getParentTierView());
        if (nextTierView == null) {
            // This is the last tier, evict all the blocks.
            for (Long blockId : candidateBlocks) {
                try {
                    BlockMeta block = mMetadataView.getBlockMeta(blockId);
                    if (block != null) {
                        candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
                        plan.toEvict().add(new Pair<>(blockId, candidateDirView.toBlockStoreLocation()));
                    }
                } catch (BlockDoesNotExistException e) {
                    continue;
                }
            }
        } else {
            for (Long blockId : candidateBlocks) {
                try {
                    BlockMeta block = mMetadataView.getBlockMeta(blockId);
                    if (block == null) {
                        continue;
                    }
                    StorageDirEvictorView nextDirView
                            = (StorageDirEvictorView) mAllocator.allocateBlockWithView(
                            Sessions.MIGRATE_DATA_SESSION_ID, block.getBlockSize(),
                            BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()), mMetadataView);
                    if (nextDirView == null) {
                        nextDirView = cascadingEvict(block.getBlockSize(),
                                BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()), plan, mode);
                    }
                    if (nextDirView == null) {
                        // If we failed to find a dir in the next tier to move this block, evict it and
                        // continue. Normally this should not happen.
                        plan.toEvict().add(new Pair<>(blockId, block.getBlockLocation()));
                        candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
                        continue;
                    }
                    plan.toMove().add(new BlockTransferInfo(blockId, block.getBlockLocation(),
                            nextDirView.toBlockStoreLocation()));
                    candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
                    nextDirView.markBlockMoveIn(blockId, block.getBlockSize());
                } catch (BlockDoesNotExistException e) {
                    continue;
                }
            }
        }

        return candidateDirView;
    }

    @Override
    protected Iterator<Long> getBlockIterator() {
        return null;
    }


    @Override
    public void onAccessBlock(long sessionId, long blockId) {
        putLRUCache(computCacheLevel(blockId), blockId);
    }

    @Override
    public void onCommitBlock(long sessionId, long blockId, BlockStoreLocation location) {
        // Since the temp block has been committed, update Evictor about the new added blocks
        putLRUCache(computCacheLevel(blockId), blockId);
    }

    @Override
    public void onRemoveBlockByClient(long sessionId, long blockId) {
        removeLRUCache(computCacheLevel(blockId), blockId);
    }

    @Override
    public void onRemoveBlockByWorker(long sessionId, long blockId) {
        removeLRUCache(computCacheLevel(blockId), blockId);
    }

    @Override
    public void onBlockLost(long blockId) {
        removeLRUCache(computCacheLevel(blockId), blockId);
    }

    @Override
    protected void onRemoveBlockFromIterator(long blockId) {
        removeLRUCache(computCacheLevel(blockId), blockId);
    }
}
