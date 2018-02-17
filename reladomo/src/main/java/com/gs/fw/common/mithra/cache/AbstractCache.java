/*
 Copyright 2016 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.cache.offheap.MasterCacheUplink;
import com.gs.fw.common.mithra.cache.offheap.MasterSyncResult;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.transaction.TransactionLocal;
import com.gs.fw.common.mithra.util.Filter2;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * AbsCache
 */
public abstract class AbstractCache implements Cache
{
    private static final AtomicInteger CACHE_ID_GENERATOR = new AtomicInteger(1);

    protected final ReadWriteLock readWriteLock = new ReadWriteLock();
    protected final UnifiedMap<Extractor, List<Integer>> attributeToIndexMap = new UnifiedMap();
    protected final TransactionLocal markedDirtyForReload = new TransactionLocal();
    protected final UnifiedSet monitoredAttributes = new UnifiedSet();
    private final int cacheId;
    protected final IndexReference noIndexReference;

    protected Index[] indices;
    protected IndexReference[] indexReferences;
    protected Boolean[] isIndexImmutable;

    protected AbstractCache(int initialIndexCount)
    {
        this.cacheId = CACHE_ID_GENERATOR.incrementAndGet();
        this.indices = new Index[initialIndexCount];
        this.indexReferences = new IndexReference[initialIndexCount];
        this.isIndexImmutable = new Boolean[initialIndexCount];
        this.noIndexReference = new IndexReference(this, -1);
    }

    public int getId()
    {
        return this.cacheId;
    }

    protected int addIndex(Index index)
    {
        int newLength = this.indices.length + 1;
        this.indices = copyAndAdd(Index.class, this.indices, index);
        this.indexReferences = copyAndAdd(IndexReference.class, this.indexReferences, new IndexReference(this, newLength));
        this.isIndexImmutable = copyAndAdd(Boolean.class, this.isIndexImmutable, false);
        return indices.length - 1;
    }

    protected int getBestIndexReferenceBasedOnAttributes(List attributes)
    {
        Index best = null;
        int bestReference = -2;
        for (int i = 0; i < this.indices.length; i++)
        {
            Index candidate = this.indices[i];
            Extractor[] candidateAttributes = candidate.getExtractors();
            if (isSubset(attributes, candidateAttributes))
            {
                if (candidate.isUnique())
                {
                    return i;
                }
                if (best == null)
                {
                    best = candidate;
                    bestReference = i;
                }
                else
                {
                    if (best.isInitialized() && candidate.isInitialized())
                    {
                        int bestReturnSize = best.getAverageReturnSize();
                        int candidateReturnSize = candidate.getAverageReturnSize();
                        if (bestReturnSize > candidateReturnSize)
                        {
                            best = candidate;
                            bestReference = i;
                        }
                        else if (bestReturnSize == candidateReturnSize && candidateAttributes.length > best.getExtractors().length)
                        {
                            best = candidate;
                            bestReference = i;
                        }
                    }
                    else
                    {
                        if (candidateAttributes.length > best.getExtractors().length)
                        {
                            best = candidate;
                            bestReference = i;
                        }
                    }
                }
            }
        }
        return bestReference;
    }

    protected IndexReference getInitializedIndexReference(int bestReference, IterableIndex iterableIndex)
    {
        Index bestIndex = this.indices[bestReference];
        if (!bestIndex.isInitialized())
        {
            synchronized (bestIndex)
            {
                if (!this.indices[bestReference].isInitialized())
                {
                    this.readWriteLock.acquireReadLock();
                    try
                    {
                        Index initialized = bestIndex.getInitialized(iterableIndex);
                        for (Object key : this.attributeToIndexMap.keySet())
                        {
                            List existing = (List) this.attributeToIndexMap.get(key);
                            for(int i=0;i<existing.size();i++)
                            {
                                if (existing.get(i) == bestIndex)
                                {
                                    existing.set(i, initialized);
                                }
                            }
                        }
                        this.indices[bestReference] = initialized;
                    }
                    finally
                    {
                        this.readWriteLock.release();
                    }
                }
            }
        }
        return this.indexReferences[bestReference];
    }

    public boolean contains(IndexReference indexRef, Object keyHolder, Extractor[] keyHolderNonDatedExtractors, final Filter2 filter)
    {
        Index nonUniqueIndex = this.indices[indexRef.indexReference - 1];

        this.readWriteLock.acquireReadLock();
        try
        {
            return nonUniqueIndex.contains(keyHolder, keyHolderNonDatedExtractors, filter);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    protected abstract boolean isSubset(List attributes, Extractor[] candidateAttributes);

    private static <T> T[] copyAndAdd(Class<T> clazz, T[] array, T newItem)
    {
        T[] newArray = (T[]) Array.newInstance(clazz, array.length + 1);
        System.arraycopy(array, 0, newArray, 0, array.length);
        newArray[array.length] = newItem;
        return newArray;
    }

    @Override
    public List<Object> collectMilestoningOverlaps()
    {
        // default implementation
        return FastList.newList();
    }

    protected ReadWriteLock getCacheLock()
    {
        return this.readWriteLock;
    }

    public boolean isOffHeap()
    {
        return false;
    }

    @Override
    public long getOffHeapAllocatedDataSize()
    {
        return 0;
    }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return 0;
    }

    @Override
    public long getOffHeapUsedDataSize()
    {
        return 0;
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return 0;
    }

    @Override
    public boolean syncWithMasterCache(MasterCacheUplink uplink)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MasterSyncResult sendSyncResult(long maxReplicatedPageVersion)
    {
        throw new RuntimeException("not implemented");
    }

    protected boolean matchesExtraExtractorsAndOp(Object object, Object parent, List<Extractor> extraExtractors, Operation extraOperation)
    {
        if (extraExtractors != null)
        {
            for(int i=0;i<extraExtractors.size();i++)
            {
                if (!extraExtractors.get(i).valueEquals(object, parent))
                {
                    return false;
                }
            }
        }
        if (extraOperation != null)
        {
            return extraOperation.matches(object);
        }
        return true;
    }
}
