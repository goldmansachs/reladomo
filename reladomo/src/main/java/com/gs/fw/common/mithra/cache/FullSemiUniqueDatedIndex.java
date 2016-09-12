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

package com.gs.fw.common.mithra.cache;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;



@SuppressWarnings({"unchecked"})
public class FullSemiUniqueDatedIndex implements SemiUniqueDatedIndex
{

    // semiUnique is a synonym for nonDated in this file.

    protected ExtractorBasedHashStrategy datedHashStrategy;
    protected ExtractorBasedHashStrategy asOfAttributeHashStrategy;
    private Extractor[] datedExtractors;
    private ExtractorBasedHashStrategy nonDatedHashStrategy;
    private Extractor[] nonDatedExtractors;
    private AsOfAttribute[] asOfAttributes;
    //private TimestampAttribute[] fromAttributes;
    /**
     * The default initial capacity -- MUST be a power of two.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private Object[] nonDatedTable;
    protected Object[] datedTable;

    private int nonDatedSize;
    private int datedSize;

    private int nonDatedThreshold;
    private int datedThreshold;

    private final byte loadFactor;
    private byte datedRightShift;
    private byte nonDatedRightShift;

    public FullSemiUniqueDatedIndex(String indexName, Extractor[] nonDatedExtractors, AsOfAttribute[] asOfAttributes)
    {
        this.nonDatedExtractors = nonDatedExtractors;
        this.asOfAttributes = asOfAttributes;
        this.populateExtractors();
        nonDatedHashStrategy = ExtractorBasedHashStrategy.create(this.nonDatedExtractors);
        this.loadFactor = (byte)(DEFAULT_LOAD_FACTOR * 100);
        this.allocate(DEFAULT_INITIAL_CAPACITY);
        this.allocateNonDated(DEFAULT_INITIAL_CAPACITY);
    }

    public FullSemiUniqueDatedIndex(ExtractorBasedHashStrategy nonDatedHashStrategy, Extractor[] nonDatedExtractors,
            AsOfAttribute[] asOfAttributes, Extractor[] pkExtractors, ExtractorBasedHashStrategy datedHashStrategy)
    {
        this.nonDatedExtractors = nonDatedExtractors;
        this.asOfAttributes = asOfAttributes;
        this.datedExtractors = pkExtractors;
        this.datedHashStrategy = datedHashStrategy;
        this.nonDatedHashStrategy = nonDatedHashStrategy;
        this.asOfAttributeHashStrategy = ExtractorBasedHashStrategy.create(this.getFromAttributes());
        this.loadFactor = (byte)(DEFAULT_LOAD_FACTOR * 100);
        this.allocate(DEFAULT_INITIAL_CAPACITY);
        this.allocateNonDated(DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public List<Object> collectMilestoningOverlaps()
    {
        if (asOfAttributes.length == 1 || asOfAttributes.length == 2)
        {
            return parallelCollectMilestoneOverlap();
        }
        else
        {
            throw new MithraException("Unsupported number of asOfAttributes");
        }
    }

    @Override
    public void destroy()
    {
        //nothing to do
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {

    }

    private List<Object> parallelCollectMilestoneOverlap()
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), this.nonDatedTable.length, 1);
        final ArrayBasedQueue queue = new ArrayBasedQueue(this.nonDatedTable.length, threadChunkSize.getChunkSize());
        int threads = threadChunkSize.getThreads();

        final List<DetectDuplicateTask> tasks = FastList.newList(threads);
        CooperativeCpuTaskFactory taskFactory = new CooperativeCpuTaskFactory(pool, threads)
        {
            @Override
            protected CpuTask createCpuTask()
            {
                DetectDuplicateTask detectDuplicateTask = new DetectDuplicateTask(queue);
                synchronized (tasks)
                {
                    tasks.add(detectDuplicateTask);
                }
                return detectDuplicateTask;
            }
        };
        taskFactory.startAndWorkUntilFinished();
        List<Object> mergedList = new FastList<Object>();
        for(int i = 0;i<tasks.size();i++)
        {
            mergedList.addAll(tasks.get(i).getDuplicates());
        }
        return mergedList;
    }

    private void delegateByType(Object obj, List<Object> duplicateData, DoProcedure tObjectProcedure)
    {
        if (obj instanceof MultiEntry)
        {
            collectMilestoningOverlaps((MultiEntry) obj, duplicateData);
        }
        else if (obj instanceof ChainedBucket)
        {
            collectMilestoningOverlaps((ChainedBucket) obj, tObjectProcedure);
        }
    }

    private void collectMilestoningOverlaps(ChainedBucket chainedBucket, DoProcedure tObjectProcedure)
    {
        chainedBucket.forAll(tObjectProcedure);
    }

    private Object checkForMilestoningOverlap(MultiEntry multiEntry, int currentIndex, AsOfAttribute[] asOfAttributes)
    {
        Object mithraObject = multiEntry.list[currentIndex];
        int lstSize = multiEntry.size;
        for (int index = currentIndex + 1; index < lstSize; index++)
        {
            if (AsOfAttribute.isMilestoningOverlap(mithraObject, multiEntry.list[index], asOfAttributes))
            {
                return multiEntry.list[index];
            }
        }
        return null;
    }

    private void collectMilestoningOverlaps(MultiEntry multiEntry, List duplicateList)
    {
        Object[] list = multiEntry.list;
        int itrSize = multiEntry.size;
        for (int i = 0; i < itrSize; i++)
        {
            Object duplicateObject = checkForMilestoningOverlap(multiEntry, i, this.asOfAttributes);
            if (duplicateObject != null)
            {
                duplicateList.add(list[i]);
                duplicateList.add(duplicateObject);
            }
        }
    }

    protected void populateExtractors()
    {
        this.asOfAttributeHashStrategy = ExtractorBasedHashStrategy.create(this.getFromAttributes());
        this.datedExtractors = createDatedExtractors();
        this.datedHashStrategy = ExtractorBasedHashStrategy.create(this.datedExtractors);
    }

    protected Extractor[] createDatedExtractors()
    {
        TimestampAttribute[] fromAttributes = this.getFromAttributes();
        Extractor[] result = new Extractor[this.nonDatedExtractors.length + fromAttributes.length];
        System.arraycopy(this.nonDatedExtractors, 0, result, 0, this.nonDatedExtractors.length);
        System.arraycopy(fromAttributes, 0, result, this.nonDatedExtractors.length, fromAttributes.length);
        return result;

    }

    private TimestampAttribute[] getFromAttributes()
    {
        TimestampAttribute[] fromAttributes = new TimestampAttribute[this.asOfAttributes.length];
        for(int i=0;i<this.asOfAttributes.length;i++)
        {
            fromAttributes[i] = this.asOfAttributes[i].getFromAttribute();
        }
        return fromAttributes;
    }

    public Extractor[] getExtractors()
    {
        return this.datedExtractors;
    }

    public Extractor[] getNonDatedExtractors()
    {
        return this.nonDatedExtractors;
    }

    private int indexFor(int h, int length, byte aRightShift)
    {
        return (h ^ (h >>> aRightShift)) & (length-1);
    }

    public int size()
    {
        if (datedSize == 0)
            return 0;
        return datedSize;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = 0; i < datedTable.length && !done; i++)
        {
            Object e = datedTable[i];
            if (e instanceof ChainedBucket)
            {
                done = ((ChainedBucket)e ).forAll(procedure);
            }
            else if (e != null)
            {
                done = procedure.execute(e);
            }
        }
        return done;
    }

    public Object getFromData(Object data, int nonDatedHashCode)
    {
        int hash = this.asOfAttributeHashStrategy.computeCombinedHashCode(data, nonDatedHashCode);
        int i = indexFor(hash, datedTable.length, this.datedRightShift);
        Object e = this.datedTable[i];
        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getFromDataForChained((ChainedBucket) e, data);
        }
        if (this.datedHashStrategy.equals(e, data))
        {
            return e;
        }
        return null;
    }

    private Object getFromDataForChained(ChainedBucket chainedBucket, Object data)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (this.datedHashStrategy.equals(bucket.zero, data))
            {
                return bucket.zero;
            }
            if (bucket.one == null) return null;
            if (this.datedHashStrategy.equals(bucket.one, data))
            {
                return bucket.one;
            }
            if (bucket.two == null) return null;
            if (this.datedHashStrategy.equals(bucket.two, data))
            {
                return bucket.two;
            }
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three == null) return null;
            if (this.datedHashStrategy.equals(bucket.three, data))
            {
                return bucket.three;
            }
            return null;
        } while(true);
    }

    public Object get(Object valueHolder, List extractors)
    {
        int hash = this.datedHashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, datedTable.length, this.datedRightShift);
        Object e = this.datedTable[i];
        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getFromDataForChained((ChainedBucket) e, valueHolder, extractors);
        }
        if (this.datedHashStrategy.equals(e, valueHolder, extractors))
        {
            return e;
        }
        return null;
    }

    private Object getFromDataForChained(ChainedBucket chainedBucket, Object valueHolder, List extractors)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (this.datedHashStrategy.equals(bucket.zero, valueHolder, extractors))
            {
                return bucket.zero;
            }
            if (bucket.one == null) return null;
            if (this.datedHashStrategy.equals(bucket.one, valueHolder, extractors))
            {
                return bucket.one;
            }
            if (bucket.two == null) return null;
            if (this.datedHashStrategy.equals(bucket.two, valueHolder, extractors))
            {
                return bucket.two;
            }
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three == null) return null;
            if (this.datedHashStrategy.equals(bucket.three, valueHolder, extractors))
            {
                return bucket.three;
            }
            return null;
        } while(true);
    }

    public Object get(Object valueHolder, Extractor[] extractors)
    {
        int hash = this.datedHashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, datedTable.length, this.datedRightShift);
        Object e = this.datedTable[i];
        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getFromDataForChained((ChainedBucket) e, valueHolder, extractors);
        }
        if (this.datedHashStrategy.equals(e, valueHolder, extractors))
        {
            return e;
        }
        return null;
    }

    private Object getFromDataForChained(ChainedBucket chainedBucket, Object valueHolder, Extractor[] extractors)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (this.datedHashStrategy.equals(bucket.zero, valueHolder, extractors))
            {
                return bucket.zero;
            }
            if (bucket.one == null) return null;
            if (this.datedHashStrategy.equals(bucket.one, valueHolder, extractors))
            {
                return bucket.one;
            }
            if (bucket.two == null) return null;
            if (this.datedHashStrategy.equals(bucket.two, valueHolder, extractors))
            {
                return bucket.two;
            }
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three == null) return null;
            if (this.datedHashStrategy.equals(bucket.three, valueHolder, extractors))
            {
                return bucket.three;
            }
            return null;
        } while(true);
    }


    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        Object candidate = this.get(keyHolder, extractors);
        return candidate != null && (filter == null || filter.matches(candidate, keyHolder));
    }

    public Object put(Object key, int nonDatedHashCode)
    {
        Object removed = this.putInDatedTable(key, nonDatedHashCode);
        putInNonDatedTable(key, nonDatedHashCode, removed);
        return removed;
    }

    public Object putSemiUnique(Object key)
    {
        return this.put(key, this.nonDatedHashStrategy.computeHashCode(key));
    }

    private Object putInDatedTable(Object key, int nonDatedHashCode)
    {
        int hash = this.asOfAttributeHashStrategy.computeCombinedHashCode(key, nonDatedHashCode);
        Object[] tab = this.datedTable;
        int index = indexFor(hash, tab.length, this.datedRightShift);

        Object cur = tab[index];
        Object removed = null;

        if (cur == null)
        {
            tab[index] = key;
        }
        else if (cur instanceof ChainedBucket)
        {
            removed = ((ChainedBucket)cur).addIfNotThere(key, this.datedHashStrategy);
        }
        else if (this.datedHashStrategy.equals(cur, key))
        {
            tab[index] = key;
            removed = cur;
        }
        else
        {
            tab[index] = new ChainedBucket(cur, key);
        }
        if (removed == null && ++this.datedSize > this.datedThreshold)
        {
            this.resizeDatedTable();
        }
        return removed;
    }

    protected int allocate(int capacity)
    {
        this.datedTable = new Object[capacity];
        this.computeMaxSize(capacity);

        return capacity;
    }

    protected int allocateNonDated(int capacity)
    {
        this.nonDatedThreshold = capacity;
        this.nonDatedTable = new Object[capacity];

        this.nonDatedRightShift = (byte) (Integer.numberOfTrailingZeros(capacity) + 1);

        return capacity;
    }

    protected void computeMaxSize(int capacity)
    {
        this.datedThreshold = capacity == MAXIMUM_CAPACITY
            ? Integer.MAX_VALUE
            : Math.min(capacity - 1, this.scaledByLoadFactor(capacity));

        this.datedRightShift = (byte) (Integer.numberOfTrailingZeros(capacity) + 1);
    }

    private int scaledByLoadFactor(int capacity)
    {
        return (int)(((long)capacity) * this.loadFactor / 100);
    }

    private void resizeDatedTable()
    {
        int newCapacity = this.datedTable.length << 1;
        resizeDatedTable(newCapacity);
    }

    private void resizeDatedTable(int newCapacity)
    {
        computeMaxSize(newCapacity);

        Object[] newTable = new Object[newCapacity];
        transferDatedTable(this.datedTable, newTable);
        datedTable = newTable;
    }

    private void transferDatedTable(Object[] src, Object[] dest)
    {
        for (int j = 0; j < src.length; ++j)
        {
            Object e = src[j];
            src[j] = null;
            if (e == null)
            {
                continue;
            }
            if (e instanceof ChainedBucket)
            {
                transferChained((ChainedBucket)e, dest);
            }
            else
            {
                transferEntry(dest, e);
            }
        }
    }

    private void transferChained(ChainedBucket chainedBucket, Object[] dest)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            transferEntry(dest, bucket.zero);
            if (bucket.one == null) return;
            transferEntry(dest, bucket.one);
            if (bucket.two == null) return;
            transferEntry(dest, bucket.two);
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three == null) return;
            transferEntry(dest, bucket.three);
            return;
        } while(true);
    }

    private void transferEntry(Object[] dest, Object entry)
    {
        int hash = this.datedHashStrategy.computeHashCode(entry);
        int index = indexFor(hash, dest.length, this.datedRightShift);
        Object cur = dest[index];
        if (cur == null)
        {
            dest[index] = entry;
        }
        else if (cur instanceof ChainedBucket)
        {
            ((ChainedBucket)cur).add(entry);
        }
        else
        {
            dest[index] = new ChainedBucket(cur, entry);
        }
    }

    public Object remove(Object key)
    {
        Object removed = this.removeFromDatedTable(key);
        if (removed != null)
        {
            this.removeNonDatedEntry(removed);
            return removed;
        }
        return null;
    }

    public Object removeUsingUnderlying(Object businessObject)
    {
        return this.remove(businessObject);
    }

    private Object removeFromDatedTable(Object underlying)
    {
        int hash = this.datedHashStrategy.computeHashCode(underlying);
        Object[] tab = this.datedTable;
        int i = indexFor(hash, tab.length, this.datedRightShift);
        Object e = tab[i];
        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return removeFromChained((ChainedBucket)e, underlying, i);
        }
        if (this.datedHashStrategy.equals(e, underlying))
        {
            datedSize--;
            tab[i] = null;
            return e;
        }
        return null;
    }

    private Object removeFromChained(ChainedBucket chainedBucket, Object underlying, int index)
    {
        ChainedBucket bucket = chainedBucket;
        Object removed = null;
        int pos = 0;
        do
        {
            if (this.datedHashStrategy.equals(bucket.zero, underlying))
            {
                datedSize--;
                removed = bucket.zero;
                chainedBucket.remove(pos);
                break;
            }
            if (bucket.one == null) return null;
            pos++;
            if (this.datedHashStrategy.equals(bucket.one, underlying))
            {
                datedSize--;
                removed = bucket.one;
                chainedBucket.remove(pos);
                break;
            }
            if (bucket.two == null) return null;
            pos++;
            if (this.datedHashStrategy.equals(bucket.two, underlying))
            {
                datedSize--;
                removed = bucket.two;
                chainedBucket.remove(pos);
                break;
            }
            if (bucket.three == null) return null;
            pos++;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (this.datedHashStrategy.equals(bucket.three, underlying))
            {
                datedSize--;
                removed = bucket.three;
                chainedBucket.remove(pos);
            }
            break;

        } while(true);

        if (chainedBucket.zero == null)
        {
            this.datedTable[index] = null;
        }
        return removed;
    }

    public List removeAll(Filter filter)
    {
        FastList result = new FastList();
        Object[] tab = this.datedTable;
        for (int i = 0; i < tab.length; i++)
        {
            Object e = tab[i];
            if (e instanceof ChainedBucket)
            {
                removeAllFromChained((ChainedBucket)e, i, result, filter);
            }
            else if (e != null && filter.matches(e))
            {
                result.add(e);
                datedSize--;
                tab[i] = null;
                removeNonDatedEntry(e);
            }
        }
        return result;
    }

    private void removeAllFromChained(ChainedBucket chainedBucket, int index, FastList result, Filter filter)
    {
        ChainedBucket bucket = chainedBucket;
        int pos = 0;
        do
        {
            while (bucket.zero != null && filter.matches(bucket.zero))
            {
                datedSize--;
                result.add(bucket.zero);
                removeNonDatedEntry(bucket.zero);
                chainedBucket.remove(pos);
            }
            if (bucket.one == null) break;
            pos++;
            while (bucket.one != null && filter.matches(bucket.one))
            {
                datedSize--;
                result.add(bucket.one);
                removeNonDatedEntry(bucket.one);
                chainedBucket.remove(pos);
            }
            if (bucket.two == null) break;
            pos++;
            while (bucket.two != null && filter.matches(bucket.two))
            {
                datedSize--;
                result.add(bucket.two);
                removeNonDatedEntry(bucket.two);
                chainedBucket.remove(pos);
            }
            if (bucket.three == null) break;
            pos++;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            while (bucket.three != null && filter.matches(bucket.three))
            {
                datedSize--;
                result.add(bucket.three);
                removeNonDatedEntry(bucket.three);
                chainedBucket.remove(pos);
            }
            break;
        } while(true);
        if (chainedBucket.zero == null)
        {
            this.datedTable[index] = null;
        }
    }

    public boolean evictCollectedReferences()
    {
        return false;
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return false;
    }

    public CommonExtractorBasedHashingStrategy getNonDatedPkHashStrategy()
    {
        return this.nonDatedHashStrategy;
    }

    public void forAllInParallel(final ParallelProcedure procedure)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), this.datedTable.length, 1);
        final ArrayBasedQueue queue = new ArrayBasedQueue(this.datedTable.length, threadChunkSize.getChunkSize());
        int threads = threadChunkSize.getThreads();
        procedure.setThreads(threads, this.datedSize /threads);
        CpuBoundTask[] tasks = new CpuBoundTask[threads];
        for(int i=0;i<threads;i++)
        {
            final int thread = i;
            tasks[i] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    ArrayBasedQueue.Segment segment = queue.borrow(null);
                    while(segment != null)
                    {
                        for (int i = segment.getStart(); i < segment.getEnd(); i++)
                        {
                            Object e = datedTable[i];
                            if (e == null) continue;
                            if (e instanceof ChainedBucket)
                            {
                                ((ChainedBucket)e).forAll(procedure, thread);
                            }
                            else
                            {
                                procedure.execute(e, thread);
                            }
                        }
                        segment = queue.borrow(segment);
                    }
                }
            };
        }
        new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();
    }

    public void ensureExtraCapacity(int extraCapacity)
    {
        int datedSize = extraCapacity + this.datedSize;
        if (datedSize > datedThreshold)
        {
            int newThreshold = datedThreshold;
            int capacity = this.datedTable.length;
            while(datedSize > newThreshold)
            {
                capacity = capacity << 1;
                newThreshold = (int)(capacity * loadFactor);
            }
            this.resizeDatedTable(capacity);
        }
        int nonDatedSize = this.nonDatedSize + extraCapacity/8;
        if (nonDatedSize > nonDatedThreshold)
        {
            int newThreshold = nonDatedThreshold;
            int capacity = this.nonDatedTable.length;
            while (nonDatedSize > newThreshold)
            {
                capacity = capacity << 1;
                newThreshold = (int)(capacity * loadFactor);
            }
            this.resizeNonDated(capacity);
        }

    }

    public void clear()
    {
        clearDatedTable();
        clearNonDatedTable();
    }

    private void clearDatedTable()
    {
        Object tab[] = datedTable;
        for (int i = 0; i < tab.length; ++i)
            tab[i] = null;
        datedSize = 0;
    }

    public Object getFromSemiUnique(Object valueHolder, List extractors)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, nonDatedTable.length, this.nonDatedRightShift);
        Object e = nonDatedTable[i];

        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getFromNonDatedChained((ChainedBucket) e, valueHolder, extractors);
        }
        else if (e instanceof MultiEntry)
        {
            return getFromNonDatedMultiEntry((MultiEntry) e, valueHolder, extractors);
        }
        else if (this.nonDatedHashStrategy.equals(e, valueHolder, extractors))
        {
            return getFromNonDatedIfMatchAsOfDates(e, valueHolder, extractors);
        }
        return null;
    }

    private Object getFromNonDatedIfMatchAsOfDates(Object e, Object valueHolder, List extractors)
    {
        AsOfExtractor extractor = (AsOfExtractor) extractors.get(this.nonDatedExtractors.length);
        if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[0]))
        {
            return null;
        }
        if (asOfAttributes.length == 2)
        {
            extractor = (AsOfExtractor) extractors.get(this.nonDatedExtractors.length+1);
            if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[1]))
            {
                return null;
            }
        }
        return e;
    }

    private Object getFromNonDatedChained(ChainedBucket chainedBucket, Object valueHolder, List extractors)
    {
        AsOfExtractor extractorOne = (AsOfExtractor) extractors.get(this.nonDatedExtractors.length);
        AsOfExtractor extractorTwo = null;
        boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
        if (asOfAttributes.length > 1)
        {
            extractorTwo = (AsOfExtractor) extractors.get(this.nonDatedExtractors.length + 1);
            matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
        }

        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.zero;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.zero, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.zero, valueHolder, extractors);
            }
            if (bucket.one == null) return null;
            if (bucket.one instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.one;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.one, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.one, valueHolder, extractors);
            }
            if (bucket.two == null) return null;
            if (bucket.two instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.two;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.two, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.two, valueHolder, extractors);
            }
            if (bucket.three == null) return null;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.three;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.three, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.three, valueHolder, extractors);
            }
            break;
        } while(true);
        return null;
    }

    private Object getFromNonDatedMultiEntry(MultiEntry multiEntry, Object valueHolder, List extractors)
    {
        if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
        {
            AsOfExtractor extractorOne = (AsOfExtractor) extractors.get(this.nonDatedExtractors.length);
            AsOfExtractor extractorTwo = null;
            boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
            if (asOfAttributes.length > 1)
            {
                extractorTwo = (AsOfExtractor) extractors.get(this.nonDatedExtractors.length + 1);
                matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
            }
            return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
        }
        return null;
    }

    public Object getFromSemiUnique(Object valueHolder, Extractor[] extractors)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, nonDatedTable.length, this.nonDatedRightShift);
        Object e = nonDatedTable[i];

        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getFromNonDatedChained((ChainedBucket) e, valueHolder, extractors);
        }
        else if (e instanceof MultiEntry)
        {
            return getFromNonDatedMultiEntry((MultiEntry) e, valueHolder, extractors);
        }
        else if (this.nonDatedHashStrategy.equals(e, valueHolder, extractors))
        {
            return getFromNonDatedIfMatchAsOfDates(e, valueHolder, extractors);
        }
        return null;
    }

    public boolean containsInSemiUnique(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(keyHolder, extractors);
        int i = indexFor(hash, nonDatedTable.length, this.nonDatedRightShift);
        Object e = nonDatedTable[i];

        if (e == null) return false;
        if (e instanceof ChainedBucket)
        {
            return containsInNonDatedChained((ChainedBucket) e, keyHolder, extractors, filter);
        }
        else if (e instanceof MultiEntry)
        {
            return containsInNonDatedMultiEntry((MultiEntry) e, keyHolder, extractors, filter);
        }
        else if (this.nonDatedHashStrategy.equals(e, keyHolder, extractors))
        {
            return containsInNonDatedIfMatchAsOfDates(e, keyHolder, extractors, filter);
        }
        return false;
    }

    private boolean containsInNonDatedMultiEntry(MultiEntry multiEntry, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        if (nonDatedHashStrategy.equals(multiEntry.getFirst(), keyHolder, extractors))
        {
            AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.nonDatedExtractors.length];
            AsOfExtractor extractorTwo = null;
            boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
            if (asOfAttributes.length > 1)
            {
                extractorTwo = (AsOfExtractor) extractors[this.nonDatedExtractors.length + 1];
                matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
            }
            return containsInNonDatedMultiEntry(multiEntry, keyHolder, extractorOne, extractorTwo, matchMoreThanOne, filter);
        }
        return false;
    }

    private boolean containsInNonDatedChained(ChainedBucket chainedBucket, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.nonDatedExtractors.length];
        AsOfExtractor extractorTwo = null;
        boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
        if (asOfAttributes.length > 1)
        {
            extractorTwo = (AsOfExtractor) extractors[this.nonDatedExtractors.length + 1];
            matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
        }

        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.zero;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), keyHolder, extractors))
                {
                    return containsInNonDatedMultiEntry(multiEntry, keyHolder, extractorOne, extractorTwo, matchMoreThanOne, filter);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.zero, keyHolder, extractors))
            {
                return containsInNonDatedIfMatchAsOfDates(bucket.zero, keyHolder, extractors, filter);
            }
            if (bucket.one == null) return false;
            if (bucket.one instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.one;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), keyHolder, extractors))
                {
                    return containsInNonDatedMultiEntry(multiEntry, keyHolder, extractorOne, extractorTwo, matchMoreThanOne, filter);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.one, keyHolder, extractors))
            {
                return containsInNonDatedIfMatchAsOfDates(bucket.one, keyHolder, extractors, filter);
            }
            if (bucket.two == null) return false;
            if (bucket.two instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.two;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), keyHolder, extractors))
                {
                    return containsInNonDatedMultiEntry(multiEntry, keyHolder, extractorOne, extractorTwo, matchMoreThanOne, filter);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.two, keyHolder, extractors))
            {
                return containsInNonDatedIfMatchAsOfDates(bucket.two, keyHolder, extractors, filter);
            }
            if (bucket.three == null) return false;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.three;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), keyHolder, extractors))
                {
                    return containsInNonDatedMultiEntry(multiEntry, keyHolder, extractorOne, extractorTwo, matchMoreThanOne, filter);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.three, keyHolder, extractors))
            {
                return containsInNonDatedIfMatchAsOfDates(bucket.three, keyHolder, extractors, filter);
            }
            break;
        } while(true);
        return false;
    }
    private boolean containsInNonDatedMultiEntry(MultiEntry multiEntry, Object keyHolder,
                                             AsOfExtractor extractorOne, AsOfExtractor extractorTwo, boolean matchMoreThanOne, Filter2 filter)
    {
        if (matchMoreThanOne)
        {
            for(int i=0;i<multiEntry.size;i++)
            {
                Object e = multiEntry.list[i];
                if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(keyHolder), asOfAttributes[0]))
                {
                    continue;
                }
                if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(keyHolder), asOfAttributes[1]))
                {
                    continue;
                }
                if (filter == null || filter.matches(e, keyHolder))
                {
                    return true;
                }
            }
        }
        else
        {
            for(int i=multiEntry.size - 1;i >= 0;i--)
            {
                Object e = multiEntry.list[i];
                if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(keyHolder), asOfAttributes[0]))
                {
                    continue;
                }
                if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(keyHolder), asOfAttributes[1]))
                {
                    continue;
                }
                if (filter == null || filter.matches(e, keyHolder))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean containsInNonDatedIfMatchAsOfDates(Object e, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        AsOfExtractor extractor = (AsOfExtractor) extractors[this.nonDatedExtractors.length];
        if (!extractor.dataMatches(e, extractor.timestampValueOf(keyHolder), asOfAttributes[0]))
        {
            return false;
        }
        if (asOfAttributes.length == 2)
        {
            extractor = (AsOfExtractor) extractors[this.nonDatedExtractors.length+1];
            if (!extractor.dataMatches(e, extractor.timestampValueOf(keyHolder), asOfAttributes[1]))
            {
                return false;
            }
        }
        return filter == null || filter.matches(e, keyHolder);
    }


    private Object getFromNonDatedIfMatchAsOfDates(Object e, Object valueHolder, Extractor[] extractors)
    {
        AsOfExtractor extractor = (AsOfExtractor) extractors[this.nonDatedExtractors.length];
        if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[0]))
        {
            return null;
        }
        if (asOfAttributes.length == 2)
        {
            extractor = (AsOfExtractor) extractors[this.nonDatedExtractors.length+1];
            if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[1]))
            {
                return null;
            }
        }
        return e;
    }

    private Object getFromNonDatedChained(ChainedBucket chainedBucket, Object valueHolder, Extractor[] extractors)
    {
        AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.nonDatedExtractors.length];
        AsOfExtractor extractorTwo = null;
        boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
        if (asOfAttributes.length > 1)
        {
            extractorTwo = (AsOfExtractor) extractors[this.nonDatedExtractors.length + 1];
            matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
        }

        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.zero;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.zero, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.zero, valueHolder, extractors);
            }
            if (bucket.one == null) return null;
            if (bucket.one instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.one;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.one, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.one, valueHolder, extractors);
            }
            if (bucket.two == null) return null;
            if (bucket.two instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.two;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.two, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.two, valueHolder, extractors);
            }
            if (bucket.three == null) return null;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.three;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.three, valueHolder, extractors))
            {
                return getFromNonDatedIfMatchAsOfDates(bucket.three, valueHolder, extractors);
            }
            break;
        } while(true);
        return null;
    }

    private Object getFromNonDatedMultiEntry(MultiEntry multiEntry, Object valueHolder, Extractor[] extractors)
    {
        if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
        {
            AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.nonDatedExtractors.length];
            AsOfExtractor extractorTwo = null;
            boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
            if (asOfAttributes.length > 1)
            {
                extractorTwo = (AsOfExtractor) extractors[this.nonDatedExtractors.length + 1];
                matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
            }
            return getFromNonDatedMultiEntry(multiEntry, valueHolder, extractorOne, extractorTwo, matchMoreThanOne);
        }
        return null;
    }

    private Object getFromNonDatedMultiEntry(MultiEntry multiEntry, Object valueHolder,
            AsOfExtractor extractorOne, AsOfExtractor extractorTwo, boolean matchMoreThanOne)
    {
        if (matchMoreThanOne)
        {
            FastList result = new FastList(multiEntry.size);
            for(int i=0;i<multiEntry.size;i++)
            {
                Object e = multiEntry.list[i];
                if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(valueHolder), asOfAttributes[0]))
                {
                    continue;
                }
                if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(valueHolder), asOfAttributes[1]))
                {
                    continue;
                }
                result.add(e);
            }
            return result.size() > 0 ? result : null;
        }
        else
        {
            for(int i=multiEntry.size - 1;i >= 0;i--)
            {
                Object e = multiEntry.list[i];
                if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(valueHolder), asOfAttributes[0]))
                {
                    continue;
                }
                if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(valueHolder), asOfAttributes[1]))
                {
                    continue;
                }
                return e;
            }
            return null;
        }
    }

    public PrimaryKeyIndex copy()
    {
        final FullUniqueIndex result = new FullUniqueIndex(this.getExtractors(), this.datedSize);
        this.forAll(new DoUntilProcedure()
        {
            public boolean execute(Object object)
            {
                result.put(object);
                return false;
            }
        });
        return result;
    }

    public Object getSemiUniqueFromData(Object data, Timestamp[] asOfDates)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, nonDatedTable.length, this.nonDatedRightShift);

        Object e = nonDatedTable[index];

        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getNonDatedFromDataChained((ChainedBucket) e, data, asOfDates);
        }
        if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
            {
                return getNonDatedFromMultiEntry(multiEntry, asOfDates);
            }
        }
        else if (this.nonDatedHashStrategy.equals(e, data))
        {
            return getFromNonDatedIfMatchesAsOfDates(asOfDates, e);
        }
        return null;
    }

    private Object getNonDatedFromDataChained(ChainedBucket chainedBucket, Object data, Timestamp[] asOfDates)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.zero;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return getNonDatedFromMultiEntry(multiEntry, asOfDates);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.zero, data))
            {
                Object e = bucket.zero;
                return getFromNonDatedIfMatchesAsOfDates(asOfDates, e);
            }
            if (bucket.one == null) return null;
            if (bucket.one instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.one;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return getNonDatedFromMultiEntry(multiEntry, asOfDates);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.one, data))
            {
                return getFromNonDatedIfMatchesAsOfDates(asOfDates, bucket.one);
            }
            if (bucket.two == null) return null;
            if (bucket.two instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.two;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return getNonDatedFromMultiEntry(multiEntry, asOfDates);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.two, data))
            {
                return getFromNonDatedIfMatchesAsOfDates(asOfDates, bucket.two);
            }
            if (bucket.three == null) return null;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.three;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return getNonDatedFromMultiEntry(multiEntry, asOfDates);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.three, data))
            {
                return getFromNonDatedIfMatchesAsOfDates(asOfDates, bucket.three);
            }
            return null;
        } while(true);
    }

    private Object getFromNonDatedIfMatchesAsOfDates(Timestamp[] asOfDates, Object e)
    {
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            if (!asOfAttributes[i].dataMatches(e, asOfDates[i])) return null;
        }
        return e;
    }

    private Object getNonDatedFromMultiEntry(MultiEntry multiEntry, Timestamp[] asOfDates)
    {
        for(int i=multiEntry.size - 1; i >= 0; i--)
        {
            Object e = multiEntry.list[i];
            boolean matches = true;
            for(int j=0;j<asOfAttributes.length && matches;j++)
            {
                if (!asOfAttributes[j].dataMatches(e, asOfDates[j])) matches = false;
            }
            if (matches) return e;
        }
        return null;
    }

    public boolean addSemiUniqueToContainer(Object data, TemporalContainer container)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, nonDatedTable.length, this.nonDatedRightShift);

        Object e = nonDatedTable[index];

        if (e == null) return false;
        if (e instanceof ChainedBucket)
        {
            return addNonDatedToContainerChained((ChainedBucket) e, data, container);
        }
        if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            return addMultiEntryToContainer(data, container, multiEntry);
        }
        else if (this.nonDatedHashStrategy.equals(e, data))
        {
            container.addCommittedData((MithraDataObject) e);
            return true;
        }
        return false;
    }

    private boolean addNonDatedToContainerChained(ChainedBucket chainedBucket, Object data, TemporalContainer container)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                if (addMultiEntryToContainer(data, container, (MultiEntry) bucket.zero)) return true;
            }
            else if (this.nonDatedHashStrategy.equals(bucket.zero, data))
            {
                container.addCommittedData((MithraDataObject) bucket.zero);
                return true;
            }
            if (bucket.one == null) return false;
            if (bucket.one instanceof MultiEntry)
            {
                if (addMultiEntryToContainer(data, container, (MultiEntry) bucket.one)) return true;
            }
            else if (this.nonDatedHashStrategy.equals(bucket.one, data))
            {
                container.addCommittedData((MithraDataObject) bucket.one);
                return true;
            }
            if (bucket.two == null) return false;
            if (bucket.two instanceof MultiEntry)
            {
                if (addMultiEntryToContainer(data, container, (MultiEntry) bucket.two)) return true;
            }
            else if (this.nonDatedHashStrategy.equals(bucket.two, data))
            {
                container.addCommittedData((MithraDataObject) bucket.two);
                return true;
            }
            if (bucket.three == null) return false;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                if (addMultiEntryToContainer(data, container, (MultiEntry) bucket.three)) return true;
            }
            else if (this.nonDatedHashStrategy.equals(bucket.three, data))
            {
                container.addCommittedData((MithraDataObject) bucket.three);
                return true;
            }
            return false;
        } while(true);
    }

    private boolean addMultiEntryToContainer(Object data, TemporalContainer container, MultiEntry multiEntry)
    {
        if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
        {
            for(int i=0;i<multiEntry.size;i++)
            {
                container.addCommittedData((MithraDataObject) multiEntry.list[i]);
            }
            return true;
        }
        return false;
    }

    public List getFromDataForAllDatesAsList(Object data)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, nonDatedTable.length, this.nonDatedRightShift);

        Object e = nonDatedTable[index];

        if (e == null) return ListFactory.EMPTY_LIST;
        if (e instanceof ChainedBucket)
        {
            return getFromDataForAllDatesAsListChained((ChainedBucket)e, data);
        }
        if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
            {
                return convertMultiEntryToList(multiEntry);
            }
        }
        else if (this.nonDatedHashStrategy.equals(e, data))
        {
            return ListFactory.create(e);
        }
        return ListFactory.EMPTY_LIST;
    }

    private List getFromDataForAllDatesAsListChained(ChainedBucket chainedBucket, Object data)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.zero;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return convertMultiEntryToList(multiEntry);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.zero, data))
            {
                return ListFactory.create(bucket.zero);
            }
            if (bucket.one == null) return ListFactory.EMPTY_LIST;
            if (bucket.one instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.one;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return convertMultiEntryToList(multiEntry);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.one, data))
            {
                return ListFactory.create(bucket.one);
            }
            if (bucket.two == null) return ListFactory.EMPTY_LIST;
            if (bucket.two instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.two;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return convertMultiEntryToList(multiEntry);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.two, data))
            {
                return ListFactory.create(bucket.two);
            }
            if (bucket.three == null) return ListFactory.EMPTY_LIST;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.three;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    return convertMultiEntryToList(multiEntry);
                }
            }
            else if (this.nonDatedHashStrategy.equals(bucket.three, data))
            {
                return ListFactory.create(bucket.three);
            }
            return ListFactory.EMPTY_LIST;
        } while(true);
    }

    private List convertMultiEntryToList(MultiEntry multiEntry)
    {
        FastList result = new FastList(multiEntry.size);
        for(int i=0;i<multiEntry.size;i++)
        {
            result.add(multiEntry.list[i]);
        }
        return result;
    }

    public Object getSemiUniqueAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int index = indexFor(nonDatedHash, nonDatedTable.length, this.nonDatedRightShift);

        Object e = nonDatedTable[index];

        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getNonDatedChained((ChainedBucket) e, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        if (e instanceof MultiEntry)
        {
            return getNonDatedMulti((MultiEntry) e, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        else if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, e, asOfDate0, asOfDate1))
        {
            return e;
        }
        return null;
    }

    private Object getNonDatedChained(ChainedBucket chainedBucket, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.zero;
                for(int i=multiEntry.size-1;i >= 0; i--)
                {
                    Object e = multiEntry.list[i];
                    if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, e, asOfDate0, asOfDate1))
                    {
                        return e;
                    }
                }
            }
            else if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, bucket.zero, asOfDate0, asOfDate1))
            {
                return bucket.zero;
            }
            if (bucket.one == null) return null;
            if (bucket.one instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.one;
                for(int i=multiEntry.size-1;i >= 0; i--)
                {
                    Object e = multiEntry.list[i];
                    if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, e, asOfDate0, asOfDate1))
                    {
                        return e;
                    }
                }
            }
            else if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, bucket.one, asOfDate0, asOfDate1))
            {
                return bucket.one;
            }
            if (bucket.two == null) return null;
            if (bucket.two instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.two;
                for(int i=multiEntry.size-1;i >= 0; i--)
                {
                    Object e = multiEntry.list[i];
                    if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, e, asOfDate0, asOfDate1))
                    {
                        return e;
                    }
                }
            }
            else if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, bucket.two, asOfDate0, asOfDate1))
            {
                return bucket.two;
            }
            if (bucket.three == null) return null;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.three;
                for(int i=multiEntry.size-1;i >= 0; i--)
                {
                    Object e = multiEntry.list[i];
                    if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, e, asOfDate0, asOfDate1))
                    {
                        return e;
                    }
                }
            }
            else if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, bucket.three, asOfDate0, asOfDate1))
            {
                return bucket.three;
            }
            return null;

        } while(true);

    }

    private Object getNonDatedMulti(MultiEntry multiEntry, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        for(int i=multiEntry.size-1;i >= 0; i--)
        {
            Object e = multiEntry.list[i];
            if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, e, asOfDate0, asOfDate1))
            {
                return e;
            }
        }
        return null;
    }

    public Object getSemiUniqueAsOneWithDates(Object valueHolder, Extractor[] extractors, Timestamp[] asOfDates, int nonDatedHash)
    {
        int index = indexFor(nonDatedHash, nonDatedTable.length, this.nonDatedRightShift);

        Object e = nonDatedTable[index];

        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return getNonDatedAsOneWithDatesChained((ChainedBucket) e, valueHolder, extractors, asOfDates);
        }
        if (e instanceof MultiEntry)
        {
            return getNonDatedAsOneWithDatesMulti((MultiEntry) e, valueHolder, extractors, asOfDates);
        }
        else if (nonDatedHashStrategy.equals(e, valueHolder, extractors) && asOfAttributes[0].dataMatches(e, asOfDates[0]) &&
                        (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(e, asOfDates[1])))
        {
            return e;
        }
        return null;
    }

    private Object getNonDatedAsOneWithDatesChained(ChainedBucket chainedBucket, Object valueHolder, Extractor[] extractors, Timestamp[] asOfDates)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            if (bucket.zero instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.zero;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return matchAsOfDates(multiEntry, asOfDates);
                }
            }
            else if (nonDatedHashStrategy.equals(bucket.zero, valueHolder, extractors) && asOfAttributes[0].dataMatches(bucket.zero, asOfDates[0]) &&
                        (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(bucket.zero, asOfDates[1])))
            {
                return bucket.zero;
            }
            if (bucket.one == null) return null;
            if (bucket.one instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.one;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return matchAsOfDates(multiEntry, asOfDates);
                }
            }
            else if (nonDatedHashStrategy.equals(bucket.one, valueHolder, extractors) && asOfAttributes[0].dataMatches(bucket.one, asOfDates[0]) &&
                        (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(bucket.one, asOfDates[1])))
            {
                return bucket.one;
            }
            if (bucket.two == null) return null;
            if (bucket.two instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.two;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return matchAsOfDates(multiEntry, asOfDates);
                }
            }
            else if (nonDatedHashStrategy.equals(bucket.two, valueHolder, extractors) && asOfAttributes[0].dataMatches(bucket.two, asOfDates[0]) &&
                        (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(bucket.two, asOfDates[1])))
            {
                return bucket.two;
            }
            if (bucket.three == null) return null;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) bucket.three;
                if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
                {
                    return matchAsOfDates(multiEntry, asOfDates);
                }
            }
            else if (nonDatedHashStrategy.equals(bucket.three, valueHolder, extractors) && asOfAttributes[0].dataMatches(bucket.three, asOfDates[0]) &&
                        (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(bucket.three, asOfDates[1])))
            {
                return bucket.three;
            }
            return null;

        } while(true);

    }

    private Object getNonDatedAsOneWithDatesMulti(MultiEntry multiEntry, Object valueHolder, Extractor[] extractors, Timestamp[] asOfDates)
    {
        if (nonDatedHashStrategy.equals(multiEntry.getFirst(), valueHolder, extractors))
        {
            return matchAsOfDates(multiEntry, asOfDates);
        }
        return null;
    }

    private Object matchAsOfDates(MultiEntry multiEntry, Timestamp[] asOfDates)
    {
        for(int i=multiEntry.size-1;i >= 0; i--)
        {
            Object e = multiEntry.list[i];
            if (asOfAttributes[0].dataMatches(e, asOfDates[0]) &&
                    (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(e, asOfDates[1])))
            {
                return e;
            }
        }
        return null;
    }

    public int getSemiUniqueSize()
    {
        return this.nonDatedSize;
    }

    public List removeOldEntryForRange(Object data)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);

        int index = indexFor(hash, this.nonDatedTable.length, this.nonDatedRightShift);

        Object e = this.nonDatedTable[index];

        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return removeOldEntryForRangeChained((ChainedBucket)e, data, index);
        }
        if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
            {
                FastList result = removeOldEntryForRangeMulti(data, multiEntry);
                if (multiEntry.size == 0)
                {
                    this.nonDatedTable[index] = null;
                    this.nonDatedSize--;
                }
                return result;
            }
        }
        else if (this.nonDatedHashStrategy.equals(e, data))
        {
            if (hasOverlap(e, data))
            {
                this.nonDatedTable[index] = null;
                this.nonDatedSize--;
                removeFromDatedTable(e);
                return ListFactory.create(e);
            }
        }
        return null;
    }

    private List removeOldEntryForRangeChained(ChainedBucket chainedBucket, Object data, int index)
    {
        ChainedBucket bucket = chainedBucket;
        List result = null;
        int pos = 0;
        do
        {
            Object e = bucket.zero;
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryForRangeChainedMulti(data, chainedBucket, pos, multiEntry);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (hasOverlap(e, data))
                {
                    result = removeOldEntryForRangeChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            e = bucket.one;
            if (e == null) break;
            pos++;
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryForRangeChainedMulti(data, chainedBucket, pos, multiEntry);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (hasOverlap(e, data))
                {
                    result = removeOldEntryForRangeChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            e = bucket.two;
            if (e == null) break;
            pos++;
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryForRangeChainedMulti(data, chainedBucket, pos, multiEntry);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (hasOverlap(e, data))
                {
                    result = removeOldEntryForRangeChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            e = bucket.three;
            if (e == null) break;
            pos++;
            if (e instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) e;
                continue;
            }
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryForRangeChainedMulti(data, chainedBucket, pos, multiEntry);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (hasOverlap(e, data))
                {
                    result = removeOldEntryForRangeChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            break;
        } while(true);
        if (chainedBucket.zero == null)
        {
            this.nonDatedTable[index] = null;
        }
        return result;
    }

    private List removeOldEntryForRangeChainedSingle(ChainedBucket bucket, int pos, Object e)
    {
        removeFromDatedTable(e);
        bucket.remove(pos);
        this.nonDatedSize--;
        return ListFactory.create(e);
    }

    private List removeOldEntryForRangeChainedMulti(Object data, ChainedBucket bucket, int pos, MultiEntry multiEntry)
    {
        List  result = removeOldEntryForRangeMulti(data, multiEntry);
        if (multiEntry.size == 0)
        {
            bucket.remove(pos);
            this.nonDatedSize--;
        }
        return result;
    }

    private FastList removeOldEntryForRangeMulti(Object data, MultiEntry multiEntry)
    {
        FastList result = null;
        for(int i = 0;i<multiEntry.size;)
        {
            Object o = multiEntry.list[i];
            if (hasOverlap(o, data))
            {
                if (result == null) result = new FastList(multiEntry.size);
                result.add(o);
                removeFromDatedTable(o);
                multiEntry.remove(i);
            }
            else
            {
                i++;
            }
        }
        return result;
    }

    private boolean hasOverlap(Object entry, Object data)
    {
        boolean fullMatch = true;
        for(int i=0;fullMatch && i<asOfAttributes.length;i++)
        {
            fullMatch = asOfAttributes[i].hasRangeOverlap((MithraDataObject) entry,
                    asOfAttributes[i].getFromAttribute().timestampValueOfAsLong(data),
                    asOfAttributes[i].getToAttribute().timestampValueOfAsLong(data));
        }
        return fullMatch;
    }

    public boolean removeAllIgnoringDate(Object data, DoProcedure procedure)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, this.nonDatedTable.length, this.nonDatedRightShift);

        Object e = this.nonDatedTable[index];

        if (e == null) return false;
        if (e instanceof ChainedBucket)
        {
            return removeAllIgnoringDateChained((ChainedBucket)e, data, procedure, index);
        }
        if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
            {
                removeMultiAndExecute(procedure, index, multiEntry);
                return true;
            }
        }
        else if (this.nonDatedHashStrategy.equals(e, data))
        {
            removeSingleAndExecute(procedure, index, e);
            return true;
        }
        return false;
    }

    private boolean removeAllIgnoringDateChained(ChainedBucket chainedBucket, Object data, DoProcedure procedure, int index)
    {
        ChainedBucket bucket = chainedBucket;
        boolean result = false;
        int pos = 0;
        do
        {
            Object e = bucket.zero;
            if (removeFromBucket(e, chainedBucket, data, procedure, pos))
            {
                result = true;
                break;
            }
            e = bucket.one;
            if (e == null) break;
            pos++;
            if (removeFromBucket(e, chainedBucket, data, procedure, pos))
            {
                result = true;
                break;
            }
            e = bucket.two;
            if (e == null) break;
            pos++;
            if (removeFromBucket(e, chainedBucket, data, procedure, pos))
            {
                result = true;
                break;
            }
            e = bucket.three;
            if (e == null) break;
            pos++;
            if (e instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) e;
                continue;
            }
            if (removeFromBucket(e, chainedBucket, data, procedure, pos))
            {
                result = true;
                break;
            }
            break;
        } while(true);
        if (chainedBucket.zero == null)
        {
            this.nonDatedTable[index] = null;
        }
        return result;
    }

    private boolean removeFromBucket(Object e, ChainedBucket bucket, Object data, DoProcedure procedure, int pos)
    {
        boolean result = false;
        if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
            {
                removeMultiFromBucketAndExecute(procedure, bucket, pos, multiEntry);
                result = true;
            }
        }
        else if (this.nonDatedHashStrategy.equals(e, data))
        {
            removeSingleFromBucketAndExecute(procedure, bucket, pos, e);
            result = true;
        }
        return result;
    }

    private void removeSingleFromBucketAndExecute(DoProcedure procedure, ChainedBucket bucket, int pos, Object e)
    {
        this.nonDatedSize--;
        bucket.remove(pos);
        removeFromDatedTable(e);
        procedure.execute(e);
    }

    private void removeMultiFromBucketAndExecute(DoProcedure procedure, ChainedBucket bucket, int pos, MultiEntry multiEntry)
    {
        this.nonDatedSize--;
        for (int i = 0; i < multiEntry.size; i++)
        {
            removeFromDatedTable(multiEntry.list[i]);
            procedure.execute(multiEntry.list[i]);
        }
        bucket.remove(pos);
    }

    private void removeSingleAndExecute(DoProcedure procedure, int index, Object e)
    {
        this.nonDatedTable[index] = null;
        this.nonDatedSize--;
        removeFromDatedTable(e);
        procedure.execute(e);
    }

    private void removeMultiAndExecute(DoProcedure procedure, int index, MultiEntry multiEntry)
    {
        this.nonDatedTable[index] = null;
        this.nonDatedSize--;
        for(int i=0;i<multiEntry.size;i++)
        {
            removeFromDatedTable(multiEntry.list[i]);
            procedure.execute(multiEntry.list[i]);
        }
    }

    public Object removeOldEntry(Object data, Timestamp[] asOfDates)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);

        int index = indexFor(hash, this.nonDatedTable.length, this.nonDatedRightShift);

        Object e = this.nonDatedTable[index];

        if (e == null) return null;
        if (e instanceof ChainedBucket)
        {
            return removeOldEntryChained((ChainedBucket)e, index, data, asOfDates);
        }
        if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
            {
                Object result = removeOldEntryMulti(multiEntry, asOfDates);
                if (multiEntry.size == 0)
                {
                    this.nonDatedTable[index] = null;
                    this.nonDatedSize--;
                }
                return result;
            }
        }
        else if (this.nonDatedHashStrategy.equals(e, data))
        {
            if (matchesAsOfDates(e, asOfDates))
            {
                this.nonDatedTable[index] = null;
                this.nonDatedSize--;
                removeFromDatedTable(e);
                return e;
            }
        }
        return null;
    }

    private Object removeOldEntryChained(ChainedBucket chainedBucket, int index, Object data, Timestamp[] asOfDates)
    {
        ChainedBucket bucket = chainedBucket;
        Object result = null;
        int pos = 0;
        do
        {
            Object e = bucket.zero;
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryChainedMulti(chainedBucket, pos, multiEntry, asOfDates);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (matchesAsOfDates(e, asOfDates))
                {
                    result = removeOldEntryChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            e = bucket.one;
            if (e == null) break;
            pos++;
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryChainedMulti(chainedBucket, pos, multiEntry, asOfDates);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (matchesAsOfDates(e, asOfDates))
                {
                    result = removeOldEntryChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            e = bucket.two;
            if (e == null) break;
            pos++;
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryChainedMulti(chainedBucket, pos, multiEntry, asOfDates);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (matchesAsOfDates(e, asOfDates))
                {
                    result = removeOldEntryChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            e = bucket.three;
            if (e == null) break;
            pos++;
            if (e instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) e;
                continue;
            }
            if (e instanceof MultiEntry)
            {
                MultiEntry multiEntry = (MultiEntry) e;
                if (this.nonDatedHashStrategy.equals(multiEntry.getFirst(), data))
                {
                    result = removeOldEntryChainedMulti(chainedBucket, pos, multiEntry, asOfDates);
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(e, data))
            {
                if (matchesAsOfDates(e, asOfDates))
                {
                    result = removeOldEntryChainedSingle(chainedBucket, pos, e);
                    break;
                }
            }
            break;
        } while(true);
        if (chainedBucket.zero == null)
        {
            this.nonDatedTable[index] = null;
        }
        return result;
    }

    private Object removeOldEntryChainedSingle(ChainedBucket bucket, int pos, Object e)
    {
        removeFromDatedTable(e);
        bucket.remove(pos);
        this.nonDatedSize--;
        return e;
    }

    private Object removeOldEntryChainedMulti(ChainedBucket bucket, int pos, MultiEntry multiEntry, Timestamp[] asOfDates)
    {
        Object result = removeOldEntryMulti(multiEntry, asOfDates);
        if (multiEntry.size == 0)
        {
            bucket.remove(pos);
            this.nonDatedSize--;
        }
        return result;
    }

    private Object removeOldEntryMulti(MultiEntry multiEntry, Timestamp[] asOfDates)
    {
        for(int i = multiEntry.size - 1;i >= 0;i--)
        {
            Object o = multiEntry.list[i];
            if (matchesAsOfDates(o, asOfDates))
            {
                removeFromDatedTable(o);
                multiEntry.remove(i);
                return o;
            }
        }
        return null;
    }

    private boolean matchesAsOfDates(Object entry, Timestamp[] asOfDates)
    {
        for(int i=0;i<asOfAttributes.length;i++)
        {
            if (!asOfAttributes[i].dataMatches(entry, asOfDates[i])) return false;
        }
        return true;
    }

    private void putInNonDatedTable(Object key, int hash, Object removed)
    {
        Object[] tab = this.nonDatedTable;
        int index = indexFor(hash, tab.length, this.nonDatedRightShift);

        Object cur = tab[index];
        boolean newEntry = false;

        if (cur == null)
        {
            tab[index] = key;
            newEntry = true;
        }
        else if (cur == removed)
        {
            tab[index] = key;
        }
        else if (cur instanceof ChainedBucket)
        {
            newEntry = ((ChainedBucket)cur).addNonDatedIfNotThere(key, this.nonDatedHashStrategy, removed);
        }
        else if (cur instanceof MultiEntry)
        {
            MultiEntry entry = (MultiEntry) cur;
            if (this.nonDatedHashStrategy.equals(entry.getFirst(), key))
            {
                entry.addNonDated(key, removed);
            }
            else
            {
                tab[index] = new ChainedBucket(cur, key);
                newEntry = true;
            }
        }
        else if (this.nonDatedHashStrategy.equals(cur, key))
        {
           tab[index] = new MultiEntry(cur, key);
        }
        else
        {
            tab[index] = new ChainedBucket(cur, key);
            newEntry = true;
        }
        if (newEntry && ++this.nonDatedSize > this.nonDatedThreshold)
        {
            this.resizeNonDatedTable();
        }
    }

    private void resizeNonDatedTable()
    {
        int newCapacity = this.nonDatedTable.length << 1;
        resizeNonDated(newCapacity);
    }

    private void resizeNonDated(int newCapacity)
    {
        this.nonDatedThreshold = Math.min(newCapacity - 1, this.scaledByLoadFactor(newCapacity));

        this.nonDatedRightShift = (byte) (Integer.numberOfTrailingZeros(newCapacity) + 1);

        if (this.nonDatedTable.length == MAXIMUM_CAPACITY)
        {
            nonDatedThreshold = Integer.MAX_VALUE;
            return;
        }

        Object[] newTable = new Object[newCapacity];
        transferNonDatedTable(this.nonDatedTable, newTable);
        nonDatedTable = newTable;
    }

    private void transferNonDatedTable(Object[] src, Object[] dest) {
        for (int j = 0; j < src.length; ++j)
        {
            Object e = src[j];
            src[j] = null;
            if (e == null)
            {
                continue;
            }
            if (e instanceof ChainedBucket)
            {
                transferNonDatedChained((ChainedBucket) e, dest);
            }
            else if (e instanceof MultiEntry)
            {
                transferNonDatedEntry(dest, e, this.nonDatedHashStrategy.computeHashCode(((MultiEntry) e).getFirst()));
            }
            else
            {
                transferNonDatedEntry(dest, e, this.nonDatedHashStrategy.computeHashCode(e));
            }
        }
    }

    private void transferNonDatedChained(ChainedBucket chainedBucket, Object[] dest)
    {
        ChainedBucket bucket = chainedBucket;
        do
        {
            transferNonDatedBucketValue(dest, bucket.zero);
            if (bucket.one == null) return;
            transferNonDatedBucketValue(dest, bucket.one);
            if (bucket.two == null) return;
            transferNonDatedBucketValue(dest, bucket.two);
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three == null) return;
            transferNonDatedBucketValue(dest, bucket.three);
            return;
        } while(true);
    }

    private void transferNonDatedBucketValue(Object[] dest, Object e)
    {
        int hash;
        if (e instanceof MultiEntry)
        {
            hash = this.nonDatedHashStrategy.computeHashCode(((MultiEntry) e).getFirst());
        }
        else
        {
            hash = this.nonDatedHashStrategy.computeHashCode(e);
        }
        transferNonDatedEntry(dest, e, hash);
    }

    private void transferNonDatedEntry(Object[] dest, Object entry, int hash)
    {
        int index = indexFor(hash, dest.length, this.nonDatedRightShift);
        Object cur = dest[index];
        if (cur == null)
        {
            dest[index] = entry;
        }
        else if (cur instanceof ChainedBucket)
        {
            ((ChainedBucket)cur).add(entry);
        }
        else
        {
            dest[index] = new ChainedBucket(cur, entry);
        }
    }

    protected void removeNonDatedEntry(Object key)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(key);
        Object[] tab = this.nonDatedTable;
        int index = indexFor(hash, tab.length, this.nonDatedRightShift);

        Object e = tab[index];

        if (e == key)
        {
            this.nonDatedSize--;
            tab[index] = null;
        }
        else if (e instanceof ChainedBucket)
        {
            removeNonDatedChained((ChainedBucket) e, index, key);
        }
        else if (e instanceof MultiEntry)
        {
            MultiEntry multiEntry = (MultiEntry) e;
            multiEntry.removeByIdentity(key);
            if (multiEntry.size == 0)
            {
                this.nonDatedSize--;
                tab[index] = null;
            }
        }
    }

    private void removeNonDatedChained(ChainedBucket chainedBucket, int index, Object key)
    {
        ChainedBucket bucket = chainedBucket;
        int pos = 0;
        do
        {
            if (bucket.zero == key)
            {
                chainedBucket.remove(pos);
                this.nonDatedSize--;
                break;
            }
            if (bucket.zero instanceof MultiEntry)
            {
                if (removeFromMultiEntryInChain(key, chainedBucket, pos, (MultiEntry) bucket.zero)) break;
            }
            if (bucket.one == null) break;
            pos++;
            if (bucket.one == key)
            {
                chainedBucket.remove(pos);
                this.nonDatedSize--;
                break;
            }
            if (bucket.one instanceof MultiEntry)
            {
                if (removeFromMultiEntryInChain(key, chainedBucket, pos, (MultiEntry) bucket.one)) break;
            }
            if (bucket.two == null) break;
            pos++;
            if (bucket.two == key)
            {
                chainedBucket.remove(pos);
                this.nonDatedSize--;
                break;
            }
            if (bucket.two instanceof MultiEntry)
            {
                if (removeFromMultiEntryInChain(key, chainedBucket, pos, (MultiEntry) bucket.two)) break;
            }
            if (bucket.three == null) break;
            pos++;
            if (bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                continue;
            }
            if (bucket.three == key)
            {
                chainedBucket.remove(pos);
                this.nonDatedSize--;
            }
            if (bucket.three instanceof MultiEntry)
            {
                if (removeFromMultiEntryInChain(key, chainedBucket, pos, (MultiEntry) bucket.three)) break;
            }
            break;

        } while(true);

        if (chainedBucket.zero == null)
        {
            this.nonDatedTable[index] = null;
        }
    }

    private boolean removeFromMultiEntryInChain(Object key, ChainedBucket bucket, int pos, MultiEntry multiEntry)
    {
        if (multiEntry.removeByIdentity(key))
        {
            if (multiEntry.size == 0)
            {
                this.nonDatedSize--;
                bucket.remove(pos);
            }
            return true;
        }
        return false;
    }

    public void clearNonDatedTable()
    {
        Object tab[] = nonDatedTable;
        for (int i = 0; i < tab.length; ++i)
            tab[i] = null;
        nonDatedSize = 0;
   }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return 0;
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return 0;
    }

    private static class MultiEntry
    {
        private Object[] list;
        private int size;

        public MultiEntry(Object first, Object second)
        {
            this.size = 2;
            this.list = new Object[2];
            list[0] = first;
            list[1] = second;
        }

        public Object getFirst()
        {
            return list[0];
        }

        public void addNonDated(Object key, Object removed)
        {
            if (removed != null)
            {
                for(int i=0;i<size;i++)
                {
                    if (list[i] == removed)
                    {
                        list[i] = key;
                        return;
                    }
                }
            }
            else
            {
                if (size == list.length)
                {
                    int newLength = size + 2;
                    Object[] newList = new Object[newLength];
                    System.arraycopy(list, 0, newList, 0, size);
                    list = newList;
                }
                list[size] = key;
                size++;
            }
        }

        public boolean removeByIdentity(Object key)
        {
            for(int i=0;i<size;i++)
            {
                if (list[i] == key)
                {
                    remove(i);
                    return true;
                }
            }
            return false;
        }

        public void remove(int i)
        {
            size--;
            list[i] = list[size];
            list[size] = null;
        }
    }

    private static final class ChainedBucket
    {
        private Object zero;
        private Object one;
        private Object two;
        private Object three;

        private ChainedBucket(Object first, Object second)
        {
            this.zero = first;
            this.one = second;
        }

        public void remove(int i)
        {
            if (i > 3)
            {
                this.removeLongChain(this, i - 3);
            }
            else
            {
                switch (i)
                {
                    case 0:
                        this.zero = this.removeLast(0);
                        return;
                    case 1:
                        this.one = this.removeLast(1);
                        return;
                    case 2:
                        this.two = this.removeLast(2);
                        return;
                    case 3:
                        if (this.three instanceof ChainedBucket)
                        {
                            this.removeLongChain(this, i - 3);
                            return;
                        }
                        this.three = null;
                }
            }
        }

        private void removeLongChain(ChainedBucket oldBucket, int i)
        {
            while(i > 3)
            {
                oldBucket = (ChainedBucket) oldBucket.three;
                i -= 3;
            }
            do
            {
                ChainedBucket bucket = (ChainedBucket) oldBucket.three;
                switch (i)
                {
                    case 0:
                        bucket.zero = bucket.removeLast(0);
                        if (bucket.zero == null)
                        {
                            oldBucket.three = null;
                        }
                        return;
                    case 1:
                        bucket.one = bucket.removeLast(1);
                        return;
                    case 2:
                        bucket.two = bucket.removeLast(2);
                        return;
                    case 3:
                        if (bucket.three instanceof ChainedBucket)
                        {
                            i -= 3;
                            oldBucket = bucket;
                            continue;
                        }
                        bucket.three = null;
                        return;
                }

            }
            while (true);
        }

        public Object get(int i)
        {
            ChainedBucket bucket = this;
            while (i > 3 && bucket.three instanceof ChainedBucket)
            {
                bucket = (ChainedBucket) bucket.three;
                i -= 3;
            }
            do
            {
                switch (i)
                {
                    case 0:
                        return bucket.zero;
                    case 1:
                        return bucket.one;
                    case 2:
                        return bucket.two;
                    case 3:
                        if (bucket.three instanceof ChainedBucket)
                        {
                            i -= 3;
                            bucket = (ChainedBucket) bucket.three;
                            continue;
                        }
                        return bucket.three;
                    case 4:
                        return null; // this happens when a bucket is exactly full and we're iterating
                }
            }
            while (true);
        }

        public Object removeLast(int cur)
        {
            if (this.three instanceof ChainedBucket)
            {
                return this.removeLast(this);
            }
            if (this.three != null)
            {
                Object result = this.three;
                this.three = null;
                return cur == 3 ? null : result;
            }
            if (this.two != null)
            {
                Object result = this.two;
                this.two = null;
                return cur == 2 ? null : result;
            }
            if (this.one != null)
            {
                Object result = this.one;
                this.one = null;
                return cur == 1 ? null : result;
            }
            this.zero = null;
            return null;
        }

        private Object removeLast(ChainedBucket oldBucket)
        {
            do
            {
                ChainedBucket bucket = (ChainedBucket) oldBucket.three;
                if (bucket.three instanceof ChainedBucket)
                {
                    oldBucket = bucket;
                    continue;
                }
                if (bucket.three != null)
                {
                    Object result = bucket.three;
                    bucket.three = null;
                    return result;
                }
                if (bucket.two != null)
                {
                    Object result = bucket.two;
                    bucket.two = null;
                    return result;
                }
                if (bucket.one != null)
                {
                    Object result = bucket.one;
                    bucket.one = null;
                    return result;
                }
                Object result = bucket.zero;
                oldBucket.three = null;
                return result;

            }
            while (true);
        }

        public Object addIfNotThere(Object key, ExtractorBasedHashStrategy hashStrategy)
        {
            ChainedBucket bucket = this;
            do
            {
                if (hashStrategy.equals(bucket.zero, key))
                {
                    Object removed = bucket.zero;
                    bucket.zero = key;
                    return removed;
                }
                if (bucket.one == null)
                {
                    bucket.one = key;
                    return null;
                }
                else if (hashStrategy.equals(bucket.one, key))
                {
                    Object removed = bucket.one;
                    bucket.one = key;
                    return removed;
                }
                if (bucket.two == null)
                {
                    bucket.two = key;
                    return null;
                }
                else if (hashStrategy.equals(bucket.two, key))
                {
                    Object removed = bucket.two;
                    bucket.two = key;
                    return removed;
                }
                if (bucket.three instanceof ChainedBucket)
                {
                    bucket = (ChainedBucket) bucket.three;
                    continue;
                }
                if (bucket.three == null)
                {
                    bucket.three = key;
                    return null;
                }
                else if (hashStrategy.equals(bucket.three, key))
                {
                    Object removed = bucket.three;
                    bucket.three = key;
                    return removed;
                }
                else
                {
                    bucket.three = new ChainedBucket(bucket.three, key);
                    return null;
                }
            }
            while (true);
        }

        public boolean forAll(DoUntilProcedure procedure)
        {
            boolean done;
            ChainedBucket bucket = this;
            do
            {
                done = procedure.execute(bucket.zero);
                if (done || bucket.one == null) break;
                done = procedure.execute(bucket.one);
                if (done || bucket.two == null) break;
                done = procedure.execute(bucket.two);
                if (done || bucket.three == null) break;
                if (bucket.three instanceof ChainedBucket)
                {
                    bucket = (ChainedBucket) bucket.three;
                    continue;
                }
                done = procedure.execute(bucket.three);
                break;
            }
            while (true);
            return done;
        }

        public void forAll(DoProcedure procedure)
        {
            ChainedBucket bucket = this;
            do
            {
                procedure.execute(bucket.zero);
                if (bucket.one == null) break;
                procedure.execute(bucket.one);
                if (bucket.two == null) break;
                procedure.execute(bucket.two);
                if (bucket.three == null) break;
                if (bucket.three instanceof ChainedBucket)
                {
                    bucket = (ChainedBucket) bucket.three;
                    continue;
                }
                procedure.execute(bucket.three);
                break;
            }
            while (true);
        }

        public boolean addNonDatedIfNotThere(Object key, HashStrategy hashStrategy, Object removed)
        {
            //choices are: null, MultiEntry, Object and ChainedBucket if last slot
            ChainedBucket bucket = this;
            do
            {
                if (bucket.zero == removed)
                {
                    bucket.zero = key;
                    return false;
                }
                if (bucket.zero instanceof MultiEntry)
                {
                    if (addToMultiEntryIfEqual(key, hashStrategy, removed, bucket.zero)) return false;
                }
                else if (hashStrategy.equals(bucket.zero, key))
                {
                    bucket.zero = new MultiEntry(bucket.zero, key);
                    return false;
                }
                if (bucket.one == null)
                {
                    bucket.one = key;
                    return true;
                }
                if (bucket.one == removed)
                {
                    bucket.one = key;
                    return false;
                }
                if (bucket.one instanceof MultiEntry)
                {
                    if (addToMultiEntryIfEqual(key, hashStrategy, removed, bucket.one)) return false;
                }
                else if (hashStrategy.equals(bucket.one, key))
                {
                    bucket.one = new MultiEntry(bucket.one, key);
                    return false;
                }
                if (bucket.two == null)
                {
                    bucket.two = key;
                    return true;
                }
                if (bucket.two == removed)
                {
                    bucket.two = key;
                    return false;
                }
                if (bucket.two instanceof MultiEntry)
                {
                    if (addToMultiEntryIfEqual(key, hashStrategy, removed, bucket.two)) return false;
                }
                else if (hashStrategy.equals(bucket.two, key))
                {
                    bucket.two = new MultiEntry(bucket.two, key);
                    return false;
                }
                if (bucket.three instanceof ChainedBucket)
                {
                    bucket = (ChainedBucket) bucket.three;
                    continue;
                }
                if (bucket.three == null)
                {
                    bucket.three = key;
                    return true;
                }
                if (bucket.three == removed)
                {
                    bucket.three = key;
                    return false;
                }
                if (bucket.three instanceof MultiEntry)
                {
                    if (addToMultiEntryIfEqual(key, hashStrategy, removed, bucket.three)) return false;
                }
                else if (hashStrategy.equals(bucket.three, key))
                {
                    bucket.three = new MultiEntry(bucket.three, key);
                    return false;
                }
                bucket.three = new ChainedBucket(bucket.three, key);
                return true;
            } while(true);
        }

        private boolean addToMultiEntryIfEqual(Object key, HashStrategy hashStrategy, Object removed, Object o)
        {
            MultiEntry entry = (MultiEntry) o;
            if (hashStrategy.equals(entry.getFirst(), key))
            {
                entry.addNonDated(key, removed);
                return true;
            }
            return false;
        }

        public void add(Object key)
        {
            ChainedBucket bucket = this;
            do
            {
                if (bucket.one == null)
                {
                    bucket.one = key;
                    return;
                }
                if (bucket.two == null)
                {
                    bucket.two = key;
                    return;
                }
                if (bucket.three instanceof ChainedBucket)
                {
                    bucket = (ChainedBucket) bucket.three;
                    continue;
                }
                if (bucket.three == null)
                {
                    bucket.three = key;
                    return;
                }
                else
                {
                    bucket.three = new ChainedBucket(bucket.three, key);
                    return;
                }
            }
            while (true);
        }

        public void forAll(ParallelProcedure procedure, int chunk)
        {
            ChainedBucket bucket = this;
            do
            {
                procedure.execute(bucket.zero, chunk);
                if (bucket.one == null) break;
                procedure.execute(bucket.one, chunk);
                if (bucket.two == null) break;
                procedure.execute(bucket.two, chunk);
                if (bucket.three == null) break;
                if (bucket.three instanceof ChainedBucket)
                {
                    bucket = (ChainedBucket) bucket.three;
                    continue;
                }
                procedure.execute(bucket.three, chunk);
                break;
            }
            while (true);
        }
    }

    /*
    Procedure to find duplicate records from ChainedBucket
     */
    private final class DuplicateProcedure implements DoProcedure
    {
        private final List<Object> duplicates;

        private DuplicateProcedure(List duplicates)
        {
            this.duplicates = duplicates;
        }

        public void execute(Object obj)
        {
            if (obj instanceof MultiEntry)
            {
                collectMilestoningOverlaps((MultiEntry) obj, duplicates);
            }
            else if (obj instanceof ChainedBucket)
            {
                collectMilestoningOverlaps((ChainedBucket) obj, this);
            }
        }
    }

    /*
      Task that calculates duplicates on data segment assigned to it.
      Result of such tasks are merged to get consolidated list
     */
    private class DetectDuplicateTask extends CpuTask
    {
        private final ArrayBasedQueue queue;
        private final DoProcedure duplicateProcedure;
        private List<Object> duplicates;

        public DetectDuplicateTask(ArrayBasedQueue queue)
        {
            this.queue = queue;
            this.duplicates = FastList.newList();
            this.duplicateProcedure = new DuplicateProcedure(duplicates);
        }

        public List<Object> getDuplicates()
        {
            return duplicates;
        }

        @Override
        public void execute()
        {
            ArrayBasedQueue.Segment segment = queue.borrow(null);
            while (segment != null)
            {
                for (int i = segment.getStart(); i < segment.getEnd(); i++)
                {
                    Object obj = nonDatedTable[i];
                    delegateByType(obj, duplicates, duplicateProcedure);
                }
                segment = queue.borrow(segment);
            }
        }
    }
}