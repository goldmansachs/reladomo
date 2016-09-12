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
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.sql.Timestamp;
import java.util.List;



public class PartialSemiUniqueDatedIndex implements SemiUniqueDatedIndex
{

    protected ExtractorBasedHashStrategy hashStrategy;
    protected ExtractorBasedHashStrategy asOfAttributeHashStrategy;
    private Extractor[] extractors;
    private final ExtractorBasedHashStrategy semiUniqueHashStrategy;
    private Extractor[] semiUniqueExtractors;
    private final AsOfAttribute[] asOfAttributes;
    private Object lastRemoved = null;
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

    /**
     * The load fast used when none specified in constructor.
     */
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The nonDatedTable, resized as necessary. Length MUST Always be a power of two.
     */
    private SemiUniqueEntry[] nonDatedTable;
    protected SingleEntry[] table;

    /**
     * The number of key-value mappings contained in this weak semiUniqueHash map.
     */
    private int nonDatedEntryCount;
    private int size;

    /**
     * The semiUniqueNext nonDatedEntryCount value at which to resize (capacity * load factor).
     */
    private int semiUniqueThreshold;
    private int threshold;

    private int expungeCount;
    private static final int MAX_EXPUNGE_COUNT = 10000;
    /**
     * The load factor for the semiUniqueHash nonDatedTable.
     */
    private final float loadFactor;

    /**
     * Reference queue for cleared WeakEntries
     */
    private final ReferenceQueue queue = new ReferenceQueue();

    private final ThreadLocal tempMatchAsOfDatesProcedure = new ThreadLocal();

    private final long timeToLive;
    private final long relationshipTimetoLive;

    /**
     * Constructs a new, empty <tt>WeakPool</tt> with the default initial
     * capacity (16) and the default load factor (0.75).
     */
    public PartialSemiUniqueDatedIndex(String indexName, Extractor[] extractors, AsOfAttribute[] asOfAttributes, long timeToLive, long relationshipTimeToLive)
    {
        this.semiUniqueExtractors = extractors;
        this.asOfAttributes = asOfAttributes;
        this.timeToLive = timeToLive;
        this.relationshipTimetoLive = relationshipTimeToLive;
        this.populateExtractors();
        semiUniqueHashStrategy = ExtractorBasedHashStrategy.create(this.semiUniqueExtractors);
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        semiUniqueThreshold = DEFAULT_INITIAL_CAPACITY;
        threshold = (int)(DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        nonDatedTable = new SemiUniqueEntry[DEFAULT_INITIAL_CAPACITY];
        table = new SingleEntry[DEFAULT_INITIAL_CAPACITY];
    }

    /**
     * Constructs a new, empty <tt>WeakPool</tt> with the default initial
     * capacity (16) and the default load factor (0.75).
     */
    public PartialSemiUniqueDatedIndex(String indexName, Extractor[] extractors,
            AsOfAttribute[] asOfAttributes, Extractor[] pkExtractors, long timeToLive, long relationshipTimeToLive)
    {
        this.semiUniqueExtractors = extractors;
        this.asOfAttributes = asOfAttributes;
        this.extractors = pkExtractors;
        this.timeToLive = timeToLive;
        this.relationshipTimetoLive = relationshipTimeToLive;
        this.asOfAttributeHashStrategy = ExtractorBasedHashStrategy.create(getFromAttributes());
        this.hashStrategy = ExtractorBasedHashStrategy.create(this.extractors);
        semiUniqueHashStrategy = ExtractorBasedHashStrategy.create(this.semiUniqueExtractors);
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        semiUniqueThreshold = DEFAULT_INITIAL_CAPACITY;
        threshold = (int)(DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        nonDatedTable = new SemiUniqueEntry[DEFAULT_INITIAL_CAPACITY];
        table = new SingleEntry[DEFAULT_INITIAL_CAPACITY];
    }

    /*
     This method give duplicate records in the cache. Duplicate here mean data with same primary key and overlapping asOfAttributes(processing date and/or business date)
     In PartailSemiUniqueDatedIndex nonDatedTable[] can have data type MultiEntry SingleEntry(when TTL is 0) and TimedSingleEntry(when TTL >0)
     MultiEntry has records matching primary key but different asOfAttributes(Multiple SingleEntry instances)
     SingleEntry has data for a single record and it can point to another SemiUniqeIndex in case of index collision.

     This check is only interested in data inside MultiEntry because it will have duplicate data(if present).
     MultiEntry can be present in top level array of nonDatedTable or in "SemiUniqueNext" of SingleEntry, in any depth.
     SingleEntry can be present is in top level array of nonDatedTable or in "SemiUniqueNext" of another SingleEntry, in any depth
    */
    @Override
    public List<Object> collectMilestoningOverlaps()
    {
        List<Object> duplicateData = FastList.newList();
        for (SemiUniqueEntry obj : nonDatedTable)
        {
            collectMilestoningOverlaps(duplicateData, obj);
        }
        return duplicateData;
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

    private void collectMilestoningOverlaps(List<Object> duplicateData, SemiUniqueEntry semiUniqueData)
    {
        while(semiUniqueData != null)
        {
            if (semiUniqueData instanceof MultiEntry)
            {
                collectMilestoningOverlaps((MultiEntry) semiUniqueData, duplicateData);
            }
            semiUniqueData = semiUniqueData.getSemiUniqueNext();
        }
    }

    private void collectMilestoningOverlaps(MultiEntry multiEntry, List duplicateList)
    {
        SingleEntry[] list = multiEntry.list;
        int itrSize = list.length - 1;
        for (int i = 0; i < itrSize; i++)
        {
            if (list[i] == null)
            {
                continue;
            }
            Object duplicateRecord = collectMilestoningOverlaps(list, i, this.asOfAttributes, duplicateList);
            if (duplicateRecord != null)
            {
                duplicateList.add(list[i]);
                duplicateList.add(duplicateRecord);
            }
        }
    }

    private Object collectMilestoningOverlaps(SingleEntry[] list, int currentIndex, AsOfAttribute[] asOfAttributes, List<Object> duplicateData)
    {
        int lstSize = list.length;
        SingleEntry singleEntryCompareWith = list[currentIndex];
        if (singleEntryCompareWith == null) return false;
        for (int index = currentIndex + 1; index < lstSize; index++)
        {
            SingleEntry singleEntryCompareTo = list[index];
            if (singleEntryCompareTo == null)
            {
                continue;
            }
            Object obj1 = singleEntryCompareWith.get();
            Object obj2 = singleEntryCompareTo.get();
            if (obj1 != null && obj2 != null && AsOfAttribute.isMilestoningOverlap(obj1, obj2, asOfAttributes))
            {
                return singleEntryCompareTo;
            }
        }
        return null;
    }

    protected MatchAsOfDatesProcedure getMatchAsOfDatesProcedure()
    {
        MatchAsOfDatesProcedure result = (MatchAsOfDatesProcedure) this.tempMatchAsOfDatesProcedure.get();
        if (result == null)
        {
            result = new MatchAsOfDatesProcedure(this.asOfAttributes);
            this.tempMatchAsOfDatesProcedure.set(result);
        }
        return result;
    }

    protected void populateExtractors()
    {
        this.asOfAttributeHashStrategy = ExtractorBasedHashStrategy.create(getFromAttributes());
        this.extractors = createPkExtractors();
        this.hashStrategy = ExtractorBasedHashStrategy.create(this.extractors);
    }

    protected Extractor[] createPkExtractors()
    {
        TimestampAttribute[] fromAttributes = getFromAttributes();
        Extractor[] result = new Extractor[this.semiUniqueExtractors.length + fromAttributes.length];
        System.arraycopy(this.semiUniqueExtractors, 0, result, 0, this.semiUniqueExtractors.length);
        System.arraycopy(fromAttributes, 0, result, this.semiUniqueExtractors.length, fromAttributes.length);
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
        return this.extractors;
    }

    public Extractor[] getNonDatedExtractors()
    {
        return this.semiUniqueExtractors;
    }
    // internal utilities

    /*
     * Return index for semiUniqueHash code h.
     */
    static int indexFor(int h, int length) {
        return h & (length-1);
    }

    /**
     * Expunge stale entries from the nonDatedTable.
     */
    private synchronized boolean expungeStaleEntries()
    {
        if (this.size == 0) return false;
        Object r;
        boolean result = false;
        while ( (r = queue.poll()) != null)
        {
            result = true;
            SingleEntry e = (SingleEntry)r;
            this.size -= e.cleanupPkTable(this.table);
            this.nonDatedEntryCount -= e.cleanupSemiUniqueTable(this.nonDatedTable);
        }
        return result;
    }

    private SingleEntry[] getTable()
    {
        expungeCount++;
        if (expungeCount >= MAX_EXPUNGE_COUNT)
        {
            expungeStaleEntries();
            expungeCount = 0;
        }
        return table;
    }

    /*
     * Return the nonDatedTable after first expunging stale entries
     */
    private SemiUniqueEntry[] getNonDatedTable() {
        expungeCount++;
        if (expungeCount >= MAX_EXPUNGE_COUNT)
        {
            expungeStaleEntries();
            expungeCount = 0;
        }
        return nonDatedTable;
    }

    public synchronized int size()
    {
        if (size == 0)
            return 0;
        expungeStaleEntries();
        return size;
    }

    public synchronized boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = 0; i < table.length && !done; i++)
        {
            SingleEntry e = table[i];
            while (e != null && !done)
            {
                Object candidate = e.get();
                if (candidate != null && (timeToLive == 0 || !((TimedSingleEntry)e).isExpired(timeToLive))) done = procedure.execute(candidate);
                e = e.pkNext;
            }
        }
        return done;
    }

    public synchronized Object getFromData(Object data, int nonDatedHashCode)
    {
        int hash = this.asOfAttributeHashStrategy.computeCombinedHashCode(data, nonDatedHashCode);
        int i = indexFor(hash, table.length);
        SingleEntry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            // we explicitly ignore the timetoLive so we can guarantee uniqueness
            if (candidate != null && e.pkHash == hash &&
                    this.hashStrategy.equals(data, candidate))
            {
                e.updateCacheTime();
                return candidate;
            }
            e = e.pkNext;
        }
    }

    public synchronized Object get(Object valueHolder, List extractors)
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, table.length);
        SingleEntry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.pkHash == hash && (timeToLive == 0 || !((TimedSingleEntry)e).isExpired(timeToLive)) &&
                    this.hashStrategy.equals(candidate, valueHolder, extractors))
                return candidate;
            e = e.pkNext;
        }
    }

    public synchronized Object get(Object valueHolder, Extractor[] extractors)
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, table.length);
        SingleEntry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.pkHash == hash && (timeToLive == 0 || !((TimedSingleEntry)e).isExpired(timeToLive)) &&
                    this.hashStrategy.equals(candidate, valueHolder, extractors))
                return candidate;
            e = e.pkNext;
        }
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        Object candidate = this.get(keyHolder, extractors);
        return candidate != null && (filter == null || filter.matches(candidate, keyHolder));
    }

    public synchronized boolean containsInSemiUnique(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        Object candidate = this.getFromSemiUnique(keyHolder, extractors);
        return candidate != null && (filter == null || filter.matches(candidate, keyHolder));
    }

    public synchronized Object put(Object key, int nonDatedHashCode)
    {
        SingleEntry newEntry = this.putInPkTable(key, nonDatedHashCode);
        Object lastRemoved = this.lastRemoved;
        this.lastRemoved = null;
        putSemiUnique(newEntry, nonDatedHashCode);
        return lastRemoved;
    }

    public Object putSemiUnique(Object key)
    {
        return this.put(key, this.semiUniqueHashStrategy.computeHashCode(key));
    }

    private synchronized SingleEntry putInPkTable(Object key, int nonDatedHashCode)
    {
        int hash = this.asOfAttributeHashStrategy.computeCombinedHashCode(key, nonDatedHashCode);
        SingleEntry[] tab = getTable();
        int i = indexFor(hash, tab.length);

        SingleEntry prev = null;
        for (SingleEntry e = tab[i]; e != null; e = e.pkNext)
        {
            Object candidate = e.get();
            if (candidate != null && e.pkHash == hash &&
                    this.hashStrategy.equals(candidate, key))
            {
                SingleEntry newEntry = createEntry(key, queue, nonDatedHashCode, hash, e.pkNext);
                if (prev != null)
                {
                    prev.pkNext = newEntry;
                }
                else
                {
                    tab[i] = newEntry;
                }
                this.lastRemoved = candidate;
                removeSemiUniqueEntry(e);
                return newEntry;
            }
            prev = e;
        }

        SingleEntry singleEntry = createEntry(key, queue, nonDatedHashCode, hash, tab[i]);
        tab[i] = singleEntry;
        size++;
        if (size >= threshold)
            resize(table.length * 2);
        return singleEntry;
    }

    private SingleEntry createEntry(Object referent, ReferenceQueue q, int semiUniqueHash, int pkHash, SingleEntry pkNext)
    {
        if (this.timeToLive == 0)
        {
            return new SingleEntry(referent, q, semiUniqueHash, pkHash, pkNext);
        }
        return new TimedSingleEntry(referent, q, semiUniqueHash, pkHash, pkNext);
    }

    private void resize(int newCapacity)
    {
        SingleEntry[] oldTable = getTable();
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY)
        {
            threshold = Integer.MAX_VALUE;
            return;
        }

        SingleEntry[] newTable = new SingleEntry[newCapacity];
        transfer(oldTable, newTable);
        table = newTable;

        /*
         * If ignoring null elements and processing ref queue caused massive
         * shrinkage, then restore old table.  This should be rare, but avoids
         * unbounded expansion of garbage-filled tables.
         */
        if (size >= threshold / 2)
        {
            threshold = (int) (newCapacity * loadFactor);
        }
        else
        {
            expungeStaleEntries();
            transfer(newTable, oldTable);
            table = oldTable;
        }
    }

    /*
     * Transfer all entries from src to dest tables
     */
    private void transfer(SingleEntry[] src, SingleEntry[] dest)
    {
        for (int j = 0; j < src.length; ++j)
        {
            SingleEntry e = src[j];
            src[j] = null;
            while (e != null)
            {
                SingleEntry next = e.pkNext;
                Object key = e.get();
                if (key == null)
                {
                    e.pkNext = null;  // Help GC
                    size--;
                }
                else
                {
                    int i = indexFor(e.pkHash, dest.length);
                    e.pkNext = dest[i];
                    dest[i] = e;
                }
                e = next;
            }
        }
    }

    public synchronized Object remove(Object key)
    {
        SingleEntry removed = this.removeFromPkTable(key);
        if (removed != null)
        {
            this.removeSemiUniqueEntry(removed);
            return removed.get();
        }
        return null;
    }

    public List removeAll(Filter filter)
    {
        FastList result = new FastList();
        SingleEntry[] tab = getTable();
        for (int i = 0; i < tab.length; i++)
        {
            SingleEntry prev = tab[i];
            SingleEntry e = prev;
            while (e != null)
            {
                SingleEntry next = e.pkNext;
                Object candidate = e.get();
                if (candidate == null || filter.matches(candidate))
                {
                    if (candidate != null)
                    {
                        result.add(candidate);
                    }
                    size--;
                    if (prev == e)
                    {
                        tab[i] = next;
                        prev = next;
                    }
                    else
                        prev.pkNext = next;
                    removeSemiUniqueEntry(e);
                }
                else
                {
                    prev = e;
                }
                e = next;
            }
        }
        return result;
    }

    public boolean evictCollectedReferences()
    {
        return this.expungeStaleEntries();
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return true;
    }

    public CommonExtractorBasedHashingStrategy getNonDatedPkHashStrategy()
    {
        return this.semiUniqueHashStrategy;
    }

    public void forAllInParallel(final ParallelProcedure procedure)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), this.table.length, 1);
        final ArrayBasedQueue queue = new ArrayBasedQueue(this.table.length, threadChunkSize.getChunkSize());
        int threads = threadChunkSize.getThreads();
        procedure.setThreads(threads, this.size/threads);
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
                            SingleEntry e = table[i];
                            while (e != null)
                            {
                                Object candidate = e.get();
                                if (candidate != null && (timeToLive == 0 || !((TimedSingleEntry)e).isExpired(timeToLive))) procedure.execute(candidate, thread);
                                e = e.pkNext;
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
        int datedSize = this.size + extraCapacity;
        if (datedSize > threshold)
        {
            int newThreshold = threshold;
            int capacity = this.table.length;
            while(datedSize > newThreshold)
            {
                capacity = capacity << 1;
                newThreshold = (int)(capacity * loadFactor);
            }
            this.resize(capacity);
        }
        int nonDatedSize = this.nonDatedEntryCount + extraCapacity /8;
        if (nonDatedSize > semiUniqueThreshold)
        {
            int newThreshold = semiUniqueThreshold;
            int capacity = this.nonDatedTable.length;
            while (nonDatedSize > newThreshold)
            {
                capacity = capacity << 1;
                newThreshold = (int)(capacity * loadFactor);
            }
            this.resizeSemiUnique(capacity);
        }
    }

    public Object removeUsingUnderlying(Object businessObject)
    {
        return this.remove(businessObject);
    }

    private synchronized SingleEntry removeFromPkTable(Object underlying)
    {
        int hash = this.hashStrategy.computeHashCode(underlying);
        SingleEntry[] tab = getTable();
        int i = indexFor(hash, tab.length);
        SingleEntry prev = tab[i];
        SingleEntry e = prev;

        while (e != null)
        {
            SingleEntry next = e.pkNext;
            Object candidate = e.get();
            if (candidate != null && e.pkHash == hash &&
                    this.hashStrategy.equals(candidate, underlying))
            {
                size--;
                if (prev == e)
                    tab[i] = next;
                else
                    prev.pkNext = next;
                return e;
            }
            prev = e;
            e = next;
        }

        return null;
    }

    public synchronized void clear()
    {
        // clear out ref queue. We don't need to expunge entries
        // since table is getting cleared.
        emptySoftQueue();

        clearPkTable();
        clearSemiUniqueTable();

        // Allocation of array may have caused GC, which may have caused
        // additional entries to go stale.  Removing these entries from the
        // reference queue will make them eligible for reclamation.
        emptySoftQueue();
    }

    private void clearPkTable()
    {
        SemiUniqueEntry tab[] = table;
        for (int i = 0; i < tab.length; ++i)
            tab[i] = null;
        size = 0;
    }

    protected void emptySoftQueue()
    {
        while (queue.poll() != null)
            ;
    }

    public synchronized Object getFromSemiUnique(Object valueHolder, List extractors)
    {
        int hash = this.semiUniqueHashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, nonDatedTable.length);
        SemiUniqueEntry e = nonDatedTable[i];
        while (true) {
            if (e == null)
                return e;
            if (e.matches(hash, this.semiUniqueHashStrategy, valueHolder, extractors))
            {
                MatchAsOfExtractorProcedure matchAsOfDatesProcedure =
                        new MatchAsOfExtractorProcedure(this.semiUniqueExtractors.length, valueHolder, extractors, this.asOfAttributes);
                e.forAllWithExpiration(matchAsOfDatesProcedure, timeToLive);
                return matchAsOfDatesProcedure.getResult();
            }
            e = e.getSemiUniqueNext();
        }
    }

    public synchronized Object getFromSemiUnique(Object valueHolder, Extractor[] extractors)
    {
        int hash = this.semiUniqueHashStrategy.computeHashCode(valueHolder, extractors);
        int i = indexFor(hash, nonDatedTable.length);
        SemiUniqueEntry e = nonDatedTable[i];
        while (true) {
            if (e == null)
                return e;
            if (e.matches(hash, this.semiUniqueHashStrategy, valueHolder, extractors))
            {
                MatchAsOfExtractorProcedure matchAsOfDatesProcedure =
                        new MatchAsOfExtractorProcedure(this.semiUniqueExtractors.length, valueHolder, extractors, this.asOfAttributes);
                e.forAllWithExpiration(matchAsOfDatesProcedure, timeToLive);
                return matchAsOfDatesProcedure.getResult();
            }
            e = e.getSemiUniqueNext();
        }
    }

    public PrimaryKeyIndex copy()
    {
        throw new RuntimeException("not implemented");
    }

    public synchronized Object getSemiUniqueFromData(Object data, Timestamp[] asOfDates)
    {
        int hash = this.semiUniqueHashStrategy.computeHashCode(data);
        int i = indexFor(hash, nonDatedTable.length);
        SemiUniqueEntry e = nonDatedTable[i];
        while (true) {
            if (e == null)
                return e;
            if (e.matches(hash, this.semiUniqueHashStrategy, data))
            {
                MatchAsOfDatesProcedure matchAsOfDatesProcedure = this.getMatchAsOfDatesProcedure();
                matchAsOfDatesProcedure.init(asOfDates);
                // we don't do a expiration check, as this is used during refresh
                e.forAll(matchAsOfDatesProcedure);
                return matchAsOfDatesProcedure.getResult();
            }
            e = e.getSemiUniqueNext();
        }
    }

    public synchronized boolean addSemiUniqueToContainer(Object data, TemporalContainer container)
    {
        int hash = this.semiUniqueHashStrategy.computeHashCode(data);
        int i = indexFor(hash, nonDatedTable.length);
        SemiUniqueEntry e = nonDatedTable[i];
        while (true) {
            if (e == null)
                return false;
            // we don't do an expiration check, as this is used during getObjectFromData or optimistic locking
            if (e.matches(hash, this.semiUniqueHashStrategy, data))
            {
                return e.addToContainer(container);
            }
            e = e.getSemiUniqueNext();
        }
    }

    public synchronized List getFromDataForAllDatesAsList(Object data)
    {
        int hash = this.semiUniqueHashStrategy.computeHashCode(data);
        int i = indexFor(hash, nonDatedTable.length);
        SemiUniqueEntry e = nonDatedTable[i];
        while (true) {
            if (e == null)
                return ListFactory.EMPTY_LIST;
            if (e.matches(hash, this.semiUniqueHashStrategy, data))
            {
                return convertEntryToList(e);
            }
            e = e.getSemiUniqueNext();
        }
    }

    private List convertEntryToList(SemiUniqueEntry e)
    {
        final List arrayList;
        if (e.getSize() == 1)
        {
            MithraDataObject mdo = (MithraDataObject) e.getTheOne();
            if (mdo != null)
            {
                arrayList = ListFactory.create(mdo);
            }
            else
            {
                arrayList = ListFactory.EMPTY_LIST;
            }
        }
        else
        {
            arrayList = new FastList(e.getSize());
            DoProcedure addProcedure = new DoProcedure()
            {
                public void execute(Object object)
                {
                    arrayList.add(object);
                }
            };

            e.forAll(addProcedure);
        }
        return arrayList;
    }

    public Object getSemiUniqueAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int i = indexFor(nonDatedHash, nonDatedTable.length);
        SemiUniqueEntry e = nonDatedTable[i];
        while (true)
        {
            if (e == null)
                return e;
            Object result = e.getMatchingForRelationship(srcObject, srcData, relationshipHashStrategy, nonDatedHash, this, asOfDate0, asOfDate1);
            if (result != null) return result;
            e = e.getSemiUniqueNext();
        }
    }

    public synchronized Object getSemiUniqueAsOneWithDates(Object valueHolder, Extractor[] extractors, Timestamp[] asOfDates, int nonDatedHash)
    {
        int i = indexFor(nonDatedHash, nonDatedTable.length);
        SemiUniqueEntry e = nonDatedTable[i];
        while (true)
        {
            if (e == null)
                return e;
            Object result = e.getMatchingForRelationship(nonDatedHash, valueHolder, extractors, asOfDates, this);
            if (result != null) return result;
            e = e.getSemiUniqueNext();
        }
    }

    public int getSemiUniqueSize()
    {
        return this.nonDatedEntryCount;
    }

    public synchronized List removeOldEntryForRange(Object data)
    {
        int hash = this.semiUniqueHashStrategy.computeHashCode(data);
        SemiUniqueEntry[] tab = getNonDatedTable();
        int i = indexFor(hash, tab.length);
        SemiUniqueEntry prev = tab[i];
        SemiUniqueEntry e = prev;

        while (e != null)
        {
            SemiUniqueEntry next = e.getSemiUniqueNext();
            if (e.matches(hash, this.semiUniqueHashStrategy, data))
            {
                boolean removeEntry = false;
                List oldValues = e.removeForRange(this.asOfAttributes, data, table);
                if (oldValues != null)
                {
                    size -= oldValues.size();
                    removeEntry = e.isEmptyAfterRemoval();
                }
                if (removeEntry)
                {
                    removeFromNonDatedTable(prev, e, next, tab, i);
                }
                return oldValues;
            }
            prev = e;
            e = next;
        }

        return null;
    }

    private void removeFromNonDatedTable(SemiUniqueEntry prev, SemiUniqueEntry e, SemiUniqueEntry next, SemiUniqueEntry[] tab, int i)
    {
        nonDatedEntryCount--;
        if (prev == e)
            tab[i] = next;
        else
            prev.setSemiUniqueNext(next);
    }

    public boolean removeAllIgnoringDate(Object data, DoProcedure procedure)
    {
        SemiUniqueEntry entryForAll = null;
        synchronized (this)
        {
            int hash = this.semiUniqueHashStrategy.computeHashCode(data);
            SemiUniqueEntry[] tab = getNonDatedTable();
            int i = indexFor(hash, tab.length);
            SemiUniqueEntry prev = tab[i];
            SemiUniqueEntry e = prev;

            while (e != null) {
                SemiUniqueEntry next = e.getSemiUniqueNext();
                if (e.matches(hash, this.semiUniqueHashStrategy, data))
                {
                    removeFromNonDatedTable(prev, e, next, tab, i);
                    size -= e.removeAllFromPkTable(table);
                    entryForAll = e;
                    break;
                }
                prev = e;
                e = next;
            }
        }
        if (entryForAll != null)
        {
            entryForAll.forAll(procedure);
            return true;
        }
        return false;
    }

    public synchronized Object removeOldEntry(Object data, Timestamp[] asOfDates)
    {
        int hash = this.semiUniqueHashStrategy.computeHashCode(data);
        SemiUniqueEntry[] tab = getNonDatedTable();
        int i = indexFor(hash, tab.length);
        SemiUniqueEntry prev = tab[i];
        SemiUniqueEntry e = prev;

        while (e != null) {
            SemiUniqueEntry next = e.getSemiUniqueNext();
            if (e.matches(hash, this.semiUniqueHashStrategy, data))
            {
                Object oldValue = e.removeMatching(this.asOfAttributes, asOfDates, table);
                if (oldValue != null)
                {
                    size--;
                    if (e.isEmptyAfterRemoval())
                    {
                        removeFromNonDatedTable(prev, e, next, tab, i);
                    }
                }
                return oldValue;
            }
            prev = e;
            e = next;
        }

        return null;
    }

    private void putSemiUnique(SingleEntry entry, int hash)
    {
        SemiUniqueEntry[] tab = getNonDatedTable();
        int i = indexFor(hash, tab.length);

        SemiUniqueEntry prev = null;
        for (SemiUniqueEntry e = tab[i]; e != null; e = e.getSemiUniqueNext())
        {
            SemiUniqueEntry newEntry = e.addIfEqual(entry, hash, this.semiUniqueHashStrategy);
            if (newEntry != null)
            {
                if (prev != null)
                {
                    prev.setSemiUniqueNext(newEntry);
                }
                else
                {
                    tab[i] = newEntry;
                }
                return;
            }
            prev = e;
        }
        entry.setSemiUniqueNext(tab[i]);
        tab[i] = entry;
        if (++nonDatedEntryCount >= semiUniqueThreshold)
            resizeSemiUnique(tab.length * 2);
    }

    /**
     * Rehashes the contents of this map into a new array with a
     * larger capacity.  This method is called automatically when the
     * number of keys in this map reaches its semiUniqueThreshold.
     *
     * If current capacity is MAXIMUM_CAPACITY, this method does not
     * resize the map, but but sets semiUniqueThreshold to Integer.MAX_VALUE.
     * This has the effect of preventing future calls.
     *
     * @param newCapacity the new capacity, MUST be a power of two;
     *        must be greater than current capacity unless current
     *        capacity is MAXIMUM_CAPACITY (in which case value
     *        is irrelevant).
     */
    private void resizeSemiUnique(int newCapacity)
    {
        SemiUniqueEntry[] oldTable = getNonDatedTable();
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY) {
            semiUniqueThreshold = Integer.MAX_VALUE;
            return;
        }

        SemiUniqueEntry[] newTable = new SemiUniqueEntry[newCapacity];
        transferSemiUnique(oldTable, newTable);
        nonDatedTable = newTable;

        /*
         * If ignoring null elements and processing ref queue caused massive
         * shrinkage, then restore old nonDatedTable.  This should be rare, but avoids
         * unbounded expansion of garbage-filled tables.
         */
        if (nonDatedEntryCount >= semiUniqueThreshold / 2) {
            semiUniqueThreshold = (int)(newCapacity * loadFactor);
        } else {
            expungeStaleEntries();
            transferSemiUnique(newTable, oldTable);
            nonDatedTable = oldTable;
        }
    }

    /* Transfer all entries from src to dest tables */
    private void transferSemiUnique(SemiUniqueEntry[] src, SemiUniqueEntry[] dest) {
        for (int j = 0; j < src.length; ++j)
        {
            SemiUniqueEntry e = src[j];
            src[j] = null;
            while (e != null)
            {
                SemiUniqueEntry next = e.getSemiUniqueNext();
                int i = indexFor(e.getSemiUniqueHash(), dest.length);
                e.setSemiUniqueNext(dest[i]);
                dest[i] = e;
                e = next;
            }
        }
    }

    /*
     * Removes and returns the entry associated with the specified key
     * in the HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     */
    protected synchronized void removeSemiUniqueEntry(SingleEntry key)
    {
        int hash = key.getSemiUniqueHash();
        SemiUniqueEntry[] tab = getNonDatedTable();
        int i = indexFor(hash, tab.length);
        SemiUniqueEntry prev = tab[i];
        SemiUniqueEntry e = prev;

        while (e != null)
        {
            SemiUniqueEntry next = e.getSemiUniqueNext();
            Object oldValue = e.removeEntryUsingIdentity(hash, key);
            if (oldValue != null)
            {
                if (e.isEmptyAfterRemoval())
                {
                    removeFromNonDatedTable(prev, e, next, tab, i);
                }
                return;
            }
            prev = e;
            e = next;
        }
    }

    /**
     * Removes all mappings from this map.
     */
    public synchronized void clearSemiUniqueTable()
    {
        SemiUniqueEntry tab[] = nonDatedTable;
        for (int i = 0; i < tab.length; ++i)
            tab[i] = null;
        nonDatedEntryCount = 0;
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

    private interface SemiUniqueEntry
    {
        public boolean forAll(DoUntilProcedure procedure);

        public void forAll(DoProcedure procedure);

        public int getSize();

        public Object getTheOne();

        public SemiUniqueEntry addIfEqual(SingleEntry entry, int hash, HashStrategy hashStrategy);

        public SemiUniqueEntry getSemiUniqueNext();

        public void setSemiUniqueNext(SemiUniqueEntry next);

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object valueHolder, List extractors);

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object valueHolder, Extractor[] extractors);

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object other);

        public Object getMatchingForRelationship(int hash, Object other,
                Extractor[] extractors, Timestamp[] asOfDates, PartialSemiUniqueDatedIndex index);

        public Object getMatchingForRelationship(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy,
                int nonDatedHash, PartialSemiUniqueDatedIndex index, Timestamp asOfDate0, Timestamp asOfDate1);

        public int getSemiUniqueHash();

        public List removeForRange(AsOfAttribute[] asOfAttributes, Object data, SingleEntry[] pkTable);

        public Object removeMatching(AsOfAttribute[] asOfAttributes, Timestamp[] asOfDates, SingleEntry[] pkTable);

        public boolean isEmptyAfterRemoval();

        public Object removeEntryUsingIdentity(int hash, SingleEntry key);

        public int cleanUpEntry(SingleEntry entry, SemiUniqueEntry prev, SemiUniqueEntry[] nonDatedTable, int index);

        public int removeAllFromPkTable(SingleEntry[] table);

        public void forAllWithExpiration(DoUntilProcedure procedure, long timetoLive);

        public boolean addToContainer(TemporalContainer container);
    }

    private static class SingleEntry extends WeakReference implements SemiUniqueEntry
    {
        private int pkHash;
        private SingleEntry pkNext;
        private int semiUniqueHash;
        private SemiUniqueEntry semiUniqueNext;

        public SingleEntry(Object referent, ReferenceQueue q, int semiUniqueHash, int pkHash, SingleEntry pkNext)
        {
            super(referent, q);
            this.semiUniqueHash = semiUniqueHash;
            this.pkHash = pkHash;
            this.pkNext = pkNext;
        }

        public void setSemiUniqueNext(SemiUniqueEntry semiUniqueNext)
        {
            this.semiUniqueNext = semiUniqueNext;
        }

        public int getSemiUniqueHash()
        {
            return semiUniqueHash;
        }

        public SemiUniqueEntry addIfEqual(SingleEntry entry, int hash, HashStrategy hashStrategy)
        {
            if (this.semiUniqueHash == hash)
            {
                Object key = entry.get();
                Object mine = this.get();
                if (mine != null)
                {
                    if (hashStrategy.equals(mine, key))
                    {
                        return new MultiEntry(this, entry);
                    }
                }
                else
                {
                    entry.setSemiUniqueNext(this.semiUniqueNext);
                    return entry;
                }
            }
            return null;
        }

        public SemiUniqueEntry getSemiUniqueNext()
        {
            return semiUniqueNext;
        }

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object valueHolder, List extractors)
        {
            Object mine = this.get();
            return mine != null && this.semiUniqueHash == hash && strategy.equals(mine, valueHolder, extractors);
        }

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object valueHolder, Extractor[] extractors)
        {
            Object mine = this.get();
            return mine != null && this.semiUniqueHash == hash && strategy.equals(mine, valueHolder, extractors);
        }

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object other)
        {
            Object mine = this.get();
            return mine != null && this.semiUniqueHash == hash && strategy.equals(mine, other);
        }

        public Object getMatchingForRelationship(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, PartialSemiUniqueDatedIndex index, Timestamp asOfDate0, Timestamp asOfDate1)
        {
            Object mine = this.get();
            if (mine != null && !(index.relationshipTimetoLive > 0 && isExpired(index.relationshipTimetoLive)) &&
                    this.semiUniqueHash == nonDatedHash && relationshipHashStrategy.equalsForRelationship(srcObject, srcData, mine, asOfDate0, asOfDate1))
            {
                return mine;
            }
            return null;
        }

        public Object getMatchingForRelationship(int hash, Object other, Extractor[] extractors, Timestamp[] asOfDates, PartialSemiUniqueDatedIndex index)
        {
            Object mine = this.get();
            if (mine != null && !(index.relationshipTimetoLive > 0 && isExpired(index.relationshipTimetoLive)) &&
                    this.semiUniqueHash == hash && index.semiUniqueHashStrategy.equals(mine, other, extractors))
            {
                if (index.asOfAttributes[0].dataMatches(mine, asOfDates[0]) &&
                        (index.asOfAttributes.length == 1 || index.asOfAttributes[1].dataMatches(mine, asOfDates[1])))
                {
                    return mine;
                }
            }
            return null;
        }

        public boolean forAll(DoUntilProcedure procedure)
        {
            boolean done = false;
            Object mine = this.get();
            if (mine != null)
            {
                done = procedure.execute(mine);
            }
            return done;
        }

        public void forAll(DoProcedure procedure)
        {
            Object mine = this.get();
            if (mine != null)
            {
                procedure.execute(mine);
            }
        }

        public int getSize()
        {
            return 1;
        }

        public Object getTheOne()
        {
            return this.get();
        }

        public int cleanUpEntry(SingleEntry entry, SemiUniqueEntry prev, SemiUniqueEntry[] nonDatedTable, int index)
        {
            if (entry == this)
            {
                if (prev == this)
                    nonDatedTable[index] = this.semiUniqueNext;
                else
                    prev.setSemiUniqueNext(this.semiUniqueNext);
                this.semiUniqueNext = null;  // Help GC
                return 1;
            }
            return -1;
        }

        public int removeAllFromPkTable(SingleEntry[] table)
        {
            return cleanupPkTable(table);
        }

        public int cleanupSemiUniqueTable(SemiUniqueEntry[] nonDatedTable)
        {
            int i = indexFor(this.semiUniqueHash, nonDatedTable.length);

            SemiUniqueEntry prev = nonDatedTable[i];
            SemiUniqueEntry p = prev;
            while (p != null)
            {
                int removed = p.cleanUpEntry(this, prev, nonDatedTable, i);
                if (removed >= 0)
                {
                    return removed;
                }
                SemiUniqueEntry next = p.getSemiUniqueNext();
                prev = p;
                p = next;
            }
            return 0;
        }

        public int cleanupPkTable(SingleEntry[] table)
        {
            int i = indexFor(this.pkHash, table.length);

            SingleEntry prev = table[i];
            SingleEntry p = prev;
            while(p != null)
            {
                if (p == this)
                {
                    if (prev == this)
                        table[i] = this.pkNext;
                    else
                        prev.pkNext = this.pkNext;
                    this.pkNext = null;
                    return 1;
                }
                SingleEntry next = p.pkNext;
                prev = p;
                p = next;
            }
            return 0;
        }

        public boolean isEmptyAfterRemoval()
        {
            return true;
        }

        public Object removeEntryUsingIdentity(int hash, SingleEntry key)
        {
            if (this == key)
            {
                return this;
            }
            return null;
        }

        public List removeForRange(AsOfAttribute[] asOfAttributes, Object data, SingleEntry[] pkTable)
        {
            Object o = this.get();
            if (o != null)
            {
                boolean fullMatch = true;
                for(int i=0;fullMatch && i<asOfAttributes.length;i++)
                {
                    fullMatch = asOfAttributes[i].hasRangeOverlap((MithraDataObject) o,
                            asOfAttributes[i].getFromAttribute().timestampValueOf(data).getTime(),
                            asOfAttributes[i].getToAttribute().timestampValueOf(data).getTime());
                }
                if (fullMatch)
                {
                    this.cleanupPkTable(pkTable);
                    return ListFactory.create(o);
                }
            }
            return null;
        }

        public Object removeMatching(AsOfAttribute[] asOfAttributes, Timestamp[] asOfDates, SingleEntry[] pkTable)
        {
            Object o = this.get();
            if (o != null)
            {
                for(int i=0;i<asOfAttributes.length;i++)
                {
                    if (!asOfAttributes[i].dataMatches(o, asOfDates[i])) return null;
                }
                this.cleanupPkTable(pkTable);
                return o;
            }
            return null;
        }

        public void forAllWithExpiration(DoUntilProcedure procedure, long timetoLive)
        {
            if (timetoLive > 0 && isExpired(timetoLive)) return;
            forAll(procedure);
        }

        public boolean addToContainer(TemporalContainer container)
        {
            Object mine = this.get();
            if (mine != null)
            {
                container.addCommittedData((MithraDataObject) mine);
                return true;
            }
            return false;
        }

        public boolean isExpired(long timeToLive)
        {
            return false;
        }

        public void updateCacheTime()
        {
            //nothing to do
        }
    }

    private static class TimedSingleEntry extends SingleEntry
    {
        private volatile long cachedTime;

        private TimedSingleEntry(Object referent, ReferenceQueue q, int semiUniqueHash, int pkHash, SingleEntry pkNext)
        {
            super(referent, q, semiUniqueHash, pkHash, pkNext);
            this.cachedTime = CacheClock.getTime();
        }

        @Override
        public boolean isExpired(long timeToLive)
        {
            return this.cachedTime + timeToLive < CacheClock.getTime();
        }

        @Override
        public void updateCacheTime()
        {
            this.cachedTime = CacheClock.getTime();
        }
    }

    private static class MultiEntry implements SemiUniqueEntry
    {
        private int semiUniqueHash;
        private SingleEntry[] list;
        private int size;
        private SemiUniqueEntry semiUniqueNext;

        public MultiEntry(SingleEntry first, SingleEntry second)
        {
            this.semiUniqueHash = first.getSemiUniqueHash();
            this.semiUniqueNext = first.getSemiUniqueNext();
            first.setSemiUniqueNext(null);
            this.size = 2;
            list = new SingleEntry[4];
            list[0] = first;
            list[1] = second;
        }

        public SemiUniqueEntry getSemiUniqueNext()
        {
            return semiUniqueNext;
        }

        public int getSemiUniqueHash()
        {
            return semiUniqueHash;
        }

        public void setSemiUniqueNext(SemiUniqueEntry semiUniqueNext)
        {
            this.semiUniqueNext = semiUniqueNext;
        }

        public SemiUniqueEntry addIfEqual(SingleEntry entry, int hash, HashStrategy hashStrategy)
        {
            if (this.semiUniqueHash == hash)
            {
                Object key = entry.get();
                for(int i=this.size - 1;i >= 0;i--)
                {
                    Object other = list[i].get();
                    if (other != null)
                    {
                        if (hashStrategy.equals(key, other)) break;
                        else return null;
                    }
                    else
                    {
                        this.removeWeakReference(i);
                    }
                }
                // we either have equality, or we're now empty
                if (size == 0)
                {
                    entry.setSemiUniqueNext(this.semiUniqueNext);
                    return entry;
                }
                if (size == list.length)
                {
                    int newLength = list.length + (list.length >> 2);
                    SingleEntry[] newList = new SingleEntry[newLength];
                    System.arraycopy(list, 0, newList, 0, list.length);
                    list = newList;
                }
                list[size] = entry;
                size++;
                return this;
            }
            return null;
        }

        private void removeWeakReference(int index)
        {
            list[index] = list[size - 1];
            list[size - 1] = null;
            size--;
        }

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object valueHolder, List extractors)
        {
            if (this.semiUniqueHash == hash)
            {
                for(int i=this.size - 1;i >= 0;i--)
                {
                    Object other = list[i].get();
                    if (other != null)
                    {
                        if (strategy.equals(other, valueHolder, extractors)) break;
                        else return false;
                    }
                    else
                    {
                        this.removeWeakReference(i);
                    }
                }
                // we either have equality, or we're now empty
                return (size != 0);
            }
            return false;
        }

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object valueHolder, Extractor[] extractors)
        {
            if (this.semiUniqueHash == hash)
            {
                for(int i=this.size - 1;i >= 0;i--)
                {
                    Object other = list[i].get();
                    if (other != null)
                    {
                        if (strategy.equals(other, valueHolder, extractors)) break;
                        else return false;
                    }
                    else
                    {
                        this.removeWeakReference(i);
                    }
                }
                // we either have equality, or we're now empty
                return (size != 0);
            }
            return false;
        }

        public boolean matches(int hash, ExtractorBasedHashStrategy strategy, Object other)
        {
            if (this.semiUniqueHash == hash)
            {
                for(int i=this.size - 1;i >= 0;i--)
                {
                    Object mine = list[i].get();
                    if (mine != null)
                    {
                        if (strategy.equals(mine, other)) break;
                        else return false;
                    }
                    else
                    {
                        this.removeWeakReference(i);
                    }
                }
                // we either have equality, or we're now empty
                return (size != 0);
            }
            return false;
        }

        public Object getMatchingForRelationship(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, PartialSemiUniqueDatedIndex index, Timestamp asOfDate0, Timestamp asOfDate1)
        {
            if (this.semiUniqueHash == nonDatedHash)
            {
                for(int i=this.size - 1;i >= 0;i--)
                {
                    if (index.relationshipTimetoLive > 0 && list[i].isExpired(index.relationshipTimetoLive)) continue;
                    Object mine = list[i].get();
                    if (mine != null && relationshipHashStrategy.equalsForRelationship(srcObject, srcData, mine, asOfDate0, asOfDate1))
                    {
                        return mine;
                    }
                }
            }
            return null;
        }

        public Object getMatchingForRelationship(int hash, Object other, Extractor[] extractors, Timestamp[] asOfDates, PartialSemiUniqueDatedIndex index)
        {
            if (this.semiUniqueHash == hash)
            {
                final AsOfAttribute[] asOfAttributes = index.asOfAttributes;
                for(int i=this.size - 1;i >= 0;i--)
                {
                    if (index.relationshipTimetoLive > 0 && list[i].isExpired(index.relationshipTimetoLive)) continue;
                    Object mine = list[i].get();
                    if (mine != null && index.semiUniqueHashStrategy.equals(mine, other, extractors) && asOfAttributes[0].dataMatches(mine, asOfDates[0]) &&
                                (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(mine, asOfDates[1])))
                    {
                        return mine;
                    }
                }
            }
            return null;
        }

        public boolean forAll(DoUntilProcedure procedure)
        {
            boolean done = false;
            for (int i = this.size - 1; !done && i >= 0; i--)
            {
                Object key = this.list[i].get();
                if (key != null)
                {
                    done = procedure.execute(key);
                }
            }
            return done;
        }

        public void forAll(DoProcedure procedure)
        {
            for (int i = this.size - 1; i >= 0; i--)
            {
                Object key = this.list[i].get();
                if (key != null)
                {
                    procedure.execute(key);
                }
            }
        }

        public void forAllWithExpiration(DoUntilProcedure procedure, long timetoLive)
        {
            boolean done = false;
            for (int i = this.size - 1; !done && i >= 0; i--)
            {
                SingleEntry singleEntry = this.list[i];
                if (timetoLive > 0 && singleEntry.isExpired(timetoLive)) continue;
                Object key = singleEntry.get();
                if (key != null)
                {
                    done = procedure.execute(key);
                }
            }
        }

        public boolean addToContainer(TemporalContainer container)
        {
            boolean added = false;
            for (int i = this.size - 1; i >= 0; i--)
            {
                Object key = this.list[i].get();
                if (key != null)
                {
                    container.addCommittedData((MithraDataObject) key);
                    added = true;
                }
            }
            return added;
        }

        public int getSize()
        {
            return this.size;
        }

        public Object getTheOne()
        {
            if (this.size > 1)
            {
                throw new RuntimeException("how did we get here?");
            }
            if (this.list[0] != null)
            {
                return this.list[0].get();
            }
            return null;
        }

        public boolean isEmptyAfterRemoval()
        {
            return this.size == 0;
        }

        public Object removeEntryUsingIdentity(int hash, SingleEntry key)
        {
            if (this.semiUniqueHash == hash)
            {
                for(int k=this.size - 1; k >= 0; k--)
                {
                    if (this.list[k] == key)
                    {
                        removeWeakReference(k);
                        return key;
                    }
                }
            }
            return null;
        }

        public List removeForRange(AsOfAttribute[] asOfAttributes, Object data, SingleEntry[] pkTable)
        {
            List result = null;
            for(int k=this.size - 1; k >= 0; k--)
            {
                SingleEntry entry = this.list[k];
                Object o = entry.get();
                if (o != null)
                {
                    boolean fullMatch = true;
                    for(int i=0;fullMatch && i<asOfAttributes.length;i++)
                    {
                        fullMatch = asOfAttributes[i].hasRangeOverlap((MithraDataObject) o,
                                asOfAttributes[i].getFromAttribute().timestampValueOfAsLong(data),
                                asOfAttributes[i].getToAttribute().timestampValueOfAsLong(data));
                    }
                    if (fullMatch)
                    {
                        removeWeakReference(k);
                        entry.cleanupPkTable(pkTable);
                        if (result == null)
                        {
                            result = new FastList(2);
                        }
                        result.add(o);
                    }
                }
                else
                {
                    removeWeakReference(k);
                }
            }
            return result;
        }

        public Object removeMatching(AsOfAttribute[] asOfAttributes, Timestamp[] asOfDates, SingleEntry[] pkTable)
        {
            for(int k=this.size - 1; k >= 0; k--)
            {
                SingleEntry entry = this.list[k];
                Object o = entry.get();
                if (o != null)
                {
                    boolean fullMatch = true;
                    for(int i=0;fullMatch && i<asOfAttributes.length;i++)
                    {
                        fullMatch = asOfAttributes[i].dataMatches(o, asOfDates[i]);
                    }
                    if (fullMatch)
                    {
                        removeWeakReference(k);
                        entry.cleanupPkTable(pkTable);
                        return o;
                    }
                }
                else
                {
                    removeWeakReference(k);
                }
            }
            return null;
        }

        public int cleanUpEntry(SingleEntry entry, SemiUniqueEntry prev, SemiUniqueEntry[] nonDatedTable, int index)
        {
            if (this.semiUniqueHash == entry.getSemiUniqueHash())
            {
                for(int k=this.size - 1; k >= 0; k--)
                {
                    if (list[k] == entry)
                    {
                        removeWeakReference(k);
                        if (this.size == 0)
                        {
                            if (prev == this)
                                nonDatedTable[index] = this.semiUniqueNext;
                            else
                                prev.setSemiUniqueNext(this.semiUniqueNext);
                            this.semiUniqueNext = null;  // Help GC
                            return 1;
                        }
                        return 0;
                    }
                }
            }
            return -1;
        }

        public int removeAllFromPkTable(SingleEntry[] table)
        {
            int removed = 0;
            for(int k=this.size - 1; k >= 0; k--)
            {
                SingleEntry entry = this.list[k];
                removed += entry.cleanupPkTable(table);
            }
            return removed;
        }
    }
}
