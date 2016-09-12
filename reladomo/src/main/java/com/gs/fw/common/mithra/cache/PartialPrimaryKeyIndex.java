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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter2;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.HashUtil;
import org.slf4j.Logger;



public class PartialPrimaryKeyIndex implements PrimaryKeyIndex, UnderlyingObjectGetter
{

    protected ExtractorBasedHashStrategy hashStrategy;
    private Extractor[] extractors;
    private UnderlyingObjectGetter underlyingObjectGetter = this;
    /**
     * The default initial capacity -- MUST be a power of two.
     */
    protected static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    protected static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The load fast used when none specified in constructor.
     */
    protected static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    protected Entry[] table;

    /**
     * The number of key-value mappings contained in this weak hash map.
     */
    private int size;

    /**
     * The next size value at which to resize (capacity * load factor).
     */
    private int threshold;

    /**
     * The load factor for the hash table.
     */
    protected final float loadFactor;

    protected int expungeCount;

    /**
     * Reference queue for cleared WeakEntries
     */
    private final ReferenceQueue queue = new ReferenceQueue();

    protected long timeToLive; // in milliseconds
    protected long relationshipTimeToLive; // in milliseconds
    protected static final int MAX_EXPUNGE_COUNT = 10000;

    private static final EntryState CLEAN_NO_NEXT_STATE = new EntryState(false, null);
    private static final EntryState DIRTY_NO_NEXT_STATE = new EntryState(true, null);
    private static TimedEntryState LAST_TIMED_NO_NEXT_STATE = new TimedEntryState(false, null, 0);


    /**
     * Constructs a new, empty <tt>WeakPool</tt> with the given initial
     * capacity and the given load factor.
     *
     * @param initialCapacity The initial capacity of the <tt>WeakHashMap</tt>
     * @param loadFactor      The load factor of the <tt>WeakHashMap</tt>
     * @throws IllegalArgumentException If the initial capacity is negative,
     *                                  or if the load factor is nonpositive.
     */
    public PartialPrimaryKeyIndex(String indexName, Extractor[] extractors, int initialCapacity, float loadFactor)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal Initial Capacity: " +
                                               initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;

        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal Load factor: " +
                                               loadFactor);
        this.extractors = extractors;
        hashStrategy = ExtractorBasedHashStrategy.create(this.extractors);

        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        table = new Entry[capacity];
        this.loadFactor = loadFactor;
        threshold = (int) (capacity * loadFactor);
    }

    /**
     * Constructs a new, empty <tt>WeakPool</tt> with the given initial
     * capacity and the default load factor, which is <tt>0.75</tt>.
     *
     * @param initialCapacity The initial capacity of the <tt>WeakHashMap</tt>
     * @throws IllegalArgumentException If the initial capacity is negative.
     */
    public PartialPrimaryKeyIndex(String indexName, Extractor[] extractors, int initialCapacity)
    {
        this(indexName, extractors, initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs a new, empty <tt>WeakPool</tt> with the default initial
     * capacity (16) and the default load factor (0.75).
     */
    public PartialPrimaryKeyIndex(String indexName, Extractor[] extractors)
    {
        this.extractors = extractors;
        hashStrategy = ExtractorBasedHashStrategy.create(this.extractors);
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = new Entry[DEFAULT_INITIAL_CAPACITY];
    }

    public PartialPrimaryKeyIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive)
    {
        this(indexName, extractors);
        this.timeToLive = timeToLive;
        this.relationshipTimeToLive = relationshipTimeToLive;
    }

    @Override
    public boolean isInitialized()
    {
        return true;
    }

    @Override
    public Index getInitialized(IterableIndex iterableIndex)
    {
        return this;
    }

    public HashStrategy getHashStrategy()
    {
        return hashStrategy;
    }
    // internal utilities

    public Extractor[] getExtractors()
    {
        return this.extractors;
    }
    // internal utilities

    /**
     * Return index for hash code h.
     */
    protected int indexFor(int h, int length)
    {
        return h & (length - 1);
    }

    /**
     * Expunge stale entries from the table.
     */
    private boolean expungeStaleEntries()
    {
        if (size == 0) return false;
        Object r;
        boolean result = false;
        while ((r = queue.poll()) != null)
        {
            result = true;
            Entry e = (Entry) r;
            int h = e.getHash();
            int i = indexFor(h, table.length);

            Entry prev = table[i];
            Entry p = prev;
            while (p != null)
            {
                Entry next = p.getState().next;
                if (p == e)
                {
                    if (prev == e)
                        table[i] = next;
                    else
                        prev.setNext(next);
                    e.setNext(null);  // Help GC
                    size--;
                    break;
                }
                prev = p;
                p = next;
            }
        }
        return result;
    }

    public boolean evictCollectedReferences()
    {
        return expungeStaleEntries();
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return true;
    }

    /**
     * Return the table after first expunging stale entries
     */
    private Entry[] getTable()
    {
        expungeCount++;
        if (expungeCount >= MAX_EXPUNGE_COUNT)
        {
            expungeStaleEntries();
            expungeCount = 0;
        }
        return table;
    }

    /**
     * Returns the number of key-value mappings in this map.
     * This result is a snapshot, and may not reflect unprocessed
     * entries that will be removed before next attempted access
     * because they are no longer referenced.
     */
    public int size()
    {
        return size;
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     * This result is a snapshot, and may not reflect unprocessed
     * entries that will be removed before next attempted access
     * because they are no longer referenced.
     */
    public boolean isEmpty()
    {
        return size() == 0;
    }

    public int getAverageReturnSize()
    {
        return 1;
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        return multiplier;
    }

    public boolean isUnique()
    {
        return true;
    }

    public List getAll()
    {
        FastList result = new FastList(this.size());
        for (int i = 0; i < table.length; i++)
        {
            Entry e = table[i];
            while (e != null)
            {
                Object candidate = e.get();
                if (candidate != null && !e.getState().isDirty(timeToLive)) result.add(candidate);
                e = e.getState().next;
            }
        }
        return result;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = 0; i < table.length && !done; i++)
        {
            Entry e = table[i];
            while (e != null && !done)
            {
                Object candidate = e.get();
                if (candidate != null && !e.getState().isDirty(timeToLive)) done = procedure.execute(candidate);
                e = e.getState().next;
            }
        }
        return done;
    }

    public Object getFromData(Object data)
    {
        int hash = this.hashStrategy.computeHashCode(data);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive) &&
                this.hashStrategy.equals(data, this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(Object key)
    {
        int hash = key.hashCode();
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && key.equals(extractors[0].valueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate))))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(byte[] indexValue)
    {
        int hash = HashUtil.hash(indexValue);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && Arrays.equals(indexValue, (byte[]) extractors[0].valueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate))))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(int key)
    {
        IntExtractor extractor = (IntExtractor) extractors[0];
        int hash = HashUtil.hash(key);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && key == extractor.intValueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(char key)
    {
        CharExtractor extractor = (CharExtractor) extractors[0];
        int hash = HashUtil.hash(key);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && key == extractor.charValueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(long key)
    {
        LongExtractor extractor = (LongExtractor) extractors[0];
        int hash = HashUtil.hash(key);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && key == extractor.longValueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(double key)
    {
        DoubleExtractor extractor = (DoubleExtractor) extractors[0];
        int hash = HashUtil.hash(key);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && key == extractor.doubleValueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(float key)
    {
        FloatExtractor extractor = (FloatExtractor) extractors[0];
        int hash = HashUtil.hash(key);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && key == extractor.floatValueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(boolean key)
    {
        BooleanExtractor extractor = (BooleanExtractor) extractors[0];
        int hash = HashUtil.hash(key);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && key == extractor.booleanValueOf(this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(final Object valueHolder, final List extractors)
    {
        final ExtractorBasedHashStrategy strategy = this.hashStrategy;
        final int hash = strategy.computeHashCode(valueHolder, extractors);
        final UnderlyingObjectGetter objectGetter = this.underlyingObjectGetter;
        Entry e = table[indexFor(hash, table.length)];
        while (true)
        {
            if (e == null)
                return e;
            final Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive) &&
                strategy.equals(objectGetter.getUnderlyingObject(candidate), valueHolder, extractors))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        final int hash = relationshipHashStrategy.computeHashCodeFromRelated(srcObject, srcData);
        Entry e = table[indexFor(hash, table.length)];
        while (true)
        {
            if (e == null)
                return e;
            final Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(relationshipTimeToLive) &&
                relationshipHashStrategy.equalsForRelationship(srcObject, srcData, this.underlyingObjectGetter.getUnderlyingObject(candidate), asOfDate0, asOfDate1))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object get(Object valueHolder, Extractor[] extractors) // for multi attribute indicies
    {
        final int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);
        Entry e = table[indexFor(hash, table.length)];
        while (true)
        {
            if (e == null)
                return e;
            final Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(relationshipTimeToLive) &&
                this.hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(candidate), valueHolder, extractors))
                return candidate;
            e = e.getState().next;
        }
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        Object result = this.get(keyHolder, extractors);
        return result != null && (filter == null || filter.matches(result, keyHolder));
    }

    public Object getNulls()
    {
        int hash = HashUtil.NULL_HASH;
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash && !e.getState().isDirty(timeToLive)
                && extractors[0].isAttributeNull(this.underlyingObjectGetter.getUnderlyingObject(candidate)))
                return candidate;
            e = e.getState().next;
        }
    }

    public Object put(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return putUsingUnderlying(key, underlying);
    }

    public Object putUsingUnderlying(Object key, Object underlying)
    {
        int hash = this.hashStrategy.computeHashCode(underlying);
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);

        Entry prev = null;
        for (Entry e = tab[i]; e != null; e = e.getState().next)
        {
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash &&
                this.hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(candidate), underlying))
            {
                Entry newEntry = createEntry(key, hash, e.getState().next);
                if (prev != null)
                {
                    prev.setNext(newEntry);
                }
                else
                {
                    tab[i] = newEntry;
                }
                return candidate;
            }
            prev = e;
        }

        tab[i] = createEntry(key, hash, tab[i]);
        size++;
        if (size >= threshold)
            resize(table.length * 2);
        return null;
    }

    protected Entry createEntry(Object key, int hash, Entry next)
    {
        EntryState state = createState(next);
        return new SoftEntry(key, queue, hash, state);
    }

    private EntryState createState(Entry next)
    {
        if (timeToLive > 0)
        {
            return createdTimedState(next);
        }
        if (next == null) return CLEAN_NO_NEXT_STATE;
        return new EntryState(false, next);
    }

    protected static EntryState createdTimedState(Entry next)
    {
        long now = CacheClock.getTime();
        if (next == null)
        {
            TimedEntryState last = LAST_TIMED_NO_NEXT_STATE;
            if (last.cachedTime == now)
            {
                return last;
            }
            last = new TimedEntryState(false, null, now);
            LAST_TIMED_NO_NEXT_STATE = last;
            return last;
        }
        return new TimedEntryState(false, next, now);
    }

    /**
     * Rehashes the contents of this map into a new array with a
     * larger capacity.  This method is called automatically when the
     * number of keys in this map reaches its threshold.
     * <p/>
     * If current capacity is MAXIMUM_CAPACITY, this method does not
     * resize the map, but but sets threshold to Integer.MAX_VALUE.
     * This has the effect of preventing future calls.
     *
     * @param newCapacity the new capacity, MUST be a power of two;
     *                    must be greater than current capacity unless current
     *                    capacity is MAXIMUM_CAPACITY (in which case value
     *                    is irrelevant).
     */
    private void resize(int newCapacity)
    {
        Entry[] oldTable = getTable();
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY)
        {
            threshold = Integer.MAX_VALUE;
            return;
        }

        Entry[] newTable = new Entry[newCapacity];
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

    /**
     * Transfer all entries from src to dest tables
     */
    private void transfer(Entry[] src, Entry[] dest)
    {
        for (int j = 0; j < src.length; ++j)
        {
            Entry e = src[j];
            src[j] = null;
            while (e != null)
            {
                Entry next = e.getState().next;
                Object key = e.get();
                if (key == null)
                {
                    e.setNext(null);  // Help GC
                    size--;
                }
                else
                {
                    int i = indexFor(e.getHash(), dest.length);
                    e.setNext(dest[i]);
                    dest[i] = e;
                }
                e = next;
            }
        }
    }

    /**
     * Removes and returns the entry associated with the specified key
     * in the HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     */
    public Object remove(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return removeUsingUnderlying(underlying);
    }

    public Object removeUsingUnderlying(Object underlying)
    {
        int hash = this.hashStrategy.computeHashCode(underlying);
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);
        Entry prev = tab[i];
        Entry e = prev;

        while (e != null)
        {
            Entry next = e.getState().next;
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash &&
                this.hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(candidate), underlying))
            {
                size--;
                if (prev == e)
                    tab[i] = next;
                else
                    prev.setNext(next);
                return candidate;
            }
            prev = e;
            e = next;
        }

        return null;
    }

    public List removeAll(Filter filter)
    {
        FastList result = new FastList();
        for (int i = 0; i < table.length; i++)
        {
            Entry prev = table[i];
            Entry e = prev;
            while (e != null)
            {
                Entry next = e.getState().next;
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
                        table[i] = next;
                        prev = next;
                    }
                    else
                        prev.setNext(next);
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

    public boolean sizeRequiresWriteLock()
    {
        return true;
    }

    @Override
    public void ensureExtraCapacity(int capacity)
    {
        ensureCapacity(capacity + this.size);
    }

    public void ensureCapacity(int capacity)
    {
        int newCapacity = Integer.highestOneBit((int) (capacity / loadFactor));
        if (newCapacity < capacity) newCapacity = newCapacity << 1;
        if (newCapacity > table.length)
        {
            Entry[] oldTable = getTable();
            int oldCapacity = oldTable.length;
            if (oldCapacity == MAXIMUM_CAPACITY)
            {
                threshold = Integer.MAX_VALUE;
                return;
            }

            Entry[] newTable = new Entry[newCapacity];
            transfer(oldTable, newTable);
            table = newTable;
            threshold = (int) (newCapacity * loadFactor);
        }
    }

    protected void emptyReferenceQueue()
    {
        while (queue.poll() != null)
            ;
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        this.underlyingObjectGetter = underlyingObjectGetter;
    }

    public Object getUnderlyingObject(Object o)
    {
        return o;
    }

    public PrimaryKeyIndex copy()
    {
        throw new RuntimeException("copying a partial cache is foolish");
    }

    // weak but clean references:

    public Object putWeak(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return putWeakUsingUnderlying(key, underlying);
    }

    public Object putWeakUsingUnderlying(Object key, Object underlying)
    {
        int hash = this.hashStrategy.computeHashCode(underlying);
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);

        Entry prev = null;
        for (Entry e = tab[i]; e != null; e = e.getState().next)
        {
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash &&
                this.hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(candidate), underlying))
            {
                Entry newEntry = createWeakEntry(key, hash, e.getState().next);
                if (prev != null)
                {
                    prev.setNext(newEntry);
                }
                else
                {
                    tab[i] = newEntry;
                }
                return candidate;
            }
            prev = e;
        }

        tab[i] = createWeakEntry(key, hash, tab[i]);
        size++;
        if (size >= threshold)
            resize(table.length * 2);
        return null;
    }

    protected Entry createWeakEntry(Object key, int hash, Entry next)
    {
        EntryState state = createState(next);
        return new WeakEntry(key, queue, hash, state);
    }

    // we don't actually clear anything, just make things weak and dirty

    public void clear()
    {
        // clear out ref queue. We don't need to expunge entries
        // since gc'ed entries won't get copied.
        emptyReferenceQueue();

        for (int i = 0; i < table.length; i++)
        {
            Entry e = table[i];
            Entry first = null;
            Entry prev = null;
            while (e != null)
            {
                Object candidate = e.get();
                if (candidate != null)
                {
                    Entry dirty = e.makeDirtyAndWeak(queue);
                    if (first == null)
                    {
                        first = dirty;
                    }
                    if (prev != null)
                    {
                        prev.setNext(dirty);
                    }
                    prev = dirty;
                }
                else
                {
                    if (prev != null)
                    {
                        prev.setNext(e.getState().next);
                    }
                    size--;
                }
                e = e.getState().next;
            }
            table[i] = first;
        }

        // Allocation of array may have caused GC, which may have caused
        // additional entries to go stale.  Removing these entries from the
        // reference queue will make them eligible for reclamation.
        expungeStaleEntries();
    }

    public Object markDirty(MithraDataObject object)
    {
        int hash = this.hashStrategy.computeHashCode(object);
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);
        Entry prev = null;
        Entry e = tab[i];

        while (e != null)
        {
            Object candidate = e.get();
            if (candidate != null && e.getHash() == hash &&
                this.hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(candidate), object))
            {
                Entry dirty = e.makeDirtyAndWeak(queue);
                if (prev == null)
                {
                    tab[i] = dirty;
                }
                else
                {
                    prev.setNext(dirty);
                }
                return candidate;
            }
            prev = e;
            e = e.getState().next;
        }
        return null;
    }

    public Object getFromDataEvenIfDirty(Object data, NonNullMutableBoolean isDirty)
    {
        int hash = this.hashStrategy.computeHashCode(data);
        int i = indexFor(hash, table.length);
        Entry e = table[i];
        Entry prev = null;
        while (true)
        {
            if (e == null)
                return e;
            Object candidate = e.get();
            // we explicitly don't check for timed expiration here, as this method is used to uniquify objects
            // coming from the database
            if (candidate != null && e.getHash() == hash &&
                this.hashStrategy.equals(data, this.underlyingObjectGetter.getUnderlyingObject(candidate)))
            {
                isDirty.value = e.getState().isDirty(timeToLive);
                return candidate;
            }
            prev = e;
            e = e.getState().next;
        }
    }

    /**
     * The entries in this hash table extend WeakReference, using its main ref
     * field as the key.
     */
    private interface Entry
    {
        public Object get();

        public int getHash();

        public EntryState getState();

        public void setNext(Entry next);

        public Entry makeDirtyAndWeak(ReferenceQueue queue);
    }

    private static class EntryState
    {
        protected boolean isDirty;
        protected Entry next;

        private EntryState()
        {
        }

        private EntryState(boolean dirty, Entry next)
        {
            isDirty = dirty;
            this.next = next;
        }

        public boolean isDirty(long timeToLive)
        {
            return this.isDirty;
        }

        public EntryState copyForNew(boolean isDirty, Entry next)
        {
            if (this.isDirty == isDirty && this.next == next) return this;
            if (next == null)
            {
                return isDirty ? DIRTY_NO_NEXT_STATE : CLEAN_NO_NEXT_STATE;
            }
            return new EntryState(isDirty, next);
        }

        public EntryState copyForNew(Entry next)
        {
            if (this.next == next) return this;
            if (next == null)
            {
                return isDirty ? DIRTY_NO_NEXT_STATE : CLEAN_NO_NEXT_STATE;
            }
            return new EntryState(isDirty, next);
        }

    }

    private static class TimedEntryState extends EntryState
    {
        private long cachedTime;

        private TimedEntryState(boolean dirty, Entry next, long cachedTime)
        {
            super(dirty, next);
            this.cachedTime = cachedTime;
        }

        @Override
        public boolean isDirty(long timeToLive)
        {
            return this.isDirty || (timeToLive > 0 && this.cachedTime + timeToLive < CacheClock.getTime());
        }

        @Override
        public EntryState copyForNew(boolean isDirty, Entry next)
        {
            if (this.isDirty == isDirty && this.next == next) return this;
            return new TimedEntryState(isDirty, next, this.cachedTime);
        }

        @Override
        public EntryState copyForNew(Entry next)
        {
            if (this.next == next) return this;
            return new TimedEntryState(this.isDirty, next, this.cachedTime);
        }

    }

    private static class SoftEntry extends SoftReference implements Entry
    {
        protected final int hash;
        protected EntryState state;

        protected SoftEntry(Object key, ReferenceQueue queue,
                            int hash, EntryState state)
        {
            super(key, queue);
            this.hash = hash;
            this.state = state;
        }

        public int getHash()
        {
            return this.hash;
        }

        public EntryState getState()
        {
            return this.state;
        }

        public void setNext(Entry next)
        {
            this.state = this.state.copyForNew(next);
        }

        public Entry makeDirtyAndWeak(ReferenceQueue queue)
        {
            return new WeakEntry(this.get(), queue, this.hash, this.state.copyForNew(true, this.state.next));
        }

    }

    private static class WeakEntry extends WeakReference implements Entry
    {
        protected final int hash;
        protected EntryState state;

        protected WeakEntry(Object key, ReferenceQueue queue,
                            int hash, EntryState state)
        {
            super(key, queue);
            this.hash = hash;
            this.state = state;
        }

        public int getHash()
        {
            return this.hash;
        }

        public EntryState getState()
        {
            return this.state;
        }

        public void setNext(Entry next)
        {
            this.state = this.state.copyForNew(next);
        }

        public Entry makeDirtyAndWeak(ReferenceQueue queue)
        {
            this.state = this.state.copyForNew(true, this.state.next);
            return this;
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
}