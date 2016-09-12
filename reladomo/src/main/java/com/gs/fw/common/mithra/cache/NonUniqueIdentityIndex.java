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

import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;



public class NonUniqueIdentityIndex implements IterableNonUniqueIndex, UnderlyingObjectGetter
{

    private ExtractorBasedHashStrategy hashStrategy;
    private Extractor[] indexExtractors;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private static final int DEFAULT_INITIAL_CAPACITY = 8;

    static final int MAXIMUM_CAPACITY = 1 << 30;

    private transient Object[] table;

    private transient int nonUniqueSize;
    private transient int uniqueSize;
    private transient SetLikeIdentityList max;

    private int maxSize;

    private final float loadFactor;
    private static final int SIXTY_FOUR_BIT_MAX = 268435456;


    public NonUniqueIdentityIndex(Extractor[] indexExtractors, int initialCapacity, float loadFactor)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal initial capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal load factor: " +
                    loadFactor);

        this.indexExtractors = indexExtractors;
        hashStrategy = ExtractorBasedHashStrategy.create(this.indexExtractors);
        // Find a power of 2 >= initialCapacity
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;

        this.loadFactor = loadFactor;
        maxSize = (int) (capacity * loadFactor);
        table = new Object[capacity];
    }

    public NonUniqueIdentityIndex(Extractor[] indexExtractors)
    {
        this(indexExtractors, ExtractorBasedHashStrategy.create(indexExtractors));
    }

    public NonUniqueIdentityIndex(Extractor[] indexExtractors,
                                  ExtractorBasedHashStrategy hashStrategy)
    {
        this.indexExtractors = indexExtractors;
        this.hashStrategy = hashStrategy;
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        maxSize = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = new Object[DEFAULT_INITIAL_CAPACITY];
    }


    public NonUniqueIdentityIndex(Extractor[] indexExtractors, int initialCapacity)
    {
        this(indexExtractors, initialCapacity, DEFAULT_LOAD_FACTOR);
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

    public Extractor[] getExtractors()
    {
        return this.indexExtractors;
    }

    public int size()
    {
        return uniqueSize;
    }

    public boolean isEmpty()
    {
        return uniqueSize == 0;
    }

    public int getAverageReturnSize()
    {
        if (nonUniqueSize == 0) return 0;
        int result = uniqueSize / nonUniqueSize;
        if (result * nonUniqueSize < uniqueSize)
        {
            result++;
        }
        return result;
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        if (multiplier >= nonUniqueSize)
        {
            return uniqueSize;
        }

        int avgSize = getAverageReturnSize();
        return EstimateDistribution.estimateMaxReturnSize(multiplier, max != null ? max.size() : avgSize << 2, uniqueSize, avgSize);
    }

    public boolean isUnique()
    {
        return false;
    }

    protected Object getOne(Object o)
    {
        if (o instanceof SetLikeIdentityList)
        {
            o = ((SetLikeIdentityList) o).getFirst();
        }
        return o;
    }

    public Object get(int key)
    {
        IntExtractor extractor = (IntExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key, extractor);
        }
        else if (cur != null && key == extractor.intValueOf(getOne(cur)))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getCompactList(Object cur)
    {
        if (cur instanceof SetLikeIdentityList)
        {
            return ((SetLikeIdentityList) cur).getAll();
        }
        return cur;
    }

    private static final DoUntilProcedure3<Object, Filter2, Object> FILTER_ADAPTOR = new DoUntilProcedure3<Object, Filter2, Object>()
    {
        public boolean execute(Object object, Filter2 filter, Object keyHolder)
        {
            return filter.matches(object, keyHolder);
        }
    };

    private boolean containsInList(Object cur, Object keyHolder, final Filter2 filter)
    {
        if (cur instanceof SetLikeIdentityList)
        {
            SetLikeIdentityList list = (SetLikeIdentityList) cur;
            if (filter == null && list.size() > 0)
            {
                return true;
            }
            return list.forAllWith(FILTER_ADAPTOR, filter, keyHolder);
        }
        return filter == null || filter.matches(cur, keyHolder);
    }

    private Object getFromChained(ChainedBucket bucket, int key, IntExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.intValueOf(getOne(cur))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(char key)
    {
        CharExtractor extractor = (CharExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key, extractor);
        }
        else if (cur != null && key == extractor.charValueOf(getOne(cur)))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, char key, CharExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.charValueOf(getOne(cur))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(Object key)
    {
        int hash = key.hashCode();

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key);
        }
        else if (cur != null && key.equals(indexExtractors[0].valueOf(getOne(cur))))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, Object key)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key.equals(indexExtractors[0].valueOf(getOne(cur)))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(byte[] key)
    {
        int hash = HashUtil.hash(key);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key);
        }
        else if (cur != null && Arrays.equals(key, (byte[]) indexExtractors[0].valueOf(getOne(cur))))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, byte[] key)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (Arrays.equals(key, (byte[]) indexExtractors[0].valueOf(getOne(cur)))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(long key)
    {
        LongExtractor extractor = (LongExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key, extractor);
        }
        else if (cur != null && key == extractor.longValueOf(getOne(cur)))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, long key, LongExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.longValueOf(getOne(cur))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(double key)
    {
        DoubleExtractor extractor = (DoubleExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key, extractor);
        }
        else if (cur != null && key == extractor.doubleValueOf(getOne(cur)))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, double key, DoubleExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.doubleValueOf(getOne(cur))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(float key)
    {
        FloatExtractor extractor = (FloatExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key, extractor);
        }
        else if (cur != null && key == extractor.floatValueOf(getOne(cur)))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, float key, FloatExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.floatValueOf(getOne(cur))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(boolean key)
    {
        BooleanExtractor extractor = (BooleanExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, key, extractor);
        }
        else if (cur != null && key == extractor.booleanValueOf(getOne(cur)))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, boolean key, BooleanExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.booleanValueOf(getOne(cur))) return getCompactList(cur);
        }
        return null;
    }

    public Object get(Object valueHolder, List extractors)
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, valueHolder, extractors);
        }
        else if (cur != null && hashStrategy.equals(getOne(cur), valueHolder, extractors))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, Object valueHolder, List extractors)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), valueHolder, extractors)) return getCompactList(cur);
        }
        return null;
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDates0, Timestamp asOfDate1)
    {
        throw new RuntimeException("not implemented. should not get here");
    }

    public Object get(Object valueHolder, Extractor[] extractors) // for multi attribute indicies
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getFromChained((ChainedBucket) cur, valueHolder, extractors);
        }
        else if (cur != null && hashStrategy.equals(getOne(cur), valueHolder, extractors))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, Object valueHolder, Extractor[] extractors)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), valueHolder, extractors)) return getCompactList(cur);
        }
        return null;
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        int hash = this.hashStrategy.computeHashCode(keyHolder, extractors);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return containsInChained((ChainedBucket) cur, keyHolder, extractors, filter);
        }
        else if (cur != null && hashStrategy.equals(getOne(cur), keyHolder, extractors))
        {
            return containsInList(cur, keyHolder, filter);
        }
        return false;
    }

    private boolean containsInChained(ChainedBucket bucket, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), keyHolder, extractors)) return containsInList(cur, keyHolder, filter);
        }
        return false;
    }

    public void findAndExecute(Object valueHolder, Extractor[] extractors, DoUntilProcedure procedure)
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            findAndExecuteChained((ChainedBucket) cur, valueHolder, extractors, procedure);
        }
        else if (cur != null && hashStrategy.equals(getOne(cur), valueHolder, extractors))
        {
            if (cur instanceof SetLikeIdentityList)
            {
                ((SetLikeIdentityList) cur).forAll(procedure);
            }
            else
            {
                procedure.execute(cur);
            }
        }
    }

    private void findAndExecuteChained(ChainedBucket bucket, Object valueHolder, Extractor[] extractors, DoUntilProcedure procedure)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), valueHolder, extractors))
            {
                if (cur instanceof SetLikeIdentityList)
                {
                    ((SetLikeIdentityList) cur).forAll(procedure);
                }
                else
                {
                    procedure.execute(cur);
                }
                return;
            }
        }
    }

    public Object put(Object key)
    {
        int hash = hashStrategy.computeHashCode(key);
        int index = hash & this.table.length - 1;
        Object cur = this.table[index];

        if (cur == null)
        {
            this.table[index] = key;
            uniqueSize++;
            nonUniqueSize++;
        }
        else if (cur instanceof ChainedBucket)
        {
            cur = putIntoChain((ChainedBucket) cur, key);
        }
        else if (hashStrategy.equals(getOne(cur), key))
        {
            SetLikeIdentityList set = null;
            if (cur instanceof SetLikeIdentityList)
            {
                set = (SetLikeIdentityList) cur;
                uniqueSize -= set.size();
                set = set.addAndGrow(key);
                table[index] = set;
                uniqueSize += set.size();
                if (max == null || max.size() < set.size())
                {
                    max = set;
                }
                return null;
            }
            else if (cur != key)
            {
                set = new DuoSetLikeIdentityList(cur, key);
                this.table[index] = set;
                uniqueSize++;
                return null;
            }
        }
        else
        {
            ChainedBucket bucket = new ChainedBucket(cur, key);
            this.table[index] = bucket;
            cur = null;
            nonUniqueSize++;
            uniqueSize++;
        }
        if (nonUniqueSize > maxSize)
        {
            int newCapacity = table.length << 1;
            rehash(newCapacity);
        }
        return cur;
    }

    private Object putIntoChain(ChainedBucket bucket, Object key)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), key))
            {
                SetLikeIdentityList set;
                if (cur instanceof SetLikeIdentityList)
                {
                    set = (SetLikeIdentityList) cur;
                    uniqueSize -= set.size();
                    set = set.addAndGrow(key);
                    uniqueSize += set.size();
                    if (max == null || max.size() < set.size())
                    {
                        max = set;
                    }
                    bucket.chain[i] = set;
                }
                else if (cur != key)
                {
                    set = new DuoSetLikeIdentityList(cur, key);
                    bucket.chain[i] = set;
                    uniqueSize++;
                }
                return null;
            }
        }
        bucket.add(key);
        nonUniqueSize++;
        uniqueSize++;
        return null;
    }

    public Object putUsingUnderlying(Object businessObject, Object underlying)
    {
        throw new RuntimeException("not implemented");
    }

    public void putForReindex(Object key, Object underlying)
    {
        int hash = hashStrategy.computeHashCode(underlying);
        int index = hash & this.table.length - 1;
        Object cur = this.table[index];

        if (cur == null)
        {
            this.table[index] = key;
            uniqueSize++;
            nonUniqueSize++;
        }
        else if (cur instanceof ChainedBucket)
        {
            putForReindexIntoChain((ChainedBucket) cur, key, underlying);
        }
        else if (hashStrategy.equals(getOne(cur), underlying))
        {
            SetLikeIdentityList set = null;
            if (cur instanceof SetLikeIdentityList)
            {
                set = (SetLikeIdentityList) cur;
                uniqueSize -= set.size();
                table[index] = set.addAndGrow(key);
                uniqueSize += set.size();
                if (max == null || max.size() < set.size())
                {
                    max = set;
                }
            }
            else
            {
                set = new DuoSetLikeIdentityList(cur, key);
                this.table[index] = set;
                uniqueSize++;
            }
        }
        else
        {
            ChainedBucket bucket = new ChainedBucket(cur, key);
            this.table[index] = bucket;
            nonUniqueSize++;
            uniqueSize++;
        }
    }

    /**
     * @param procedure is executed on the list of objects with the same key or single object
     */
    public boolean doUntil(DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = this.table.length; !done && i-- > 0; )
        {
            Object cur = this.table[i];
            if (cur == null)
            {
            }
            else if (cur instanceof ChainedBucket)
            {
                ChainedBucket bucket = (ChainedBucket) cur;
                for (int j = bucket.size; !done && j-- > 0; )
                {
                    done = procedure.execute(this.getCompactList(bucket.chain[j]));
                }
            }
            else
            {
                done = procedure.execute(this.getCompactList(cur));
            }
        }
        return done;
    }

    private void putForReindexIntoChain(ChainedBucket bucket, Object key, Object underlying)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), underlying))
            {
                SetLikeIdentityList set = null;
                if (cur instanceof SetLikeIdentityList)
                {
                    set = (SetLikeIdentityList) cur;
                    uniqueSize -= set.size();
                    bucket.chain[i] = set.addAndGrow(key);
                    uniqueSize += set.size();
                    if (max == null || max.size() < set.size())
                    {
                        max = set;
                    }
                }
                else
                {
                    set = new DuoSetLikeIdentityList(cur, key);
                    bucket.chain[i] = set;
                    uniqueSize++;
                }
                return;
            }
        }
        bucket.add(key);
        nonUniqueSize++;
        uniqueSize++;
    }

    private int fastCeil(float v)
    {
        int possibleResult = (int) v;
        if (v - possibleResult > 0) possibleResult++;
        return possibleResult;
    }

    private void computeMaxSize(int capacity)
    {
        if (capacity == SIXTY_FOUR_BIT_MAX)
        {
            maxSize = SIXTY_FOUR_BIT_MAX - 1;
        }
        else
        {
            maxSize = Math.min(capacity,
                    fastCeil(capacity * loadFactor));
        }
    }

    private int allocateTable(int capacity)
    {
        if (capacity > SIXTY_FOUR_BIT_MAX)
        {
            capacity = SIXTY_FOUR_BIT_MAX;
        }
        table = new Object[capacity];
        return capacity;
    }

    protected void rehash(int newCapacity)
    {
        int oldCapacity = table.length;
        Object oldSet[] = table;

        int capacity = this.allocateTable(newCapacity);
        this.computeMaxSize(capacity);
        this.nonUniqueSize = 0;

        for (int i = 0; i < oldCapacity; i++)
        {
            Object cur = oldSet[i];
            if (cur instanceof ChainedBucket)
            {
                this.rehashFromChain((ChainedBucket) cur);
            }
            else if (cur != null)
            {
                Object one = cur;
                if (one instanceof SetLikeIdentityList)
                {
                    SetLikeIdentityList list = (SetLikeIdentityList) one;
                    one = list.getFirst();
                }
                this.putForRehash(cur, one);
            }
        }
    }

    private void rehashFromChain(ChainedBucket bucket)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            Object one = cur;
            if (one instanceof SetLikeIdentityList)
            {
                SetLikeIdentityList list = (SetLikeIdentityList) one;
                one = list.getFirst();
            }
            this.putForRehash(cur, one);
        }
    }

    private void putForRehash(Object everything, Object key)
    {
        int hash = hashStrategy.computeHashCode(key);
        int index = hash & this.table.length - 1;
        Object cur = this.table[index];

        nonUniqueSize++;
        if (cur == null)
        {
            this.table[index] = everything;
        }
        else if (cur instanceof ChainedBucket)
        {
            ((ChainedBucket) cur).add(everything);
        }
        else
        {
            ChainedBucket bucket = new ChainedBucket(cur, everything);
            this.table[index] = bucket;
        }
    }

    public Object getNulls()
    {
        int hash = HashUtil.NULL_HASH;

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return getNullsFromChained((ChainedBucket) cur);
        }
        else if (cur != null && indexExtractors[0].isAttributeNull(getOne(cur)))
        {
            return getCompactList(cur);
        }
        return null;
    }

    private Object getNullsFromChained(ChainedBucket bucket)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (indexExtractors[0].isAttributeNull(getOne(cur))) return getCompactList(cur);
        }
        return null;
    }

    public Object remove(Object key)
    {
        int hash = hashStrategy.computeHashCode(key);
        int length = this.table.length - 1;
        int index = hash & length;
        Object cur = this.table[index];

        if (cur != null)
        {
            if (cur instanceof ChainedBucket)
            {
                return removeFromChain((ChainedBucket) cur, key, index);
            }
            else if (hashStrategy.equals(getOne(cur), key))
            {
                uniqueSize--;
                Object removed = null;
                if (cur instanceof SetLikeIdentityList)
                {
                    SetLikeIdentityList set = (SetLikeIdentityList) cur;
                    table[index] = set.removeAndShrink(key);
                    if (max == set && set.size() - 1 <= this.getAverageReturnSize())
                    {
                        max = null;
                    }
                }
                else
                {
                    if (cur == key)
                    {
                        removed = cur;
                        table[index] = null;
                    }
                }
                return removed;
            }
        }
        return null;
    }

    public Object removeUsingUnderlying(Object underlying)
    {
        throw new RuntimeException("not implemented");
    }

    private Object removeFromChain(ChainedBucket bucket, Object key, int index)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), key))
            {
                uniqueSize--;
                boolean removeEntry = false;
                Object removed = null;
                if (cur instanceof SetLikeIdentityList)
                {
                    SetLikeIdentityList set = (SetLikeIdentityList) cur;
                    bucket.chain[i] = set.removeAndShrink(key);
                    if (max == set && set.size() - 1 <= this.getAverageReturnSize())
                    {
                        max = null;
                    }
                }
                else
                {
                    if (cur == key)
                    {
                        removed = cur;
                        removeEntry = true;
                    }
                }
                if (removeEntry)
                {
                    nonUniqueSize--;
                    if (bucket.size == 1)
                    {
                        table[index] = null;
                    }
                    else
                    {
                        bucket.size--;
                        bucket.chain[i] = bucket.chain[bucket.size];
                        bucket.chain[bucket.size] = null;
                    }
                }
                return removed;
            }
        }
        return null;
    }

    public void clear()
    {
        nonUniqueSize = 0;
        uniqueSize = 0;
        max = null;
        Object[] set = this.table;

        for (int i = set.length; i-- > 0; )
        {
            set[i] = null;
        }
    }

    public Object getUnderlyingObject(Object o)
    {
        return o;
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        throw new RuntimeException("not implemeted");
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

    private static final class ChainedBucket
    {
        private int size;
        private Object[] chain;

        public ChainedBucket(Object first, Object second)
        {
            chain = new Object[4];
            chain[0] = first;
            chain[1] = second;
            size = 2;
        }

        public void add(Object o)
        {
            if (this.size == chain.length) expand();
            chain[size] = o;
            size++;
        }

        public void expand()
        {
            Object[] newChain = new Object[this.size + 4];
            System.arraycopy(chain, 0, newChain, 0, this.size);
            this.chain = newChain;
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
    public void ensureExtraCapacity(int size)
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
