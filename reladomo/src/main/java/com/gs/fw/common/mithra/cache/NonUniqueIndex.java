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



public class NonUniqueIndex implements IterableNonUniqueIndex, UnderlyingObjectGetter
{

    private ExtractorBasedHashStrategy hashStrategy;
    private ExtractorBasedHashStrategy pkHashStrategy;
    private Extractor[] pkExtractors;
    private Extractor[] indexExtractors;
    private UnderlyingObjectGetter underlyingObjectGetter = this;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private static final int DEFAULT_INITIAL_CAPACITY = 8;

    static final int MAXIMUM_CAPACITY = 1 << 30;

    private transient Object[] table;

    private transient int nonUniqueSize;
    private transient int uniqueSize;
    private transient FullUniqueIndex max;

    private int maxSize;

    private final float loadFactor;
    private static final int SIXTY_FOUR_BIT_MAX = 268435456;


    public NonUniqueIndex(String indexName, Extractor[] pkExtractors, Extractor[] indexExtractors, int initialCapacity, float loadFactor)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal initial capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal load factor: " +
                    loadFactor);

        this.pkExtractors = pkExtractors;
        this.indexExtractors = indexExtractors;
        hashStrategy = ExtractorBasedHashStrategy.create(this.indexExtractors);
        pkHashStrategy = ExtractorBasedHashStrategy.create(this.pkExtractors);
        // Find a power of 2 >= initialCapacity
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;

        this.loadFactor = loadFactor;
        maxSize = (int) (capacity * loadFactor);
        table = new Object[capacity];
    }

    public NonUniqueIndex(String indexName, Extractor[] pkExtractors, Extractor[] indexExtractors)
    {
        this(indexName, pkExtractors, indexExtractors, ExtractorBasedHashStrategy.create(pkExtractors), ExtractorBasedHashStrategy.create(indexExtractors));
    }

    public NonUniqueIndex(String indexName, Extractor[] pkExtractors, Extractor[] indexExtractors,
                          ExtractorBasedHashStrategy pkHashStrategy, ExtractorBasedHashStrategy hashStrategy)
    {
        this.pkExtractors = pkExtractors;
        this.indexExtractors = indexExtractors;
        this.pkHashStrategy = pkHashStrategy;
        this.hashStrategy = hashStrategy;
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        maxSize = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = new Object[DEFAULT_INITIAL_CAPACITY];
    }


    public NonUniqueIndex(String indexName, Extractor[] pkExtractors, Extractor[] indexExtractors, int initialCapacity)
    {
        this(indexName, pkExtractors, indexExtractors, initialCapacity, DEFAULT_LOAD_FACTOR);
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

    public int getNonUniqueSize()
    {
        return nonUniqueSize;
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
        if (o instanceof FullUniqueIndex)
        {
            o = this.underlyingObjectGetter.getUnderlyingObject(((FullUniqueIndex) o).getFirst());
        }
        return this.underlyingObjectGetter.getUnderlyingObject(o);
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, int key, IntExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.intValueOf(getOne(cur))) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, char key, CharExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.charValueOf(getOne(cur))) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, Object key)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key.equals(indexExtractors[0].valueOf(getOne(cur)))) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, byte[] key)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (Arrays.equals(key, (byte[]) indexExtractors[0].valueOf(getOne(cur)))) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, long key, LongExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.longValueOf(getOne(cur))) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, double key, DoubleExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.doubleValueOf(getOne(cur))) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, float key, FloatExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.floatValueOf(getOne(cur))) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, boolean key, BooleanExtractor extractor)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (key == extractor.booleanValueOf(getOne(cur))) return cur;
        }
        return null;
    }

    public Object removeGroup(Object valueHolder, List extractors)
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            ChainedBucket bucket = (ChainedBucket) cur;
            for (int i = 0; i < bucket.size; i++)
            {
                Object next = bucket.chain[i];
                if (hashStrategy.equals(getOne(next), valueHolder, extractors))
                {
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
                    this.uniqueSize -= next instanceof FullUniqueIndex ? ((FullUniqueIndex) next).size() : 1;
                    this.nonUniqueSize--;
                    if (max == next) max = null;
                    return next;
                }
            }
            return null;
        }
        else if (cur != null && hashStrategy.equals(getOne(cur), valueHolder, extractors))
        {
            this.table[index] = null;
            this.uniqueSize -= cur instanceof FullUniqueIndex ? ((FullUniqueIndex) cur).size() : 1;
            this.nonUniqueSize--;
            if (max == cur) max = null;
            return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, Object valueHolder, List extractors)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), valueHolder, extractors)) return cur;
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
            return cur;
        }
        return null;
    }

    private Object getFromChained(ChainedBucket bucket, Object valueHolder, Extractor[] extractors)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), valueHolder, extractors)) return cur;
        }
        return null;
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter) // for multi attribute indicies
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
            return containsInUniqueSet(cur, keyHolder, filter);
        }
        return false;
    }

    private boolean containsInChained(ChainedBucket bucket, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), keyHolder, extractors))
            {
                return containsInUniqueSet(cur, keyHolder, filter);
            }
        }
        return false;
    }

    private static final DoUntilProcedure3<Object, Filter2, Object> FILTER_ADAPTOR = new DoUntilProcedure3<Object, Filter2, Object>()
    {
        public boolean execute(Object object, Filter2 filter, Object keyHolder)
        {
            return filter.matches(object, keyHolder);
        }
    };

    private boolean containsInUniqueSet(Object cur, Object keyHolder, final Filter2 filter)
    {
        if (cur instanceof FullUniqueIndex)
        {
            final FullUniqueIndex set = (FullUniqueIndex) cur;
            return (filter == null && set.size() > 0) || set.forAllWith(FILTER_ADAPTOR, filter, keyHolder);
        }

        return filter == null || filter.matches(cur, keyHolder);
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
            if (cur instanceof FullUniqueIndex)
            {
                ((FullUniqueIndex) cur).forAll(procedure);
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
                if (cur instanceof FullUniqueIndex)
                {
                    ((FullUniqueIndex) cur).forAll(procedure);
                }
                else
                {
                    procedure.execute(cur);
                }
                return;
            }
        }
    }

    public boolean contains(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        int hash = this.hashStrategy.computeHashCode(underlying);

        int index = hash & (this.table.length - 1);
        Object cur = this.table[index];

        if (cur instanceof ChainedBucket)
        {
            return containsFromChained((ChainedBucket) cur, key);
        }
        else if (cur != null && this.hashStrategy.equals(getOne(cur), key))
        {
            if (cur instanceof FullUniqueIndex)
            {
                return ((FullUniqueIndex) cur).contains(key);
            }
            return this.pkHashStrategy.equals(cur, key);
        }
        return false;
    }

    private boolean containsFromChained(ChainedBucket bucket, Object key)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (this.hashStrategy.equals(getOne(cur), key))
            {
                if (cur instanceof FullUniqueIndex)
                {
                    return ((FullUniqueIndex) cur).contains(key);
                }
                return this.pkHashStrategy.equals(cur, key);
            }
        }
        return false;
    }

    public Object put(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return putUsingUnderlying(key, underlying);
    }

    public Object putUsingUnderlying(Object key, Object underlying)
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
            cur = putUsingUnderlyingIntoChain((ChainedBucket) cur, key, underlying);
        }
        else if (hashStrategy.equals(getOne(cur), underlying))
        {
            FullUniqueIndex set = null;
            if (cur instanceof FullUniqueIndex)
            {
                set = (FullUniqueIndex) cur;
                uniqueSize -= set.size();
            }
            else if (pkHashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), underlying))
            {
                this.table[index] = key;
                return cur;
            }
            else
            {
                set = new FullUniqueIndex(pkHashStrategy);
                set.setUnderlyingObjectGetter(this.underlyingObjectGetter);
                set.put(cur);
                this.table[index] = set;
                uniqueSize--;
            }
            Object oldValue = set.putUsingUnderlying(key, underlying);
            uniqueSize += set.size();
            if (max == null || set.size() > max.size())
            {
                max = set;
            }
            return oldValue;
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
            rehashForUnderlying(newCapacity, key, underlying);
        }
        return cur;
    }

    private Object putUsingUnderlyingIntoChain(ChainedBucket bucket, Object key, Object underlying)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), underlying))
            {
                FullUniqueIndex set = null;
                if (cur instanceof FullUniqueIndex)
                {
                    set = (FullUniqueIndex) cur;
                    uniqueSize -= set.size();
                }
                else
                {
                    set = new FullUniqueIndex(pkHashStrategy);
                    set.setUnderlyingObjectGetter(this.underlyingObjectGetter);
                    set.put(cur);
                    bucket.chain[i] = set;
                    uniqueSize--;
                }
                Object oldValue = set.putUsingUnderlying(key, underlying);
                uniqueSize += set.size();
                if (max == null || set.size() > max.size())
                {
                    max = set;
                }
                return oldValue;
            }
        }
        bucket.add(key);
        nonUniqueSize++;
        uniqueSize++;
        return null;
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

    protected void rehashForUnderlying(int newCapacity, Object businessObject, Object underlying)
    {
        int oldCapacity = table.length;
        Object oldSet[] = table;

        int capacity = allocateTable(newCapacity);
        computeMaxSize(capacity);
        this.nonUniqueSize = 0;

        for (int i = 0; i < oldCapacity; i++)
        {
            Object cur = oldSet[i];
            if (cur instanceof ChainedBucket)
            {
                rehashForUnderlyingFromChain((ChainedBucket) cur, businessObject, underlying);
            }
            else if (cur != null)
            {
                Object one = cur;
                if (one instanceof FullUniqueIndex)
                {
                    one = ((FullUniqueIndex) one).getFirst();
                }
                if (one == businessObject)
                {
                    one = underlying;
                }
                else
                {
                    one = this.underlyingObjectGetter.getUnderlyingObject(one);
                }
                this.putUsingUnderlying(cur, one);
                uniqueSize--;
            }
        }
    }

    private void rehashForUnderlyingFromChain(ChainedBucket bucket, Object businessObject, Object underlying)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            Object one = cur;
            if (one instanceof FullUniqueIndex)
            {
                one = ((FullUniqueIndex) one).getFirst();
            }
            if (one == businessObject)
            {
                one = underlying;
            }
            else
            {
                one = this.underlyingObjectGetter.getUnderlyingObject(one);
            }
            this.putUsingUnderlying(cur, one);
            uniqueSize--;
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
            return cur;
        }
        return null;
    }

    private Object getNullsFromChained(ChainedBucket bucket)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (indexExtractors[0].isAttributeNull(getOne(cur))) return cur;
        }
        return null;
    }

    public Object remove(Object key)
    {
        return this.removeUsingUnderlying(this.underlyingObjectGetter.getUnderlyingObject(key));
    }

    public Object removeUsingUnderlying(Object underlying)
    {
        int hash = hashStrategy.computeHashCode(underlying);
        int length = this.table.length - 1;
        int index = hash & length;
        Object cur = this.table[index];

        if (cur != null)
        {
            if (cur instanceof ChainedBucket)
            {
                return removeFromChainUsingUnderlying((ChainedBucket) cur, underlying, index);
            }
            else if (hashStrategy.equals(getOne(cur), underlying))
            {
                uniqueSize--;
                boolean removeEntry = false;
                Object removed = null;
                if (cur instanceof FullUniqueIndex)
                {
                    FullUniqueIndex set = (FullUniqueIndex) cur;
                    removed = set.removeUsingUnderlying(underlying);
                    removeEntry = set.isEmpty();
                }
                else
                {
                    if (this.pkHashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), underlying))
                    {
                        removed = cur;
                        removeEntry = true;
                    }
                }
                if (removeEntry)
                {
                    if (max == cur) max = null;
                    nonUniqueSize--;
                    table[index] = null;
                }
                return removed;
            }
        }
        return null;
    }

    private Object removeFromChainUsingUnderlying(ChainedBucket bucket, Object underlying, int index)
    {
        for (int i = 0; i < bucket.size; i++)
        {
            Object cur = bucket.chain[i];
            if (hashStrategy.equals(getOne(cur), underlying))
            {
                uniqueSize--;
                boolean removeEntry = false;
                Object removed = null;
                if (cur instanceof FullUniqueIndex)
                {
                    FullUniqueIndex set = (FullUniqueIndex) cur;
                    removed = set.removeUsingUnderlying(underlying);
                    removeEntry = set.isEmpty();
                    if (max == set) max = null;
                }
                else
                {
                    if (this.pkHashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), underlying))
                    {
                        removed = cur;
                        removeEntry = true;
                    }
                }
                if (removeEntry)
                {
                    if (max == cur) max = null;
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
        this.underlyingObjectGetter = underlyingObjectGetter;
    }

    public boolean forEachGroup(DoUntilProcedure<Object> procedure)
    {
        boolean done = false;
        for (int i = 0; !done && i < table.length; i++)
        {
            Object cur = table[i];
            if (cur instanceof ChainedBucket)
            {
                done = forEachGroupInChain((ChainedBucket) cur, procedure);
            }
            else if (cur != null)
            {
                done = procedure.execute(cur);
            }
        }
        return done;
    }

    private boolean forEachGroupInChain(ChainedBucket bucket, DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = 0; i < bucket.size && !done; i++)
        {
            done = procedure.execute(bucket.chain[i]);
        }
        return done;
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

    public boolean nonUniqueDoUntil(DoUntilProcedure proc)
    {
        boolean done = false;
        for (int i = 0; !done && i < table.length; i++)
        {
            Object cur = table[i];
            if (cur instanceof ChainedBucket)
            {
                done = nonUniqueDoUntilInChain((ChainedBucket) cur, proc);
            }
            else if (cur != null)
            {
                done = proc.execute(cur);
            }
        }
        return done;
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

    private boolean nonUniqueDoUntilInChain(ChainedBucket bucket, DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = 0; i < bucket.size && !done; i++)
        {
            Object cur = bucket.chain[i];
            done = procedure.execute(cur);
        }
        return done;
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
