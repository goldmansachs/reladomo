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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.extractor.BooleanExtractor;
import com.gs.fw.common.mithra.extractor.CharExtractor;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.FloatExtractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.extractor.LongExtractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.util.ArrayBasedQueue;
import com.gs.fw.common.mithra.util.CpuBoundTask;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.DoUntilProcedure2;
import com.gs.fw.common.mithra.util.DoUntilProcedure3;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Filter2;
import com.gs.fw.common.mithra.util.FixedCountTaskFactory;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraCpuBoundThreadPool;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.ThreadChunkSize;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;


public class FullUniqueIndex<T> implements PrimaryKeyIndex, UnderlyingObjectGetter, SetLikeIdentityList<T>
{
    private static final int REMOVE_RESIZE_THRESHOLD = 6;
    private static Logger logger = LoggerFactory.getLogger(FullUniqueIndex.class.getName());

    private static final Object REMOVED = new Object(), FREE = new Object();
    private static final float DEFAULT_LOAD_FACTOR = 0.5f;
    private static final int DEFAULT_INITIAL_CAPACITY = 5;
    private static final int SIXTY_FOUR_BIT_MAX = 268435456;

    private final ExtractorBasedHashStrategy hashStrategy;
    private UnderlyingObjectGetter underlyingObjectGetter = this;

    private transient Object[] table;

    private transient int occupied;

    private transient int free;

    private final byte loadFactor;
    private byte rightShift;

    private int maxSize;

    private Object any;

    public FullUniqueIndex(Extractor[] extractors, int initialCapacity, float loadFactor)
    {
        hashStrategy = ExtractorBasedHashStrategy.create(extractors);
        this.loadFactor = (byte) (loadFactor * 100);
        init(scaledByLoadFactor(initialCapacity));
    }

    public FullUniqueIndex(String indexName, Extractor[] extractors)
    {
        hashStrategy = ExtractorBasedHashStrategy.create(extractors);
        this.loadFactor = (byte) (DEFAULT_LOAD_FACTOR * 100);
        this.init(scaledByLoadFactor(DEFAULT_INITIAL_CAPACITY));
    }


    public FullUniqueIndex(Extractor[] extractors, int initialCapacity)
    {
        this(extractors, initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    public FullUniqueIndex(ExtractorBasedHashStrategy hashStrategy)
    {
        this(hashStrategy, DEFAULT_INITIAL_CAPACITY);
    }

    public FullUniqueIndex(ExtractorBasedHashStrategy hashStrategy, int initialCapacity)
    {
        this.hashStrategy = hashStrategy;
        this.loadFactor = (byte) (DEFAULT_LOAD_FACTOR * 100);
        this.init(scaledByLoadFactor(initialCapacity));
    }

    private int scaledByLoadFactor(int initialCapacity)
    {
        return (int) (initialCapacity * 100L / loadFactor);
    }

    private FullUniqueIndex(ExtractorBasedHashStrategy hashStrategy, UnderlyingObjectGetter uog, byte loadFactor)
    {
        // copy contructor;
        this.hashStrategy = hashStrategy;
        this.underlyingObjectGetter = uog;
        this.loadFactor = loadFactor;
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

    public boolean isEmpty()
    {
        return 0 == occupied;
    }

    public int size()
    {
        return occupied;
    }

    public boolean contains(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return index(underlying) >= 0;
    }

    private void computeMaxSize(int capacity)
    {
        if (capacity == SIXTY_FOUR_BIT_MAX)
        {
            maxSize = SIXTY_FOUR_BIT_MAX - 1;
            free = maxSize - occupied;
        }
        else
        {
            // need at least one free slot for open addressing
            maxSize = (int) Math.min(capacity - 1,
                    (((long) capacity) * loadFactor / 100));
            free = ((capacity + maxSize) >> 1) - occupied;
        }
    }

    private int capacity()
    {
        return table.length;
    }

    private T removeAt(int index)
    {
        occupied--;
        Object removed = table[index];
        table[index] = REMOVED;
        if (any == removed) any = null;
        return (T) removed;
    }

    private int init(int initialCapacity)
    {
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;

        capacity = allocateTable(capacity);
        computeMaxSize(capacity);

        return capacity;
    }

    private int allocateTable(int capacity)
    {
        if (capacity > SIXTY_FOUR_BIT_MAX)
        {
            capacity = SIXTY_FOUR_BIT_MAX;
        }
        table = new Object[capacity];
        this.rightShift = (byte) (Integer.numberOfTrailingZeros(capacity) + 1);
        Arrays.fill(table, FREE);
        return capacity;
    }

    /**
     * @param procedure the code to execute. if the procedure returns true, the loop will stop
     * @return whether or not the procedure returned true
     */
    public boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = false;
        Object[] set = this.table;
        for (int i = set.length; !done && i-- > 0; )
        {
            if (set[i] != FREE && set[i] != REMOVED)
            {
                done = procedure.execute(set[i]);
            }
        }
        return done;
    }

    public boolean forAllWith(DoUntilProcedure2 procedure, Object param)
    {
        boolean done = false;
        Object[] set = this.table;
        for (int i = set.length; !done && i-- > 0; )
        {
            if (set[i] != FREE && set[i] != REMOVED)
            {
                done = procedure.execute(set[i], param);
            }
        }
        return done;
    }

    public boolean forAllWith(DoUntilProcedure3 procedure, Object param1, Object param2)
    {
        boolean done = false;
        Object[] set = this.table;
        for (int i = set.length; !done && i-- > 0; )
        {
            if (set[i] != FREE && set[i] != REMOVED)
            {
                done = procedure.execute(set[i], param1, param2);
            }
        }
        return done;
    }

    private int index(Object obj)
    {
        int hash = hashStrategy.computeHashCode(obj);
        return index(hash, obj);
    }

    private int index(int hash, Object obj)
    {
        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), obj)))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), obj)));
        }

        return cur == FREE ? -1 : index;
    }

    private int indexOf(int hash, int length)
    {
        return (hash ^ (hash >>> this.rightShift)) & length;
    }

    public T getFromData(Object data)
    {
        int index = index(data);
        if (index >= 0)
        {
            return (T) this.table[index];
        }
        return null;
    }

    public T get(int key)
    {
        IntExtractor extractor = (IntExtractor) hashStrategy.getFirstExtractor();
        int hash = HashUtil.hash(key);

        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || key != extractor.intValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || key != extractor.intValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(char key)
    {
        CharExtractor extractor = (CharExtractor) hashStrategy.getFirstExtractor();
        int hash = HashUtil.hash(key);

        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || key != extractor.charValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || key != extractor.charValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(Object key)
    {
        int hash = key.hashCode();
        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || !key.equals(hashStrategy.getFirstExtractor().valueOf(this.underlyingObjectGetter.getUnderlyingObject(cur)))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || !key.equals(hashStrategy.getFirstExtractor().valueOf(this.underlyingObjectGetter.getUnderlyingObject(cur)))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(byte[] indexValue)
    {
        int hash = HashUtil.hash(indexValue);
        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || !Arrays.equals(((byte[]) hashStrategy.getFirstExtractor().valueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))), indexValue)))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || !Arrays.equals(((byte[]) hashStrategy.getFirstExtractor().valueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))), indexValue)));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(long key)
    {
        LongExtractor extractor = (LongExtractor) hashStrategy.getFirstExtractor();
        int hash = HashUtil.hash(key);
        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || key != extractor.longValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || key != extractor.longValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(double key)
    {
        DoubleExtractor extractor = (DoubleExtractor) hashStrategy.getFirstExtractor();
        int hash = HashUtil.hash(key);
        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || key != extractor.doubleValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || key != extractor.doubleValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(float key)
    {
        FloatExtractor extractor = (FloatExtractor) hashStrategy.getFirstExtractor();
        int hash = HashUtil.hash(key);
        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || key != extractor.floatValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || key != extractor.floatValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(boolean key)
    {
        BooleanExtractor extractor = (BooleanExtractor) hashStrategy.getFirstExtractor();
        int hash = HashUtil.hash(key);
        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || key != extractor.booleanValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || key != extractor.booleanValueOf(this.underlyingObjectGetter.getUnderlyingObject(cur))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public T get(Object valueHolder, List extractors)
    {
        int hash = hashStrategy.computeHashCode(valueHolder, extractors);

        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), valueHolder, extractors)))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), valueHolder, extractors)));
        }

        return cur == FREE ? null : (T) cur;
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int hash = relationshipHashStrategy.computeHashCodeFromRelated(srcObject, srcData);

        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || !relationshipHashStrategy.equalsForRelationship(srcObject, srcData, this.underlyingObjectGetter.getUnderlyingObject(cur), asOfDate0, asOfDate1)))
        {
            cur = probeFurther(srcObject, srcData, relationshipHashStrategy, hash, set, length, index, asOfDate0, asOfDate1);
        }

        return cur == FREE ? null : (T) cur;
    }

    private Object probeFurther(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int hash, Object[] set, int length, int index, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        Object cur;// from Knuth's Art of Computer Programming, Vol 3
        int probe = 0;

        do
        {
            hash += (probe += 17);
            index = hash & length;
            cur = set[index];
        } while (cur != FREE
                && (cur == REMOVED || !relationshipHashStrategy.equalsForRelationship(srcObject, srcData, this.underlyingObjectGetter.getUnderlyingObject(cur), asOfDate0, asOfDate1)));
        return cur;
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        Object result = this.get(keyHolder, extractors);
        return result != null && (filter == null || filter.matches(result, keyHolder));
    }

    public T get(Object valueHolder, Extractor[] extractors) // for multi attribute indicies
    {
        int hash = hashStrategy.computeHashCode(valueHolder, extractors);

        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), valueHolder, extractors)))
        {
            cur = probeFurther(valueHolder, extractors, hash, set, length, index);
        }

        return cur == FREE ? null : (T) cur;
    }

    private Object probeFurther(Object valueHolder, Extractor[] extractors, int hash, Object[] set, int length, int index)
    {
        Object cur;// from Knuth's Art of Computer Programming, Vol 3
        int probe = 0;

        do
        {
            hash += (probe += 17);
            index = hash & length;
            cur = set[index];
        } while (cur != FREE
                && (cur == REMOVED || !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), valueHolder, extractors)));
        return cur;
    }

    public T getNulls()
    {
        int hash = HashUtil.NULL_HASH;

        Object[] set = this.table;
        int length = set.length - 1;
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur != FREE
                && (cur == REMOVED || !hashStrategy.getFirstExtractor().isAttributeNull(this.underlyingObjectGetter.getUnderlyingObject(cur))))
        {
            // from Knuth's Art of Computer Programming, Vol 3
            int probe = 0;

            do
            {
                hash += (probe += 17);
                index = hash & length;
                cur = set[index];
            } while (cur != FREE
                    && (cur == REMOVED || !hashStrategy.getFirstExtractor().isAttributeNull(this.underlyingObjectGetter.getUnderlyingObject(cur))));
        }

        return cur == FREE ? null : (T) cur;
    }

    public List<T> getAll()
    {
        //todo: parallelize
        MithraFastList<T> result = new MithraFastList<T>(this.size());
        for (int i = 0; i < table.length; i++)
        {
            Object e = table[i];
            if (e != FREE && e != REMOVED) result.add((T) e);
        }
        return result;
    }

    protected int insertionIndex(Object obj)
    {
        Object[] set = this.table;
        int length = set.length - 1;
        int hash = hashStrategy.computeHashCode(obj);
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur == FREE)
        {
            return index;
        }
        else if (cur != REMOVED && hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), obj))
        {
            return -index - 1;
        }
        else
        {
            int probe = 0;

            int removedCount = 0;
            if (cur != REMOVED)
            {
                do
                {
                    removedCount += (cur == REMOVED) ? 1 : 0;
                    hash += (probe += 17);
                    index = hash & length;
                    cur = set[index];
                } while (cur != FREE
                        && cur != REMOVED
                        && !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), obj));
            }

            if (cur == REMOVED)
            {
                int firstRemoved = index;
                while (cur != FREE
                        && (cur == REMOVED || !hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), obj)))
                {
                    removedCount += (cur == REMOVED) ? 1 : 0;
                    hash += (probe += 17);
                    index = hash & length;
                    cur = set[index];
                }
                return cur != FREE ? -index - 1 : firstRemoved;
            }
            if (removedCount > REMOVE_RESIZE_THRESHOLD)
            {
                this.rehashForUnderlying(this.capacity(), null, null);
            }
            return cur != FREE ? -index - 1 : index;
        }
    }

    protected int updatedInsertionIndex(Object obj, AttributeUpdateWrapper updateWrapper)
    {
        Object[] set = this.table;
        int length = set.length - 1;
        int hash = hashStrategy.computeUpdatedHashCode(obj, updateWrapper);
        int index = indexOf(hash, length);
        Object cur = set[index];

        if (cur == FREE)
        {
            return index;
        }
        else if (cur != REMOVED && hashStrategy.equalsIncludingUpdate(this.underlyingObjectGetter.getUnderlyingObject(cur), obj, updateWrapper))
        {
            return -index - 1;
        }
        else
        {
            int probe = 0;

            if (cur != REMOVED)
            {
                do
                {
                    hash += (probe += 17);
                    index = hash & length;
                    cur = set[index];
                } while (cur != FREE
                        && cur != REMOVED
                        && !hashStrategy.equalsIncludingUpdate(this.underlyingObjectGetter.getUnderlyingObject(cur), obj, updateWrapper));
            }

            if (cur == REMOVED)
            {
                int firstRemoved = index;
                while (cur != FREE
                        && (cur == REMOVED || !hashStrategy.equalsIncludingUpdate(this.underlyingObjectGetter.getUnderlyingObject(cur), obj, updateWrapper)))
                {
                    hash += (probe += 17);
                    index = hash & length;
                    cur = set[index];
                }
                return cur != FREE ? -index - 1 : firstRemoved;
            }
            return cur != FREE ? -index - 1 : index;
        }
    }

    public T putWeak(Object key)
    {
        return put(key);
    }

    public Object putWeakUsingUnderlying(Object businessObject, Object underlying)
    {
        return putUsingUnderlying(businessObject, underlying);
    }

    public List<T> removeAll(Filter filter)
    {
        FastList<T> result = new FastList<T>();
        Object[] set = this.table;
        for (int i = set.length; i-- > 0; )
        {
            if (set[i] != FREE && set[i] != REMOVED && filter.matches(set[i]))
            {
                result.add((T) set[i]);
                removeAt(i);
            }
        }
        return result;
    }

    public boolean sizeRequiresWriteLock()
    {
        return false;
    }

    public void ensureCapacity(int capacity)
    {
        int newCapacity = Integer.highestOneBit(scaledByLoadFactor(capacity));
        if (newCapacity < capacity) newCapacity = newCapacity << 1;
        if (newCapacity > table.length) rehashForUnderlying(newCapacity, null, null);
    }

    public void ensureExtraCapacity(int capacity)
    {
        ensureCapacity(capacity + this.size());
    }

    public T put(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return putUsingUnderlying(key, underlying);
    }

    public T putUsingUnderlying(Object key, Object underlying)
    {
        Object result = null;
        int index = insertionIndex(underlying);

        if (index < 0)
        {
            index = -(index + 1);
            result = table[index];
            table[index] = key;
        }
        else
        {
            Object old = table[index];
            table[index] = key;
            if (old == FREE)
            {
                free--;
            }

            if (++occupied > maxSize || free == 0)
            {
                int newCapacity = occupied > maxSize ? capacity() << 1 : capacity();
                rehashForUnderlying(newCapacity, key, underlying);
            }
        }
        if (this.any == null) this.any = key;
        return (T) result;
    }

    protected void rehashForUnderlying(int newCapacity, Object businessObject, Object underlying)
    {
        int oldCapacity = table.length;
        Object oldSet[] = table;

        allocateTable(newCapacity);

        for (int i = oldCapacity; i-- > 0; )
        {
            if (oldSet[i] != FREE && oldSet[i] != REMOVED)
            {
                Object o = oldSet[i];
                Object underlyingObject;
                if (o != businessObject)
                {
                    underlyingObject = this.underlyingObjectGetter.getUnderlyingObject(o);
                    int index = insertionIndex(underlyingObject);

                    // This is for debugging purposes
                    if (index < 0)
                    {
                        this.reportBadObject(underlyingObject, oldSet);
                    }

                    table[index] = o;
                }
            }
        }
        if (underlying != null)
        {
            int index = insertionIndex(underlying);
            if (index < 0)
            {
                reportBadObject(underlying, oldSet);
            }

            table[index] = businessObject;
        }
        computeMaxSize(capacity());
    }

    private void reportBadObject(Object underlyingObject, Object[] oldSet)
    {
        logger.error("Problematic object primary key: " + this.asPrintable(underlyingObject));
        logger.error("Content of Full Unique Index: ");
        for (int k = 0; k < oldSet.length; k++)
        {
            Object obj = oldSet[k];
            if (obj != FREE && obj != REMOVED)
            {
                Object underObj = this.underlyingObjectGetter.getUnderlyingObject(obj);
                logger.error("Index location " + k + " - primaryKey: " + this.asPrintable(underObj));
            }
            else
            {
                logger.error("Object in index location " + k + " has been REMOVED or the location is FREE");
            }
        }
    }

    private String asPrintable(Object obj)
    {
        if (obj instanceof MithraObject)
        {
            obj = ((MithraObject) obj).zGetCurrentData();
        }
        if (obj instanceof MithraDataObject)
        {
            return ((MithraDataObject) obj).zGetPrintablePrimaryKey();
        }

        return obj.toString();
    }

    protected void rehashForReindex(int newCapacity, Object businessObject, AttributeUpdateWrapper updateWrapper)
    {
        int oldCapacity = table.length;
        Object oldSet[] = table;

        allocateTable(newCapacity);

        for (int i = oldCapacity; i-- > 0; )
        {
            if (oldSet[i] != FREE && oldSet[i] != REMOVED)
            {
                Object o = oldSet[i];
                int index = (o == businessObject)
                        ? updatedInsertionIndex(businessObject, updateWrapper)
                        : insertionIndex(this.underlyingObjectGetter.getUnderlyingObject(o));

                table[index] = o;
            }
        }
        computeMaxSize(capacity());
    }

    public T getFirst()
    {
        if (any != null) return (T) any;
        for (int i = table.length - 1; i >= 0; i--) //we loop backwards because of an odd use case when clearing a large non-unique index via reload
        {
            Object cur = table[i];
            if (cur != null && cur != FREE && cur != REMOVED)
            {
                this.any = cur;
                return (T) cur;
            }
        }
        return null;
    }

    public void clear()
    {
        occupied = 0;
        free = (capacity() + maxSize) >> 1;
        Object[] set = this.table;

        for (int i = set.length; i-- > 0; )
        {
            set[i] = FREE;
        }
    }

    public T remove(Object key)
    {
        return this.removeUsingUnderlying(this.underlyingObjectGetter.getUnderlyingObject(key));
    }

    public T removeUsingUnderlying(Object underlying)
    {
        if (this.occupied == 0) return null;
        int index = index(underlying);
        if (index >= 0)
        {
            return removeAt(index);
        }
        return null;
    }

    public Object markDirty(MithraDataObject object)
    {
        throw new RuntimeException("this should be call only on a primary key index of a partial cache");
    }

    public Object getFromDataEvenIfDirty(Object data, NonNullMutableBoolean isDirty)
    {
        isDirty.value = false;
        return this.getFromData(data);
    }

    public FullUniqueIndex<T> copy()
    {
        FullUniqueIndex<T> result = new FullUniqueIndex<T>(this.hashStrategy, this.underlyingObjectGetter, this.loadFactor);

        result.occupied = this.occupied;
        result.free = this.free;
        result.maxSize = this.maxSize;
        result.rightShift = this.rightShift;

        result.table = new Object[this.table.length];

        System.arraycopy(this.table, 0, result.table, 0, this.table.length);

        return result;
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

    public Extractor[] getExtractors()
    {
        return this.hashStrategy.getExtractors();
    }

    public HashStrategy getHashStrategy()
    {
        return this.hashStrategy;
    }

    public Object getUnderlyingObject(Object o)
    {
        return o;
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        this.underlyingObjectGetter = underlyingObjectGetter;
    }

    public SetLikeIdentityList<T> addAndGrow(T toAdd)
    {
        this.put(toAdd);
        return this;
    }

    public Object removeAndShrink(T toRemove)
    {
        this.remove(toRemove);
        if (this.size() < 10)
        {
            return new ArraySetLikeIdentityList<T>(this.getAll());
        }
        return this;
    }

    public void addAll(Collection<T> c)
    {
        if (c instanceof RandomAccess)
        {
            List list = (List) c;
            int size = c.size();
            for (int i = 0; i < size; i++)
            {
                this.put(list.get(i));
            }
        }
        else
        {
            for (T t : c)
            {
                this.put(t);
            }
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

    public boolean equalsByExtractedValues(FullUniqueIndex other)
    {
        if (this.size() != other.size()) return false;
        Object[] set = this.table;
        Extractor[] thisExtractors = this.hashStrategy.getExtractors();
        for (int i = 0; i < set.length; i++)
        {
            if (set[i] != FREE && set[i] != REMOVED)
            {
                if (other.get(set[i], thisExtractors) == null) return false;
            }
        }
        return true;
    }

    public int roughHashCode()
    {
        int hash = this.size();
        Object[] set = this.table;
        if (hash < 10000)
        {
            for (int i = 0; i < set.length; i++)
            {
                if (set[i] != FREE && set[i] != REMOVED)
                {
                    hash += this.hashStrategy.computeHashCode(set[i]);
                }
            }
        }
        else
        {
            for (int i = 0; i < set.length; i++)
            {
                if (set[i] != FREE && set[i] != REMOVED)
                {
                    hash = HashUtil.combineHashes(hash, i);
                }
            }
        }
        return hash;
    }

    public void forAllInParallel(final ParallelProcedure procedure)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), this.table.length, 1);
        final ArrayBasedQueue queue = new ArrayBasedQueue(this.table.length, threadChunkSize.getChunkSize());
        int threads = threadChunkSize.getThreads();
        procedure.setThreads(threads, this.occupied / threads);
        CpuBoundTask[] tasks = new CpuBoundTask[threads];
        for (int i = 0; i < threads; i++)
        {
            final int thread = i;
            tasks[i] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    ArrayBasedQueue.Segment segment = queue.borrow(null);
                    while (segment != null)
                    {
                        for (int i = segment.getStart(); i < segment.getEnd(); i++)
                        {
                            Object e = table[i];
                            if (e == null) continue;
                            if (e != FREE && e != REMOVED)
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
