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

package com.gs.fw.common.mithra.cache.offheap;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.EstimateDistribution;
import com.gs.fw.common.mithra.util.Filter2;
import com.gs.fw.common.mithra.util.HashUtil;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;



public class NonUniqueOffHeapIndex implements IterableNonUniqueIndex, UnderlyingObjectGetter
{
    private static final int FREE = 0;

    private static final int UPPER_BIT_MASK = 0xC0000000;
    private static final int UNIQUE_INDEX_PATTERN = 0x80000000;
    private static final int CHAINED_BUCKET_PATTERN = 0x40000000;

    private OffHeapDataStorage dataStorage;
    private OffHeapIntArrayStorage storage = new FastUnsafeOffHeapIntArrayStorage();
    private int arrayRef;
    private ExtractorBasedOffHeapHashStrategy hashStrategy;
    private Extractor[] onHeapIndexExtractors;
    private OffHeapExtractor[] indexExtractors;

    private int nonUniqueSize;
    private int uniqueSize;
    private int largest;

    public int getNonUniqueSize()
    {
        return nonUniqueSize;
    }

    private int getTableAt(int index)
    {
        return storage.getInt(arrayRef, index);
    }

    private void setTableAt(int index, int value)
    {
        storage.setInt(arrayRef, index, value);
    }

    private void setChainAt(int index, int chainRef)
    {
        storage.setInt(arrayRef, index, chainRef | CHAINED_BUCKET_PATTERN);
    }

    private void setUniqueIndexAt(int index, int indexRef)
    {
        storage.setInt(arrayRef, index, indexRef | UNIQUE_INDEX_PATTERN);
    }

    private int getTableLength()
    {
        return storage.getLength(arrayRef);
    }

    private boolean isChainedBucket(int value)
    {
        return (value & UPPER_BIT_MASK) == CHAINED_BUCKET_PATTERN;
    }

    private boolean isUniqueIndex(int value)
    {
        return (value & UPPER_BIT_MASK) == UNIQUE_INDEX_PATTERN;
    }

    public NonUniqueOffHeapIndex(Extractor[] onHeapIndexExtractors, int initialCapacity, OffHeapDataStorage dataStorage)
    {
        this.dataStorage = dataStorage;
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal initial capacity: " +
                                               initialCapacity);
        this.onHeapIndexExtractors = onHeapIndexExtractors; //get off heap extractors instead, but keep these for the external view
        this.indexExtractors = ExtractorBasedOffHeapHashStrategy.makeOffHeap(onHeapIndexExtractors);
        hashStrategy = ExtractorBasedOffHeapHashStrategy.create(onHeapIndexExtractors);
        // Find a power of 2 >= initialCapacity
        int capacity = 4;
        while (capacity < initialCapacity)
            capacity <<= 1;

        this.arrayRef = storage.allocate(capacity);
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
        return this.onHeapIndexExtractors;
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
        int uniqueSize = this.uniqueSize;
        int nonUniqueSize = getNonUniqueSize();
        if (nonUniqueSize == 0) return 0;
        int result = uniqueSize/nonUniqueSize;
        if (result * nonUniqueSize < uniqueSize)
        {
            result++;
        }
        return result;
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        int uniqueSize = this.uniqueSize;
        int nonUniqueSize = getNonUniqueSize();
        if (multiplier >= nonUniqueSize)
        {
            return uniqueSize;
        }

        int avgSize = getAverageReturnSize();
        return EstimateDistribution.estimateMaxReturnSize(multiplier, largest != FREE ? UniqueOffHeapIntIndex.size(storage, largest) : avgSize << 2, uniqueSize, avgSize);
    }

    public boolean isUnique()
    {
        return false;
    }

    protected int getOne(int o)
    {
        if (isUniqueIndex(o))
        {
            o = UniqueOffHeapIntIndex.getFirst(storage, o & ~UPPER_BIT_MASK);
        }
        return o;
    }

    private Object convertToObject(int cur)
    {
        if (isUniqueIndex(cur))
        {
            return UniqueOffHeapIntIndex.getAllAsList(storage, cur & ~UPPER_BIT_MASK, dataStorage);
        }
        return dataStorage.getDataAsObject(cur);
    }

    private boolean containsInList(int cur, Object keyHolder, Filter2 filter)
    {
        if (filter == null)
        {
            return !isUniqueIndex(cur) || UniqueOffHeapIntIndex.size(storage, cur & ~UPPER_BIT_MASK) > 0;
        }
        if (isUniqueIndex(cur))
        {
            return UniqueOffHeapIntIndex.contains(storage, cur & ~UPPER_BIT_MASK, dataStorage, keyHolder, filter);
        }
        return filter.matches(dataStorage.getDataAsObject(cur), keyHolder);
    }

    public Object get(int key)
    {
        OffHeapIntExtractor extractor = (OffHeapIntExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, key, extractor);
        }
        else if (cur != FREE && key == extractor.intValueOf(dataStorage, getOne(cur)))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, int key, OffHeapIntExtractor extractor)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (key == extractor.intValueOf(dataStorage, getOne(cur))) return convertToObject(cur);
        }
        return null;
    }

    public Object get(char key)
    {
        OffHeapCharExtractor extractor = (OffHeapCharExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, key, extractor);
        }
        else if (cur != FREE && key == extractor.charValueOf(dataStorage, getOne(cur)))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, char key, OffHeapCharExtractor extractor)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (key == extractor.charValueOf(dataStorage, getOne(cur))) return convertToObject(cur);
        }
        return null;
    }

    public Object get(Object key)
    {
        OffHeapExtractor extractor = indexExtractors[0];
        int hash = extractor.computeHashFromValue(key);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, key, extractor);
        }
        else if (cur != FREE && extractor.valueEquals(dataStorage, getOne(cur), key))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, Object key, OffHeapExtractor extractor)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (extractor.valueEquals(dataStorage, getOne(cur), key)) return convertToObject(cur);
        }
        return null;
    }

    public Object get(byte[] key)
    {
        throw new RuntimeException("should not get here");
    }

    public Object get(long key)
    {
        OffHeapLongExtractor extractor = (OffHeapLongExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, key, extractor);
        }
        else if (cur != FREE && key == extractor.longValueOf(dataStorage, getOne(cur)))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, long key, OffHeapLongExtractor extractor)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (key == extractor.longValueOf(dataStorage, getOne(cur))) return convertToObject(cur);
        }
        return null;
    }

    public Object get(double key)
    {
        OffHeapDoubleExtractor extractor = (OffHeapDoubleExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, key, extractor);
        }
        else if (cur != FREE && key == extractor.doubleValueOf(dataStorage, getOne(cur)))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, double key, OffHeapDoubleExtractor extractor)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (key == extractor.doubleValueOf(dataStorage, getOne(cur))) return convertToObject(cur);
        }
        return null;
    }

    public Object get(float key)
    {
        OffHeapFloatExtractor extractor = (OffHeapFloatExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, key, extractor);
        }
        else if (cur != FREE && key == extractor.floatValueOf(dataStorage, getOne(cur)))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, float key, OffHeapFloatExtractor extractor)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (key == extractor.floatValueOf(dataStorage, getOne(cur))) return convertToObject(cur);
        }
        return null;
    }

    public Object get(boolean key)
    {
        OffHeapBooleanExtractor extractor = (OffHeapBooleanExtractor) indexExtractors[0];
        int hash = HashUtil.hash(key);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, key, extractor);
        }
        else if (cur != FREE && key == extractor.booleanValueOf(dataStorage, getOne(cur)))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, boolean key, OffHeapBooleanExtractor extractor)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (key == extractor.booleanValueOf(dataStorage, getOne(cur))) return convertToObject(cur);
        }
        return null;
    }

    public Object get(Object valueHolder, List extractors)
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, valueHolder, extractors);
        }
        else if (cur != FREE && hashStrategy.equals(dataStorage, getOne(cur), valueHolder, extractors))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, Object valueHolder, List extractors)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (hashStrategy.equals(dataStorage, getOne(cur), valueHolder, extractors)) return convertToObject(cur);
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

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getFromChained(cur & ~UPPER_BIT_MASK, valueHolder, extractors);
        }
        else if (cur != FREE && hashStrategy.equals(dataStorage, getOne(cur), valueHolder, extractors))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getFromChained(int bucket, Object valueHolder, Extractor[] extractors)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (hashStrategy.equals(dataStorage, getOne(cur), valueHolder, extractors)) return convertToObject(cur);
        }
        return null;
    }

    public boolean contains (Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        int hash = this.hashStrategy.computeHashCode(keyHolder, extractors);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return containsInChained(cur & ~UPPER_BIT_MASK, keyHolder, extractors, filter);
        }
        else if (cur != FREE && hashStrategy.equals(dataStorage, getOne(cur), keyHolder, extractors))
        {
            return containsInList(cur, keyHolder, filter);
        }
        return false;
    }

    private boolean containsInChained(int bucket, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            final int object = getOne(cur);
            if (hashStrategy.equals(dataStorage, object, keyHolder, extractors))
            {
                return containsInList(cur, keyHolder, filter);
            }
        }
        return false;
    }

    public void findAndExecute(Object valueHolder, Extractor[] extractors, DoUntilProcedure procedure)
    {
        int hash = this.hashStrategy.computeHashCode(valueHolder, extractors);

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            findAndExecuteChained(cur & ~UPPER_BIT_MASK, valueHolder, extractors, procedure);
        }
        else if (cur != FREE && hashStrategy.equals(dataStorage, getOne(cur), valueHolder, extractors))
        {
            if (isUniqueIndex(cur))
            {
                UniqueOffHeapIntIndex.forAll(storage, cur & ~UPPER_BIT_MASK, dataStorage, procedure);
            }
            else
            {
                procedure.execute(dataStorage.getDataAsObject(cur));
            }
        }
    }

    private void findAndExecuteChained(int bucket, Object valueHolder, Extractor[] extractors, DoUntilProcedure procedure)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (hashStrategy.equals(dataStorage, getOne(cur), valueHolder, extractors))
            {
                if (isUniqueIndex(cur))
                {
                    UniqueOffHeapIntIndex.forAll(storage, cur & ~UPPER_BIT_MASK, dataStorage, procedure);
                }
                else
                {
                    procedure.execute(dataStorage.getDataAsObject(cur));
                }
                return;
            }
        }
    }

    public Object put(Object key)
    {
        return putUsingUnderlying(key, key);
    }

    public Object putUsingUnderlying(Object key, Object underlying)
    {
        assert key == underlying;

        MithraOffHeapDataObject data = (MithraOffHeapDataObject) key;
        int dataOffset = data.zGetOffset();

        int hash = hashStrategy.computeHashCode(dataStorage, dataOffset);
        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (cur == FREE)
        {
            this.setTableAt(index, dataOffset);
            uniqueSize++;
            nonUniqueSize++;
        }
        else if (cur == dataOffset)
        {
            return key;
        }
        else if (isChainedBucket(cur))
        {
            this.setChainAt(index, putUsingUnderlyingIntoChain(index, cur & ~UPPER_BIT_MASK, dataOffset));
        }
        else if (hashStrategy.equals(dataStorage, getOne(cur), dataOffset))
        {
            int uniqueIndexRef;
            boolean setLargest = false;
            if (isUniqueIndex(cur))
            {
                uniqueIndexRef = cur & ~UPPER_BIT_MASK;
                setLargest = largest == uniqueIndexRef;
                uniqueSize -= UniqueOffHeapIntIndex.size(storage, uniqueIndexRef);
            }
            else
            {
                uniqueIndexRef = UniqueOffHeapIntIndex.allocate(storage, 4);
                uniqueIndexRef = UniqueOffHeapIntIndex.put(storage, uniqueIndexRef, cur);
                uniqueSize--;
            }
            uniqueIndexRef = UniqueOffHeapIntIndex.put(storage, uniqueIndexRef, dataOffset);
            setUniqueIndexAt(index, uniqueIndexRef);
            int setSize = UniqueOffHeapIntIndex.size(storage, uniqueIndexRef);
            uniqueSize += setSize;
            if (setLargest || largest == 0 || setSize > UniqueOffHeapIntIndex.size(storage, largest))
            {
                largest = uniqueIndexRef;
            }

            return key;
        }
        else
        {
            int bucket = ChainedBucket.allocate(storage);
            bucket = ChainedBucket.add(storage, bucket, cur);
            bucket = ChainedBucket.add(storage, bucket, dataOffset);
            this.setChainAt(index, bucket);
            uniqueSize++;
            nonUniqueSize++;
        }
        int threshold = length >> 1;
        threshold += (threshold >> 1); // threshold = length * 0.75 = length/2 + length/4
        if (getNonUniqueSize() > threshold)
        {
            int newCapacity = length << 1;
            resize(newCapacity);
        }
        return key;
    }

    private int putUsingUnderlyingIntoChain(int index, int bucket, int value)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (hashStrategy.equals(dataStorage, getOne(cur), value))
            {
                int uniqueIndexRef;
                boolean setLargest = false;
                if (isUniqueIndex(cur))
                {
                    uniqueIndexRef = cur & ~UPPER_BIT_MASK;
                    uniqueSize -= UniqueOffHeapIntIndex.size(storage, uniqueIndexRef);
                    setLargest = largest == uniqueIndexRef;
                }
                else
                {
                    uniqueIndexRef = UniqueOffHeapIntIndex.allocate(storage, 4);
                    uniqueIndexRef = UniqueOffHeapIntIndex.put(storage, uniqueIndexRef, cur);
                    uniqueSize--;
                }
                uniqueIndexRef = UniqueOffHeapIntIndex.put(storage, uniqueIndexRef, value);
                ChainedBucket.setAt(storage, bucket, i, uniqueIndexRef | UNIQUE_INDEX_PATTERN);
                int setSize = UniqueOffHeapIntIndex.size(storage, uniqueIndexRef);
                uniqueSize += setSize;
                if (setLargest || largest == 0 || setSize > UniqueOffHeapIntIndex.size(storage, largest))
                {
                    largest = uniqueIndexRef;
                }
                return bucket;
            }
        }
        bucket = ChainedBucket.add(storage, bucket, value);
        setChainAt(index, bucket);
        uniqueSize++;
        nonUniqueSize++;

        return bucket;
    }

    protected void resize(int newCapacity)
    {
        int oldArrayRef = this.arrayRef;
        int oldLength = storage.getLength(oldArrayRef);
        this.arrayRef = storage.allocate(newCapacity);

        int shiftBit = (newCapacity >> 1);

        for(int i=0; i < oldLength; i++)
        {
            int cur = storage.getInt(oldArrayRef, i);
            if (cur != FREE)
            {
                if (isChainedBucket(cur))
                {
                    transferChain(cur & ~UPPER_BIT_MASK, shiftBit, i);
                }
                else
                {
                    int hash = hashStrategy.computeHashCode(dataStorage, getOne(cur));
                    int index = hash & (newCapacity - 1);
                    setTableAt(index, cur);
                }
            }
        }
        storage.free(oldArrayRef);
        copyIntoNewStorage();
    }

    private void copyIntoNewStorage()
    {
//        OffHeapIntArrayStorage newStorage = new FastUnsafeOffHeapIntArrayStorage(storage.getSizeInBytes());
//        int newArrayRef = newStorage.copyFrom(storage, this.arrayRef);
//        int length = newStorage.getLength(newArrayRef);
//        for(int i=0;i<length;i++)
//        {
//            int cur = newStorage.getInt(newArrayRef, i);
//            if ()
//        }
    }

    private void transferChain(int bucket, int shiftBit, int index)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        if (size == 1)
        {
            int cur = ChainedBucket.getAt(storage, bucket, 0);
            int hash = hashStrategy.computeHashCode(dataStorage, getOne(cur));
            setTableAt(index | (hash & shiftBit), cur);
            // the line above is the same as this:
/*
            if ((hash & shiftBit) == 0)
            {
                setTableAt(index, cur);
            }
            else
            {
                setTableAt(index | shiftBit, cur);
            }
*/
            storage.free(bucket);
            return;
        }

        int otherChain = ChainedBucket.allocate(storage, size);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            int hash = hashStrategy.computeHashCode(dataStorage, getOne(cur));
            if ((hash & shiftBit) != 0)
            {
                otherChain = ChainedBucket.add(storage, otherChain, cur);
                ChainedBucket.removeAt(storage, bucket, i);
                size--;
                i--;
            }
        }
        if (size == 0)
        {
            storage.free(bucket);
        }
        else if (size == 1)
        {
            setTableAt(index, ChainedBucket.getAt(storage, bucket, 0));
            storage.free(bucket);
        }
        else
        {
            setChainAt(index, bucket);
        }
        int otherSize = ChainedBucket.getSize(storage, otherChain);
        if (otherSize == 0)
        {
            storage.free(otherChain);
        }
        else if (otherSize == 1)
        {
            setTableAt(index | shiftBit, ChainedBucket.getAt(storage, otherChain, 0));
            storage.free(otherChain);
        }
        else
        {
            setChainAt(index | shiftBit, otherChain);
        }
    }

    public Object getNulls()
    {
        int hash = HashUtil.NULL_HASH;

        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (isChainedBucket(cur))
        {
            return getNullsFromChained(cur & ~UPPER_BIT_MASK);
        }
        else if (cur != FREE && indexExtractors[0].isAttributeNull(dataStorage, getOne(cur)))
        {
            return convertToObject(cur);
        }
        return null;
    }

    private Object getNullsFromChained(int bucket)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (indexExtractors[0].isAttributeNull(dataStorage, getOne(cur))) return convertToObject(cur);
        }
        return null;
    }

    public Object remove(Object key)
    {
        return this.removeUsingUnderlying(key);
    }

    public Object removeUsingUnderlying(Object key)
    {
        MithraOffHeapDataObject data = (MithraOffHeapDataObject) key;
        int dataOffset = data.zGetOffset();

        int hash = hashStrategy.computeHashCode(dataStorage, dataOffset);
        int length = getTableLength();
        int index = hash & (length - 1);
        int cur = getTableAt(index);

        if (cur != FREE)
        {
            if (cur == dataOffset)
            {
                setTableAt(index, FREE);
                uniqueSize--;
                nonUniqueSize--;
                return key;
            }
            else if (isChainedBucket(cur))
            {
                return removeFromChain(cur & ~UPPER_BIT_MASK, dataOffset, index, key);
            }
            else if (isUniqueIndex(cur))
            {
                int uniqueIndexRef = cur & ~UPPER_BIT_MASK;
                if (UniqueOffHeapIntIndex.remove(storage, uniqueIndexRef, dataOffset))
                {
                    uniqueSize--;
                    if (UniqueOffHeapIntIndex.size(storage, uniqueIndexRef) == 0)
                    {
                        storage.free(uniqueIndexRef);
                        setTableAt(index, FREE);
                        nonUniqueSize--;
                        if (largest == uniqueIndexRef)
                        {
                            largest = FREE;
                        }
                    }
                    return key;
                }
            }
        }
        return null;
    }

    private Object removeFromChain(int bucket, int value, int index, Object key)
    {
        int size = ChainedBucket.getSize(storage, bucket);
        for(int i=0;i< size;i++)
        {
            int cur = ChainedBucket.getAt(storage, bucket, i);
            if (cur == value)
            {
                ChainedBucket.removeAt(storage, bucket, i);
                uniqueSize--;
                nonUniqueSize--;
                if (size == 1) freeChain(bucket, index);
                return key;
            }
            else if (isUniqueIndex(cur))
            {
                int uniqueIndexRef = cur & ~UPPER_BIT_MASK;
                if (UniqueOffHeapIntIndex.remove(storage, uniqueIndexRef, value))
                {
                    uniqueSize--;
                    if (UniqueOffHeapIntIndex.size(storage, uniqueIndexRef) == 0)
                    {
                        storage.free(uniqueIndexRef);
                        ChainedBucket.removeAt(storage, bucket, i);
                        nonUniqueSize--;
                        if (largest == uniqueIndexRef)
                        {
                            largest = FREE;
                        }
                        if (size == 1) freeChain(bucket, index);
                    }
                    return key;
                }
            }

        }
        return null;
    }

    private void freeChain(int bucket, int index)
    {
        storage.free(bucket);
        setTableAt(index, FREE);
    }

    public void clear()
    {
        int size = storage.getLength(arrayRef);
        for(int i=0; i < size; i++)
        {
            int cur = storage.getInt(arrayRef, i);
            if ((cur & UPPER_BIT_MASK) != 0)
            {
                storage.free(cur & ~UPPER_BIT_MASK);
            }
        }
        storage.clear(arrayRef);
        nonUniqueSize = 0;
        uniqueSize = 0;
        largest = FREE;

    }

    public Object getUnderlyingObject(Object o)
    {
        return o;
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        throw new RuntimeException("not implemented");
    }

    public void destroy()
    {
        this.storage.destroy();
        this.storage = null;
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {
        String msg = className+ " index on ";
        for(Extractor e: onHeapIndexExtractors)
        {
            msg += ((Attribute) e).getAttributeName()+", ";
        }
        this.storage.reportSpaceUsage(logger, msg);
    }

    @Override
    public void ensureExtraCapacity(int size)
    {
        this.storage.ensureCapacity((size+this.size())*12L);
    }

    private static final class ChainedBucket extends OffHeapIntList
    {
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

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return this.storage.getAllocatedSize();
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return this.storage.getUsedSize();
    }
}
