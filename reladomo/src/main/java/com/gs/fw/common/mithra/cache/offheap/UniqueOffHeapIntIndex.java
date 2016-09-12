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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter2;

import java.util.List;


public class UniqueOffHeapIntIndex
{
    private static final int FREE = 0;
    private static final int UPPER_BIT_MASK = 0x80000000;

    private static final int SIZE_INDEX = 0;
    private static final int ANY_INDEX = 1;
    private static final int TABLE_INDEX = 2;

    private UniqueOffHeapIntIndex()
    {
    }

    /*
    The array has the following structure:
    0: size (# of items currently held)
    1: any object
    2+: the actual data
     */

    private static int getSize(OffHeapIntArrayStorage storage, int arrayRef)
    {
        return storage.getInt(arrayRef, SIZE_INDEX);
    }

    private static int getAny(OffHeapIntArrayStorage storage, int arrayRef)
    {
        return storage.getInt(arrayRef, ANY_INDEX);
    }

    private static int getTableAt(OffHeapIntArrayStorage storage, int arrayRef, int tablePos)
    {
        return storage.getInt(arrayRef, tablePos + TABLE_INDEX);
    }

    public static int allocate(OffHeapIntArrayStorage storage, int initialCapacity)
    {
        int capacity = 1;
        while (capacity < initialCapacity)
        {
            capacity <<= 1;
        }

        return storage.allocate((capacity << 1) + TABLE_INDEX);
    }

    public static boolean isEmpty(OffHeapIntArrayStorage storage, int arrayRef)
    {
        return 0 == getSize(storage, arrayRef);
    }

    public static int size(OffHeapIntArrayStorage storage, int arrayRef)
    {
        return getSize(storage, arrayRef);
    }

    private static int getTableLength(OffHeapIntArrayStorage storage, int arrayRef)
    {
        return storage.getLength(arrayRef) - TABLE_INDEX;
    }

    private static void setTableAt(OffHeapIntArrayStorage storage, int arrayRef, int index, int value)
    {
        storage.setInt(arrayRef, index + TABLE_INDEX, value);
    }

    private static int incrementSizeAndResizeIfNecessary(OffHeapIntArrayStorage storage, int arrayRef, int length)
    {
        int currentSize = storage.incrementAndGet(arrayRef, SIZE_INDEX, 1);
        int threshold = length >> 1;
        threshold += (threshold >> 1); // threshold = length * 0.75 = length/2 + length/4
        if (currentSize > threshold)
        {
            return resize(storage, arrayRef, length);
        }
        return arrayRef;
    }

    private static int resize(OffHeapIntArrayStorage storage, int arrayRef, int length)
    {
        int newArrayRef = storage.allocate((length << 1) + TABLE_INDEX);
        for (int i = 0; i < length; i++)
        {
            int cur = getTableAt(storage, arrayRef, i);
            if (cur != FREE)
            {
                if ((cur & UPPER_BIT_MASK) == 0)
                {
                    put(storage, newArrayRef, cur);
                }
                else
                {
                    transferChain(storage, newArrayRef, cur & ~UPPER_BIT_MASK);
                }
            }
        }
        storage.free(arrayRef);
        return newArrayRef;
    }

    private static void transferChain(OffHeapIntArrayStorage storage, int newArrayRef, int chain)
    {
        int length = ChainedBucket.getSize(storage, chain);
        for (int i = 0; i < length; i++)
        {
            put(storage, newArrayRef, ChainedBucket.getAt(storage, chain, i));
        }
        storage.free(chain);
    }

    public static int getFirst(OffHeapIntArrayStorage storage, int arrayRef)
    {
        if (isEmpty(storage, arrayRef))
        {
            return 0;
        }
        int any = getAny(storage, arrayRef);
        if (any != 0) return any;
        int length = getTableLength(storage, arrayRef);
        for (int i = length - 1; i >= 0; i--) //we loop backwards because of an odd use case when clearing a large non-unique index via reload
        {
            int cur = getTableAt(storage, arrayRef, i);
            if (cur != FREE)
            {
                if ((cur & UPPER_BIT_MASK) == 0)
                {
                    any = cur;
                }
                else
                {
                    int chain = cur & ~UPPER_BIT_MASK;
                    any = ChainedBucket.getFirst(storage, chain);
                }
                setAny(storage, arrayRef, any);
                return any;
            }
        }
        return 0;
    }

    private static int hash(int key)
    {
        int h = key;
        h ^= (h >>> 20) ^ (h >>> 12);
        h = h ^ (h >>> 7) ^ (h >>> 4);
        return h;
    }

    public static int put(OffHeapIntArrayStorage storage, int arrayRef, int value)
    {
        int hash = hash(value);
        int tableLength = getTableLength(storage, arrayRef);
        int index = hash & (tableLength - 1);
        int existing = getTableAt(storage, arrayRef, index);
        int any = getAny(storage, arrayRef);
        if (any == FREE)
        {
            setAny(storage, arrayRef, value);
        }
        if (existing == value)
        {
            return arrayRef;
        }
        if (existing == FREE)
        {
            setTableAt(storage, arrayRef, index, value);
            return incrementSizeAndResizeIfNecessary(storage, arrayRef, tableLength);
        }
        if ((existing & UPPER_BIT_MASK) == 0)
        {
            int chain = ChainedBucket.allocateWithTwoElements(storage, existing, value);
            setTableAt(storage, arrayRef, index, chain | UPPER_BIT_MASK);
            return incrementSizeAndResizeIfNecessary(storage, arrayRef, tableLength);
        }
        if (putIntoChain(storage, arrayRef, index, existing & ~UPPER_BIT_MASK, value))
        {
            return incrementSizeAndResizeIfNecessary(storage, arrayRef, tableLength);
        }
        return arrayRef;
    }

    private static void setAny(OffHeapIntArrayStorage storage, int arrayRef, int value)
    {
        storage.setInt(arrayRef, ANY_INDEX, value);
    }

    private static boolean putIntoChain(OffHeapIntArrayStorage storage, int arrayRef, int index, int chain, int value)
    {
        if (ChainedBucket.contains(storage, chain, value))
        {
            return false;
        }
        int newChain = ChainedBucket.add(storage, chain, value);
        if (newChain != chain)
        {
            setTableAt(storage, arrayRef, index, newChain | UPPER_BIT_MASK);
        }
        return true;
    }

    public static boolean remove(OffHeapIntArrayStorage storage, int arrayRef, int value)
    {
        int hash = hash(value);
        int tableLength = getTableLength(storage, arrayRef);
        int index = hash & (tableLength - 1);
        int existing = getTableAt(storage, arrayRef, index);
        int any = getAny(storage, arrayRef);
        if (any == value)
        {
            setAny(storage, arrayRef, 0);
        }
        if (existing == value)
        {
            setTableAt(storage, arrayRef, index, FREE);
            storage.incrementAndGet(arrayRef, SIZE_INDEX, -1);
            return true;
        }
        if ((existing & UPPER_BIT_MASK) == 0) // also covers the FREE case
        {
            return false;
        }

        return removeFromChain(storage, arrayRef, index, existing & ~UPPER_BIT_MASK, value);
    }

    private static boolean removeFromChain(OffHeapIntArrayStorage storage, int arrayRef, int index, int chain, int value)
    {
        int usedChain = ChainedBucket.getSize(storage, chain);
        for (int i = 0; i < usedChain; i++)
        {
            int cur = ChainedBucket.getAt(storage, chain, i);
            if (cur == value)
            {
                if (i == 0 && usedChain == 1) //remove the chain
                {
                    storage.free(chain);
                    setTableAt(storage, arrayRef, index, FREE);
                }
                else
                {
                    ChainedBucket.removeAt(storage, chain, i);
                }
                storage.incrementAndGet(arrayRef, SIZE_INDEX, -1);
                return true;
            }
        }
        return false;
    }

    public static boolean doUntil(OffHeapIntArrayStorage storage, int arrayRef, IntegerProcedure proc)
    {
        boolean done = false;
        int tableLength = getTableLength(storage, arrayRef);
        for (int i = 0; !done && i < tableLength; i++)
        {
            int cur = getTableAt(storage, arrayRef, i);
            if (cur != FREE)
            {
                if ((cur & UPPER_BIT_MASK) == 0)
                {
                    done = proc.execute(cur, null);
                }
                else
                {
                    int chain = cur & ~UPPER_BIT_MASK;
                    done = ChainedBucket.doUntilChain(storage, chain, proc);
                }
            }
        }
        return done;
    }

    public static List getAllAsList(OffHeapIntArrayStorage storage, int arrayRef, OffHeapDataStorage dataStorage)
    {
        FastList result = FastList.newList(getSize(storage, arrayRef));
        int tableLength = getTableLength(storage, arrayRef);
        for (int i = 0; i < tableLength; i++)
        {
            int cur = getTableAt(storage, arrayRef, i);
            if (cur != FREE)
            {
                if ((cur & UPPER_BIT_MASK) == 0)
                {
                    result.add(dataStorage.getDataAsObject(cur));
                }
                else
                {
                    int chain = cur & ~UPPER_BIT_MASK;
                    ChainedBucket.getAllAsListFromChain(storage, chain, result, dataStorage);
                }
            }
        }
        return result;

    }

    public static boolean contains(OffHeapIntArrayStorage storage, int arrayRef, OffHeapDataStorage dataStorage, Object keyHolder, Filter2 filter)
    {
        int tableLength = getTableLength(storage, arrayRef);
        for (int i = 0; i < tableLength; i++)
        {
            int cur = getTableAt(storage, arrayRef, i);
            if (cur != FREE)
            {
                if ((cur & UPPER_BIT_MASK) == 0)
                {
                    if (filter.matches(dataStorage.getDataAsObject(cur), keyHolder))
                    {
                        return true;
                    }
                }
                else
                {
                    int chain = cur & ~UPPER_BIT_MASK;
                    if (ChainedBucket.containsInChain(storage, chain, dataStorage, keyHolder, filter))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static void forAll(OffHeapIntArrayStorage storage, int arrayRef, OffHeapDataStorage dataStorage, DoUntilProcedure procedure)
    {
        int tableLength = getTableLength(storage, arrayRef);
        boolean done = false;
        for (int i = 0; i < tableLength && !done; i++)
        {
            int cur = getTableAt(storage, arrayRef, i);
            if (cur != FREE)
            {
                if ((cur & UPPER_BIT_MASK) == 0)
                {
                    done = procedure.execute(dataStorage.getDataAsObject(cur));
                }
                else
                {
                    int chain = cur & ~UPPER_BIT_MASK;
                    done = ChainedBucket.forAllFromChain(storage, chain, dataStorage, procedure);
                }
            }
        }
    }

    private static class ChainedBucket extends OffHeapIntList
    {
        public static void getAllAsListFromChain(OffHeapIntArrayStorage storage, int chain, FastList result, OffHeapDataStorage dataStorage)
        {
            int usedChain = ChainedBucket.getSize(storage, chain);
            for (int i = 0; i < usedChain; i++)
            {
                int cur = ChainedBucket.getAt(storage, chain, i);
                result.add(dataStorage.getDataAsObject(cur));
            }
        }

        public static boolean containsInChain(OffHeapIntArrayStorage storage, int chain, OffHeapDataStorage dataStorage, Object keyHolder, Filter2 filter)
        {
            int usedChain = ChainedBucket.getSize(storage, chain);
            for (int i = 0; i < usedChain; i++)
            {
                int cur = ChainedBucket.getAt(storage, chain, i);
                if (filter.matches(dataStorage.getDataAsObject(cur), keyHolder)) return true;
            }

            return false;
        }

        public static boolean forAllFromChain(OffHeapIntArrayStorage storage, int chain, OffHeapDataStorage dataStorage, DoUntilProcedure procedure)
        {
            boolean done = false;
            int usedChain = ChainedBucket.getSize(storage, chain);
            for (int i = 0; i < usedChain && !done; i++)
            {
                int cur = ChainedBucket.getAt(storage, chain, i);
                done = procedure.execute(dataStorage.getDataAsObject(cur));
            }
            return done;
        }

        public static boolean doUntilChain(OffHeapIntArrayStorage storage, int chain, IntegerProcedure proc)
        {
            int usedChain = ChainedBucket.getSize(storage, chain);
            boolean done = false;
            for (int i = 0; !done && i < usedChain; i++)
            {
                int cur = ChainedBucket.getAt(storage, chain, i);
                done = proc.execute(cur, null);
            }
            return done;
        }
    }

}
