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


public class OffHeapIntList
{
    public static final int FREE = 0;

    public static int add(OffHeapIntArrayStorage storage, int listRef, int value)
    {
        int used = getSize(storage, listRef);
        int length = storage.getLength(listRef) - 1;
        if (used == length)
        {
            int newSize = length == 2 ? 4 : (length + 1) << 1; // keep things power of 2
            listRef = storage.reallocate(listRef, newSize);
        }
        setAt(storage, listRef, used, value);
        storage.incrementAndGet(listRef, 0, 1);
        return listRef;
    }

    public static int getSize(OffHeapIntArrayStorage storage, int listRef)
    {
        return storage.getInt(listRef, 0);
    }

    public static int getAt(OffHeapIntArrayStorage storage, int listRef, int pos)
    {
        return storage.getInt(listRef, pos + 1);
    }

    public static void setAt(OffHeapIntArrayStorage storage, int listRef, int pos, int value)
    {
        storage.setInt(listRef, pos + 1, value);
    }

    public static int allocateWithTwoElements(OffHeapIntArrayStorage storage, int one, int two)
    {
        int chainRef = storage.allocate(3);
        setAt(storage, chainRef, 0, one);
        setAt(storage, chainRef, 1, two);
        storage.incrementAndGet(chainRef, 0, 2); // set the size to 2
        return chainRef;
    }

    public static int allocate(OffHeapIntArrayStorage storage)
    {
        return storage.allocate(3);
    }

    public static int allocate(OffHeapIntArrayStorage storage, int size)
    {
        return storage.allocate(size + 1);
    }

    public static void removeAt(OffHeapIntArrayStorage storage, int listRef, int index)
    {
        int size = getSize(storage, listRef);
        OffHeapIntList.setAt(storage, listRef, index, OffHeapIntList.getAt(storage, listRef, size - 1));
        OffHeapIntList.setAt(storage, listRef, size - 1, FREE);
        storage.incrementAndGet(listRef, 0, -1);
    }

    public static int getFirst(OffHeapIntArrayStorage storage, int listRef)
    {
        return storage.getInt(listRef, 1);
    }

    public static boolean contains(OffHeapIntArrayStorage storage, int listRef, int value)
    {
        int size = getSize(storage, listRef);
        for (int i = 0; i < size; i++)
        {
            int cur = getAt(storage, listRef, i);
            if (cur == value)
            {
                return true;
            }
        }
        return false;
    }
}
