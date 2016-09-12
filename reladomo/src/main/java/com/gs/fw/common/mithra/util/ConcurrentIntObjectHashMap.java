
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

package com.gs.fw.common.mithra.util;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.primitive.IntObjectProcedure;
import com.gs.collections.impl.list.mutable.FastList;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.*;

public final class ConcurrentIntObjectHashMap<V> implements Externalizable
{
    private static final long serialVersionUID = 1L;

    private static final Object RESIZE_SENTINEL = new Object();
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final AtomicReferenceFieldUpdater<ConcurrentIntObjectHashMap, AtomicReferenceArray> TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ConcurrentIntObjectHashMap.class, AtomicReferenceArray.class, "table");
    private static final AtomicIntegerFieldUpdater<ConcurrentIntObjectHashMap> SIZE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ConcurrentIntObjectHashMap.class, "size");
    private static final Object RESIZED = new Object();
    private static final Object RESIZING = new Object();
    private static final int PARTITIONED_SIZE_THRESHOLD = 4096; // chosen to keep size below 1% of the total size of the map
    private static final int SIZE_BUCKETS = 7;

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile AtomicReferenceArray table;

    private AtomicIntegerArray partitionedSize;

    @SuppressWarnings("UnusedDeclaration")
    private volatile int size; // updated via atomic field updater

    public ConcurrentIntObjectHashMap()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public ConcurrentIntObjectHashMap(int initialCapacity)
    {
        if (initialCapacity < 0)
        {
            throw new IllegalArgumentException("Illegal Initial Capacity: " + initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY)
        {
            initialCapacity = MAXIMUM_CAPACITY;
        }

        int threshold = initialCapacity;
        threshold += threshold >> 1; // threshold = length * 0.75

        int capacity = 1;
        while (capacity < threshold)
        {
            capacity <<= 1;
        }
        if (capacity >= PARTITIONED_SIZE_THRESHOLD)
        {
            this.partitionedSize = new AtomicIntegerArray(SIZE_BUCKETS * 16); // we want 7 extra slots and 64 bytes for each slot. int is 4 bytes, so 64 bytes is 16 ints.
        }
        this.table = new AtomicReferenceArray(capacity + 1);
    }

    public static <V> ConcurrentIntObjectHashMap<V> newMap()
    {
        return new ConcurrentIntObjectHashMap<V>();
    }

    public static <V> ConcurrentIntObjectHashMap<V> newMap(int newSize)
    {
        return new ConcurrentIntObjectHashMap<V>(newSize);
    }

    private static int indexFor(int h, int length)
    {
        return h & length - 2;
    }

//    public V putIfAbsent(int key, V value)
//    {
//        int hash = this.hash(key);
//        AtomicReferenceArray currentArray = this.table;
//        while (true)
//        {
//            int length = currentArray.length();
//            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//            Object o = currentArray.get(index);
//            if (o == RESIZED || o == RESIZING)
//            {
//                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
//            }
//            else
//            {
//                Entry<V> e = (Entry<V>) o;
//                while (e != null)
//                {
//                    int candidate = e.getKey();
//                    if (candidate== key)
//                    {
//                        return e.getValue();
//                    }
//                    e = e.getNext();
//                }
//                Entry<V> newEntry = new Entry<V>(key, value, (Entry<V>) o);
//                if (currentArray.compareAndSet(index, o, newEntry))
//                {
//                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
//                    return null; // per the contract of putIfAbsent, we return null when the map didn't have this key before
//                }
//            }
//        }
//    }

    private void incrementSizeAndPossiblyResize(AtomicReferenceArray currentArray, int length, Object prev)
    {
        this.addToSize(1);
        if (prev != null)
        {
            int localSize = this.size();
            int threshold = (length >> 1) + (length >> 2); // threshold = length * 0.75
            if (localSize + 1 > threshold)
            {
                this.resize(currentArray);
            }
        }
    }

    private int hash(int key)
    {
        int h = key;
        h ^= h >>> 20 ^ h >>> 12;
        h ^= h >>> 7 ^ h >>> 4;
        return h;
    }

    private AtomicReferenceArray helpWithResizeWhileCurrentIndex(AtomicReferenceArray currentArray, int index)
    {
        AtomicReferenceArray newArray = this.helpWithResize(currentArray);
        int helpCount = 0;
        while (currentArray.get(index) != RESIZED)
        {
            helpCount++;
            newArray = this.helpWithResize(currentArray);
            if ((helpCount & 7) == 0)
            {
                Thread.yield();
            }
        }
        return newArray;
    }

    private AtomicReferenceArray helpWithResize(AtomicReferenceArray currentArray)
    {
        ResizeContainer resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
        AtomicReferenceArray newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT)
        {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void resize(AtomicReferenceArray oldTable)
    {
        this.resize(oldTable, (oldTable.length() - 1 << 1) + 1);
    }

    // newSize must be a power of 2 + 1
    @SuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    private void resize(AtomicReferenceArray oldTable, int newSize)
    {
        int oldCapacity = oldTable.length();
        int end = oldCapacity - 1;
        Object last = oldTable.get(end);
        if (this.size() < end && last == RESIZE_SENTINEL)
        {
            return;
        }
        if (oldCapacity >= MAXIMUM_CAPACITY)
        {
            throw new RuntimeException("index is too large!");
        }
        ResizeContainer resizeContainer = null;
        boolean ownResize = false;
        if (last == null || last == RESIZE_SENTINEL)
        {
            synchronized (oldTable) // allocating a new array is too expensive to make this an atomic operation
            {
                if (oldTable.get(end) == null)
                {
                    oldTable.set(end, RESIZE_SENTINEL);
                    if (this.partitionedSize == null && newSize >= PARTITIONED_SIZE_THRESHOLD)
                    {
                        this.partitionedSize = new AtomicIntegerArray(SIZE_BUCKETS * 16);
                    }
                    resizeContainer = new ResizeContainer(new AtomicReferenceArray(newSize), oldTable.length() - 1);
                    oldTable.set(end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize)
        {
            this.transfer(oldTable, resizeContainer);
            AtomicReferenceArray src = this.table;
            while (!TABLE_UPDATER.compareAndSet(this, oldTable, resizeContainer.nextArray))
            {
                // we're in a double resize situation; we'll have to go help until it's our turn to set the table
                if (src != oldTable)
                {
                    this.helpWithResize(src);
                }
            }
        }
        else
        {
            this.helpWithResize(oldTable);
        }
    }

    /*
     * Transfer all entries from src to dest tables
     */
    private void transfer(AtomicReferenceArray src, ResizeContainer resizeContainer)
    {
        AtomicReferenceArray dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length() - 1; )
        {
            Object o = src.get(j);
            if (o == null)
            {
                if (src.compareAndSet(j, null, RESIZED))
                {
                    j++;
                }
            }
            else if (o == RESIZED || o == RESIZING)
            {
                j = (j & ~(ResizeContainer.QUEUE_INCREMENT - 1)) + ResizeContainer.QUEUE_INCREMENT;
                if (resizeContainer.resizers.get() == 1)
                {
                    break;
                }
            }
            else
            {
                Entry<V> e = (Entry<V>) o;
                if (src.compareAndSet(j, o, RESIZING))
                {
                    while (e != null)
                    {
                        this.unconditionalCopy(dest, e);
                        e = e.getNext();
                    }
                    src.set(j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }

    private void reverseTransfer(AtomicReferenceArray src, ResizeContainer resizeContainer)
    {
        AtomicReferenceArray dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0)
        {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0)
            {
                if (start < 0)
                {
                    start = 0;
                }
                for (int j = end - 1; j >= start; )
                {
                    Object o = src.get(j);
                    if (o == null)
                    {
                        if (src.compareAndSet(j, null, RESIZED))
                        {
                            j--;
                        }
                    }
                    else if (o == RESIZED || o == RESIZING)
                    {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    }
                    else
                    {
                        Entry<V> e = (Entry<V>) o;
                        if (src.compareAndSet(j, o, RESIZING))
                        {
                            while (e != null)
                            {
                                this.unconditionalCopy(dest, e);
                                e = e.getNext();
                            }
                            src.set(j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    private void unconditionalCopy(AtomicReferenceArray dest, Entry<V> toCopyEntry)
    {
        int hash = this.hash(toCopyEntry.getKey());
        AtomicReferenceArray currentArray = dest;
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            }
            else
            {
                Entry<V> newEntry;
                if (o == null)
                {
                    if (toCopyEntry.getNext() == null)
                    {
                        newEntry = toCopyEntry; // no need to duplicate
                    }
                    else
                    {
                        newEntry = new Entry<V>(toCopyEntry.getKey(), toCopyEntry.getValue());
                    }
                }
                else
                {
                    newEntry = new Entry<V>(toCopyEntry.getKey(), toCopyEntry.getValue(), (Entry<V>) o);
                }
                if (currentArray.compareAndSet(index, o, newEntry))
                {
                    return;
                }
            }
        }
    }

//    public V getIfAbsentPut(int key, Function0<? extends V> factory)
//    {
//        int hash = this.hash(key);
//        AtomicReferenceArray currentArray = this.table;
//        V newValue = null;
//        boolean createdValue = false;
//        while (true)
//        {
//            int length = currentArray.length();
//            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//            Object o = currentArray.get(index);
//            if (o == RESIZED || o == RESIZING)
//            {
//                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
//            }
//            else
//            {
//                Entry<V> e = (Entry<V>) o;
//                while (e != null)
//                {
//                    int candidate = e.getKey();
//                    if (candidate == key)
//                    {
//                        return e.getValue();
//                    }
//                    e = e.getNext();
//                }
//                if (!createdValue)
//                {
//                    createdValue = true;
//                    newValue = factory.value();
//                }
//                Entry<V> newEntry = new Entry<V>(key, newValue, (Entry<V>) o);
//                if (currentArray.compareAndSet(index, o, newEntry))
//                {
//                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
//                    return newValue;
//                }
//            }
//        }
//    }

    public boolean remove(int key, Object value)
    {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        //noinspection LabeledStatement
        outer:
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry<V> e = (Entry<V>) o;
                while (e != null)
                {
                    int candidate = e.getKey();
                    if (candidate == key && this.nullSafeEquals(e.getValue(), value))
                    {
                        Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                        if (currentArray.compareAndSet(index, o, replacement))
                        {
                            this.addToSize(-1);
                            return true;
                        }
                        //noinspection ContinueStatementWithLabel
                        continue outer;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

    private void addToSize(int value)
    {
        if (this.partitionedSize != null)
        {
            if (this.incrementPartitionedSize(value))
            {
                return;
            }
        }
        this.incrementLocalSize(value);
    }

    private boolean incrementPartitionedSize(int value)
    {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & SIZE_BUCKETS;
        if (h != 0)
        {
            h = (h - 1) << 4;
            while (true)
            {
                int localSize = this.partitionedSize.get(h);
                if (this.partitionedSize.compareAndSet(h, localSize, localSize + value))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private void incrementLocalSize(int value)
    {
        while (true)
        {
            int localSize = this.size;
            if (SIZE_UPDATER.compareAndSet(this, localSize, localSize + value))
            {
                break;
            }
        }
    }

    public int size()
    {
        int localSize = this.size;
        if (this.partitionedSize != null)
        {
            for (int i = 0; i < SIZE_BUCKETS; i++)
            {
                localSize += this.partitionedSize.get(i << 4);
            }
        }
        return localSize;
    }

    public boolean isEmpty()
    {
        return this.size() == 0;
    }

    public boolean containsKey(int key)
    {
        return this.getEntry(key) != null;
    }

    public boolean containsValue(Object value)
    {
        AtomicReferenceArray currentArray = this.table;
        ResizeContainer resizeContainer;
        do
        {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++)
            {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING)
                {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                }
                else if (o != null)
                {
                    Entry<V> e = (Entry<V>) o;
                    while (e != null)
                    {
                        Object v = e.getValue();
                        if (this.nullSafeEquals(v, value))
                        {
                            return true;
                        }
                        e = e.getNext();
                    }
                }
            }
            if (resizeContainer != null)
            {
                if (resizeContainer.isNotDone())
                {
                    this.helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        }
        while (resizeContainer != null);
        return false;
    }

    private boolean nullSafeEquals(Object v, Object value)
    {
        return v == value || v != null && v.equals(value);
    }

    public V get(int key)
    {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        int index = ConcurrentIntObjectHashMap.indexFor(hash, currentArray.length());
        Object o = currentArray.get(index);
        if (o == RESIZED || o == RESIZING)
        {
            return this.slowGet(key, hash, index, currentArray);
        }
        for (Entry<V> e = (Entry<V>) o; e != null; e = e.getNext())
        {
            int k;
            if ((k = e.key) == key)
            {
                return e.value;
            }
        }
        return null;
    }

    private V slowGet(int key, int hash, int index, AtomicReferenceArray currentArray)
    {
        while (true)
        {
            int length = currentArray.length();
            index = ConcurrentIntObjectHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry<V> e = (Entry<V>) o;
                while (e != null)
                {
                    int candidate = e.getKey();
                    if (candidate == key)
                    {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    private Entry<V> getEntry(int key)
    {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry<V> e = (Entry<V>) o;
                while (e != null)
                {
                    int candidate = e.getKey();
                    if (candidate == key)
                    {
                        return e;
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    public V put(int key, V value)
    {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null)
        {
            Entry<V> newEntry = new Entry<V>(key, value, null);
            if (currentArray.compareAndSet(index, null, newEntry))
            {
                this.addToSize(1);
                return null;
            }
        }
        return this.slowPut(key, value, hash, currentArray);
    }

    private V slowPut(int key, V value, int hash, AtomicReferenceArray currentArray)
    {
        //noinspection LabeledStatement
        outer:
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry<V> e = (Entry<V>) o;
                while (e != null)
                {
                    int candidate = e.getKey();
                    if (candidate == key)
                    {
                        V oldValue = e.getValue();
                        Entry<V> newEntry = new Entry<V>(e.getKey(), value, this.createReplacementChainForRemoval((Entry<V>) o, e));
                        if (!currentArray.compareAndSet(index, o, newEntry))
                        {
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return oldValue;
                    }
                    e = e.getNext();
                }
                Entry<V> newEntry = new Entry<V>(key, value, (Entry<V>) o);
                if (currentArray.compareAndSet(index, o, newEntry))
                {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return null;
                }
            }
        }
    }

    public void clear()
    {
        AtomicReferenceArray currentArray = this.table;
        ResizeContainer resizeContainer;
        do
        {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++)
            {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING)
                {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                }
                else if (o != null)
                {
                    Entry<V> e = (Entry<V>) o;
                    if (currentArray.compareAndSet(i, o, null))
                    {
                        int removedEntries = 0;
                        while (e != null)
                        {
                            removedEntries++;
                            e = e.getNext();
                        }
                        this.addToSize(-removedEntries);
                    }
                }
            }
            if (resizeContainer != null)
            {
                if (resizeContainer.isNotDone())
                {
                    this.helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        }
        while (resizeContainer != null);
    }

//    public IntSet keySet()
//    {
//        return new KeySet();
//    }

    public Collection<V> values()
    {
        return new Values();
    }

//    public boolean replace(int key, V oldValue, V newValue)
//    {
//        int hash = this.hash(key);
//        AtomicReferenceArray currentArray = this.table;
//        int length = currentArray.length();
//        int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//        Object o = currentArray.get(index);
//        if (o == RESIZED || o == RESIZING)
//        {
//            return this.slowReplace(key, oldValue, newValue, hash, currentArray);
//        }
//        Entry<V> e = (Entry<V>) o;
//        while (e != null)
//        {
//            int candidate = e.getKey();
//            if (candidate == key || candidate == key)
//            {
//                if (oldValue == e.getValue() || (oldValue != null && oldValue.equals(e.getValue())))
//                {
//                    Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
//                    Entry<V> newEntry = new Entry<V>(key, newValue, replacement);
//                    return currentArray.compareAndSet(index, o, newEntry) || this.slowReplace(key, oldValue, newValue, hash, currentArray);
//                }
//                return false;
//            }
//            e = e.getNext();
//        }
//        return false;
//    }

    private boolean slowReplace(int key, V oldValue, V newValue, int hash, AtomicReferenceArray currentArray)
    {
        //noinspection LabeledStatement
        outer:
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry<V> e = (Entry<V>) o;
                while (e != null)
                {
                    int candidate = e.getKey();
                    if (candidate == key || candidate == key)
                    {
                        if (oldValue == e.getValue() || (oldValue != null && oldValue.equals(e.getValue())))
                        {
                            Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                            Entry<V> newEntry = new Entry<V>(key, newValue, replacement);
                            if (currentArray.compareAndSet(index, o, newEntry))
                            {
                                return true;
                            }
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return false;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

//    public V replace(int key, V value)
//    {
//        int hash = this.hash(key);
//        AtomicReferenceArray currentArray = this.table;
//        int length = currentArray.length();
//        int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//        Object o = currentArray.get(index);
//        if (o == null)
//        {
//            return null;
//        }
//        return this.slowReplace(key, value, hash, currentArray);
//    }
//
//    private V slowReplace(int key, V value, int hash, AtomicReferenceArray currentArray)
//    {
//        //noinspection LabeledStatement
//        outer:
//        while (true)
//        {
//            int length = currentArray.length();
//            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//            Object o = currentArray.get(index);
//            if (o == RESIZED || o == RESIZING)
//            {
//                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
//            }
//            else
//            {
//                Entry<V> e = (Entry<V>) o;
//                while (e != null)
//                {
//                    int candidate = e.getKey();
//                    if (candidate == key)
//                    {
//                        V oldValue = e.getValue();
//                        Entry<V> newEntry = new Entry<V>(e.getKey(), value, this.createReplacementChainForRemoval((Entry<V>) o, e));
//                        if (!currentArray.compareAndSet(index, o, newEntry))
//                        {
//                            //noinspection ContinueStatementWithLabel
//                            continue outer;
//                        }
//                        return oldValue;
//                    }
//                    e = e.getNext();
//                }
//                return null;
//            }
//        }
//    }

    public V remove(int key)
    {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == RESIZED || o == RESIZING)
        {
            return this.slowRemove(key, hash, currentArray);
        }
        Entry<V> e = (Entry<V>) o;
        while (e != null)
        {
            int candidate = e.getKey();
            if (candidate == key)
            {
                Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                if (currentArray.compareAndSet(index, o, replacement))
                {
                    this.addToSize(-1);
                    return e.getValue();
                }
                return this.slowRemove(key, hash, currentArray);
            }
            e = e.getNext();
        }
        return null;
    }

    private V slowRemove(int key, int hash, AtomicReferenceArray currentArray)
    {
        //noinspection LabeledStatement
        outer:
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry<V> e = (Entry<V>) o;
                while (e != null)
                {
                    int candidate = e.getKey();
                    if (candidate == key)
                    {
                        Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                        if (currentArray.compareAndSet(index, o, replacement))
                        {
                            this.addToSize(-1);
                            return e.getValue();
                        }
                        //noinspection ContinueStatementWithLabel
                        continue outer;
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    private Entry<V> createReplacementChainForRemoval(Entry<V> original, Entry<V> toRemove)
    {
        if (original == toRemove)
        {
            return original.getNext();
        }
        Entry<V> replacement = null;
        Entry<V> e = original;
        while (e != null)
        {
            if (e != toRemove)
            {
                replacement = new Entry<V>(e.getKey(), e.getValue(), replacement);
            }
            e = e.getNext();
        }
        return replacement;
    }

    public void parallelForEachKeyValue(List<IntObjectProcedure<V>> blocks, Executor executor)
    {
        final AtomicReferenceArray currentArray = this.table;
        int chunks = blocks.size();
        if (chunks > 1)
        {
            FutureTask<?>[] futures = new FutureTask<?>[chunks];
            int chunkSize = currentArray.length() / chunks;
            if (currentArray.length() % chunks != 0)
            {
                chunkSize++;
            }
            for (int i = 0; i < chunks; i++)
            {
                final int start = i * chunkSize;
                final int end = Math.min((i + 1) * chunkSize, currentArray.length());
                final IntObjectProcedure<V> block = blocks.get(i);
                futures[i] = new FutureTask(new Runnable()
                {
                    public void run()
                    {
                        ConcurrentIntObjectHashMap.this.sequentialForEachKeyValue(block, currentArray, start, end);
                    }
                }, null);
                executor.execute(futures[i]);
            }
            for (int i = 0; i < chunks; i++)
            {
                try
                {
                    futures[i].get();
                }
                catch (Exception e)
                {
                    throw new RuntimeException("parallelForEachKeyValue failed", e);
                }
            }
        }
        else
        {
            this.sequentialForEachKeyValue(blocks.get(0), currentArray, 0, currentArray.length());
        }
    }

    private void sequentialForEachKeyValue(IntObjectProcedure<V> block, AtomicReferenceArray currentArray, int start, int end)
    {
        for (int i = start; i < end; i++)
        {
            Object o = currentArray.get(i);
            if (o == RESIZED || o == RESIZING)
            {
                throw new ConcurrentModificationException("can't iterate while resizing!");
            }
            Entry<V> e = (Entry<V>) o;
            while (e != null)
            {
                int key = e.getKey();
                Object value = e.getValue();
                block.value(key, (V) value);
                e = e.getNext();
            }
        }
    }

    public void parallelForEachValue(List<Procedure<V>> blocks, Executor executor)
    {
        final AtomicReferenceArray currentArray = this.table;
        int chunks = blocks.size();
        if (chunks > 1)
        {
            FutureTask<?>[] futures = new FutureTask<?>[chunks];
            int chunkSize = currentArray.length() / chunks;
            if (currentArray.length() % chunks != 0)
            {
                chunkSize++;
            }
            for (int i = 0; i < chunks; i++)
            {
                final int start = i * chunkSize;
                final int end = Math.min((i + 1) * chunkSize, currentArray.length() - 1);
                final Procedure<V> block = blocks.get(i);
                futures[i] = new FutureTask(new Runnable()
                {
                    public void run()
                    {
                        ConcurrentIntObjectHashMap.this.sequentialForEachValue(block, currentArray, start, end);
                    }
                }, null);
                executor.execute(futures[i]);
            }
            for (int i = 0; i < chunks; i++)
            {
                try
                {
                    futures[i].get();
                }
                catch (Exception e)
                {
                    throw new RuntimeException("parallelForEachKeyValue failed", e);
                }
            }
        }
        else
        {
            this.sequentialForEachValue(blocks.get(0), currentArray, 0, currentArray.length());
        }
    }

    private void sequentialForEachValue(Procedure<V> block, AtomicReferenceArray currentArray, int start, int end)
    {
        for (int i = start; i < end; i++)
        {
            Object o = currentArray.get(i);
            if (o == RESIZED || o == RESIZING)
            {
                throw new ConcurrentModificationException("can't iterate while resizing!");
            }
            Entry<V> e = (Entry<V>) o;
            while (e != null)
            {
                Object value = e.getValue();
                block.value((V) value);
                e = e.getNext();
            }
        }
    }

    
    public int hashCode()
    {
        int h = 0;
        AtomicReferenceArray currentArray = this.table;
        for (int i = 0; i < currentArray.length() - 1; i++)
        {
            Object o = currentArray.get(i);
            if (o == RESIZED || o == RESIZING)
            {
                throw new ConcurrentModificationException("can't compute hashcode while resizing!");
            }
            Entry<V> e = (Entry<V>) o;
            while (e != null)
            {
                int key = e.getKey();
                Object value = e.getValue();
                h += key ^ (value == null ? 0 : value.hashCode());
                e = e.getNext();
            }
        }
        return h;
    }

    
//    public boolean equals(Object o)
//    {
//        if (o == this)
//        {
//            return true;
//        }
//
//        if (!(o instanceof ConcurrentIntObjectHashMap))
//        {
//            return false;
//        }
//        ConcurrentIntObjectHashMap<V> m = (ConcurrentIntObjectHashMap<V>) o;
//        if (m.size() != this.size())
//        {
//            return false;
//        }
//
//        Iterator<Map.Entry<V>> i = this.entrySet().iterator();
//        while (i.hasNext())
//        {
//            Map.Entry<V> e = i.next();
//            int key = e.getKey();
//            V value = e.getValue();
//            if (value == null)
//            {
//                if (!(m.get(key) == null && m.containsKey(key)))
//                {
//                    return false;
//                }
//            }
//            else
//            {
//                if (!value.equals(m.get(key)))
//                {
//                    return false;
//                }
//            }
//        }
//        return true;
//    }

    
//    public String toString()
//    {
//        if (this.isEmpty())
//        {
//            return "{}";
//        }
//        Iterator<Map.Entry<V>> iterator = this.entrySet().iterator();
//
//        StringBuilder sb = new StringBuilder();
//        sb.append('{');
//        while (true)
//        {
//            Map.Entry<V> e = iterator.next();
//            int key = e.getKey();
//            V value = e.getValue();
//            sb.append(key == this ? "(this Map)" : key);
//            sb.append('=');
//            sb.append(value == this ? "(this Map)" : value);
//            if (!iterator.hasNext())
//            {
//                return sb.append('}').toString();
//            }
//            sb.append(", ");
//        }
//    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int size = in.readInt();
        int capacity = 1;
        while (capacity < size)
        {
            capacity <<= 1;
        }
        this.table = new AtomicReferenceArray(capacity + 1);
        for (int i = 0; i < size; i++)
        {
            this.put(in.readInt(), (V) in.readObject());
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        int size = this.size();
        out.writeInt(size);
        int count = 0;
        for (int i = 0; i < this.table.length() - 1; i++)
        {
            Object o = this.table.get(i);
            if (o == RESIZED || o == RESIZING)
            {
                throw new ConcurrentModificationException("Can't serialize while resizing!");
            }
            Entry<V> e = (Entry<V>) o;
            while (e != null)
            {
                count++;
                out.writeInt(e.getKey());
                out.writeObject(e.getValue());
                e = e.getNext();
            }
        }
        if (count != size)
        {
            throw new ConcurrentModificationException("Map changed while serializing");
        }
    }

    private static final class IteratorState
    {
        private AtomicReferenceArray currentTable;
        private int start;
        private int end;

        private IteratorState(AtomicReferenceArray currentTable)
        {
            this.currentTable = currentTable;
            this.end = this.currentTable.length() - 1;
        }

        private IteratorState(AtomicReferenceArray currentTable, int start, int end)
        {
            this.currentTable = currentTable;
            this.start = start;
            this.end = end;
        }
    }

    private abstract class HashIterator<E>
    {
        private List<IteratorState> todo = null;
        private IteratorState currentState;
        private Entry<V> next;
        private int index = 0;
        private Entry<V> current;

        protected HashIterator()
        {
            if (!ConcurrentIntObjectHashMap.this.isEmpty())
            {
                this.currentState = new IteratorState(ConcurrentIntObjectHashMap.this.table);
                this.findNext();
            }
        }

        private void findNext()
        {
            while (this.index < this.currentState.end)
            {
                Object o = this.currentState.currentTable.get(this.index);
                if (o == RESIZED || o == RESIZING)
                {
                    AtomicReferenceArray nextArray = ConcurrentIntObjectHashMap.this.helpWithResizeWhileCurrentIndex(this.currentState.currentTable, this.index);
                    int endResized = this.index + 1;
                    while (endResized < this.currentState.end)
                    {
                        if (this.currentState.currentTable.get(endResized) != RESIZED)
                        {
                            break;
                        }
                        endResized++;
                    }
                    if (this.todo == null)
                    {
                        this.todo = new FastList<IteratorState>(4);
                    }
                    if (endResized < this.currentState.end)
                    {
                        this.todo.add(new IteratorState(this.currentState.currentTable, endResized, this.currentState.end));
                    }
                    int powerTwoLength = this.currentState.currentTable.length() - 1;
                    this.todo.add(new IteratorState(nextArray, this.index + powerTwoLength, endResized + powerTwoLength));
                    this.currentState.currentTable = nextArray;
                    this.currentState.end = endResized;
                    this.currentState.start = this.index;
                }
                else if (o != null)
                {
                    this.next = (Entry<V>) o;
                    this.index++;
                    break;
                }
                else
                {
                    this.index++;
                }
            }
            if (this.next == null && this.index == this.currentState.end && this.todo != null && !this.todo.isEmpty())
            {
                this.currentState = this.todo.remove(this.todo.size() - 1);
                this.index = this.currentState.start;
                this.findNext();
            }
        }

        public final boolean hasNext()
        {
            return this.next != null;
        }

        final Entry<V> nextEntry()
        {
            Entry<V> e = this.next;
            if (e == null)
            {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null)
            {
                this.findNext();
            }
            this.current = e;
            return e;
        }

        public void remove()
        {
            if (this.current == null)
            {
                throw new IllegalStateException();
            }
            int key = this.current.key;
            this.current = null;
            ConcurrentIntObjectHashMap.this.remove(key);
        }
    }

    private final class ValueIterator extends HashIterator<V> implements Iterator<V>
    {
        public V next()
        {
            return this.nextEntry().value;
        }
    }

    private final class KeyIterator extends HashIterator
    {
        public int next()
        {
            return this.nextEntry().getKey();
        }
    }

//    private final class EntryIterator extends HashIterator<Map.Entry<V>>
//    {
//        public Map.Entry<V> next()
//        {
//            return this.nextEntry();
//        }
//    }

//    private final class KeySet implements IntSet
//    {
//
//        public IntIterator iterator()
//        {
//            return new KeyIterator();
//        }
//
//
//        public int size()
//        {
//            return ConcurrentIntObjectHashMap.this.size;
//        }
//
//
//        public boolean contains(Object o)
//        {
//            return ConcurrentIntObjectHashMap.this.containsKey(o);
//        }
//
//
//        public boolean remove(Object o)
//        {
//            return ConcurrentIntObjectHashMap.this.remove(o) != null;
//        }
//
//
//        public void clear()
//        {
//            ConcurrentIntObjectHashMap.this.clear();
//        }
//    }

    private final class Values extends AbstractCollection<V>
    {
        
        public Iterator<V> iterator()
        {
            return new ValueIterator();
        }

        
        public int size()
        {
            return ConcurrentIntObjectHashMap.this.size;
        }

        
        public boolean contains(Object o)
        {
            return ConcurrentIntObjectHashMap.this.containsValue(o);
        }

        
        public void clear()
        {
            ConcurrentIntObjectHashMap.this.clear();
        }
    }


    private static final class Entry<V>
    {
        private final int key;
        private final V value;
        private final Entry<V> next;

        private Entry(int key, V value)
        {
            this.key = key;
            this.value = value;
            this.next = null;
        }

        private Entry(int key, V value, Entry<V> next)
        {
            this.key = key;
            this.value = value;
            this.next = next;
        }

        public int getKey()
        {
            return this.key;
        }

        public V getValue()
        {
            return this.value;
        }

        public V setValue(V value)
        {
            throw new RuntimeException("not implemented");
        }

        public Entry<V> getNext()
        {
            return this.next;
        }

        
        public boolean equals(Object o)
        {
            if (!(o instanceof Entry))
            {
                return false;
            }
            Entry<V> e = (Entry<V>) o;
            int k1 = this.key;
            int k2 = e.getKey();
            if (k1 == k2)
            {
                V v1 = this.value;
                Object v2 = e.getValue();
                if (v1 == v2 || v1 != null && v1.equals(v2))
                {
                    return true;
                }
            }
            return false;
        }

        
        public int hashCode()
        {
            return this.key ^ (this.value == null ? 0 : this.value.hashCode());
        }

        
        public String toString()
        {
            return this.key + "=" + this.value;
        }
    }

    private static final class ResizeContainer
    {
        private static final int QUEUE_INCREMENT = Math.min(1 << 10, Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        private final AtomicInteger resizers = new AtomicInteger(1);
        private final AtomicReferenceArray nextArray;
        private final AtomicInteger queuePosition;

        private ResizeContainer(AtomicReferenceArray nextArray, int oldSize)
        {
            this.nextArray = nextArray;
            this.queuePosition = new AtomicInteger(oldSize);
        }

        public void incrementResizer()
        {
            this.resizers.incrementAndGet();
        }

        public void decrementResizerAndNotify()
        {
            int remaining = this.resizers.decrementAndGet();
            if (remaining == 0)
            {
                synchronized (this)
                {
                    this.notifyAll();
                }
            }
        }

        public int getQueuePosition()
        {
            return this.queuePosition.get();
        }

        public int subtractAndGetQueuePosition()
        {
            return this.queuePosition.addAndGet(-QUEUE_INCREMENT);
        }

        public void waitForAllResizers()
        {
            if (this.resizers.get() > 0)
            {
                for (int i = 0; i < 16; i++)
                {
                    if (this.resizers.get() == 0)
                    {
                        break;
                    }
                }
                for (int i = 0; i < 16; i++)
                {
                    if (this.resizers.get() == 0)
                    {
                        break;
                    }
                    Thread.yield();
                }
            }
            if (this.resizers.get() > 0)
            {
                synchronized (this)
                {
                    while (this.resizers.get() > 0)
                    {
                        try
                        {
                            this.wait();
                        }
                        catch (InterruptedException e)
                        {
                            // ignore
                        }
                    }
                }
            }
        }

        public boolean isNotDone()
        {
            return this.resizers.get() > 0;
        }

        public void zeroOutQueuePosition()
        {
            this.queuePosition.set(0);
        }
    }

    public boolean notEmpty()
    {
        return !this.isEmpty();
    }

    public void forEach(Procedure<? super V> procedure)
    {
        for(Iterator<V> iterator = this.values().iterator(); iterator.hasNext(); )
        {
            procedure.value(iterator.next());
        }
    }

    public Iterator<V> iterator()
    {
        return this.values().iterator();
    }

    public void forEachValue(Procedure<? super V> procedure)
    {
        this.forEach(procedure);
    }

//    public void forEachKey(IntProcedure procedure)
//    {
//        IterableIterate.forEach(this.keySet(), procedure);
//    }

//    public void forEachKeyValue(Procedure2<? super K, ? super V> procedure)
//    {
//        IterableIterate.forEach(this.entrySet(), new MapEntryToProcedure2<K, V>(procedure));
//    }

    public V removeKey(int key)
    {
        return this.remove(key);
    }

    
//    public <P> V getIfAbsentPutWith(int key, Function<? super P, ? extends V> function, P parameter)
//    {
//        int hash = this.hash(key);
//        AtomicReferenceArray currentArray = this.table;
//        V newValue = null;
//        boolean createdValue = false;
//        while (true)
//        {
//            int length = currentArray.length();
//            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//            Object o = currentArray.get(index);
//            if (o == RESIZED || o == RESIZING)
//            {
//                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
//            }
//            else
//            {
//                Entry<V> e = (Entry<V>) o;
//                while (e != null)
//                {
//                    int candidate = e.getKey();
//                    if (candidate == key)
//                    {
//                        return e.getValue();
//                    }
//                    e = e.getNext();
//                }
//                if (!createdValue)
//                {
//                    createdValue = true;
//                    newValue = function.valueOf(parameter);
//                }
//                Entry<V> newEntry = new Entry<V>(key, newValue, (Entry<V>) o);
//                if (currentArray.compareAndSet(index, o, newEntry))
//                {
//                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
//                    return newValue;
//                }
//            }
//        }
//    }
//
//
//    public V getIfAbsent(int key, Function0<? extends V> function)
//    {
//        V result = this.get(key);
//        if (result == null)
//        {
//            return function.value();
//        }
//        return result;
//    }
//
//
//    public <P> V getIfAbsentWith(
//            int key,
//            Function<? super P, ? extends V> function,
//            P parameter)
//    {
//        V result = this.get(key);
//        if (result == null)
//        {
//            return function.valueOf(parameter);
//        }
//        return result;
//    }
//
//
//    public <A> A ifPresentApply(int key, Function<? super V, ? extends A> function)
//    {
//        V result = this.get(key);
//        return result == null ? null : function.valueOf(result);
//    }
//
//
//    public V updateValue(int key, Function0<? extends V> factory, Function<? super V, ? extends V> function)
//    {
//        int hash = this.hash(key);
//        AtomicReferenceArray currentArray = this.table;
//        int length = currentArray.length();
//        int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//        Object o = currentArray.get(index);
//        if (o == null)
//        {
//            V result = function.valueOf(factory.value());
//            Entry<V> newEntry = new Entry<V>(key, result, null);
//            if (currentArray.compareAndSet(index, null, newEntry))
//            {
//                this.addToSize(1);
//                return result;
//            }
//        }
//        return this.slowUpdateValue(key, factory, function, hash, currentArray);
//    }
//
//    private V slowUpdateValue(int key, Function0<? extends V> factory, Function<? super V, ? extends V> function, int hash, AtomicReferenceArray currentArray)
//    {
//        //noinspection LabeledStatement
//        outer:
//        while (true)
//        {
//            int length = currentArray.length();
//            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//            Object o = currentArray.get(index);
//            if (o == RESIZED || o == RESIZING)
//            {
//                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
//            }
//            else
//            {
//                Entry<V> e = (Entry<V>) o;
//                while (e != null)
//                {
//                    int candidate = e.getKey();
//                    if (candidate == key)
//                    {
//                        V oldValue = e.getValue();
//                        V newValue = function.valueOf(oldValue);
//                        Entry<V> newEntry = new Entry<V>(e.getKey(), newValue, this.createReplacementChainForRemoval((Entry<V>) o, e));
//                        if (!currentArray.compareAndSet(index, o, newEntry))
//                        {
//                            //noinspection ContinueStatementWithLabel
//                            continue outer;
//                        }
//                        return newValue;
//                    }
//                    e = e.getNext();
//                }
//                V result = function.valueOf(factory.value());
//                Entry<V> newEntry = new Entry<V>(key, result, (Entry<V>) o);
//                if (currentArray.compareAndSet(index, o, newEntry))
//                {
//                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
//                    return result;
//                }
//            }
//        }
//    }
//
//    public <P> V updateValueWith(int key, Function0<? extends V> factory, Function2<? super V, ? super P, ? extends V> function, P parameter)
//    {
//        int hash = this.hash(key);
//        AtomicReferenceArray currentArray = this.table;
//        int length = currentArray.length();
//        int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//        Object o = currentArray.get(index);
//        if (o == null)
//        {
//            V result = function.value(factory.value(), parameter);
//            Entry<V> newEntry = new Entry<V>(key, result, null);
//            if (currentArray.compareAndSet(index, null, newEntry))
//            {
//                this.addToSize(1);
//                return result;
//            }
//        }
//        return this.slowUpdateValueWith(key, factory, function, parameter, hash, currentArray);
//    }
//
//    private <P> V slowUpdateValueWith(
//            int key,
//            Function0<? extends V> factory,
//            Function2<? super V, ? super P, ? extends V> function,
//            P parameter,
//            int hash,
//            AtomicReferenceArray currentArray)
//    {
//        //noinspection LabeledStatement
//        outer:
//        while (true)
//        {
//            int length = currentArray.length();
//            int index = ConcurrentIntObjectHashMap.indexFor(hash, length);
//            Object o = currentArray.get(index);
//            if (o == RESIZED || o == RESIZING)
//            {
//                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
//            }
//            else
//            {
//                Entry<V> e = (Entry<V>) o;
//                while (e != null)
//                {
//                    int candidate = e.getKey();
//                    if (candidate == key)
//                    {
//                        V oldValue = e.getValue();
//                        V newValue = function.value(oldValue, parameter);
//                        Entry<V> newEntry = new Entry<V>(e.getKey(), newValue, this.createReplacementChainForRemoval((Entry<V>) o, e));
//                        if (!currentArray.compareAndSet(index, o, newEntry))
//                        {
//                            //noinspection ContinueStatementWithLabel
//                            continue outer;
//                        }
//                        return newValue;
//                    }
//                    e = e.getNext();
//                }
//                V result = function.value(factory.value(), parameter);
//                Entry<V> newEntry = new Entry<V>(key, result, (Entry<V>) o);
//                if (currentArray.compareAndSet(index, o, newEntry))
//                {
//                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
//                    return result;
//                }
//            }
//        }
//    }
}
