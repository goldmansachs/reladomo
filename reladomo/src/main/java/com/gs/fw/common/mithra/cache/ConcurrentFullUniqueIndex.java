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

import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.*;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


public class ConcurrentFullUniqueIndex<T> implements UnderlyingObjectGetter
{
    private static final Object RESIZE_SENTINEL = new Object();

    private static final AtomicReferenceFieldUpdater TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ConcurrentFullUniqueIndex.class, Object[].class, "table");

    private static final int DEFAULT_INITIAL_CAPACITY = 5;
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private final ExtractorBasedHashStrategy hashStrategy;
    private UnderlyingObjectGetter underlyingObjectGetter = this;

    private volatile Object[] table;

    private final int[] partitionedSize = new int[SIZE_BUCKETS * 16]; // we want 64 bytes for each slot. int is 4 bytes, so 64 bytes is 16 ints.

    private volatile Object any;

    private static final Object RESIZED = new Object();
    private static final Object RESIZING = new Object();

    private static final Object DEAD = new Object();

    private static final Unsafe UNSAFE;
    private static final long OBJECT_ARRAY_BASE;
    private static final int OBJECT_ARRAY_SHIFT;
    private static final long INT_ARRAY_BASE;
    private static final int INT_ARRAY_SHIFT;
    private static final int SIZE_BUCKETS = 8;

    static
    {
        try
        {
            UNSAFE = MithraUnsafe.getUnsafe();
            Class<?> objectArrayClass = Object[].class;
            OBJECT_ARRAY_BASE = UNSAFE.arrayBaseOffset(objectArrayClass);
            int objectArrayScale = UNSAFE.arrayIndexScale(objectArrayClass);
            if ((objectArrayScale & (objectArrayScale - 1)) != 0)
            {
                throw new AssertionError("data type scale not a power of two");
            }
            OBJECT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(objectArrayScale);

            Class<?> intArrayClass = int[].class;
            INT_ARRAY_BASE = UNSAFE.arrayBaseOffset(intArrayClass);
            int intArrayScale = UNSAFE.arrayIndexScale(intArrayClass);
            if ((intArrayScale & (intArrayScale - 1)) != 0)
            {
                throw new AssertionError("data type scale not a power of two");
            }
            INT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(intArrayScale);

        }
        catch (SecurityException e)
        {
            throw new AssertionError(e);
        }
    }

    public ConcurrentFullUniqueIndex(Extractor[] extractors, int initialCapacity)
    {
        hashStrategy = ExtractorBasedHashStrategy.create(extractors);
        init(scaledByLoadFactor(initialCapacity));
    }

    public ConcurrentFullUniqueIndex(Extractor[] extractors)
    {
        hashStrategy = ExtractorBasedHashStrategy.create(extractors);
        this.init(scaledByLoadFactor(DEFAULT_INITIAL_CAPACITY));
    }

    public ConcurrentFullUniqueIndex(ExtractorBasedHashStrategy hashStrategy)
    {
        this(hashStrategy, DEFAULT_INITIAL_CAPACITY);
    }

    public ConcurrentFullUniqueIndex(ExtractorBasedHashStrategy hashStrategy, int initialCapacity)
    {
        this.hashStrategy = hashStrategy;
        this.init(scaledByLoadFactor(initialCapacity));
    }

    private static Object arrayAt(Object[] array, int index)
    {
        return UNSAFE.getObjectVolatile(array, ((long) index << OBJECT_ARRAY_SHIFT) + OBJECT_ARRAY_BASE);
    }

    private static boolean casArrayAt(Object[] array, int index, Object expected, Object newValue)
    {
        return UNSAFE.compareAndSwapObject(array, ((long) index << OBJECT_ARRAY_SHIFT) + OBJECT_ARRAY_BASE, expected, newValue);
    }

    private static void setArrayAt(Object[] array, int index, Object newValue)
    {
        UNSAFE.putObjectVolatile(array, ((long) index << OBJECT_ARRAY_SHIFT) + OBJECT_ARRAY_BASE, newValue);
    }

    private int addToSizeReturnLocalSize(int value)
    {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & (SIZE_BUCKETS - 1);
        h = h << 4;
        long address = ((long) h << INT_ARRAY_SHIFT) + INT_ARRAY_BASE;
        while (true)
        {
            int localSize = UNSAFE.getIntVolatile(this.partitionedSize, address);
            if (UNSAFE.compareAndSwapInt(this.partitionedSize, address, localSize, localSize + value))
            {
                return localSize + value;
            }
        }
    }

    private int scaledByLoadFactor(int initialCapacity)
    {
        return initialCapacity << 1;
    }

    public boolean isEmpty()
    {
        return 0 == size();
    }

    public int size()
    {
        int localSize = 0;
        for (int i = 0; i < SIZE_BUCKETS; i++)
        {
            localSize += this.partitionedSize[i << 4];
        }
        return localSize;
    }

    public boolean contains(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return this.getFromUnderlyingKey(underlying) != null;
    }

    private void init(int initialCapacity)
    {
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;

        this.table = allocateTable(capacity);
    }

    private Object[] allocateTable(int capacity)
    {
        Object[] tmp = new Object[capacity+2];
        setArrayAt(tmp, capacity, Integer.numberOfTrailingZeros(capacity) + 1);
        return tmp;
    }

    private int indexOf(int hash, int lengthMask, int rightShift)
    {
        return (hash ^ (hash >>> rightShift)) & lengthMask;
    }

    public T getFromUnderlyingKey(Object underlying)
    {
        int initialHash = hashStrategy.computeHashCode(underlying);
        outer:
        while (true)
        {
            Object[] set = this.table;
            int lengthMask = set.length - 3;
            int rightShift = (Integer) arrayAt(set, set.length - 2);
            int hash = initialHash;
            int index = indexOf(hash, lengthMask, rightShift);

            int probe = 17;
            while(true)
            {
                Object cur = arrayAt(set, index);

                if (cur == RESIZING || cur == RESIZED)
                {
                    helpWithResize(set);
                    set = this.table;
                    lengthMask = set.length - 3;
                    rightShift = (Integer) arrayAt(set, set.length - 2);
                    hash = initialHash;
                    index = indexOf(hash, lengthMask, rightShift);
                    probe = 17;
                    continue;
                }
                if (cur == null || hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), underlying))
                {
                    return (T) cur;
                }
                hash += probe;
                probe += 17;
                index = hash & lengthMask;
            }
        }
    }

    public T get(Object valueHolder, Extractor[] extractors) // for multi attribute indicies
    {
        int initialHash = hashStrategy.computeHashCode(valueHolder, extractors);
        Object[] set = this.table;
        int lengthMask = set.length - 3;
        int rightShift = (Integer) arrayAt(set, set.length - 2);
        int hash = initialHash;
        int index = indexOf(hash, lengthMask, rightShift);

        int probe = 17;
        while(true)
        {
            Object cur = arrayAt(set, index);

            if (cur == RESIZING || cur == RESIZED)
            {
                helpWithResize(set);
                set = this.table;
                lengthMask = set.length - 3;
                rightShift = (Integer) arrayAt(set, set.length - 2);
                hash = initialHash;
                index = indexOf(hash, lengthMask, rightShift);
                probe = 17;
                continue;
            }
            if (cur == null || hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), valueHolder, extractors))
            {
                return (T) cur;
            }
            hash += probe;
            probe += 17;
            index = hash & lengthMask;
        }
    }

    public List<T> getAll()
    {
        //todo: parallelize
        MithraFastList<T> result = new MithraFastList<T>(this.size());
        Object[] set = table;
        for (int i = 0; i < set.length - 2; i++)
        {
            Object e = arrayAt(set, i);
            if (e != null && e != DEAD) result.add((T) e);
        }
        return result;
    }

    public void ensureCapacity(int capacity)
    {
        int newCapacity = Integer.highestOneBit(scaledByLoadFactor(capacity));
        if (newCapacity < capacity) newCapacity = newCapacity << 1;
        if (newCapacity > table.length - 2) resize(this.table, newCapacity);
    }

    public boolean putIfAbsent(Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        return putUsingUnderlying(key, underlying);
    }

    protected boolean putUsingUnderlying(Object key, Object underlying)
    {
        int initialHash = hashStrategy.computeHashCode(underlying);
        outer:
        while(true)
        {
            Object[] set = this.table;
            int lengthMask = set.length - 3;
            int rightShift = (Integer) arrayAt(set, set.length - 2);
            int hash = initialHash;
            int index = indexOf(hash, lengthMask, rightShift);

            Object cur = arrayAt(set, index);
            if (cur == RESIZED || cur == RESIZING)
            {
                helpWithResize(set);
                continue;
            }
            if (cur == null)
            {
                if (addAtIndex(set, index, key))
                {
                    checkForResize(set, lengthMask, index);
                    return true;
                }
            }
            else if (hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), underlying))
            {
                return false;
            }
            else
            {
                int probe = 17;
                do
                {
                    hash += probe;
                    probe += 17;
                    index = hash & lengthMask;
                    cur = arrayAt(set, index);
                    if (cur == RESIZED || cur == RESIZING)
                    {
                        helpWithResize(set);
                        continue outer;
                    }
                } while (cur != null
                        && ! hashStrategy.equals(this.underlyingObjectGetter.getUnderlyingObject(cur), underlying));
                if (cur == null)
                {
                    if (addAtIndex(set, index, key))
                    {
                        int threshold = (set.length - 2) >> 1;
                        if (size() > threshold)
                        {
                            resize(set);
                        }
                        return true;
                    }
                }
                else
                {
                    return false;
                }
            }
        }
    }

    private void checkForResize(Object[] set, int lengthMask, int index)
    {
        if (set[(index+1) & lengthMask] != null || set[(index - 1) & lengthMask] != null)
        {
            int threshold = (set.length - 2) >> 1;
            if (size() > threshold)
            {
                resize(set);
            }
        }
    }

    public T removeByIdentity(Object underlying) // only works when the hash strategy is identity
    {
        int initialHash = hashStrategy.computeHashCode(underlying);
        outer:
        while(true)
        {
            Object[] set = this.table;
            int lengthMask = set.length - 3;
            int rightShift = (Integer) arrayAt(set, set.length - 2);
            int hash = initialHash;
            int index = indexOf(hash, lengthMask, rightShift);

            Object cur = arrayAt(set, index);
            if (cur == RESIZED || cur == RESIZING)
            {
                helpWithResize(set);
                continue;
            }
            if (cur == null)
            {
                return null;
            }
            else if (cur == underlying)
            {
                if (removeAtIndex(set, index, cur))
                {
                    return (T) cur;
                }
            }
            else
            {
                int probe = 17;
                do
                {
                    hash += probe;
                    probe += 17;
                    index = hash & lengthMask;
                    cur = arrayAt(set, index);
                    if (cur == RESIZED || cur == RESIZING)
                    {
                        helpWithResize(set);
                        continue outer;
                    }
                } while (cur != null
                        && cur != underlying);
                if (cur == null)
                {
                    return null;
                }
                else
                {
                    if (removeAtIndex(set, index, cur))
                    {
                        return (T) cur;
                    }
                }
            }
        }
    }

    private boolean removeAtIndex(Object[] set, int index, Object key)
    {
        if (casArrayAt(set, index, key, DEAD))
        {
            addToSizeReturnLocalSize(-1);
            return true;
        }
        return false;
    }

    private boolean addAtIndex(Object[] set, int index, Object key)
    {
        if (casArrayAt(set, index, null, key))
        {
            if (this.any == null) this.any = key;
            addToSizeReturnLocalSize(1);
            return true;
        }
        return false;
    }

    private Object[] helpWithResize(Object[] currentArray)
    {
        ResizeContainer resizeContainer = (ResizeContainer) arrayAt(currentArray, currentArray.length - 1);
        Object[] newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT)
        {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void resize(Object[] oldTable)
    {
        this.resize(oldTable, ((oldTable.length - 2) << 1));
    }

    // newSize must be a power of 2 + 1
    private void resize(Object[] oldTable, int newSize)
    {
        int oldCapacity = oldTable.length;
        int end = oldCapacity - 1;
        Object last = arrayAt(oldTable, end);
        if (this.size() < ((end*3) >> 2) && last == RESIZE_SENTINEL)
        {
            return;
        }
        if (oldCapacity >= MAXIMUM_CAPACITY)
        {
            throw new RuntimeException("max capacity of map exceeded");
        }
        ResizeContainer resizeContainer = null;
        boolean ownResize = false;
        if (last == null || last == RESIZE_SENTINEL)
        {
            synchronized (oldTable) // allocating a new array is too expensive to make this an atomic operation
            {
                if (arrayAt(oldTable, end) == null)
                {
                    setArrayAt(oldTable, end, RESIZE_SENTINEL);
                    resizeContainer = new ResizeContainer(allocateTable(newSize), oldTable.length - 2);
                    setArrayAt(oldTable, end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize)
        {
            this.transfer(oldTable, resizeContainer);

            Object[] src = this.table;
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

    private void transfer(Object[] src, ResizeContainer resizeContainer)
    {
        Object[] dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length - 2; )
        {
            Object o = arrayAt(src, j);
            if (o == null)
            {
                if (casArrayAt(src, j, null, RESIZED))
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
                if (casArrayAt(src, j, o, RESIZING))
                {
                    this.unconditionalCopy(dest, o);
                    setArrayAt(src, j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }

    private void reverseTransfer(Object[] src, ResizeContainer resizeContainer)
    {
        Object[] dest = resizeContainer.nextArray;
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
                    Object o = arrayAt(src, j);
                    if (o == null)
                    {
                        if (casArrayAt(src, j, null, RESIZED))
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
                        if (casArrayAt(src, j, o, RESIZING))
                        {
                            this.unconditionalCopy(dest, o);
                            setArrayAt(src, j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    protected void unconditionalCopy(Object[] dest, Object key)
    {
        Object underlying = this.underlyingObjectGetter.getUnderlyingObject(key);
        int hash = hashStrategy.computeHashCode(underlying);
        int lengthMask = dest.length - 3;
        int rightShift = (Integer) arrayAt(dest, dest.length - 2);
        int index = indexOf(hash, lengthMask, rightShift);
        Object cur = arrayAt(dest, index);
        if (cur == null && casArrayAt(dest, index, null, key)) return;
        if (cur == RESIZED || cur == RESIZING)
        {
            throw new RuntimeException("impossible double resize!!");
        }
        int probe = 17;
        do
        {
            hash += probe;
            probe += 17;
            index = hash & lengthMask;
            cur = arrayAt(dest, index);
            if (cur == null && casArrayAt(dest, index, null, key)) return;
            if (cur == RESIZED || cur == RESIZING)
            {
                throw new RuntimeException("impossible double resize!!");
            }
        } while (true);
    }

    public T getFirst()
    {
        if (any != null) return (T) any;
        return null;
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

    public boolean doUntil(DoUntilProcedure procedure)
    {
        boolean done = false;
        Object[] set = this.table;
        int realLength = set.length - 2;
        for (int i = 0; i < realLength && !done; i++)
        {
            Object o = arrayAt(set, i);
            if (o != null)
            {
                done = procedure.execute(o);
            }
        }
        return done;
    }

    public void addAll(Collection<T> c)
    {
        if (c instanceof List)
        {
            List list = (List) c;
            if (list.size() > 1 && MithraCpuBoundThreadPool.isParallelizable(list.size()))
            {
                addAllInParallel(list);
            }
            else
            {
                addAllInSequence(list);
            }
        }
        else
        {
            for(T t: c)
            {
                this.putIfAbsent(t);
            }
        }
    }

    private void addAllInSequence(List list)
    {
        int size = list.size();
        for(int i=0;i<size;i++)
        {
            this.putIfAbsent(list.get(i));
        }
    }

    private void addAllInParallel(List list)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), list.size(), 1);
        final ListBasedQueue queue = ListBasedQueue.createQueue(list, threadChunkSize.getChunkSize());
        CooperativeCpuTaskFactory taskFactory = new CooperativeCpuTaskFactory(pool, threadChunkSize.getThreads())
        {
            @Override
            protected CpuTask createCpuTask()
            {
                return new CpuTask()
                {
                    @Override
                    public void execute()
                    {
                        List subList = queue.borrow(null);
                        while (subList != null)
                        {
                            addAllInSequence(subList);
                            subList = queue.borrow(subList);
                        }
                    }
                };
            }
        };
        taskFactory.startAndWorkUntilFinished();
    }

    public void removeAllWithIdentity(Collection<T> c)
    {
        if (c instanceof List)
        {
            List list = (List) c;
            if (list.size() > 1 && MithraCpuBoundThreadPool.isParallelizable(list.size()))
            {
                removeAllWithIdentitylInParallel(list);
            }
            else
            {
                removeAllWithIdentityInSequence(list);
            }
        }
        else
        {
            for(T t: c)
            {
                this.removeByIdentity(t);
            }
        }
    }

    private void removeAllWithIdentityInSequence(List list)
    {
        int size = list.size();
        for(int i=0;i<size;i++)
        {
            this.removeByIdentity(list.get(i));
        }
    }

    private void removeAllWithIdentitylInParallel(List list)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), list.size(), 1);
        final ListBasedQueue queue = ListBasedQueue.createQueue(list, threadChunkSize.getChunkSize());
        CooperativeCpuTaskFactory taskFactory = new CooperativeCpuTaskFactory(pool, threadChunkSize.getThreads())
        {
            @Override
            protected CpuTask createCpuTask()
            {
                return new CpuTask()
                {
                    @Override
                    public void execute()
                    {
                        List subList = queue.borrow(null);
                        while (subList != null)
                        {
                            removeAllWithIdentityInSequence(subList);
                            subList = queue.borrow(subList);
                        }
                    }
                };
            }
        };
        taskFactory.startAndWorkUntilFinished();
    }

    public boolean evictCollectedReferences()
    {
        return false;
    }

    public boolean equalsByExtractedValues(ConcurrentFullUniqueIndex other)
    {
        if (this.size() != other.size()) return false;
        Object[] set = this.table;
        Extractor[] thisExtractors = this.hashStrategy.getExtractors();
        for (int i = 0; i < set.length - 2; i++)
        {
            Object o = arrayAt(set, i);
            if (o != null)
            {
                if (other.get(o, thisExtractors) == null) return false;
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
            for (int i = 0; i < set.length - 2; i++)
            {
                Object o = arrayAt(set, i);
                if (o != null)
                {
                    hash += this.hashStrategy.computeHashCode(o);
                }
            }
        }
        else
        {
            //todo parallelize
            for (int i = 0; i < set.length - 2; i++)
            {
                if (arrayAt(set, i) != null)
                {
                    hash = HashUtil.combineHashes(hash, i);
                }
            }
        }
        return hash;
    }

    // does not allow concurrent iteration with additions to the index
    public void forAllInParallel(final ParallelProcedure procedure)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        final Object[] set = this.table;
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), set.length - 2, 1);
        final ArrayBasedQueue queue =new ArrayBasedQueue(set.length - 2, threadChunkSize.getChunkSize());
        int threads = threadChunkSize.getThreads();
        procedure.setThreads(threads, this.size() /threads);
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
                            Object e = arrayAt(set, i);
                            if (e != null)
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

    // does not allow concurrent iteration with additions to the index
    public Iterator<T> iterator()
    {
        return new IteratorImpl(0, this.table.length - 2);
    }

    // does not allow concurrent iteration with additions to the index
    public ParallelIterator<T> parallelIterator(int perItemWeight)
    {
        return new ParallelIteratorImpl<T>(perItemWeight);
    }

    private class ParallelIteratorImpl<T> implements ParallelIterator<T>
    {
        private ArrayBasedQueue queue;
        private int threads;

        private ParallelIteratorImpl(int perItemWeight)
        {
            ThreadChunkSize threadChunkSize = new ThreadChunkSize(MithraCpuBoundThreadPool.getInstance().getThreads(), table.length - 2, perItemWeight);
            this.queue = new ArrayBasedQueue(table.length - 2, threadChunkSize.getChunkSize());
            this.threads = threadChunkSize.getThreads();
        }

        public int getThreads()
        {
            return threads;
        }

        public Iterator<T> makeOrRefillIterator(Iterator<T> it)
        {
            if (it == null)
            {
                ArrayBasedQueue.Segment segment = queue.borrow(null);
                if (segment != null)
                {
                    return new IteratorImpl<T>(segment);
                }
            }
            else
            {
                IteratorImpl impl = (IteratorImpl) it;
                if (queue.borrow(impl.getSegment()) != null)
                {
                    impl.resetFromSegment();
                    return impl;
                }
            }
            return null;
        }
    }

    private class IteratorImpl<T> implements Iterator<T>
    {
        private int end;
        private int current;
        private ArrayBasedQueue.Segment segment;

        private IteratorImpl(int current, int end)
        {
            this.current = current;
            this.end = end;
            findNext();
        }

        private IteratorImpl(ArrayBasedQueue.Segment segment)
        {
            this(segment.getStart(), segment.getEnd());
            this.segment = segment;
        }

        private void findNext()
        {
            while(current < end && arrayAt(table, current) == null)
            {
                current++;
            }
        }

        public boolean hasNext()
        {
            return current < end;
        }

        public T next()
        {
            T result = (T) arrayAt(table, current);
            current++;
            findNext();
            return result;
        }

        protected ArrayBasedQueue.Segment getSegment()
        {
            return segment;
        }

        protected void resetFromSegment()
        {
            this.current = segment.getStart();
            this.end = segment.getEnd();
            findNext();
        }

        public void remove()
        {
            throw new UnsupportedOperationException("ConcurrentFullUniqueIndex does not support removal");
        }
    }

    private static final class ResizeContainer
    {
        private static final int QUEUE_INCREMENT = Math.min(1 << 10, Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        private final AtomicInteger resizers = new AtomicInteger(1);
        private final Object[] nextArray;
        private final AtomicInteger queuePosition;

        private ResizeContainer(Object[] nextArray, int oldSize)
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
                            //ginore
                        }
                    }
                }
            }
        }

        public void zeroOutQueuePosition()
        {
            this.queuePosition.set(0);
        }
    }

    public static ConcurrentFullUniqueIndex parallelConstructIndexWithoutNulls(List dataHolders, Extractor[] extractors)
    {
        ConcurrentFullUniqueIndex fullUniqueIndex;
        if (dataHolders.size() > 1 && MithraCpuBoundThreadPool.isParallelizable(dataHolders.size()))
        {
            fullUniqueIndex = parallelAddToIndexWithoutNulls(dataHolders, extractors);
        }
        else
        {
            fullUniqueIndex = new ConcurrentFullUniqueIndex(extractors);
            addToIndexWithoutNulls(fullUniqueIndex, dataHolders, extractors);
        }
        return fullUniqueIndex;
    }

    private static ConcurrentFullUniqueIndex parallelAddToIndexWithoutNulls(List dataHolders, final Extractor[] extractors)
    {
        final ConcurrentFullUniqueIndex result = createIndexWithSizeSampling(dataHolders, extractors);

        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), dataHolders.size(), 1);
        final ListBasedQueue queue = ListBasedQueue.createQueue(dataHolders, threadChunkSize.getChunkSize());
        CooperativeCpuTaskFactory taskFactory = new CooperativeCpuTaskFactory(pool, threadChunkSize.getThreads())
        {
            @Override
            protected CpuTask createCpuTask()
            {
                return new CpuTask()
                {
                    @Override
                    public void execute()
                    {
                        List subList = queue.borrow(null);
                        while (subList != null)
                        {
                            addToIndexWithoutNulls(result, subList, extractors);
                            subList = queue.borrow(subList);
                        }
                    }
                };
            }
        };
        taskFactory.startAndWorkUntilFinished();
        return result;
    }

    private static ConcurrentFullUniqueIndex createIndexWithSizeSampling(List dataHolders, Extractor[] extractors)
    {
        if (dataHolders.size() > EstimateDistribution.SAMPLE_SIZE*2)
        {
            Random rand = new Random();
            ConcurrentFullUniqueIndex cfui = new ConcurrentFullUniqueIndex(extractors, 256);
            addRandomToIndex(dataHolders, extractors, rand, cfui);
            int firstSize = cfui.size();
            addRandomToIndex(dataHolders, extractors, rand, cfui);
            int secondSize = cfui.size();
            return new ConcurrentFullUniqueIndex(extractors, EstimateDistribution.estimateSize(firstSize, secondSize, dataHolders.size()));
        }
        else
        {
            return new ConcurrentFullUniqueIndex(extractors, dataHolders.size());
        }
    }

    private static void addRandomToIndex(List dataHolders, Extractor[] extractors, Random rand, ConcurrentFullUniqueIndex cfui)
    {
        for(int i=0;i< EstimateDistribution.SAMPLE_SIZE;i++)
        {
            int pos = rand.nextInt(dataHolders.size());
            addToIndexWithoutNulls(cfui, extractors, dataHolders.get(pos));
        }
    }

    private static void addToIndexWithoutNulls(ConcurrentFullUniqueIndex fullUniqueIndex, List dataHolders, Extractor[] extractors)
    {
        int size = dataHolders.size();
        for(int i= 0;i< size;i++)
        {
            Object dataHolder = dataHolders.get(i);
            addToIndexWithoutNulls(fullUniqueIndex, extractors, dataHolder);
        }
    }

    private static void addToIndexWithoutNulls(ConcurrentFullUniqueIndex index, Extractor[] extractors, Object dataHolder)
    {
        boolean hasNullAttribute = false;
        for (int j = 0; j < extractors.length; j++)
        {
            if (extractors[j].isAttributeNull(dataHolder))
            {
                // in a relationship, null does not match anything, including another null
                // same holds true when this is used for tuple.in
                hasNullAttribute = true;
                break;
            }
        }
        if (!hasNullAttribute)
        {
            index.putIfAbsent(dataHolder);
        }
    }
}
