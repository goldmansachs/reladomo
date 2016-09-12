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

package com.gs.fw.common.mithra.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.test.domain.OrderDatabaseObject;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.util.MithraConfigurationManager;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.List;
import java.util.Random;



public class TestPartialCache extends TestCase
{
    static private Logger logger = LoggerFactory.getLogger(TestPartialCache.class.getName());
    private Random random;
    private Integer integerOne;
    private Integer integerTwo;

    protected void setUp() throws Exception
    {
        super.setUp();
        OrderDatabaseObject odo = new OrderDatabaseObject();
        odo.instantiatePartialCache(new DummyConfig(100, 100, false, 0, 0));
        this.integerOne = 128;
        this.integerTwo = 256;
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
        OrderFinder.zResetPortal();
        this.integerOne = null;
        this.integerTwo = null;
    }

    public void testQueryIndexDoesNotRunOutOfMemory()
    {
        LruQueryIndex index = new LruQueryIndex(16, 10000, 0, 0);
        for (int i = 0; i < 1000000; i++)
        {
            CachedQuery key = new CachedQuery(OrderFinder.userId().eq(i), null);
            index.put(key, (i % 2) > 0);
        }
    }

    public void testQueryIndexPutGet()
    {
        LruQueryIndex index = new LruQueryIndex(16, 10000, 0, 0);
        CachedQuery key = new CachedQuery(OrderFinder.userId().eq(50), null);
        index.put(key, false);
        assertSame(key, index.get(OrderFinder.userId().eq(50), false));
    }

    public void testQueryIndexExpiration()
    {
        LruQueryIndex index = new LruQueryIndex(16, 10000, 0, 0);
        CachedQuery key = new CachedQuery(OrderFinder.userId().eq(50), null);
        index.put(key, false);
        OrderFinder.userId().incrementUpdateCount();
        assertNull(index.get(OrderFinder.userId().eq(50), false));
    }

    public void testQueryRelationshipsAreNotGced()
    {
        LruQueryIndex index = new LruQueryIndex(16, 100, 0, 0);
        CachedQuery key = new CachedQuery(OrderFinder.orderId().eq(50), null);
        index.put(key, true);
        key = null; // so it can be gc'ed.
        for (int i = 0; i < 1000000; i++)
        {
            key = new CachedQuery(OrderFinder.userId().eq(i), null);
            index.put(key, false);
        }
        assertNotNull(index.get(OrderFinder.orderId().eq(50), false));
    }

    public void testQueryIndexLru()
    {
        LruQueryIndex index = new LruQueryIndex(4, 100, 0, 0);
        for (int i = 0; i < 1000000; i++)
        {
            CachedQuery key = new CachedQuery(OrderFinder.userId().eq(i), null);
            index.put(key, (i % 2) > 0);
            for (int j = 0; j < 4; j++)
            {
                index.get(OrderFinder.userId().eq(j), false);
            }
        }
        for (int j = 0; j < 4; j++)
        {
            assertNotNull(index.get(OrderFinder.userId().eq(j), false));
        }
    }

    public void sleep(long millis)
    {
        long now = System.currentTimeMillis();
        long target = now + millis;
        while (now < target)
        {
            try
            {
                Thread.sleep(target - now);
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            now = System.currentTimeMillis();
        }
        CacheClock.forceTick();
    }

    public void testQueryIndexTimeout()
    {
        int timeToLive = 100;
        LruQueryIndex index = new LruQueryIndex(100, 100, timeToLive, 0);
        for (int i = 0; i < 100; i++)
        {
            CachedQuery key = new CachedQuery(OrderFinder.userId().eq(i), null);
            index.put(key, (i % 2) > 0);
        }
        for (int i = 0; i < 100; i++)
        {
            Operation op = OrderFinder.userId().eq(i);
            assertNotNull(index.get(op, false));
        }
        sleep(timeToLive + 10);
        for (int i = 0; i < 100; i++)
        {
            Operation op = OrderFinder.userId().eq(i);
            assertNull(index.get(op, false));
            assertNotNull(index.get(op, true));
        }
    }

    public void testQueryIndexTimeoutForRelationship()
    {
        int timeToLive = 1000;
        LruQueryIndex index = new LruQueryIndex(100, 100, timeToLive, timeToLive);
        for (int i = 0; i < 100; i++)
        {
            CachedQuery key = new CachedQuery(OrderFinder.userId().eq(i), null);
            index.put(key, (i % 2) > 0);
        }
        for (int i = 0; i < 100; i++)
        {
            Operation op = OrderFinder.userId().eq(i);
            assertNotNull(index.get(op, false));
        }
        sleep(timeToLive + 10);
        for (int i = 0; i < 100; i++)
        {
            Operation op = OrderFinder.userId().eq(i);
            assertNull(index.get(op, false));
            assertNull(index.get(op, true));
        }
    }

    public void testReadWriteLockUpgrade()
    {
        ReadWriteLock lock = new ReadWriteLock();
        lock.acquireReadLock();
        assertFalse(lock.upgradeToWriteLock());
        lock.release();
        lock.acquireWriteLock();
        assertFalse(lock.upgradeToWriteLock());
        lock.release();
        lock.acquireReadLock();
        lock.upgradeToWriteLock();
        assertFalse(lock.upgradeToWriteLock());
        lock.release();
    }

    public void testReadWriteLockUpgrade2()
    {
        ReadWriteLock lock = new ReadWriteLock();
        lock.acquireReadLock();
        lock.acquireReadLock(); // this is not a test of reentrancy. it works only because there are no other threads hitting this lock
        lock.acquireReadLock();
        lock.release();
        lock.release();
        lock.release();
        lock.acquireWriteLock();
        assertFalse(lock.upgradeToWriteLock());
        lock.release();
        lock.acquireReadLock();
        lock.upgradeToWriteLock();
        assertFalse(lock.upgradeToWriteLock());
        lock.release();
    }

    public void testReadWriteLock()
    {
//        SecureRandom rand = new SecureRandom();
//        doTestReadWriteLock(-4135425739232911178L);
//        doTestReadWriteLock(1164042248396L);
//        doTestReadWriteLock(-211736321239012386L);
//        doTestReadWriteLock(1163959036160L);
//        doTestReadWriteLock(1164042248393L);
//        for(int i=0;i<1000;i++)
//        {
//            doTestReadWriteLock(rand.nextLong());
//        }
        doTestReadWriteLock(System.currentTimeMillis());
    }

    public void doTestReadWriteLock(long seed)
    {
        System.out.print("testing seed " + seed);
        long now = System.nanoTime();
        this.random = new Random(seed);
        final ReadWriteLock lock = new ReadWriteLock();
        final ReadLockTester readLockTester = new ReadLockTester();
        int maxThreads = 50;
        TesterThread[] all = new TesterThread[maxThreads];
        for (int i = 0; i < maxThreads; i++)
        {
            all[i] = new TesterThread(lock, readLockTester);
        }
        for (int i = 0; i < maxThreads; i++)
        {
            all[i].start();
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < maxThreads; i++)
        {
            try
            {
                all[i].join(600000); // 10 minuntes
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            if (System.currentTimeMillis() - start >= 600000)
            {
                for (int j = 0; j < maxThreads; j++)
                {
                    all[j].printStatus();
                }
                fail("took too long. random seed was " + seed);
            }
        }
        for (int i = 0; i < maxThreads; i++)
        {
            assertFalse(all[i].isBad());
        }
        assertEquals(0, readLockTester.getReaders());
        assertFalse(readLockTester.isWriter());
        assertTrue(readLockTester.getMaxReaders() > 1);
        System.out.println(" took " + (double) (System.nanoTime() - now) / maxThreads / 1000 + " ns per thread");
    }

    public void xtestReadWriteLockReadersOnly()
    {
        doTestReadWriteLockOneTypeOnly(1163959036160L, 0.6);
        doTestReadWriteLockOneTypeOnly(1164042248396L, 0.6);
        doTestReadWriteLockOneTypeOnly(1164042248393L, 0.6);
        doTestReadWriteLockOneTypeOnly(System.currentTimeMillis(), 0.6);
    }

    public void doTestReadWriteLockOneTypeOnly(long seed, double type)
    {
        System.out.println("testing with seed " + seed);
        this.random = new Random(seed);
        final ReadWriteLock lock = new ReadWriteLock();
        final ReadLockTester readLockTester = new ReadLockTester();
        int maxThreads = 4;
        TesterThread[] all = new TesterThread[maxThreads];
        for (int i = 0; i < maxThreads; i++)
        {
            all[i] = new FixedTesterThread(lock, readLockTester, type);
        }
        for (int i = 0; i < maxThreads; i++)
        {
            all[i].start();
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < maxThreads; i++)
        {
            try
            {
                all[i].join(600000); // 10 minuntes
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            if (System.currentTimeMillis() - start >= 600000)
            {
                for (int j = 0; j < maxThreads; j++)
                {
                    all[j].printStatus();
                }
                fail("took too long. random seed was " + seed);
            }
        }
        for (int i = 0; i < maxThreads; i++)
        {
            assertFalse(all[i].isBad());
        }
        assertEquals(0, readLockTester.getReaders());
        assertFalse(readLockTester.isWriter());
    }

    public void xtestReadWriteLockWritersOnly()
    {
        doTestReadWriteLockOneTypeOnly(1163959036160L, 0.05);
        doTestReadWriteLockOneTypeOnly(1164042248396L, 0.05);
        doTestReadWriteLockOneTypeOnly(1164042248393L, 0.05);
        doTestReadWriteLockOneTypeOnly(System.currentTimeMillis(), 0.05);
    }

    public void xtestReadWriteLockUpgradersOnly()
    {
        doTestReadWriteLockOneTypeOnly(1163959036160L, 0.15);
        doTestReadWriteLockOneTypeOnly(1164042248396L, 0.15);
        doTestReadWriteLockOneTypeOnly(1164042248393L, 0.15);
        for (int i = 0; i < 1000; i++)
        {
            doTestReadWriteLockOneTypeOnly(System.currentTimeMillis(), 0.15);
        }
    }

    public void xtestReadWriteLockNoUpgraders()
    {
        doTestReadWriteLockNoUpgraders(1163959036160L);
        doTestReadWriteLockNoUpgraders(1164042248396L);
        doTestReadWriteLockNoUpgraders(1164042248393L);
        doTestReadWriteLockNoUpgraders(System.currentTimeMillis());
    }

    public void doTestReadWriteLockNoUpgraders(long seed)
    {
        this.random = new Random(seed);
        final ReadWriteLock lock = new ReadWriteLock();
        final ReadLockTester readLockTester = new ReadLockTester();
        int maxThreads = 50;
        TesterThread[] all = new TesterThread[maxThreads];
        for (int i = 0; i < maxThreads; i++)
        {
            if (i % 2 == 0)
            {
                all[i] = new FixedTesterThread(lock, readLockTester, 0.05);
            }
            else
            {
                all[i] = new FixedTesterThread(lock, readLockTester, 0.6);
            }
        }
        for (int i = 0; i < maxThreads; i++)
        {
            all[i].start();
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < maxThreads; i++)
        {
            try
            {
                all[i].join(600000); // 10 minuntes
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            if (System.currentTimeMillis() - start >= 600000)
            {
                for (int j = 0; j < maxThreads; j++)
                {
                    all[j].printStatus();
                }
                fail("took too long. random seed was " + seed);
            }
        }
        for (int i = 0; i < maxThreads; i++)
        {
            assertFalse(all[i].isBad());
        }
        assertEquals(0, readLockTester.getReaders());
        assertFalse(readLockTester.isWriter());
//        assertTrue(readLockTester.getMaxReaders() > 1);
    }

    private static class ReadLockTester
    {
        private int readers;
        private volatile boolean writer;
        private Thread writerThread;
        private int maxReaders;

        public int getMaxReaders()
        {
            return maxReaders;
        }

        public int getReaders()
        {
            return readers;
        }

        public boolean isWriter()
        {
            return writer;
        }

        public void startReader()
        {
            if (writer)
            {
                throw new RuntimeException("can't read while writing!");
            }
            synchronized (this)
            {
                readers++;
                if (readers > maxReaders)
                {
                    maxReaders = readers;
                }
            }
        }

        public void endReader()
        {
            synchronized (this)
            {
                readers--;
            }
            if (readers < 0)
            {
                throw new RuntimeException("readers are out of wack!");
            }
        }

        public void startWriter()
        {
            if (writer)
            {
                throw new RuntimeException("too many writers!");
            }
            synchronized (this)
            {
                if (readers > 0)
                {
                    throw new RuntimeException("can't write while reading!");
                }
            }
            this.writer = true;
            this.writerThread = Thread.currentThread();
        }

        public void endWriter()
        {
            if (!writer)
            {
                throw new RuntimeException("not writing!");
            }
            synchronized (this)
            {
                if (readers > 0)
                {
                    throw new RuntimeException("writing and reading??");
                }
            }
            this.writer = false;
            this.writerThread = null;
        }
    }

    public void reallyTestPartialIndex(Index index)
    {
        int testSize = 10000;
        index.put(new Integer(1000));
        assertNotNull(index.get(new Integer(1000)));
        Integer[] array = new Integer[testSize];
        for (int i = 0; i < testSize; i++)
        {
            array[i] = new Integer(2000 + i);
            index.put(array[i]);
        }
        for (int i = 0; i < testSize; i++)
        {
            assertSame(array[i], index.get(new Integer(2000 + i)));
        }
    }

    public void testPartialIndex()
    {
        PartialUniqueIndex index = new PartialUniqueIndex("test", new Extractor[]{new IntegerExtractor()});
        reallyTestPartialIndex(index);
    }

    public void testTransactionalPartialIndex()
    {
        TransactionalPartialUniqueIndex index = new TransactionalPartialUniqueIndex("test", new Extractor[]{new IntegerExtractor()}, 0, 0);
        reallyTestPartialIndex(index);
    }

    public void testPartialPrimaryKeyIndex()
    {
        int testSize = 10000;
        PartialPrimaryKeyIndex index = new PartialPrimaryKeyIndex("test", new Extractor[]{new IntegerExtractor()}, 0L, 0L);
        Integer firstInteger = new Integer(1000);
        index.put(firstInteger);
        assertNotNull(index.get(new Integer(1000)));
        Integer[] array = new Integer[testSize];
        for (int i = 0; i < testSize; i++)
        {
            array[i] = new Integer(2000 + i);
            index.put(array[i]);
        }
        for (int i = 0; i < testSize; i++)
        {
            assertSame(array[i], index.get(new Integer(2000 + i)));
        }
        index.clear();
        // make sure soft index is empty
//        assertEquals(0, index.size());
        assertNull(index.get(new Integer(1000)));
        for (int i = 0; i < testSize; i++)
        {
            assertNull(index.get(new Integer(2000 + i)));
        }
        // make sure the weak index is holding what we need
        NonNullMutableBoolean isDirty = new NonNullMutableBoolean();
        for (int i = 0; i < testSize; i++)
        {
            assertSame(array[i], index.getFromDataEvenIfDirty(new Integer(2000 + i), isDirty));
            assertTrue(isDirty.value);
        }
        assertSame(firstInteger, index.getFromDataEvenIfDirty(new Integer(1000), isDirty));
        assertTrue(isDirty.value);
        //make sure put moves things to clean again
        for (int i = 0; i < testSize; i++)
        {
            index.put(array[i]);
        }
        // make sure the re-accessed objects are now moved to the soft index
        for (int i = 0; i < testSize; i++)
        {
            assertSame(array[i], index.get(new Integer(2000 + i)));
            assertSame(array[i], index.getFromDataEvenIfDirty(new Integer(2000 + i), isDirty));
            assertFalse(isDirty.value);
        }
    }

    public void testPartialPrimaryKeyIndexRandom()
    {
        Random random = new Random();
        int testSize = 10000;
        PartialPrimaryKeyIndex index = new PartialPrimaryKeyIndex("test", new Extractor[]{new IntegerExtractor()}, 0L, 0L);
        Integer firstInteger = new Integer(random.nextInt());
        index.put(firstInteger);
        assertNotNull(index.get(firstInteger));
        Integer[] array = new Integer[testSize];
        for (int i = 0; i < testSize; i++)
        {
            array[i] = new Integer(random.nextInt());
            while(index.getFromData(array[i]) != null)
            {
                array[i] = new Integer(random.nextInt());
            }
            index.put(array[i]);
        }
        for (int i = 0; i < testSize; i++)
        {
            assertSame(array[i], index.get(array[i]));
        }
        index.clear();
        // make sure soft index is empty
        assertNull(index.get(firstInteger));
        for (int i = 0; i < testSize; i++)
        {
            assertNull(index.get(array[i]));
        }
        // make sure the weak index is holding what we need
        NonNullMutableBoolean isDirty = new NonNullMutableBoolean();
        for (int i = 0; i < testSize; i++)
        {
            assertSame(array[i], index.getFromDataEvenIfDirty(array[i], isDirty));
            assertTrue(isDirty.value);
        }
        assertSame(firstInteger, index.getFromDataEvenIfDirty(firstInteger, isDirty));
        assertTrue(isDirty.value);
        for (int i = 0; i < testSize; i++)
        {
            index.put(array[i]);
        }
        // make sure the re-accessed objects are now moved to the soft index
        for (int i = 0; i < testSize; i++)
        {
            assertSame(array[i], index.get(array[i]));
            assertSame(array[i], index.getFromDataEvenIfDirty(array[i], isDirty));
            assertFalse(isDirty.value);
        }
    }

    public void testClear() throws InterruptedException
    {
        PartialPrimaryKeyIndex index = new PartialPrimaryKeyIndex("test", new Extractor[]{new IntegerExtractor()});

        index.put(this.integerOne);
        index.put(this.integerTwo);
        assertEquals(2, index.getAll().size());
        assertEquals(2, index.size());
        index.clear();
        assertEquals(0, index.getAll().size());
        assertEquals(2, index.size());

        this.integerOne = null;

        this.askForGc();
        this.askForGc();

        for (int i = 0; i < 10; i++)
        {
            index.clear();
        }
        assertEquals(0, index.getAll().size());
        assertEquals(1, index.size());
    }

    private void askForGc()
    {
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        try
        {
            Thread.sleep(100);
        }
        catch (InterruptedException e)
        {
            // nop
        }
        System.gc();
        Thread.yield();
    }

    private static class IntegerExtractor implements Extractor
    {
        public void setValue(Object o, Object newValue)
        {
            throw new RuntimeException("not implemented");
        }

        public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
        {
            throw new RuntimeException("not implemented");
        }

        public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
        {
            throw new RuntimeException("not implemented");
        }

        public void setValueNull(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public boolean isAttributeNull(Object o)
        {
            return false;
        }

        public int valueHashCode(Object o)
        {
            return ((Integer) o).intValue();
        }

        public boolean valueEquals(Object first, Object second)
        {
            return first.equals(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            throw new RuntimeException("not implemented");
        }

        public OrderBy ascendingOrderBy()
        {
            throw new RuntimeException("not implemented");
        }

        public OrderBy descendingOrderBy()
        {
            throw new RuntimeException("not implemented");
        }

        public Object valueOf(Object anObject)
        {
            return anObject;
        }
    }

    private class TesterThread extends Thread
    {
        private boolean bad;
        private final ReadWriteLock lock;
        private final ReadLockTester readLockTester;
        private double lastType;
        private double currentType;
        private int threadId;
//        private IntArrayList trace = new IntArrayList(1000);

        public TesterThread(ReadWriteLock lock, ReadLockTester readLockTester)
        {
            this.lock = lock;
            this.readLockTester = readLockTester;
            bad = false;
        }

        public boolean isBad()
        {
            return bad;
        }

        public void run()
        {
            threadId = ((int) Thread.currentThread().getId() & 15) + 1;

            for (int i = 0; i < 1000; i++)
            {
//                if ((i % 4) == 0) trace.clear();
//                trace.add(50000);
                double type = getType();
                this.lastType = currentType;
                this.currentType = type;
                if (type < 0.1)
                {
                    // plain writer
                    lock.acquireWriteLock();
                    try
                    {
                        readLockTester.startWriter();
                        sleepSome();
                        readLockTester.endWriter();
                    }
                    catch (Exception e)
                    {
                        logger.error("died while writing", e);
                        bad = true;
                    }
                    finally
                    {
                        lock.release();
                    }
                }
                else if (type < 0.2)
                {
                    // read, then upgrade
                    lock.acquireReadLock();
                    try
                    {
                        readLockTester.startReader();
                        sleepSome();
                        readLockTester.endReader();
                        lock.upgradeToWriteLock();
                        readLockTester.startWriter();
                        sleepSome();
                        readLockTester.endWriter();
                    }
                    catch (Exception e)
                    {
                        logger.error("died while upgrading", e);
//                        TestPartialCache.this.sleep(10000000);
                        bad = true;
                    }
                    finally
                    {
                        lock.release();
                    }

                }
                else if (type < 0.3)
                {
                    // write then upgrade
                    lock.acquireWriteLock();
                    try
                    {
                        readLockTester.startWriter();
                        sleepSome();
                        readLockTester.endWriter();
                        assertFalse(lock.upgradeToWriteLock());
                        readLockTester.startWriter();
                        sleepSome();
                        readLockTester.endWriter();
                    }
                    catch (Exception e)
                    {
                        logger.error("died while upgrading", e);
                        bad = true;
                    }
                    finally
                    {
                        lock.release();
                    }
                }
                else if (type < 0.4)
                {
                    // read, then upgrade twice
                    lock.acquireReadLock();
                    try
                    {
                        readLockTester.startReader();
                        sleepSome();
                        readLockTester.endReader();
                        lock.upgradeToWriteLock();
                        readLockTester.startWriter();
                        sleepSome();
                        readLockTester.endWriter();
                        assertFalse(lock.upgradeToWriteLock());
                        readLockTester.startWriter();
                        sleepSome();
                        readLockTester.endWriter();
                    }
                    catch (Exception e)
                    {
                        logger.error("died while upgrading", e);
                        bad = true;
                    }
                    finally
                    {
                        lock.release();
                    }

                }
                else
                {
                    // plain reader
                    lock.acquireReadLock();
                    try
                    {
                        readLockTester.startReader();
                        sleepSome();
                        readLockTester.endReader();
                    }
                    catch (Exception e)
                    {
                        logger.error("died while reading", e);
                        bad = true;
                    }
                    finally
                    {
                        lock.release();
                    }

                }
            }
        }

        protected double getType()
        {
            return random.nextDouble();
        }

        public void printStatus()
        {
            logger.error("current " + currentType + " previous " + lastType);
        }

        private void sleepSome()
        {
            double max = 500 + random.nextDouble() * 500;
            for (int i = 0; i < max; i++)
                Math.cbrt(random.nextDouble());
        }
    }

    private class FixedTesterThread extends TesterThread
    {
        private double type;

        private FixedTesterThread(ReadWriteLock lock, ReadLockTester readLockTester, double type)
        {
            super(lock, readLockTester);
            this.type = type;
        }

        protected double getType()
        {
            return type;
        }
    }

    private static class DummyConfig extends MithraConfigurationManager.Config
    {
        public DummyConfig(int relationshipCacheSize, int minQueriesToKeep, boolean disableCache,
                           long cacheTimeToLive, long relationshipCacheTimeToLive)
        {
            this.relationshipCacheSize = relationshipCacheSize;
            this.minQueriesToKeep = minQueriesToKeep;
            this.disableCache = disableCache;
            this.cacheTimeToLive = cacheTimeToLive;
            this.relationshipCacheTimeToLive = relationshipCacheTimeToLive;
        }

        public MithraObjectPortal initializeObject(List<String> mithraInitializationErrors)
        {
            return null;
        }

        public void initializePortal(MithraObjectPortal portal)
        {
        }
    }
}
