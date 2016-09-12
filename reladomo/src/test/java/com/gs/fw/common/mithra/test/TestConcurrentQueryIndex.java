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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.cache.ConcurrentWeakPool;
import com.gs.fw.common.mithra.cache.NonLruQueryIndex;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.test.domain.OrderDatabaseObject;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.util.ExceptionCatchingThread;
import com.gs.fw.common.mithra.util.MithraConfigurationManager;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicInteger;



public class TestConcurrentQueryIndex extends TestCase
{
    static private Logger logger = LoggerFactory.getLogger(TestConcurrentQueryIndex.class.getName());

    private static final List FAKE_LARGE_LIST = new FakeList();

    protected void setUp() throws Exception
    {
        super.setUp();
        OrderDatabaseObject odo = new OrderDatabaseObject();
        odo.instantiatePartialCache(new DummyConfig(100, 100, false, 0, 0));
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    private <T> T[] shuffle(T[] array)
    {
        Object[] result = new Object[array.length];
        Random rand = new Random(12345678912345L);
        int left = array.length;
        for (int i = 0; i < array.length; i++)
        {
            int chosen = rand.nextInt(left);
            result[i] = array[chosen];
            left--;
            array[chosen] = array[left];
            array[left] = null;
        }
        System.arraycopy(result, 0, array, 0, array.length);
        return array;
    }

    public void testPutWithManyThreads() throws Exception
    {
        int max = 1000000;
        CachedQuery data[] = new CachedQuery[max];
        for(int i=0;i<max;i++)
        {
            CachedQuery o = createCachedQuery(i);
            data[i] = o;
        }
        data = shuffle(data);
        runPut(1, data, max);
        runPut(2, data, max);
        runPut(5, data, max);
        runPut(10, data, max);
        runPut(50, data, max);
        runPut(100, data, max);
    }

    private CachedQuery createCachedQuery(int i)
    {
        CachedQuery cachedQuery = new CachedQuery(OrderFinder.orderId().eq(i), null);
        cachedQuery.setResult((i % 3) == 0 ? Collections.EMPTY_LIST : FAKE_LARGE_LIST);
        return cachedQuery;
    }

    private void runPut(int threads, CachedQuery[] data, int max)
            throws ParseException
    {
        NonLruQueryIndex index = new NonLruQueryIndex();
        ExceptionCatchingThread[] runners = new ExceptionCatchingThread[threads];
        PutRunnable[] runnables = new PutRunnable[threads];
        int chunkSize = max/threads;
        for(int i = 0;i<threads;i++)
        {
            runnables[i] = new PutRunnable(chunkSize * i, chunkSize * (i+1), data, index);
            runners[i] = new ExceptionCatchingThread(runnables[i]);
        }
        forceGC();
        long start = System.currentTimeMillis();
        for(int i = 0;i<threads;i++)
        {
            runners[i].start();
        }

        for(int i = 0;i<threads;i++)
        {
            runners[i].joinWithExceptionHandling();
        }
        System.out.println("running with "+threads+" threads took "+(System.currentTimeMillis() - start)/1000.0+" s");
        assertEquals(index.size(), index.getEntryCount());
        for(int i = 0;i<threads;i++)
        {
            runnables[i].verifyExistence();
        }

        assertEquals(max, index.getEntryCount());
        assertEquals(max, index.size());
    }

    public void testPutAndClearWithManyThreads() throws Exception
    {
        int max = 1000000;
        CachedQuery data[] = new CachedQuery[max];
        for(int i=0;i<max;i++)
        {
            CachedQuery o = createCachedQuery(i);
            data[i] = o;
        }
        data = shuffle(data);
        runPutAndClear(1, data, max);
        runPutAndClear(2, data, max);
        runPutAndClear(5, data, max);
        runPutAndClear(10, data, max);
        runPutAndClear(50, data, max);
        runPutAndClear(100, data, max);
    }

    public void testClear()
    {
        NonLruQueryIndex index = new NonLruQueryIndex();
        for(int i=0;i<100000;i++)
        {
            index.put(createCachedQuery(-i), false);
        }
        forceGC();
        index.clear();
        assertEquals(index.size(), index.getEntryCount());
        assertEquals(0, index.size());
        for(int i=0;i<100000;i++)
        {
            assertNull(index.get(createCachedQuery(-i).getOperation(), false));
        }

    }

    private void runPutAndClear(int threads, CachedQuery[] data, int max)
            throws ParseException
    {
        NonLruQueryIndex index = new NonLruQueryIndex();
        ExceptionCatchingThread[] runners = new ExceptionCatchingThread[threads];
        PutWithConcurrentClearRunnable[] runnables = new PutWithConcurrentClearRunnable[threads];
        int chunkSize = max/threads;
        for(int i = 0;i<threads;i++)
        {
            runnables[i] = new PutWithConcurrentClearRunnable(chunkSize * i, chunkSize * (i+1), data, index);
            runners[i] = new ExceptionCatchingThread(runnables[i]);
        }
        for(int i=1;i<100000;i++)
        {
            index.put(createCachedQuery(-i), false);
        }
        forceGC();
        long start = System.currentTimeMillis();
        for(int i = 0;i<threads;i++)
        {
            runners[i].start();
        }
        Thread.yield();
        Thread.yield();

        index.clear();
        for(int i = 0;i<threads;i++)
        {
            runners[i].joinWithExceptionHandling();
        }
        System.out.println("running with "+threads+" threads took "+(System.currentTimeMillis() - start)/1000.0+" s");
        assertEquals(index.size(), index.getEntryCount());
        for(int i=1;i<100000;i++)
        {
            if (index.get(createCachedQuery(-i).getOperation(), false) != null)
            {
                fail("entry existed for "+(-i));
            }
        }
    }

    private static class PutWithConcurrentClearRunnable implements Runnable
    {
        private int start;
        private int max;
        private CachedQuery[] data;
        private NonLruQueryIndex index;

        private PutWithConcurrentClearRunnable(int start, int max, CachedQuery[] data, NonLruQueryIndex index)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
        }

        public void run()
        {
            for(int i=start;i<max;i++)
            {
                index.put(data[i], (i & 1) == 0);
            }
        }

    }

    private static class PutRunnable implements Runnable
    {
        private int start;
        private int max;
        private CachedQuery[] data;
        private NonLruQueryIndex index;
        private Object[] results;

        private PutRunnable(int start, int max, CachedQuery[] data, NonLruQueryIndex index)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
            this.results = new Object[max - start];
        }

        public void run()
        {
            for(int i=start;i<max;i++)
            {
                results[i-start] = index.put(data[i], (i & 1) == 0);
                assertSame(results[i-start], index.get(data[i].getOperation(), (i & 1) == 0));
            }
            verifyExistence();
        }

        public void verifyExistence()
        {
            for(int i=start;i<max;i++)
            {
                assertSame(results[i-start], index.get(data[i].getOperation(), (i & 1) == 0));
            }
        }
    }

    public void testWeakCollection() throws Exception
    {
        final int max = 1 << 20;
        final CachedQuery[] toKeep = new CachedQuery[1 << 16];
        final NonLruQueryIndexWithSyncEviction index = new NonLruQueryIndexWithSyncEviction();
        index.zRemoveListener();
        final AtomicInteger done = new AtomicInteger(0);
        ExceptionCatchingThread putter = new ExceptionCatchingThread(new Runnable()
        {
            public void run()
            {
                for(int i=0;i<max;i++)
                {
                    if (i == 1 << 17)
                    {
                        forceGC();
                    }
                    CachedQuery data = createCachedQuery(i);
                    CachedQuery result = index.put(data, false);
                    if (i % ((1 << 4)-1) == 0) toKeep[i >> 4] = result;
                }
                done.set(1);
            }
        });
        ExceptionCatchingThread evictor = new ExceptionCatchingThread(new Runnable()
        {
            public void run()
            {
                while(done.get() == 0)
                    index.reallyEvict();
                index.reallyEvict();
            }
        });
        evictor.start();
        putter.start();
        putter.joinWithExceptionHandling();
        evictor.joinWithExceptionHandling();
        assertEquals(index.size(), index.getEntryCount());
        System.out.println("Total evicted before GC: "+(max - index.size()));
        forceGC();
        index.reallyEvict();
        System.out.println("Total evicted after GC: "+(max - index.size()));
        assertEquals(index.size(), index.getEntryCount());
        assertTrue(index.size() >= toKeep.length);
        for(int i=0;i<toKeep.length;i++)
        {
            assertSame(toKeep[i], index.get(toKeep[i].getOperation(), false));
        }
    }

    private static class NonLruQueryIndexWithSyncEviction extends NonLruQueryIndex
    {
        @Override
        public boolean evictCollectedReferences()
        {
            // do nothing.
            return false;
        }

        public void reallyEvict()
        {
            super.evictCollectedReferences();
        }
    }

    private void forceGC()
    {
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
    }

    protected static Object waitForOtherThread(final Exchanger exchanger)
    {
        return waitForOtherThreadAndPassObject(exchanger, null);
    }

    protected static Object waitForOtherThreadAndPassObject(final Exchanger exchanger, Object object)
    {
        Object result = null;
        try
        {
            result = exchanger.exchange(object);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        return result;
    }

    public void testDuplicatePut()
    {
        NonLruQueryIndex index = new NonLruQueryIndex();

        index.put(createCachedQuery(10), false);
        index.put(createCachedQuery(10), false);
        assertEquals(1, index.getEntryCount());
        assertEquals(1, index.size());
    }

    public void testContendedPut() throws Exception
    {
        final int max = 1000000;
        CachedQuery orderedData[] = new CachedQuery[max];
        for(int i=0;i<max;i++)
        {
            CachedQuery o = createCachedQuery(i);
            orderedData[i] = o;
        }
        final CachedQuery data[] = shuffle(orderedData);
        final Exchanger exchanger = new Exchanger();
        final NonLruQueryIndex index = new NonLruQueryIndex();
        PutRunnableWithExchange first = new PutRunnableWithExchange(0, max, data, index, exchanger);
        PutRunnableWithExchange second = new PutRunnableWithExchange(0, max, data, index, exchanger);
        ExceptionCatchingThread firstThread = new ExceptionCatchingThread(first);
        ExceptionCatchingThread secondThread = new ExceptionCatchingThread(second);
        firstThread.start();
        secondThread.start();
        firstThread.joinWithExceptionHandling();
        secondThread.joinWithExceptionHandling();
        assertEquals(max, index.size());
        assertEquals(max, index.getEntryCount());
        first.verifyExistence();
        second.verifyExistence();
    }

    private static class PutRunnableWithExchange implements Runnable
    {
        private int start;
        private int max;
        private CachedQuery[] data;
        private NonLruQueryIndex index;
        private CachedQuery[] results;
        private Exchanger exchanger;

        private PutRunnableWithExchange(int start, int max, CachedQuery[] data, NonLruQueryIndex index, Exchanger exchanger)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
            this.results = new CachedQuery[max - start];
            this.exchanger = exchanger;
        }

        public void run()
        {
            for(int i=start;i<max;i++)
            {
                waitForOtherThread(exchanger);
                results[i-start] = index.put(data[i], false);
                assertSame(results[i-start], index.put(data[i], false));
                assertSame(results[i-start], index.get(data[i].getOperation(), false));
            }
            verifyExistence();
        }

        public void verifyExistence()
        {
            for(int i=start;i<max;i++)
            {
                assertSame(results[i - start], index.get(data[i].getOperation(), false));
            }
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

    private static class FakeList extends AbstractList implements List
    {
        @Override
        public Object get(int index)
        {
            return null;
        }

        @Override
        public int size()
        {
            return 100000;
        }
    }
}