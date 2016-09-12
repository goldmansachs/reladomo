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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;
import com.gs.fw.common.mithra.util.ExceptionCatchingThread;
import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicInteger;



public class TestConcurrentDatedObjectIndex extends TestCase
{
    static private Logger logger = LoggerFactory.getLogger(TestConcurrentDatedObjectIndex.class.getName());
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected void setUp() throws Exception
    {
        super.setUp();
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

    public void testGetIfAbsentPut() throws Exception
    {
        ConcurrentDatedObjectIndex index = new ConcurrentDatedObjectIndex(new Extractor[] {BitemporalOrderFinder.orderId()},
                BitemporalOrderFinder.getAsOfAttributes(), new BitemporalOrderDatabaseObject());
        Timestamp jan = new Timestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new Timestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
//        Timestamp in = new Timestamp(1275233931251L);
//        Timestamp in = new Timestamp(1275234837913L);
//        Timestamp in = new Timestamp(1275235191697L);
        Timestamp in = new Timestamp(System.currentTimeMillis());
        int max = 10000;
        BitemporalOrderData data[] = new BitemporalOrderData[max];
        for(int i=0;i<max;i++)
        {
            BitemporalOrderData o = createData(in, jan, i);
            data[i] = o;
        }
        data = shuffle(data);
        Object[] results = new Object[max];
        Timestamp[] asOfDates = new Timestamp[] { feb, in };
        ExtractorBasedHashStrategy pkStrategy = ExtractorBasedHashStrategy.create(BitemporalOrderFinder.getPrimaryKeyAttributes());
        for(int i=0;i<max;i++)
        {
            results[i] = index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0);
            assertSame(results[i], index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0));
            assertEquals(i+1, index.size());
        }
        for(int i=0;i<max;i++)
        {
            assertSame(results[i], index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0));
        }
        assertEquals(max, index.size());
    }

    private BitemporalOrderData createData(Timestamp in, Timestamp from, int orderId)
    {
        BitemporalOrderData o = BitemporalOrderDatabaseObject.allocateOnHeapData();
        o.setOrderId(orderId);
        o.setProcessingDateFrom(in);
        o.setProcessingDateTo(DefaultInfinityTimestamp.getDefaultInfinity());
        o.setBusinessDateFrom(from);
        o.setBusinessDateTo(DefaultInfinityTimestamp.getDefaultInfinity());
        return o;
    }

    public void testGetIfAbsentPutWithManyThreads() throws Exception
    {
        Timestamp jan = new ImmutableTimestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new ImmutableTimestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
//        Timestamp in = new ImmutableTimestamp(1275233931251L);
//        Timestamp in = new ImmutableTimestamp(1275234837913L);
//        Timestamp in = new ImmutableTimestamp(1275235191697L);
        Timestamp in = new ImmutableTimestamp(System.currentTimeMillis());
        int max = 1000000;
        BitemporalOrderData data[] = new BitemporalOrderData[max];
        for(int i=0;i<max;i++)
        {
            BitemporalOrderData o = createData(in, jan, i);
            data[i] = o;
        }
        data = shuffle(data);
        Timestamp[] asOfDates = new Timestamp[] { feb, in };
        runGetIfAbsentPut(1, data, max, asOfDates);
        runGetIfAbsentPut(2, data, max, asOfDates);
        runGetIfAbsentPut(5, data, max, asOfDates);
        runGetIfAbsentPut(10, data, max, asOfDates);
        runGetIfAbsentPut(50, data, max, asOfDates);
        runGetIfAbsentPut(100, data, max, asOfDates);
    }

    private void runGetIfAbsentPut(int threads, Object[] data, int max, Timestamp[] asOfDates)
            throws ParseException
    {
        ConcurrentDatedObjectIndex index = new ConcurrentDatedObjectIndex(new Extractor[] {BitemporalOrderFinder.orderId()},
                BitemporalOrderFinder.getAsOfAttributes(), new BitemporalOrderDatabaseObject());
        ExceptionCatchingThread[] runners = new ExceptionCatchingThread[threads];
        GetIfAbsentPutRunnable[] runnables = new GetIfAbsentPutRunnable[threads];
        ExtractorBasedHashStrategy pkStrategy = ExtractorBasedHashStrategy.create(BitemporalOrderFinder.getPrimaryKeyAttributes());
        int chunkSize = max/threads;
        for(int i = 0;i<threads;i++)
        {
            runnables[i] = new GetIfAbsentPutRunnable(chunkSize * i, chunkSize * (i+1), data, index, asOfDates, pkStrategy);
            runners[i] = new ExceptionCatchingThread(runnables[i]);
        }
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
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

    private static class GetIfAbsentPutRunnable implements Runnable
    {
        private int start;
        private int max;
        private Object[] data;
        private ConcurrentDatedObjectIndex index;
        private Timestamp[] asOfDates;
        private ExtractorBasedHashStrategy pkStrategy;
        private Object[] results;

        private GetIfAbsentPutRunnable(int start, int max, Object[] data, ConcurrentDatedObjectIndex index,
                Timestamp[] asOfDates, ExtractorBasedHashStrategy pkStrategy)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
            this.asOfDates = asOfDates;
            this.pkStrategy = pkStrategy;
            this.results = new Object[max - start];
        }

        public void run()
        {
            for(int i=start;i<max;i++)
            {
                results[i-start] = index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0);
                assertSame(results[i-start], index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0));
            }
            verifyExistence();
        }

        public void verifyExistence()
        {
            for(int i=start;i<max;i++)
            {
                assertSame(results[i - start], index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0));
            }
        }
    }

    public void testGetIfAbsentPutAndRemoveWithManyThreads() throws Exception
    {
        Timestamp jan = new ImmutableTimestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new ImmutableTimestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
//        Timestamp in = new ImmutableTimestamp(1275233931251L);
//        Timestamp in = new ImmutableTimestamp(1275234837913L);
//        Timestamp in = new ImmutableTimestamp(1275235191697L);
        Timestamp in = new ImmutableTimestamp(System.currentTimeMillis());
        int max = 1000000;
        BitemporalOrderData data[] = new BitemporalOrderData[max];
        for(int i=0;i<max;i++)
        {
            BitemporalOrderData o = createData(in, jan, i);
            data[i] = o;
        }
        data = shuffle(data);
        Timestamp[] asOfDates = new Timestamp[] { feb, in };
        runGetIfAbsentPutAndRemove(1, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(2, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(5, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(10, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(50, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(100, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(1, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(2, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(5, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(10, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(50, data, max, asOfDates);
        runGetIfAbsentPutAndRemove(100, data, max, asOfDates);
    }

    private void runGetIfAbsentPutAndRemove(int threads, Object[] data, int max, Timestamp[] asOfDates)
            throws ParseException
    {
        ConcurrentDatedObjectIndex index = new ConcurrentDatedObjectIndex(new Extractor[] {BitemporalOrderFinder.orderId()},
                BitemporalOrderFinder.getAsOfAttributes(), new BitemporalOrderDatabaseObject());
        ExceptionCatchingThread[] runners = new ExceptionCatchingThread[threads*2];
        GetIfAbsentPutAndRemoveRunnable[] putters = new GetIfAbsentPutAndRemoveRunnable[threads];
        RemoveRunnable[] removers = new RemoveRunnable[threads];
        ExtractorBasedHashStrategy pkStrategy = ExtractorBasedHashStrategy.create(BitemporalOrderFinder.getPrimaryKeyAttributes());
        int chunkSize = max/threads;
        for(int i = 0;i<threads;i++)
        {
            putters[i] = new GetIfAbsentPutAndRemoveRunnable(chunkSize * i, chunkSize * (i+1), data, index, asOfDates, pkStrategy);
            runners[i*2] = new ExceptionCatchingThread(putters[i]);
            removers[i] = new RemoveRunnable(chunkSize * i, chunkSize * (i+1), data, index, asOfDates, pkStrategy);
            runners[i*2+1] = new ExceptionCatchingThread(removers[i]);
        }
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        long start = System.currentTimeMillis();
        for(int i = 0;i<threads*2;i++)
        {
            runners[i].start();
        }

        for(int i = 0;i<threads*2;i++)
        {
            runners[i].joinWithExceptionHandling();
        }
        System.out.println("running with "+threads+" threads took "+(System.currentTimeMillis() - start)/1000.0+" s");
        assertEquals(index.size(), index.getEntryCount());
        for(int i = 0;i<threads;i++)
        {
            putters[i].verifyExistence();
        }

        assertEquals(max/2, index.size());
    }

    private static class GetIfAbsentPutAndRemoveRunnable implements Runnable
    {
        private int start;
        private int max;
        private Object[] data;
        private ConcurrentDatedObjectIndex index;
        private Timestamp[] asOfDates;
        private ExtractorBasedHashStrategy pkStrategy;
        private Object[] results;

        private GetIfAbsentPutAndRemoveRunnable(int start, int max, Object[] data, ConcurrentDatedObjectIndex index,
                Timestamp[] asOfDates, ExtractorBasedHashStrategy pkStrategy)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
            this.asOfDates = asOfDates;
            this.pkStrategy = pkStrategy;
            this.results = new Object[max - start];
        }

        public void run()
        {
            for(int i=start;i<max;i++)
            {
                results[i-start] = index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0);
            }
            verifyExistence();
        }

        public void verifyExistence()
        {
            for(int i=start;i<max;i+=2)
            {
                assertSame(results[i - start], index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0));
            }
        }
    }

    private static class RemoveRunnable implements Runnable
    {
        private int start;
        private int max;
        private Object[] data;
        private ConcurrentDatedObjectIndex index;
        private Timestamp[] asOfDates;
        private ExtractorBasedHashStrategy pkStrategy;
        private BitemporalOrderDatabaseObject factory;

        private RemoveRunnable(int start, int max, Object[] data, ConcurrentDatedObjectIndex index,
                Timestamp[] asOfDates, ExtractorBasedHashStrategy pkStrategy)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
            this.asOfDates = asOfDates;
            this.pkStrategy = pkStrategy;
            factory = new BitemporalOrderDatabaseObject();
        }

        public void run()
        {
            for(int i=start+1;i<max;)
            {
                Object o = index.remove(factory.createObject((MithraDataObject) data[i], asOfDates));
                if (o != null) i+=2;
            }
            verifyNonExistence();
        }

        public void verifyNonExistence()
        {
            for(int i=start+1;i<max;i+=2)
            {
                assertNull(index.remove(factory.createObject((MithraDataObject) data[i], asOfDates)));
            }
        }
    }

    public void testWeakCollection() throws Exception
    {
        final Timestamp jan = new ImmutableTimestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        final Timestamp feb = new ImmutableTimestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
        final Timestamp in = new ImmutableTimestamp(System.currentTimeMillis());
        final int max = 1 << 18;
        final Object[] toKeep = new Object[1 << 14];
        final ConcurrentDatedObjectIndex index = new ConcurrentDatedObjectIndex(new Extractor[] {BitemporalOrderFinder.orderId()},
                BitemporalOrderFinder.getAsOfAttributes(), new BitemporalOrderDatabaseObject());
        final ExtractorBasedHashStrategy pkStrategy = ExtractorBasedHashStrategy.create(BitemporalOrderFinder.getPrimaryKeyAttributes());
        final Timestamp[] asOfDates = new Timestamp[] { feb, in };
        final AtomicInteger done = new AtomicInteger(0);
        ExceptionCatchingThread putter = new ExceptionCatchingThread(new Runnable()
        {
            public void run()
            {
                int shift = Integer.numberOfTrailingZeros(max / toKeep.length);
                for(int i=0;i<max;i++)
                {
                    Object data = createData(in, jan, i);
                    Object result = index.getFromDataOrPutIfAbsent(data, asOfDates, pkStrategy.computeHashCode(data), true);
                    if (i % ((1 << shift)-1) == 0) toKeep[i >> shift] = result;
                }
                done.set(1);
            }
        });
        ExceptionCatchingThread evictor = new ExceptionCatchingThread(new Runnable()
        {
            public void run()
            {
                while(done.get() == 0)
                    index.evictCollectedReferences();
                index.evictCollectedReferences();
            }
        });
        evictor.start();
        putter.start();
        putter.joinWithExceptionHandling();
        evictor.joinWithExceptionHandling();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        assertTrue(index.size() >= toKeep.length);
        index.evictCollectedReferences();
        assertTrue(index.size() >= toKeep.length);
        for(int i=0;i<toKeep.length;i++)
        {
            assertSame(toKeep[i], index.getFromDataOrPutIfAbsent(((MithraDatedObject)toKeep[i]).zGetCurrentData(), asOfDates, pkStrategy.computeHashCode(toKeep[i]), true));
        }
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

    public void testContendedGetIfAbsentPut() throws Exception
    {
        Timestamp jan = new ImmutableTimestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new ImmutableTimestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
        final Timestamp in = new ImmutableTimestamp(System.currentTimeMillis());
        final int max = 1000000;
        BitemporalOrderData orderedData[] = new BitemporalOrderData[max];
        for(int i=0;i<max;i++)
        {
            BitemporalOrderData o = createData(in, jan, i);
            orderedData[i] = o;
        }
        final BitemporalOrderData data[] = shuffle(orderedData);
        final Timestamp[] asOfDates = new Timestamp[] { feb, in };
        final Exchanger exchanger = new Exchanger();
        final ConcurrentDatedObjectIndex index = new ConcurrentDatedObjectIndex(new Extractor[] {BitemporalOrderFinder.orderId()},
                BitemporalOrderFinder.getAsOfAttributes(), new BitemporalOrderDatabaseObject());
        final ExtractorBasedHashStrategy pkStrategy = ExtractorBasedHashStrategy.create(BitemporalOrderFinder.getPrimaryKeyAttributes());
        GetIfAbsentPutRunnableWithExchange first = new GetIfAbsentPutRunnableWithExchange(0, max, data, index, asOfDates, pkStrategy, exchanger);
        GetIfAbsentPutRunnableWithExchange second = new GetIfAbsentPutRunnableWithExchange(0, max, data, index, asOfDates, pkStrategy, exchanger);
        ExceptionCatchingThread firstThread = new ExceptionCatchingThread(first);
        ExceptionCatchingThread secondThread = new ExceptionCatchingThread(second);
        firstThread.start();
        secondThread.start();
        firstThread.joinWithExceptionHandling();
        secondThread.joinWithExceptionHandling();
        assertEquals(max, index.size());
        first.verifyExistence();
        second.verifyExistence();
    }

    private static class GetIfAbsentPutRunnableWithExchange implements Runnable
    {
        private int start;
        private int max;
        private Object[] data;
        private ConcurrentDatedObjectIndex index;
        private Timestamp[] asOfDates;
        private ExtractorBasedHashStrategy pkStrategy;
        private Object[] results;
        private Exchanger exchanger;

        private GetIfAbsentPutRunnableWithExchange(int start, int max, Object[] data, ConcurrentDatedObjectIndex index,
                Timestamp[] asOfDates, ExtractorBasedHashStrategy pkStrategy, Exchanger exchanger)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
            this.asOfDates = asOfDates;
            this.pkStrategy = pkStrategy;
            this.results = new Object[max - start];
            this.exchanger = exchanger;
        }

        public void run()
        {
            for(int i=start;i<max;i++)
            {
                waitForOtherThread(exchanger);
                results[i-start] = index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0);
                assertSame(results[i-start], index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0));
            }
            verifyExistence();
        }

        public void verifyExistence()
        {
            for(int i=start;i<max;i++)
            {
                assertSame(results[i - start], index.getFromDataOrPutIfAbsent(data[i], asOfDates, pkStrategy.computeHashCode(data[i]), (i & 1) == 0));
            }
        }
    }
}