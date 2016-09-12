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

import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.AbstractStringExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.util.EstimateDistribution;
import com.gs.fw.common.mithra.util.ExceptionCatchingThread;
import junit.framework.TestCase;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Exchanger;


public class TestFullUniqueIndex extends TestCase
{
    private static final int MAX = 10000;

    public void xtestPerfFui()
    {
        for(int i=0;i<10;i++)
        {
            long start = System.currentTimeMillis();
            testPutAndGet();
            System.out.println("fui took "+(System.currentTimeMillis() - start));
        }
    }

    public void xtestPerfConcFui()
    {
        for(int i=0;i<10;i++)
        {
            long start = System.currentTimeMillis();
            testPutAndGet2();
            System.out.println("conc fui took "+(System.currentTimeMillis() - start));
        }
    }

    public void testPutAndGet()
    {
        runTestPutAndGet(0);
        runTestPutAndGet(1);
        runTestPutAndGet(2);
        runTestPutAndGet(3);
    }

    public void testPutAndRemoveThrashingInt()
    {
        FullUniqueIndex fullUniqueIndex = new FullUniqueIndex(new Extractor[]{new ShiftedIntExtractor(0)},1000000);
        long startTime = System.currentTimeMillis();
        for(int i=0;i<300;i++)
        {
            for(int y=0;y<100000;y++)
            {
                TestObject value = new TestObject((int) (Math.random() * (i*y)));
                fullUniqueIndex.put(value);
                fullUniqueIndex.remove(value);
            }
            if (i > 100)
            {
                assertTrue(i*500 > (System.currentTimeMillis() - startTime));
            }
        }
    }

    public void testPutAndGet2()
    {
        runTestPutAndGet2(0);
        runTestPutAndGet2(1);
        runTestPutAndGet2(2);
        runTestPutAndGet2(3);
    }

    public void runTestPutAndGet (int shift)
    {
        Extractor[] extractors = {new ShiftedIntExtractor(shift)};
        FullUniqueIndex<TestObject> index = new FullUniqueIndex(extractors, 7);
        for (int i=0; i<MAX; i++)
        {
            TestObject value = new TestObject(i);
            index.put(value);
        }
        for (int i=0; i<MAX; i++)
        {
            TestObject key = new TestObject(i);
            TestObject value = index.get(key, extractors);

            assertNotSame(key, value);
            assertEquals(key.n, value.n);

            assertTrue(index.contains(key, extractors, null));
        }
    }

    public void runTestPutAndGet2(int shift)
    {
        Extractor[] extractors = {new ShiftedIntExtractor(shift)};
        ConcurrentFullUniqueIndex<TestObject> index = new ConcurrentFullUniqueIndex(extractors, 7);
        for (int i=0; i<MAX; i++)
        {
            TestObject value = new TestObject(i);
            index.putIfAbsent(value);
        }
        for (int i=0; i<MAX; i++)
        {
            TestObject key = new TestObject(i);
            TestObject value = index.get(key, extractors);

            assertNotSame(key, value);
            assertEquals(key.n, value.n);
        }
    }

    public void testCopy()
    {
        Extractor[] extractors = {new ShiftedIntExtractor(3)};
        FullUniqueIndex<TestObject> index = new FullUniqueIndex(extractors, 7);
        for (int i=0; i<MAX; i++)
        {
            TestObject value = new TestObject(i);
            index.put(value);
        }
        index = index.copy();
        for (int i=0; i<MAX; i++)
        {
            TestObject key = new TestObject(i);
            TestObject value = index.get(key, extractors);

            assertNotSame(key, value);
            assertEquals(key.n, value.n);
        }
    }

    public void testIterator()
    {
        Extractor[] extractors = {new ShiftedIntExtractor(0)};
        ConcurrentFullUniqueIndex<TestObject> index = new ConcurrentFullUniqueIndex(extractors, 7);
        for (int i=0; i<MAX; i++)
        {
            TestObject value = new TestObject(i);
            index.putIfAbsent(value);
        }
        Iterator it = index.iterator();
        FullUniqueIndex index2 = new FullUniqueIndex(extractors, 7);
        int count = 0;
        while(it.hasNext())
        {
            index2.put(it.next());
            count++;
            assertTrue(count <= MAX);
        }
        assertEquals(index2.size(), index.size());
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

    public void testPutIfAbsentWithManyThreads() throws Exception
    {
        int max = 1000000;
        TestObject data[] = new TestObject[max];
        for(int i=0;i<max;i++)
        {
            TestObject o = new TestObject(i);
            data[i] = o;
        }
        data = shuffle(data);
        runPutIfAbsent(1, data, max);
        runPutIfAbsent(2, data, max);
        runPutIfAbsent(5, data, max);
        runPutIfAbsent(10, data, max);
        runPutIfAbsent(50, data, max);
        runPutIfAbsent(100, data, max);
        runPutIfAbsent(1, data, max);
        runPutIfAbsent(2, data, max);
        runPutIfAbsent(5, data, max);
        runPutIfAbsent(10, data, max);
        runPutIfAbsent(50, data, max);
        runPutIfAbsent(100, data, max);
    }

    private void runPutIfAbsent(int threads, Object[] data, int max)
            throws ParseException
    {
        Extractor[] extractors = {new ShiftedIntExtractor(0)};
        ConcurrentFullUniqueIndex<TestObject> index = new ConcurrentFullUniqueIndex(extractors, 7);
        ExceptionCatchingThread[] runners = new ExceptionCatchingThread[threads];
        PutIfAbsentRunnable[] runnables = new PutIfAbsentRunnable[threads];
        int chunkSize = max/threads;
        for(int i = 0;i<threads;i++)
        {
            runnables[i] = new PutIfAbsentRunnable(chunkSize * i, chunkSize * (i+1), data, index);
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
        System.out.println("running with " + threads + " threads took " + (System.currentTimeMillis() - start) / 1000.0 + " s");
        for(int i = 0;i<threads;i++)
        {
            runnables[i].verifyExistence();
        }

        assertEquals(max, index.size());
    }

    private static class PutIfAbsentRunnable implements Runnable
    {
        private int start;
        private int max;
        private Object[] data;
        private ConcurrentFullUniqueIndex index;

        private PutIfAbsentRunnable(int start, int max, Object[] data, ConcurrentFullUniqueIndex index)
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
                assertTrue(index.putIfAbsent(data[i]));
                assertFalse(index.putIfAbsent(data[i]));
                assertSame("not same at element " +i, data[i], index.getFromUnderlyingKey(data[i]));
            }
            verifyExistence();
        }

        public void verifyExistence()
        {
            for(int i=start;i<max;i++)
            {
                assertSame(data[i], index.getFromUnderlyingKey(data[i]));
            }
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
        final int max = 1000000;
        TestObject orderedData[] = new TestObject[max];
        for(int i=0;i<max;i++)
        {
            TestObject o = new TestObject(i);
            orderedData[i] = o;
        }
        final TestObject data[] = shuffle(orderedData);
        final Exchanger exchanger = new Exchanger();
        Extractor[] extractors = {new ShiftedIntExtractor(1)};
        final ConcurrentFullUniqueIndex<TestObject> index = new ConcurrentFullUniqueIndex(extractors, 7);
        PutIfAbsentRunnableWithExchange first = new PutIfAbsentRunnableWithExchange(0, max, data, index, exchanger);
        PutIfAbsentRunnableWithExchange second = new PutIfAbsentRunnableWithExchange(0, max, data, index, exchanger);
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

    private static class PutIfAbsentRunnableWithExchange implements Runnable
    {
        private int start;
        private int max;
        private Object[] data;
        private ConcurrentFullUniqueIndex index;
        private boolean[] results;
        private Exchanger exchanger;

        private PutIfAbsentRunnableWithExchange(int start, int max, Object[] data, ConcurrentFullUniqueIndex index, Exchanger exchanger)
        {
            this.start = start;
            this.max = max;
            this.data = data;
            this.index = index;
            this.results = new boolean[max - start];
            this.exchanger = exchanger;
        }

        public void run()
        {
            for(int i=start;i<max;i++)
            {
                waitForOtherThread(exchanger);
                results[i-start] = index.putIfAbsent(data[i]);
                if (results[i-start])
                {
                    assertSame(data[i], index.getFromUnderlyingKey(data[i]));
                }
            }
            verifyExistence();
        }

        public void verifyExistence()
        {
            for(int i=start;i<max;i++)
            {
                if (results[i-start])
                {
                    assertSame(data[i], index.getFromUnderlyingKey(data[i]));
                }
            }
        }
    }

    public void testEstimateDistribution()
    {
        List<Integer> list = new ArrayList<Integer>(1000000);
        for(int i=0;i<1000000;i++)
        {
            list.add(i/500);
        }
        estimateDistributionManyTimes(list, "uniform distribution", 2000);
        list.clear();
        Random r = new Random();
        for(int i=0;i<2000;i++)
        {
            int end = r.nextInt(1000);
            for(int j=0;j<end;j++)
            {
                list.add(i);
            }
            list.add(i/1000);
        }

        estimateDistributionManyTimes(list, "random distribution", 2000);
        list.clear();
        for(int i=0;i<500000;i++)
        {
            list.add(i/1000);
        }
        for(int i=500000;i<1000000;i++)
        {
            list.add(1000000);
        }

        estimateDistributionManyTimes(list, "skewed distribution2", 501);
    }

    private void estimateDistributionManyTimes(List<Integer> list, String message, int expected)
    {
        Collections.shuffle(list);
        estimateDistribution(list, message, expected);
        Collections.shuffle(list);
        estimateDistribution(list, message, expected);
        Collections.shuffle(list);
        estimateDistribution(list, message, expected);
        Collections.shuffle(list);
        estimateDistribution(list, message, expected);
    }

    private void estimateDistribution(List<Integer> list, String message, int expected)
    {
        Set<Integer> set = new HashSet<Integer>();

        Random r = new Random();

        for(int i=0;i< EstimateDistribution.SAMPLE_SIZE;i++)
        {
            set.add(list.get(r.nextInt(list.size())));
        }
        int size1 = set.size();

        for(int i=0;i< EstimateDistribution.SAMPLE_SIZE;i++)
        {
            set.add(list.get(r.nextInt(list.size())));
        }
        int size2 = set.size();
        int estimate = EstimateDistribution.estimateSize(size1, size2, list.size());
        assertTrue("bad estimate for "+message, estimate < 2 * expected && estimate > expected / 2);
    }


    private class TestObject
    {
        int n;

        private TestObject(int n)
        {
            this.n = n;
        }
    }

    private static class ShiftedIntExtractor implements IntExtractor
    {
        private int shift;

        private ShiftedIntExtractor(int shift)
        {
            this.shift = shift;
        }

        public int intValueOf(Object o)
        {
            return ((TestObject)o).n;
        }

        public void setIntValue(Object o, int newValue)
        {
            ((TestObject)o).n = newValue;
        }

        public void setValue(Object o, Object newValue)
        {
        }

        public void setValueNull(Object o)
        {
        }

        public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
        {
        }

        public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
        {
        }

        public boolean isAttributeNull(Object o)
        {
            return false;
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            return ((TestObject)first).n == ((TestObject)second).n;
        }

        public int valueHashCode(Object object)
        {
            return ((TestObject)object).n >> shift;
        }

        public boolean valueEquals(Object first, Object second)
        {
            return ((TestObject)first).n == ((TestObject)second).n;
        }

        public Object valueOf(Object object)
        {
            return ((TestObject)object).n;
        }
    }
}
