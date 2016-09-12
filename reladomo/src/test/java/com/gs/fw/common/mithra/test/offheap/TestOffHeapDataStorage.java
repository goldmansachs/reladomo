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

package com.gs.fw.common.mithra.test.offheap;

import com.gs.collections.api.iterator.IntIterator;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.cache.ReadWriteLock;
import com.gs.fw.common.mithra.cache.offheap.*;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderData;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderDatabaseObject;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import com.gs.fw.common.mithra.util.StringPool;
import junit.framework.TestCase;
import org.junit.Assert;
import sun.misc.Unsafe;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;


public class TestOffHeapDataStorage extends TestCase
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final int MAX = 100000;

    private HarnessedFastUnsafeOffHeapDataStorage dataStorage;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        dataStorage = new HarnessedFastUnsafeOffHeapDataStorage(BitemporalOrderData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        BitemporalOrderData.BitemporalOrderDataOffHeap.zSetStorage(dataStorage);
        StringPool.getInstance().enableOffHeapSupport();
    }
/*
    needs testing:
    forAll
    forAllInParallel
    addSemiUniqueToContainer
*/

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        dataStorage.destroy();
    }

    private void renewStorage()
    {
        dataStorage.destroy();
        dataStorage = new HarnessedFastUnsafeOffHeapDataStorage(BitemporalOrderData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        BitemporalOrderData.BitemporalOrderDataOffHeap.zSetStorage(dataStorage);
    }

    private BitemporalOrderData createOrder(int orderId, int userId)
    {
        BitemporalOrderData o = BitemporalOrderDatabaseObject.allocateOffHeapData();
        o.setOrderId(orderId);
        o.setUserId(userId);
        return o;
    }

    public void testConcurrentScanPagesToSend() throws Exception
    {
        // This test is to verify that FastUnsafeOffHeapDataStorage.scanPagesToSend() is able to run concurrently with no issues.
        // We exercise it with an extreme scenario of 10 replica sync threads, 1 insert thread and 1 update thread.

        // Add some pages to start with
        for (int pageNumber = 0; pageNumber < 1000; pageNumber++)
        {
            dataStorage.addPageVersion(1);
        }
        dataStorage.setInitialCurrentPageVersion(2);

        List<Thread> threads = FastList.newList();
        List<ConcurrentReplicaSimulator> replicas = FastList.newList();

        ConcurrentInsertSimulator inserter = new ConcurrentInsertSimulator("ConcurrentInsertSimulator", 100000);
        Thread insertThread = new Thread(inserter);
        insertThread.setName(inserter.getName());
        threads.add(insertThread);

        ConcurrentUpdateSimulator updater = new ConcurrentUpdateSimulator("ConcurrentUpdateSimulator", 100000);
        Thread updateThread = new Thread(updater);
        updateThread.setName(updater.getName());
        threads.add(updateThread);

        for (int i = 0; i < 10; i++)
        {
            ConcurrentReplicaSimulator replica = new ConcurrentReplicaSimulator("ConcurrentReplicaSimulator-" + i, 10000, 250000);
            replicas.add(replica);
            Thread replicaThread = new Thread(replica);
            replicaThread.setName(replica.getName());
            threads.add(replicaThread);
        }

        //System.out.println("Starting threads");

        for (Thread thread : threads)
        {
            thread.start();
        }

        for (Thread thread : threads)
        {
            thread.join();
        }

        //System.out.println("Checking results");

        for (ConcurrentReplicaSimulator replica : replicas)
        {
            if (!replica.finishedWithoutError())
            {
                ByteArrayOutputStream stackTraceBuffer = new ByteArrayOutputStream();
                replica.getError().printStackTrace(new PrintStream(stackTraceBuffer));
                Assert.fail("One or more replicas failed. There may be additional error details for other replicas in the logs above.\n"
                        + "The exception from one of these replicas (" + replica.getName() + ") was as follows: " + stackTraceBuffer.toString());
            }

            // Clean up anything left over if the replica finished before the inserter/updater
            replica.runSingleIteration();

            IntHashSet insertedAndUpdatedPagesCombined = new IntHashSet();
            insertedAndUpdatedPagesCombined.addAll(inserter.getInsertedPageNumbers());
            insertedAndUpdatedPagesCombined.addAll(updater.getUpdatedPageNumbers());

            if (!replica.getPagesFoundByScan().containsAll(insertedAndUpdatedPagesCombined))
            {
                IntIterator it = inserter.getInsertedPageNumbers().intIterator();
                while (it.hasNext())
                {
                    int pageNumber = it.next();
                    if (!replica.getPagesFoundByScan().contains(pageNumber))
                    {
                        System.out.println(replica.getName() + "Inserted/updated page is missing from scan results: " + pageNumber);
                    }
                }
            }

            Assert.assertTrue(replica.getPagesFoundByScan().containsAll(insertedAndUpdatedPagesCombined));
            Assert.assertTrue(insertedAndUpdatedPagesCombined.containsAll(replica.getPagesFoundByScan()));

            replica.destroy();
        }

        //System.out.println("Done");

    }

    private class ConcurrentReplicaSimulator implements Runnable
    {
        private boolean finished = false;
        private IntHashSet pagesFoundByScan = new IntHashSet(1000);
        private String name;
        private int iterationsToRun;
        private FastUnsafeOffHeapIntList pagesToSend;
        private FastUnsafeOffHeapIntList repeatPagesToSend;
        private long maxVersion = 1;
        private Throwable error = null;

        public ConcurrentReplicaSimulator(String name, int iterationsToRun, int approxMaxNumberOfPages)
        {
            this.name = name;
            this.iterationsToRun = iterationsToRun;
            this.pagesToSend = new FastUnsafeOffHeapIntList(approxMaxNumberOfPages);
            this.repeatPagesToSend = new FastUnsafeOffHeapIntList(approxMaxNumberOfPages);
        }

        @Override
        public void run()
        {
            try
            {
                for (int iteration = 0; iteration < this.iterationsToRun; iteration++)
                {
//                    if (iteration % 1000 == 0)
//                    {
//                        System.out.println(this.name + " : iteration = " + iteration);
//                    }

                    runSingleIteration();
                }
                finished = true;
            }
            catch (Throwable e)
            {
                this.error = e;
                System.err.print(this.name + " failed : ");
                e.printStackTrace(System.err);
            }
        }

        public void runSingleIteration()
        {

            long firstNewMaxVersion;

            IntHashSet pagesStampedbyFirstScan = new IntHashSet(100);
            pagesToSend.clear();
            dataStorage.getReadWriteLock().acquireReadLock();
            try
            {
                firstNewMaxVersion = dataStorage.scanPagesToSend(maxVersion, pagesToSend);
                for (int pageIndex = 0; pageIndex < pagesToSend.size(); pageIndex++)
                {
                    int pageNumber = pagesToSend.get(pageIndex);
                    long pageVersion = dataStorage.getPageVersion(pageNumber);

                    Assert.assertTrue(pageVersion > maxVersion);
                    if (pageVersion == firstNewMaxVersion)
                    {
                        pagesStampedbyFirstScan.add(pageNumber);
                    }
                }
            }
            finally
            {
                dataStorage.getReadWriteLock().release();
            }

            if (pagesToSend.size() > 0)
            {
                Assert.assertTrue(firstNewMaxVersion > maxVersion);

                // Now, repeat scanPagesToSend() a second time to check nothing was missed the first time

                long secondNewMaxVersion;
                IntHashSet pagesStampedBySecondScan = new IntHashSet(100);
                repeatPagesToSend.clear();
                dataStorage.getReadWriteLock().acquireReadLock();
                try
                {
                    secondNewMaxVersion = dataStorage.scanPagesToSend(maxVersion, repeatPagesToSend);
                    for (int pageIndex = 0; pageIndex < repeatPagesToSend.size(); pageIndex++)
                    {
                        int pageNumber = repeatPagesToSend.get(pageIndex);
                        long pageVersion = dataStorage.getPageVersion(pageNumber);

                        Assert.assertTrue(pageVersion > maxVersion);
                        if (pageVersion == firstNewMaxVersion)
                        {
                            pagesStampedBySecondScan.add(pageNumber);
                        }
                    }
                }
                finally
                {
                    dataStorage.getReadWriteLock().release();
                }

                if (!pagesStampedbyFirstScan.containsAll(pagesStampedBySecondScan))
                {
                    IntArrayList firstAsList = new IntArrayList();
                    firstAsList.addAll(pagesStampedbyFirstScan);
                    firstAsList.sortThis();

                    IntArrayList secondAsList = new IntArrayList();
                    secondAsList.addAll(pagesStampedBySecondScan);
                    secondAsList.sortThis();

                    System.err.println(this.name + " : Detected missing pages. Details follow:");
                    for (int pageIndex = 0; pageIndex < secondAsList.size(); pageIndex++)
                    {
                        int pageNumber = secondAsList.get(pageIndex);
                        if (!pagesStampedbyFirstScan.contains(pageNumber))
                        {
                            System.err.println(this.name + " : Page number is missing from first set: " + pageNumber);
                        }
                    }
                    System.err.println(this.name + " : Pages found by scan #1 = " + firstAsList.toString());
                    System.err.println(this.name + " : Pages found by scan #2 = " + secondAsList.toString());
                    System.err.flush();

                    Assert.fail("Found page with the same max version number which was not included in the first scan result");
                }

                Assert.assertTrue(repeatPagesToSend.size() >= pagesToSend.size());
                if (repeatPagesToSend.size() > pagesToSend.size())
                {
                    Assert.assertTrue(secondNewMaxVersion > firstNewMaxVersion);
                }
                else
                {
                    Assert.assertTrue(secondNewMaxVersion >= firstNewMaxVersion);
                }

                IntHashSet pagesFoundByFirstScan = getAsIntHashSet(pagesToSend);
                IntHashSet pagesFoundBySecondScan = getAsIntHashSet(repeatPagesToSend);

                Assert.assertTrue(pagesFoundBySecondScan.containsAll(pagesFoundByFirstScan));
                if (secondNewMaxVersion == firstNewMaxVersion)
                {
                    Assert.assertTrue(pagesFoundByFirstScan.containsAll(pagesFoundBySecondScan));
                }

                // Keep track for a final reconciliation once all threads have finished
                this.pagesFoundByScan.addAll(pagesFoundBySecondScan);

                maxVersion = secondNewMaxVersion;
            } else
            {
                Assert.assertEquals(firstNewMaxVersion, maxVersion);
            }
        }

        private IntHashSet getAsIntHashSet(FastUnsafeOffHeapIntList pagesToSend)
        {
            IntHashSet pagesInFirstCall = new IntHashSet(pagesToSend.size());
            for (int pageIndex = 0; pageIndex < pagesToSend.size(); pageIndex++)
            {
                int pageNumber = pagesToSend.get(pageIndex);
                pagesInFirstCall.add(pageNumber);
            }
            return pagesInFirstCall;
        }

        public boolean finishedWithoutError()
        {
            return finished;
        }

        public IntHashSet getPagesFoundByScan()
        {
            return pagesFoundByScan;
        }

        public String getName()
        {
            return name;
        }

        public Throwable getError()
        {
            return error;
        }

        public void destroy()
        {
            this.pagesToSend.destroy();
            this.repeatPagesToSend.destroy();
        }
    }

    private class ConcurrentInsertSimulator implements Runnable
    {
        private IntHashSet insertedPageNumbers;
        private String name;
        private int iterationsToRun;

        public ConcurrentInsertSimulator(String name, int iterationsToRun)
        {
            this.name = name;
            this.iterationsToRun = iterationsToRun;
            this.insertedPageNumbers = new IntHashSet(iterationsToRun);
        }

        @Override
        public void run()
        {
            for (int iteration = 0; iteration < this.iterationsToRun; iteration++)
            {
//                if (iteration % 1000 == 0)
//                {
//                    System.out.println(this.name + " : iteration = " + iteration);
//                }
                insertedPageNumbers.add(dataStorage.addPageVersion(0));
            }
        }

        public IntHashSet getInsertedPageNumbers()
        {
            return insertedPageNumbers;
        }

        public String getName()
        {
            return name;
        }
    }

    private class ConcurrentUpdateSimulator implements Runnable
    {
        private IntHashSet updatedPageNumbers;
        private String name;
        private int iterationsToRun;

        public ConcurrentUpdateSimulator(String name, int iterationsToRun)
        {
            this.name = name;
            this.iterationsToRun = iterationsToRun;
            this.updatedPageNumbers = new IntHashSet(iterationsToRun);
        }

        @Override
        public void run()
        {
            for (int iteration = 0; iteration < this.iterationsToRun; iteration++)
            {
//                if (iteration % 1000 == 0)
//                {
//                    System.out.println(this.name + " : iteration = " + iteration);
//                }
                int pageNumber = dataStorage.setVersionOfRandomPage(0L);
                updatedPageNumbers.add(pageNumber);
            }
        }

        public IntHashSet getUpdatedPageNumbers()
        {
            return updatedPageNumbers;
        }

        public String getName()
        {
            return name;
        }
    }

    public void testLongListSet()
    {
        FastUnsafeOffHeapLongList list = new FastUnsafeOffHeapLongList(0);

        list.set(1000, 12);
        assertEquals(12, list.get(1000));
        assertEquals(0, list.get(500));

        list.set(50000, 45);
        assertEquals(45, list.get(50000));
        assertEquals(0, list.get(10000));
        list.destroy();
        list = new FastUnsafeOffHeapLongList(1000);
        for (int i = 0; i < 1000000; i++)
        {
            list.set(i, i + 1);
        }
        list.destroy();

        list = new FastUnsafeOffHeapLongList(1000);
        list.set(Integer.MAX_VALUE - 1, 345);
        assertEquals(345, list.get(Integer.MAX_VALUE - 1));
        list.destroy();
    }

    public void testLongListClearAndCopy()
    {
        FastUnsafeOffHeapLongList original = new FastUnsafeOffHeapLongList(0);
        int arraySizeForTest = 1000;
        for (int i = 0; i < arraySizeForTest; i++)
        {
            original.add(i);
        }
        assertEquals(arraySizeForTest, original.size());

        FastUnsafeOffHeapLongList copy = new FastUnsafeOffHeapLongList(0);
        copy.add(999); // set some initial data to be overwritten
        assertEquals(1, copy.size());

        copy.clearAndCopy(original);
        assertEquals(arraySizeForTest, copy.size());

        // Mangle the original to prove that the copy is independent of it
        for (int i = 0; i < arraySizeForTest; i++)
        {
            original.set(i, -i);
        }
        original.destroy();

        for (int i = 0; i < arraySizeForTest; i++)
        {
            assertEquals(i, copy.get(i));
        }

        FastUnsafeOffHeapLongList emptyList = new FastUnsafeOffHeapLongList(0);
        copy.clearAndCopy(emptyList);
        assertEquals(0, copy.size());

        copy.destroy();
    }

    public void testIntListRandom()
    {
        FastUnsafeOffHeapIntList intList = new FastUnsafeOffHeapIntList(10);

        long seed = System.currentTimeMillis();
        Random r = new Random(seed);
        for (int i = 0; i < 1000; i++)
        {
            intList.add(r.nextInt());
        }
        assertEquals(1000, intList.size());
        r = new Random(seed);
        for (int i = 0; i < 1000; i++)
        {
            assertEquals(r.nextInt(), intList.get(i));
        }
        intList.sort();
        assertEquals(1000, intList.size());
        for (int i = 1; i < 1000; i++)
        {
            assertTrue(intList.get(i - 1) <= intList.get(i));
        }
        intList.destroy();
    }

    public void testIntList()
    {
        FastUnsafeOffHeapIntList intList = new FastUnsafeOffHeapIntList(10);

        for (int i = 0; i < 10; i++)
        {
            intList.add(1000 - i * 10);
        }
        assertEquals(10, intList.size());
        for (int i = 0; i < 10; i++)
        {
            assertEquals(1000 - i * 10, intList.get(i));
        }
        intList.sort();
        assertEquals(10, intList.size());
        for (int i = 0; i < 10; i++)
        {
            assertEquals(1000 - (9 - i) * 10, intList.get(i));
        }
        intList.destroy();
    }

    public void testIntArrayStorage()
    {
        FastUnsafeOffHeapIntArrayStorage intArrayStorage = new FastUnsafeOffHeapIntArrayStorage();

        try
        {
            {
                int size = 1024;
                int ref = intArrayStorage.allocate(size);
                assertEquals(size, intArrayStorage.getLength(ref));

                assertIntArrayIsAllZeroes(intArrayStorage, size, ref);

                setIntArraySampleValues(intArrayStorage, size, ref);
                assertIntArraySampleValues(intArrayStorage, size, ref);

                intArrayStorage.clear(ref);
                assertIntArrayIsAllZeroes(intArrayStorage, size, ref);

                setIntArraySampleValues(intArrayStorage, size, ref);
                assertIntArraySampleValues(intArrayStorage, size, ref);

                intArrayStorage.free(ref);
            }

            {
                int size = 1024;
                int ref = intArrayStorage.allocate(size);
                assertEquals(size, intArrayStorage.getLength(ref));
                assertIntArrayIsAllZeroes(intArrayStorage, size, ref);

                intArrayStorage.free(ref);
            }

            {
                int size1 = 256;
                int ref1 = intArrayStorage.allocate(size1);
                assertEquals(size1, intArrayStorage.getLength(ref1));
                assertIntArrayIsAllZeroes(intArrayStorage, size1, ref1);

                int size2 = 512;
                int ref2 = intArrayStorage.allocate(size2);
                assertEquals(size2, intArrayStorage.getLength(ref2));
                assertIntArrayIsAllZeroes(intArrayStorage, size2, ref2);

                setIntArraySampleValues(intArrayStorage, size1, ref1);
                assertIntArraySampleValues(intArrayStorage, size1, ref1);
                assertIntArrayIsAllZeroes(intArrayStorage, size2, ref2);

                int newSize1 = size1 + 1024;
                ref1 = intArrayStorage.reallocate(ref1, newSize1);
                assertEquals(newSize1, intArrayStorage.getLength(ref1));
                assertIntArraySampleValues(intArrayStorage, size1, ref1);
                for (int i = size1; i < newSize1; i++)
                {
                    assertEquals(0, intArrayStorage.getInt(ref1, i));
                }

                setIntArraySampleValues(intArrayStorage, newSize1, ref1);
                assertIntArraySampleValues(intArrayStorage, newSize1, ref1);

                assertIntArrayIsAllZeroes(intArrayStorage, size2, ref2);

                intArrayStorage.free(ref1);
                intArrayStorage.free(ref2);
            }
        }
        finally
        {
            intArrayStorage.destroy();
        }
    }

    private void assertIntArraySampleValues(FastUnsafeOffHeapIntArrayStorage intArrayStorage, int size, int ref)
    {
        for (int i = 0; i < size; i++)
        {
            assertEquals(i + 10000000, intArrayStorage.getInt(ref, i));
        }
    }

    private void setIntArraySampleValues(FastUnsafeOffHeapIntArrayStorage intArrayStorage, int size, int ref)
    {
        for (int i = 0; i < size; i++)
        {
            intArrayStorage.setInt(ref, i, i + 10000000);
        }
    }

    private void assertIntArrayIsAllZeroes(FastUnsafeOffHeapIntArrayStorage intArrayStorage, int size, int ref)
    {
        for (int i = 0; i < size; i++)
        {
            assertEquals(0, intArrayStorage.getInt(ref, 0));
        }
    }

    public void testUnsafeZeroMemory()
    {
        FastUnsafeOffHeapMemoryInitialiser initialiser = new FastUnsafeOffHeapMemoryInitialiser();
        Unsafe unsafe = MithraUnsafe.getUnsafe();

        long limit = 1024;
        long ref = unsafe.allocateMemory(limit);

        try
        {
            for (long size = 1; size < limit; size++)
            {
                // Pre-populate the buffer with non-zero data
                for (long i = 0; i < limit; i++)
                {
                    unsafe.putByte(ref + i, (byte) 0xff);
                }

                // Execute code under test
                initialiser.unsafeZeroMemory(ref, size, ref, limit);

                for (long i = 0; i < limit; i++)
                {
                    long byteRef = ref + i;
                    if (i < size)
                    {
                        assertEquals("Buffer data content at location " + i + " (buffer size = " + size + ") should have been zeroed but was NOT", (byte) 0x00, unsafe.getByte(byteRef));
                    } else
                    {
                        assertEquals("Buffer data content at location " + i + " (buffer size = " + size + ") was modified but should NOT have been", (byte) 0xff, unsafe.getByte(byteRef));
                    }
                }
            }
        }
        finally
        {
            unsafe.freeMemory(ref);
        }
    }

    // This is a performance test which should not be executed in the continuous build but may be uncommented for adhoc testing
//    public void testUnsafeZeroMemoryPerformance()
//    {
//        executeTestUnsafeZeroMemoryPerformance();
//
//        // Intentionally fail the test: this is intended as a reminder to disable the test before committing
//        fail("Performance test finished");
//    }

    // The main body of the performance test which can be executed on an adhoc basis by uncommenting testUnsafeZeroMemoryPerformance()
    // Do not comment/remove this method.
    public void executeTestUnsafeZeroMemoryPerformance()
    {
        HarnessedFastUnsafeOffHeapMemoryInitialiser initialiser = new HarnessedFastUnsafeOffHeapMemoryInitialiser();
        Unsafe unsafe = MithraUnsafe.getUnsafe();

        long limit = 1 << 30;
        long ref = unsafe.allocateMemory(limit); // 1GB

        try
        {
            for (long size = 1; size < limit; size <<= 1)
            {
                long iterationCount = (limit / size) * 32L; // fewer iterations are necessary to get accurate timing for larger sizes
                iterationCount = Math.min(1L << 28, iterationCount);
                iterationCount = Math.max(128L, iterationCount);

                long duration1;
                long duration2;

                // Original code
                {
                    long startTime = System.currentTimeMillis();
                    for (long i = 0; i < iterationCount; i++)
                    {
                        initialiser.unsafeZeroMemoryOptimisedForLargeBuffer(ref, size);
                    }
                    long endTime = System.currentTimeMillis();
                    duration1 = endTime - startTime;
                }

                // Optimised code
                {
                    long startTime = System.currentTimeMillis();
                    for (long i = 0; i < iterationCount; i++)
                    {
                        initialiser.unsafeZeroMemoryOptimisedForSmallBuffer(ref, size);
                    }
                    long endTime = System.currentTimeMillis();
                    duration2 = endTime - startTime;
                }

                double msPerMb1 = (double) duration1 / ((double) (iterationCount * size) / (double) (1L << 20));
                double msPerMb2 = (double) duration2 / ((double) (iterationCount * size) / (double) (1L << 20));
                double percentFaster = (double) (duration1 - duration2) / (double) duration1 * 100.0;

                DecimalFormat format = new DecimalFormat("#.##");

                System.out.println("size = " + size + " : setMemory = " + duration1 + " msecs (" + format.format(msPerMb1) + " ms/MB), " +
                        "putLong/putByte = " + duration2 + " msecs (" + format.format(msPerMb2) + " ms/MB), " +
                        "improvement = " + format.format(percentFaster) + "% faster (iteration count = " + iterationCount + ")");
            }
        }
        finally
        {
            unsafe.freeMemory(ref);
        }
    }

    private class HarnessedFastUnsafeOffHeapMemoryInitialiser extends FastUnsafeOffHeapMemoryInitialiser
    {
        // The purpose of this subclass is just to make protected methods available for performance testing

        @Override
        protected void unsafeZeroMemoryOptimisedForLargeBuffer(long address, long sizeInBytes)
        {
            super.unsafeZeroMemoryOptimisedForLargeBuffer(address, sizeInBytes);
        }

        @Override
        protected void unsafeZeroMemoryOptimisedForSmallBuffer(long address, long sizeInBytes)
        {
            super.unsafeZeroMemoryOptimisedForSmallBuffer(address, sizeInBytes);
        }
    }

    public void testIntListAddAlot()
    {
        FastUnsafeOffHeapIntList intList = new FastUnsafeOffHeapIntList(10);
        int arraySizeToTest = 10000000;
        for(int i=0;i<arraySizeToTest;i++)
        {
            intList.add(1000-i*10);
        }
        assertEquals(arraySizeToTest, intList.size());
        for(int i=0;i<arraySizeToTest;i++)
        {
            assertEquals(1000-i*10, intList.get(i));
        }
        intList.sort();
        for(int i=1;i<arraySizeToTest;i++)
        {
            assertTrue(intList.get(i- 1) <= intList.get(i));
        }
        intList.destroy();
    }

    public void testIntListAddAll()
    {
        FastUnsafeOffHeapIntList intList = new FastUnsafeOffHeapIntList(10);
        for(int i=0;i<10;i++)
        {
            intList.add(1000-i*10);
        }
        IntHashSet set = new IntHashSet();
        for(int i=0;i<intList.size();i++)
        {
            set.add(intList.get(i));
        }
        for(int i=0;i<intList.size();i++)
        {
            set.add(intList.get(i));
        }
        intList.clear();
        intList.addAll(set);
        intList.sort();
        assertEquals(10, intList.size());
        intList.destroy();
    }

    public void testFree() throws Exception
    {
        int nominalCapacity = dataStorage.freeCapacity() - 10;
        assertTrue(nominalCapacity > 0);
        assertTrue(nominalCapacity > 100);

        int max = 0;
        for(int i=0;i<nominalCapacity;i++)
        {
            max = ((MithraOffHeapDataObject) createOrder(i, i+1)).zGetOffset();
        }
        int freeAfterInitial = dataStorage.freeCapacity();
        assertTrue(freeAfterInitial < nominalCapacity/4);
        for(int i=5;i<nominalCapacity;i+=2)
        {
            dataStorage.free(i);
        }
        for(int i=0;i<10;i++)
        {
            gc();
            dataStorage.evictCollectedReferences();
            if (dataStorage.freeCapacity() > nominalCapacity/4 + freeAfterInitial)
            {
                break;
            }
            if (i == 9)
            {
                fail("did not free enough");
            }
        }
        int newMax = 0;
        for(int i=0;i<nominalCapacity/4;i++)
        {
            newMax = Math.max(newMax, ((MithraOffHeapDataObject) createOrder(i+100000, i+1)).zGetOffset());
        }
        assertTrue(newMax < max);
    }

    private void gc() throws Exception
    {
        System.gc();
        Thread.yield();
        Thread.sleep(20);
        Thread.yield();
        System.gc();
        Thread.yield();
        Thread.sleep(20);
        Thread.yield();
    }

    private class HarnessedFastUnsafeOffHeapDataStorage extends FastUnsafeOffHeapDataStorage
    {
        public HarnessedFastUnsafeOffHeapDataStorage(int dataSize, ReadWriteLock lock)
        {
            super(dataSize, lock);
        }

        @Override
        // This method is overridden to make it visible so that we can test it
        protected long scanPagesToSend(long maxClientReplicatedPageVersion, FastUnsafeOffHeapIntList pagesToSend)
        {
            return super.scanPagesToSend(maxClientReplicatedPageVersion, pagesToSend);
        }

        public long getPageVersion(int pageNumber)
        {
            return this.pageVersionList.get(pageNumber);
        }

        public int addPageVersion(long pageVersion)
        {
            this.readWriteLock.acquireWriteLock();
            int newPageNumber = this.pageVersionList.size();
            this.pageVersionList.add(pageVersion);
            this.readWriteLock.release();
            return newPageNumber;
        }

        public ReadWriteLock getReadWriteLock()
        {
            return this.readWriteLock;
        }

        public void setInitialCurrentPageVersion(int pageVersion)
        {
            this.currentPageVersion = pageVersion;
        }

        public int setVersionOfRandomPage(long pageVersion)
        {
            this.readWriteLock.acquireWriteLock();
            int pageNumber = (int) (((double)this.pageVersionList.size()) * Math.random());
            this.pageVersionList.set(pageNumber, pageVersion);
            this.readWriteLock.release();
            return pageNumber;
        }
    }
}
