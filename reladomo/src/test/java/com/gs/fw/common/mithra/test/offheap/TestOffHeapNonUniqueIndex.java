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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.cache.ReadWriteLock;
import com.gs.fw.common.mithra.cache.offheap.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.extractor.OffHeapableExtractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.StringPool;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;


public class TestOffHeapNonUniqueIndex extends TestCase
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final int MAX = 100000;

    private OffHeapDataStorage dataStorage;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        dataStorage = new FastUnsafeOffHeapDataStorage(BitemporalOrderData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
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
        dataStorage = new FastUnsafeOffHeapDataStorage(BitemporalOrderData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        BitemporalOrderData.BitemporalOrderDataOffHeap.zSetStorage(dataStorage);
    }

    public void testNonUniqueIndex1()
    {
        runNonUniqueIndex1(0);
//        renewStorage();
//        runNonUniqueIndex1(1);
//        renewStorage();
//        runNonUniqueIndex1(2);
//        renewStorage();
//        runNonUniqueIndex1(3);
    }

    public void runNonUniqueIndex1(int shift)
    {
        NonUniqueOffHeapIndex index = new NonUniqueOffHeapIndex(createExtractors(shift), 8, dataStorage);
        try
        {
            BitemporalOrderData[] firstTen = new BitemporalOrderData[10];
            for(int i=0;i<10;i++)
            {
                firstTen[i] = createOrder(i,i);
                index.put(firstTen[i]);
//                for(int j=0;j<i+1;j++)
//                {
//                    assertEquals(j, ((BitemporalOrderData)index.get(j)).getOrderId());
//                }
            }
            assertEquals(10, index.size());
            assertEquals(10, index.getNonUniqueSize());
            for(int i=0;i<10;i++)
            {
                assertEquals(i, ((BitemporalOrderData)index.get(i)).getOrderId());
            }
            BitemporalOrderData[] secondTenK = new BitemporalOrderData[10000];
            for(int i=10000;i<20000;i++)
            {
                secondTenK[i - 10000] = createOrder(i, i >> 1);
                index.put(secondTenK[i - 10000]); // 2 per slot
//                for(int j=0;j<10;j++)
//                {
//                    Object o = index.get(j);
//                    if (o == null)
//                    {
//                        System.out.println("went missing");
//                    }
//                    assertEquals(j, ((BitemporalOrderData) o).getOrderId());
//                }
//                for(int j=10000;j<i;j++)
//                {
//                    List list = (List) index.get(j >> 1);
//                    if (list == null)
//                    {
//                        System.out.println("went missing");
//                    }
//                    assertEquals(2, list.size());
//                    assertEquals(j >> 1, ((BitemporalOrderData)list.get(0)).getUserId());
//                }
            }
            assertEquals(10010, index.size());
            assertEquals(5010, index.getNonUniqueSize());
            for(int i=0;i<10;i++)
            {
                assertEquals(i, ((BitemporalOrderData)index.get(i)).getOrderId());
            }
            BitemporalOrderData[] thirdTenK = new BitemporalOrderData[10000];

            for(int i=1010000;i<1020000;i++)
            {
                thirdTenK[i - 1010000] = createOrder(i, i >> 4);
                index.put(thirdTenK[i - 1010000]); // 16 per slot
            }
            assertEquals(20010, index.size());
            assertEquals(5010+10000/16, index.getNonUniqueSize());
            for(int i=0;i<10;i++)
            {
                assertEquals(i, ((BitemporalOrderData)index.get(i)).getOrderId());
            }
            for(int i=10000;i<20000;i++)
            {
                List list = (List) index.get(i >> 1);
                assertEquals(2, list.size());
                assertEquals(i >> 1, ((BitemporalOrderData)list.get(0)).getUserId());
            }
            for(int i=1010000;i<1020000;i++)
            {
                List list = (List) index.get(i >> 4);
                assertEquals(16, list.size());
                assertEquals(i >> 4, ((BitemporalOrderData)list.get(0)).getUserId());
            }
            for(int i=0;i<10;i+=2)
            {
                index.remove(firstTen[i]);
            }
            assertEquals(20005, index.size());
            assertEquals(5000+5+10000/16, index.getNonUniqueSize());
            for(int i=10000;i<20000;i+=2)
            {
                index.remove(secondTenK[i - 10000]); // 2 per slot
            }
            assertEquals(5+5000+10000, index.size());
            assertEquals(5+5000+10000/16, index.getNonUniqueSize());
            for(int i=1010000;i<1020000;i+=2)
            {
                index.remove(thirdTenK[i - 1010000]); // 16 per slot
            }
            assertEquals(5+5000+5000, index.size());
            assertEquals(5+5000+10000/16, index.getNonUniqueSize());
            for(int i=10001;i<15001;i+=2)
            {
                index.remove(secondTenK[i - 10000]); // 2 per slot
            }
            assertEquals(5+2500+5000, index.size());
            assertEquals(5+2500+10000/16, index.getNonUniqueSize());
            index.ensureExtraCapacity(50000);
        }
        finally
        {
            index.destroy();
        }

    }

    private Extractor[] createExtractors(int shift)
    {
        return new Extractor[]{new ShiftedIntExtractor(BitemporalOrderFinder.userId(), shift)};
    }

    public void runNonUniqueIndex(int shift)
    {
        NonUniqueOffHeapIndex index = new NonUniqueOffHeapIndex(createExtractors(shift), 8, dataStorage);
        try
        {
            for(int i=0;i<10;i++)
            {
                index.put(createOrder(i,i));
            }
            assertEquals(10, index.size());
            assertEquals(10, index.getNonUniqueSize());
            for(int i=10000;i<20000;i++)
            {
                index.put(createOrder(i, i >> 1)); // 2 per slot
            }
            assertEquals(10010, index.size());
            assertEquals(5010, index.getNonUniqueSize());
            for(int i=1010000;i<1020000;i++)
            {
                index.put(createOrder(i, i >> 4)); // 16 per slot
            }
            assertEquals(20010, index.size());
            assertEquals(5010+10000/16, index.getNonUniqueSize());
            for(int i=0;i<10;i++)
            {
                assertEquals(i, ((Order)index.get(i)).getOrderId());
            }
            for(int i=10000;i<20000;i++)
            {
                List list = (List) index.get(i >> 1);
                assertEquals(2, list.size());
                assertEquals(i >> 1, ((BitemporalOrderData)list.get(0)).getUserId());
            }
            for(int i=1010000;i<1020000;i++)
            {
                List list = (List) index.get(i >> 4);
                assertEquals(16, list.size());
                assertEquals(i >> 4, ((BitemporalOrderData)list.get(0)).getUserId());
            }
            for(int i=0;i<10;i+=2)
            {
                index.remove(createOrder(i,i));
            }
            assertEquals(20005, index.size());
            assertEquals(5000+5+10000/16, index.getNonUniqueSize());
            for(int i=10000;i<20000;i+=2)
            {
                index.remove(createOrder(i, i >> 1)); // 2 per slot
            }
            assertEquals(5+5000+10000, index.size());
            assertEquals(5+5000+10000/16, index.getNonUniqueSize());
            for(int i=1010000;i<1020000;i+=2)
            {
                index.remove(createOrder(i, i >> 4)); // 16 per slot
            }
            assertEquals(5+5000+5000, index.size());
            assertEquals(5+5000+10000/16, index.getNonUniqueSize());
            for(int i=10001;i<15001;i+=2)
            {
                index.remove(createOrder(i, i >> 1)); // 2 per slot
            }
            assertEquals(5+2500+5000, index.size());
            assertEquals(5+2500+10000/16, index.getNonUniqueSize());
        }
        finally
        {
            index.destroy();
        }

    }

    private BitemporalOrderData createOrder(int orderId, int userId)
    {
        BitemporalOrderData o = BitemporalOrderDatabaseObject.allocateOffHeapData();
        o.setOrderId(orderId);
        o.setUserId(userId);
        return o;
    }


    public void testPutAndGet()  throws Exception
    {
        runTestPutAndGet(0);
        renewStorage();
        runTestPutAndGet(1);
        renewStorage();
        runTestPutAndGet(2);
        renewStorage();
        runTestPutAndGet(3);
    }

    private void runTestPutAndGet(final int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.userId(), shift)};
        NonUniqueOffHeapIndex index = new NonUniqueOffHeapIndex(createExtractors(shift), 8, dataStorage);
        try
        {
            runPutAndGetOnIndex(shift, shiftedExtractor, index);
        }
        finally
        {
            index.destroy();
        }
    }

    public void testClear()  throws Exception
    {
        runTestClear(0);
        renewStorage();
        runTestClear(1);
        renewStorage();
        runTestClear(2);
        renewStorage();
        runTestClear(3);
    }

    private void runTestClear(final int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.userId(), shift)};
        NonUniqueOffHeapIndex index = new NonUniqueOffHeapIndex(createExtractors(shift), 8, dataStorage);
        try
        {
            for(int i=0;i<3;i++)
            {
                runPutAndGetOnIndex(shift, shiftedExtractor, index);
                index.clear();
            }
        }
        finally
        {
            index.destroy();
        }
    }

    private void runPutAndGetOnIndex(int hashShift, Extractor[] shiftedExtractor, NonUniqueOffHeapIndex index) throws ParseException
    {
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());

        BitemporalOrderData[] array = new BitemporalOrderData[MAX];
        long start = System.currentTimeMillis();
        for (int i=0;i< MAX;i++)
        {
            BitemporalOrderData data = createOrder(i, i);

            array[i] = data;
            index.put(data);
        }
        long end = System.currentTimeMillis();
        System.out.println("index creation took: "+(end - start)+" ms");
        assertEquals(MAX, index.size());
        assertEquals(MAX, index.getNonUniqueSize());
        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.userId(), hashShift));
        start = System.currentTimeMillis();
        for(int i=0;i<MAX;i++)
        {
            uniqueProbe.setUserId(i);
            assertSame(array[i], index.get(uniqueProbe, extractors));
        }
        end = System.currentTimeMillis();
        System.out.println("query took: "+(end - start)+" ms");
    }

    public void testPutAndGetTwoEach()  throws Exception
    {
        runTestPutAndGetTwoEach(0);
        renewStorage();
        runTestPutAndGetTwoEach(1);
        renewStorage();
        runTestPutAndGetTwoEach(2);
        renewStorage();
        runTestPutAndGetTwoEach(3);
    }

    private void runTestPutAndGetTwoEach(final int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.userId(), shift)};
        NonUniqueOffHeapIndex index = new NonUniqueOffHeapIndex(createExtractors(shift), 8, dataStorage);
        try
        {
            runPutAndGetOnIndexTwoEach(shift, shiftedExtractor, index);
        }
        finally
        {
            index.destroy();
        }
    }

    private void runPutAndGetOnIndexTwoEach(int hashShift, Extractor[] shiftedExtractor, NonUniqueOffHeapIndex index) throws ParseException
    {
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());

        BitemporalOrderData[] array = new BitemporalOrderData[MAX];
        long start = System.currentTimeMillis();
        for (int i=0;i< MAX;i++)
        {
            BitemporalOrderData data = createOrder(i, i >> 1);

            array[i] = data;
            index.put(data);
        }
        long end = System.currentTimeMillis();
        System.out.println("index creation took: "+(end - start)+" ms");
        assertEquals(MAX, index.size());
        assertEquals(MAX/2, index.getNonUniqueSize());
        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.userId(), hashShift));
        start = System.currentTimeMillis();
        for(int i=0;i<MAX;i+=2)
        {
            uniqueProbe.setUserId(i >> 1);
            List list = (List) index.get(uniqueProbe, extractors);
            assertEquals(2, list.size());
            Object zero = list.get(0);
            Object one = list.get(1);
            assertTrue(zero != one);
            assertTrue(zero == array[i] || zero == array[i+1]);
            assertTrue(one == array[i] || one == array[i+1]);
        }
        end = System.currentTimeMillis();
        System.out.println("query took: "+(end - start)+" ms");
    }
//
//    public void testPutAndGetMultiMix()  throws Exception
//    {
//        runTestPutAndGetMultiMix(0);
//        renewStorage();
//        runTestPutAndGetMultiMix(1);
//        renewStorage();
//        runTestPutAndGetMultiMix(2);
//        renewStorage();
//        runTestPutAndGetMultiMix(3);
//    }
//
//    private void runTestPutAndGetMultiMix(int shift) throws Exception
//    {
//        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift)};
//        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
//                shiftedExtractor,
//                BitemporalOrderFinder.getAsOfAttributes(), dataStorage);
//        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
//        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
//        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
//        Timestamp to = new Timestamp(from.getTime() + monthTime);
//
//        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
//        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
//        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
//        BitemporalOrderData[] array = new BitemporalOrderData[MAX*5];
//        prepareIndex(index, from, in, monthTime, to, array);
//        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
//        BitemporalOrder semiUniqueProbe = new BitemporalOrder(fromProbe, inProbe);
//        RelationshipHashStrategy rhs = new BitemporalOrderShiftedRhs(shift);
//        List extractors = new FastList();
//        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift));
//        extractors.add(BitemporalOrderFinder.businessDate());
//        extractors.add(BitemporalOrderFinder.processingDate());
//        for(int i=0;i<MAX;i++)
//        {
//            assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//            List list = index.getFromDataForAllDatesAsList(array[i].copy());
//            assertEquals(1, list.size());
//            assertSame(array[i], list.get(0));
//            assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), probes));
//            uniqueProbe.setBalanceId(i);
//            semiUniqueProbe.setBalanceId(i);
//            assertSame(array[i], index.get(uniqueProbe, extractors));
//            assertSame(array[i], index.getFromSemiUnique(semiUniqueProbe, extractors));
//            assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//            assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), probes[0], probes[1]));
//        }
//        for(int i=MAX;i<MAX*5;i++)
//        {
//            assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//            int stride = (i - MAX) % 4;
//            long month = stride *monthTime;
//            List list = index.getFromDataForAllDatesAsList(array[i].copy());
//            assertEquals(4, list.size());
//            assertTrue(list.contains(array[i]));
//            Timestamp localFromProbe = new Timestamp(fromProbe.getTime() + month);
//            Timestamp[] localProbes = {localFromProbe, inProbe};
//            assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), localProbes));
//            Timestamp localFrom = new Timestamp(from.getTime() + month);
//            BitemporalOrder localUniqueProbe = new BitemporalOrder(localFrom, in);
//            localUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
//            assertSame(array[i], index.get(localUniqueProbe, extractors));
//            BitemporalOrder localSemiUniqueProbe = new BitemporalOrder(localFromProbe, inProbe);
//            localSemiUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
//            assertSame(array[i], index.getFromSemiUnique(localSemiUniqueProbe, extractors));
//            assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, localProbes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//            assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), localProbes[0], localProbes[1]));
//        }
//    }
//
//    private void prepareIndex(OffHeapSemiUniqueDatedIndex index, Timestamp from, Timestamp in, long monthTime, Timestamp to, BitemporalOrderData[] array)
//    {
//        for (int i=0;i< MAX;i++)
//        {
//            BitemporalOrderData data = BitemporalOrderDatabaseObject.allocateOffHeapData();
//            data.setBalanceId(i);
//            data.setBusinessDateFrom(from);
//            data.setBusinessDateTo(to);
//            data.setProcessingDateFrom(in);
//            data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
//            array[i] = data;
//            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
//        }
//        for (int i=MAX;i<MAX*5;i++)
//        {
//            BitemporalOrderData data = BitemporalOrderDatabaseObject.allocateOffHeapData();
//            data.setBalanceId((i-MAX)/4 + MAX);
//            long month = ((i-MAX) % 4)*monthTime;
//            data.setBusinessDateFrom(new Timestamp(from.getTime() + month));
//            data.setBusinessDateTo(new Timestamp(to.getTime() + month));
//            data.setProcessingDateFrom(in);
//            data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
//            array[i] = data;
//            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
//        }
//        assertEquals(MAX*5, index.size());
//        assertEquals(MAX*2, index.getSemiUniqueSize());
//    }
//
//    public void testPutAndGetMultiMixRemove()  throws Exception
//    {
//        runTestPutAndGetMultiMixRemove(0);
//        renewStorage();
//        runTestPutAndGetMultiMixRemove(1);
//        renewStorage();
//        runTestPutAndGetMultiMixRemove(2);
//        renewStorage();
//        runTestPutAndGetMultiMixRemove(3);
//    }
//
//    private void runTestPutAndGetMultiMixRemove(int shift) throws Exception
//    {
//        System.out.println(shift);
//        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift)};
//        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
//                shiftedExtractor,
//                BitemporalOrderFinder.getAsOfAttributes(), dataStorage);
//        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
//        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
//        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
//        Timestamp to = new Timestamp(from.getTime() + monthTime);
//
//        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
//        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
//        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
//        BitemporalOrderData[] array = new BitemporalOrderData[MAX*5];
//        prepareIndex(index, from, in, monthTime, to, array);
//        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
//        BitemporalOrder semiUniqueProbe = new BitemporalOrder(fromProbe, inProbe);
//        RelationshipHashStrategy rhs = new BitemporalOrderShiftedRhs(shift);
//        List extractors = new FastList();
//        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift));
//        extractors.add(BitemporalOrderFinder.businessDate());
//        extractors.add(BitemporalOrderFinder.processingDate());
//
//        BitemporalOrderData[] removedArray = new BitemporalOrderData[MAX*5];
//
//
//        for(int i=0;i<MAX;i+=5)
//        {
//            assertSame(array[i], index.remove(array[i].copy()));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//        for(int i=MAX;i<MAX*3;i+=3)
//        {
//            assertSame(array[i], index.remove(array[i].copy()));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//
//        for(int i=MAX*3;i<MAX*4;i++)
//        {
//            assertSame(array[i], index.remove(array[i].copy()));
//            removedArray[i] = array[i];
//            array[i] = null;
//            if (i % 4 == 3) i++;
//        }
//
//        for(int i=MAX*4;i<MAX*5;i++)
//        {
//            assertSame(array[i], index.remove(array[i].copy()));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//
//        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
//    }
//
//    public void testPutAndGetMultiMixRemoveOldEntry()  throws Exception
//    {
//        runTestPutAndGetMultiMixRemoveOldEntry(0);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveOldEntry(1);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveOldEntry(2);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveOldEntry(3);
//    }
//
//    private void runTestPutAndGetMultiMixRemoveOldEntry(int shift) throws Exception
//    {
//        System.out.println(shift);
//        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift)};
//        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
//                shiftedExtractor,
//                BitemporalOrderFinder.getAsOfAttributes(), dataStorage);
//        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
//        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
//        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
//        Timestamp to = new Timestamp(from.getTime() + monthTime);
//
//        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
//        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
//        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
//        BitemporalOrderData[] array = new BitemporalOrderData[MAX*5];
//        prepareIndex(index, from, in, monthTime, to, array);
//        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
//        BitemporalOrder semiUniqueProbe = new BitemporalOrder(fromProbe, inProbe);
//        RelationshipHashStrategy rhs = new BitemporalOrderShiftedRhs(shift);
//        List extractors = new FastList();
//        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift));
//        extractors.add(BitemporalOrderFinder.businessDate());
//        extractors.add(BitemporalOrderFinder.processingDate());
//
//        BitemporalOrderData[] removedArray = new BitemporalOrderData[MAX*5];
//
//
//        for(int i=0;i<MAX;i+=5)
//        {
//            assertSame(array[i], index.removeOldEntry(array[i].copy(), probes));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//        for(int i=MAX;i<MAX*3;i+=3)
//        {
//            long month = (i - MAX) % 4 *monthTime;
//            Timestamp[] localProbes = {new Timestamp(fromProbe.getTime() + month), inProbe};
//            assertSame(array[i], index.removeOldEntry(array[i].copy(), localProbes));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//
//        for(int i=MAX*3;i<MAX*4;i++)
//        {
//            long month = (i - MAX) % 4 *monthTime;
//            Timestamp[] localProbes = {new Timestamp(fromProbe.getTime() + month), inProbe};
//            assertSame(array[i], index.removeOldEntry(array[i].copy(), localProbes));
//            removedArray[i] = array[i];
//            array[i] = null;
//            if (i % 4 == 3) i++;
//        }
//
//        for(int i=MAX*4;i<MAX*5;i++)
//        {
//            long month = (i - MAX) % 4 *monthTime;
//            Timestamp[] localProbes = {new Timestamp(fromProbe.getTime() + month), inProbe};
//            assertSame(array[i], index.removeOldEntry(array[i].copy(), localProbes));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//
//        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
//    }
//
//    public void testPutAndGetMultiMixRemoveOldEntryForRange()  throws Exception
//    {
//        runTestPutAndGetMultiMixRemoveOldEntryForRange(0);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveOldEntryForRange(1);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveOldEntryForRange(2);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveOldEntryForRange(3);
//    }
//
//    private void runTestPutAndGetMultiMixRemoveOldEntryForRange(int shift) throws Exception
//    {
//        System.out.println(shift);
//        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift)};
//        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
//                shiftedExtractor,
//                BitemporalOrderFinder.getAsOfAttributes(), dataStorage);
//        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
//        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
//        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
//        Timestamp to = new Timestamp(from.getTime() + monthTime);
//
//        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
//        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
//        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
//        BitemporalOrderData[] array = new BitemporalOrderData[MAX*5];
//        prepareIndex(index, from, in, monthTime, to, array);
//        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
//        BitemporalOrder semiUniqueProbe = new BitemporalOrder(fromProbe, inProbe);
//        RelationshipHashStrategy rhs = new BitemporalOrderShiftedRhs(shift);
//        List extractors = new FastList();
//        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift));
//        extractors.add(BitemporalOrderFinder.businessDate());
//        extractors.add(BitemporalOrderFinder.processingDate());
//
//        BitemporalOrderData[] removedArray = new BitemporalOrderData[MAX*5];
//
//
//        for(int i=0;i<MAX;i+=5)
//        {
//            List list = index.removeOldEntryForRange(array[i].copy());
//            assertEquals(1, list.size());
//            assertTrue(list.contains(array[i]));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//        for(int i=MAX;i<MAX*3;i+=3)
//        {
//            List list = index.removeOldEntryForRange(array[i].copy());
//            assertEquals(1, list.size());
//            assertTrue(list.contains(array[i]));
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//
//        for(int i=MAX*3;i<MAX*4;i++)
//        {
//            List list = index.removeOldEntryForRange(array[i].copy());
//            assertEquals(1, list.size());
//            assertTrue(list.contains(array[i]));
//            removedArray[i] = array[i];
//            array[i] = null;
//            if (i % 4 == 3) i++;
//        }
//
//        for(int i=MAX*4;i<MAX*5;i+=4)
//        {
//            BitemporalOrderData data = (BitemporalOrderData) array[i].copy();
//            data.setBusinessDateTo(BitemporalOrderFinder.businessDate().getInfinityDate());
//            List list = index.removeOldEntryForRange(data);
//            assertEquals(4, list.size());
//            for(int j=0;j<4;j++)
//            {
//                assertTrue(list.contains(array[i+j]));
//                removedArray[i+j] = array[i+j];
//                array[i+j] = null;
//            }
//        }
//
//        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
//    }
//
//    private void checkIndexAfterRemove(Extractor[] shiftedExtractor, OffHeapSemiUniqueDatedIndex index, Timestamp from, Timestamp in, long monthTime, Timestamp fromProbe, Timestamp inProbe, Timestamp[] probes, BitemporalOrderData[] array, BitemporalOrder uniqueProbe, BitemporalOrder semiUniqueProbe, List extractors, BitemporalOrderData[] removedArray, RelationshipHashStrategy rhs)
//    {
//        for(int i=0;i<MAX;i++)
//        {
//            if (array[i] != null)
//            {
//                assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//                List list = index.getFromDataForAllDatesAsList(array[i].copy());
//                assertEquals(1, list.size());
//                assertSame(array[i], list.get(0));
//                assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), probes));
//                uniqueProbe.setBalanceId(i);
//                semiUniqueProbe.setBalanceId(i);
//                assertSame(array[i], index.get(uniqueProbe, extractors));
//                assertSame(array[i], index.getFromSemiUnique(semiUniqueProbe, extractors));
//                assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//                assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), probes[0], probes[1]));
//            }
//        }
//        for(int i=MAX;i<MAX*5;i++)
//        {
//            if (array[i] != null)
//            {
//                assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//                int stride = (i - MAX) % 4;
//                long month = stride *monthTime;
//                List list = index.getFromDataForAllDatesAsList(array[i].copy());
//                assertTrue(list.contains(array[i]));
//                Timestamp localFromProbe = new Timestamp(fromProbe.getTime() + month);
//                Timestamp[] localProbes = {localFromProbe, inProbe};
//                assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), localProbes));
//                Timestamp localFrom = new Timestamp(from.getTime() + month);
//                BitemporalOrder localUniqueProbe = new BitemporalOrder(localFrom, in);
//                localUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
//                assertSame(array[i], index.get(localUniqueProbe, extractors));
//                BitemporalOrder localSemiUniqueProbe = new BitemporalOrder(localFromProbe, inProbe);
//                localSemiUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
//                assertSame(array[i], index.getFromSemiUnique(localSemiUniqueProbe, extractors));
//                assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, localProbes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
//                assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), localProbes[0], localProbes[1]));
//            }
//        }
//        for(int i=0;i<MAX;i++)
//        {
//            if (removedArray[i] != null)
//            {
//                assertNull(index.getFromData(removedArray[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
//                assertEquals(0, index.getFromDataForAllDatesAsList(removedArray[i].copy()).size());
//                assertNull(index.getSemiUniqueFromData(removedArray[i].copy(), probes));
//                uniqueProbe.setBalanceId(i);
//                semiUniqueProbe.setBalanceId(i);
//                assertNull(index.get(uniqueProbe, extractors));
//                assertNull(index.getFromSemiUnique(semiUniqueProbe, extractors));
//                assertNull(index.getSemiUniqueAsOneWithDates(removedArray[i].copy(), shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
//                assertNull(index.getSemiUniqueAsOne(null, removedArray[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i]), probes[0], probes[1]));
//            }
//        }
//        for(int i=MAX;i<MAX*5;i++)
//        {
//            if (removedArray[i] != null)
//            {
//                assertNull(index.getFromData(removedArray[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
//                int stride = (i - MAX) % 4;
//                long month = stride *monthTime;
//                List list = index.getFromDataForAllDatesAsList(removedArray[i].copy());
//                assertTrue(!list.contains(removedArray[i]));
//                Timestamp localFromProbe = new Timestamp(fromProbe.getTime() + month);
//                Timestamp[] localProbes = {localFromProbe, inProbe};
//                assertNull(index.getSemiUniqueFromData(removedArray[i].copy(), localProbes));
//                Timestamp localFrom = new Timestamp(from.getTime() + month);
//                BitemporalOrder localUniqueProbe = new BitemporalOrder(localFrom, in);
//                localUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
//                assertNull(index.get(localUniqueProbe, extractors));
//                BitemporalOrder localSemiUniqueProbe = new BitemporalOrder(localFromProbe, inProbe);
//                localSemiUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
//                assertNull(index.getFromSemiUnique(localSemiUniqueProbe, extractors));
//                assertNull(index.getSemiUniqueAsOneWithDates(removedArray[i].copy(), shiftedExtractor, localProbes, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
//                assertNull(index.getSemiUniqueAsOne(null, removedArray[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i]), localProbes[0], localProbes[1]));
//            }
//        }
//    }
//
//    public void testPutAndGetMultiMixRemoveAllFilter()  throws Exception
//    {
//        runTestPutAndGetMultiMixRemoveAllFilter(0);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveAllFilter(1);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveAllFilter(2);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveAllFilter(3);
//    }
//
//    private void runTestPutAndGetMultiMixRemoveAllFilter(int shift) throws Exception
//    {
//        System.out.println(shift);
//        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift)};
//        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
//                shiftedExtractor,
//                BitemporalOrderFinder.getAsOfAttributes(), dataStorage);
//        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
//        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
//        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
//        Timestamp to = new Timestamp(from.getTime() + monthTime);
//
//        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
//        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
//        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
//        BitemporalOrderData[] array = new BitemporalOrderData[MAX*5];
//        for (int i=0;i< MAX;i++)
//        {
//            BitemporalOrderData data = BitemporalOrderDatabaseObject.allocateOffHeapData();
//            data.setBalanceId(i);
//            data.setBusinessDateFrom(from);
//            data.setBusinessDateTo(to);
//            data.setProcessingDateFrom(in);
//            data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
//            array[i] = data;
//            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
//        }
//        for (int i=MAX;i<MAX*5;i++)
//        {
//            BitemporalOrderData data = BitemporalOrderDatabaseObject.allocateOffHeapData();
//            data.setQuantity(i);
//            data.setBalanceId((i-MAX)/4 + MAX);
//            long month = ((i-MAX) % 4)*monthTime;
//            data.setBusinessDateFrom(new Timestamp(from.getTime() + month));
//            data.setBusinessDateTo(new Timestamp(to.getTime() + month));
//            data.setProcessingDateFrom(in);
//            data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
//            array[i] = data;
//            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
//        }
//        assertEquals(MAX*5, index.size());
//        assertEquals(MAX*2, index.getSemiUniqueSize());
//        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
//        BitemporalOrder semiUniqueProbe = new BitemporalOrder(fromProbe, inProbe);
//        RelationshipHashStrategy rhs = new BitemporalOrderShiftedRhs(shift);
//        List extractors = new FastList();
//        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift));
//        extractors.add(BitemporalOrderFinder.businessDate());
//        extractors.add(BitemporalOrderFinder.processingDate());
//
//        BitemporalOrderData[] removedArray = new BitemporalOrderData[MAX*5];
//
//
//        for(int i=0;i<MAX;i+=5)
//        {
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//        index.removeAll(new Filter()
//        {
//            public boolean matches(Object o)
//            {
//                BitemporalOrderData data = (BitemporalOrderData) o;
//                return data.getBalanceId() < MAX && data.getBalanceId() % 5 == 0;
//            }
//        });
//
//        for(int i=MAX;i<MAX*3;i+=3)
//        {
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//        index.removeAll(new Filter()
//        {
//            public boolean matches(Object o)
//            {
//                BitemporalOrderData data = (BitemporalOrderData) o;
//                int i = (int) Math.round(data.getQuantity());
//                return i >= MAX && i < MAX*3 && (i-MAX) % 3 == 0;
//            }
//        });
//
//        for(int i=MAX*3;i<MAX*4;i++)
//        {
//            if (i % 4 != 3)
//            {
//                removedArray[i] = array[i];
//                array[i] = null;
//            }
//        }
//        index.removeAll(new Filter()
//        {
//            public boolean matches(Object o)
//            {
//                BitemporalOrderData data = (BitemporalOrderData) o;
//                int i = (int) Math.round(data.getQuantity());
//                return i >= MAX*3 && i < MAX*4 && i % 4 != 3;
//            }
//        });
//
//        for(int i=MAX*4;i<MAX*5;i++)
//        {
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//        index.removeAll(new Filter()
//        {
//            public boolean matches(Object o)
//            {
//                BitemporalOrderData data = (BitemporalOrderData) o;
//                int i = (int) Math.round(data.getQuantity());
//                return i >= MAX*4;
//            }
//        });
//
//        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
//    }
//
//    public void testPutAndGetMultiMixRemoveAllIgnoringDate()  throws Exception
//    {
//        runTestPutAndGetMultiMixRemoveAllIgnoringDate(0);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveAllIgnoringDate(1);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveAllIgnoringDate(2);
//        renewStorage();
//        runTestPutAndGetMultiMixRemoveAllIgnoringDate(3);
//    }
//
//    private void runTestPutAndGetMultiMixRemoveAllIgnoringDate(int shift) throws Exception
//    {
//        System.out.println(shift);
//        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift)};
//        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
//                shiftedExtractor,
//                BitemporalOrderFinder.getAsOfAttributes(), dataStorage);
//        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
//        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
//        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
//        Timestamp to = new Timestamp(from.getTime() + monthTime);
//
//        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
//        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
//        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
//        BitemporalOrderData[] array = new BitemporalOrderData[MAX*5];
//        for (int i=0;i< MAX;i++)
//        {
//            BitemporalOrderData data = BitemporalOrderDatabaseObject.allocateOffHeapData();
//            data.setBalanceId(i);
//            data.setBusinessDateFrom(from);
//            data.setBusinessDateTo(to);
//            data.setProcessingDateFrom(in);
//            data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
//            array[i] = data;
//            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
//        }
//        for (int i=MAX;i<MAX*5;i++)
//        {
//            BitemporalOrderData data = BitemporalOrderDatabaseObject.allocateOffHeapData();
//            data.setQuantity(i);
//            data.setBalanceId((i-MAX)/4 + MAX);
//            long month = ((i-MAX) % 4)*monthTime;
//            data.setBusinessDateFrom(new Timestamp(from.getTime() + month));
//            data.setBusinessDateTo(new Timestamp(to.getTime() + month));
//            data.setProcessingDateFrom(in);
//            data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
//            array[i] = data;
//            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
//        }
//        assertEquals(MAX*5, index.size());
//        assertEquals(MAX*2, index.getSemiUniqueSize());
//        BitemporalOrder uniqueProbe = new BitemporalOrder(from, in);
//        BitemporalOrder semiUniqueProbe = new BitemporalOrder(fromProbe, inProbe);
//        RelationshipHashStrategy rhs = new BitemporalOrderShiftedRhs(shift);
//        List extractors = new FastList();
//        extractors.add(new ShiftedIntExtractor(BitemporalOrderFinder.balanceId(), shift));
//        extractors.add(BitemporalOrderFinder.businessDate());
//        extractors.add(BitemporalOrderFinder.processingDate());
//
//        BitemporalOrderData[] removedArray = new BitemporalOrderData[MAX*5];
//
//        final int[] count = new int[1];
//        TObjectProcedure proc = new TObjectProcedure()
//        {
//            public boolean execute(Object object)
//            {
//                count[0]++;
//                return true;
//            }
//        };
//
//        for(int i=0;i<MAX;i+=5)
//        {
//            count[0] = 0;
//            assertTrue(index.removeAllIgnoringDate(array[i], proc));
//            assertEquals(1, count[0]);
//            removedArray[i] = array[i];
//            array[i] = null;
//        }
//
//        for(int i=MAX;i<MAX*5;i+=8)
//        {
//            count[0] = 0;
//            assertTrue(index.removeAllIgnoringDate(array[i], proc));
//            assertEquals(4, count[0]);
//            for(int j=0;j<4;j++)
//            {
//                removedArray[i+j] = array[i+j];
//                array[i+j] = null;
//            }
//        }
//
//        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
//    }
//
//
    private static class ShiftedIntExtractor implements IntExtractor, OffHeapableExtractor
    {
        private int shift;
        private IntExtractor delegate;

        private ShiftedIntExtractor(IntExtractor delegate, int shift)
        {
            this.delegate = delegate;
            this.shift = shift;
        }

        @Override
        public OffHeapExtractor zCreateOffHeapExtractor()
        {
            return new ShiftedIntOffHeapExtractor(delegate, shift);
        }

        public int intValueOf(Object o)
        {
            return delegate.intValueOf(o);
        }

        public void setIntValue(Object o, int newValue)
        {
            delegate.setIntValue(o, newValue);
        }

        public void setValue(Object o, Object newValue)
        {
            delegate.setValue(o, newValue);
        }

        public void setValueNull(Object o)
        {
            delegate.setValueNull(o);
        }

        public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
        {
            delegate.setValueUntil(o, newValue, exclusiveUntil);
        }

        public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
        {
            delegate.setValueNullUntil(o, exclusiveUntil);
        }

        public boolean isAttributeNull(Object o)
        {
            return delegate.isAttributeNull(o);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            return delegate.valueEquals(first, second, secondExtractor);
        }

        public int valueHashCode(Object object)
        {
            return delegate.valueHashCode(object) >> shift;
        }

        public boolean valueEquals(Object first, Object second)
        {
            return delegate.valueEquals(first, second);
        }

        public Object valueOf(Object object)
        {
            return delegate.valueOf(object);
        }

    }

    private static class ShiftedIntOffHeapExtractor implements OffHeapIntExtractor
    {

        private int shift;
        private OffHeapIntExtractor delegate;

        private ShiftedIntOffHeapExtractor(IntExtractor delegate, int shift)
        {
            this.delegate = (OffHeapIntExtractor) ((OffHeapableExtractor)delegate).zCreateOffHeapExtractor();
            this.shift = shift;
        }

        public int intValueOf(OffHeapDataStorage dataStorage, int dataOffset)
        {
            return delegate.intValueOf(dataStorage, dataOffset);
        }

        @Override
        public int computeHash(OffHeapDataStorage dataStorage, int dataOffset)
        {
            return delegate.computeHash(dataStorage, dataOffset) >> shift;
        }

        @Override
        public int computeHashFromOnHeapExtractor(Object valueHolder, Extractor onHeapExtractor)
        {
            return delegate.computeHashFromOnHeapExtractor(valueHolder, onHeapExtractor);
        }

        @Override
        public int computeHashFromValue(Object key)
        {
            return delegate.computeHashFromValue(key) >> shift;
        }

        @Override
        public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, long otherDataAddress)
        {
            return delegate.valueEquals(dataStorage, dataOffset, otherDataAddress);
        }

        @Override
        public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, Extractor extractor)
        {
            return delegate.equals(dataStorage, dataOffset, valueHolder, extractor);
        }

        @Override
        public boolean isAttributeNull(OffHeapDataStorage dataStorage, int dataOffset)
        {
            return delegate.isAttributeNull(dataStorage, dataOffset);
        }

        @Override
        public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, Object key)
        {
            return delegate.valueEquals(dataStorage, dataOffset, key);
        }

        @Override
        public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, int secondOffset)
        {
            return delegate.valueEquals(dataStorage, dataOffset, secondOffset);
        }
    }

}
