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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.cache.FullSemiUniqueDatedIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.TinyBalanceData;
import com.gs.fw.common.mithra.test.domain.TinyBalanceDatabaseObject;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import com.gs.fw.common.mithra.util.DoProcedure;
import com.gs.fw.common.mithra.util.Filter;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;


public class TestFullSemiUniqueDatedIndex extends TestCase
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final int MAX = 10000;

    public void testPutAndGet()  throws Exception
    {
        for(int i=0;i<10;i++)
        {
            runTestPutAndGet(0);
            System.gc();
            Thread.yield();
            System.gc();
            Thread.yield();
        }
        runTestPutAndGet(1);
        runTestPutAndGet(2);
        runTestPutAndGet(3);
    }

    private void runTestPutAndGet(final int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        FullSemiUniqueDatedIndex index = new FullSemiUniqueDatedIndex("",
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes());
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        Timestamp to = new Timestamp(from.getTime() + 30*24*3600*1000L); // about a month

        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
        TinyBalanceData[] array = new TinyBalanceData[MAX];
        long start = System.currentTimeMillis();
        for (int i=0;i< MAX;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBalanceId(i);
            data.setBusinessDateFrom(from);
            data.setBusinessDateTo(to);
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            array[i] = data;
            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
        }
        long end = System.currentTimeMillis();
        System.out.println("index creation took: "+(end - start)+" ms");
        assertEquals(MAX, index.size());
        assertEquals(MAX, index.getSemiUniqueSize());
        RelationshipHashStrategy rhs = new TinyBalanceShiftedRhs(shift);
        TinyBalance uniqueProbe = new TinyBalance(from, in);
        TinyBalance semiUniqueProbe = new TinyBalance(fromProbe, inProbe);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift));
        extractors.add(TinyBalanceFinder.businessDate());
        extractors.add(TinyBalanceFinder.processingDate());
        Extractor[] extractorArray = new Extractor[extractors.size()];
        extractors.toArray(extractorArray);
        start = System.currentTimeMillis();
        for(int i=0;i<MAX;i++)
        {
            uniqueProbe.setBalanceId(i);
            semiUniqueProbe.setBalanceId(i);

            assertSame(array[i], index.getFromData(array[i], index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            List list = index.getFromDataForAllDatesAsList(array[i]);
            assertEquals(1, list.size());
            assertSame(array[i], list.get(0));
            assertSame(array[i], index.getSemiUniqueFromData(array[i], probes));
            assertTrue(index.contains(uniqueProbe, extractorArray, null));
            assertFalse(index.contains(semiUniqueProbe, extractorArray, null));
            assertSame(array[i], index.get(uniqueProbe, extractors));
            assertSame(array[i], index.getFromSemiUnique(semiUniqueProbe, extractors));
            assertTrue(index.containsInSemiUnique(semiUniqueProbe, extractorArray, null));
            assertFalse(index.containsInSemiUnique(uniqueProbe, extractorArray, null));
            assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i], shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            assertSame(array[i], index.getSemiUniqueAsOne(null, array[i], rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), probes[0], probes[1]));
        }
        end = System.currentTimeMillis();
        System.out.println("no copy query took: " + (end - start) + " ms");
        start = System.currentTimeMillis();
        for(int i=0;i<MAX;i++)
        {
            uniqueProbe.setBalanceId(i);
            semiUniqueProbe.setBalanceId(i);

            assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            List list = index.getFromDataForAllDatesAsList(array[i].copy());
            assertEquals(1, list.size());
            assertSame(array[i], list.get(0));
            assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), probes));
            assertSame(array[i], index.get(uniqueProbe, extractors));
            assertSame(array[i], index.getFromSemiUnique(semiUniqueProbe, extractors));
            assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), probes[0], probes[1]));
        }
        end = System.currentTimeMillis();
        System.out.println("query took: "+(end - start)+" ms");
    }

    public void testPutAndGetMultiMix()  throws Exception
    {
        runTestPutAndGetMultiMix(0);
        runTestPutAndGetMultiMix(1);
        runTestPutAndGetMultiMix(2);
        runTestPutAndGetMultiMix(3);
    }

    private void runTestPutAndGetMultiMix(int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        FullSemiUniqueDatedIndex index = new FullSemiUniqueDatedIndex("",
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes());
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
        Timestamp to = new Timestamp(from.getTime() + monthTime);

        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
        TinyBalanceData[] array = new TinyBalanceData[MAX*5];
        prepareIndex(index, from, in, monthTime, to, array);
        TinyBalance uniqueProbe = new TinyBalance(from, in);
        TinyBalance semiUniqueProbe = new TinyBalance(fromProbe, inProbe);
        RelationshipHashStrategy rhs = new TinyBalanceShiftedRhs(shift);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift));
        extractors.add(TinyBalanceFinder.businessDate());
        extractors.add(TinyBalanceFinder.processingDate());
        for(int i=0;i<MAX;i++)
        {
            assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            List list = index.getFromDataForAllDatesAsList(array[i].copy());
            assertEquals(1, list.size());
            assertSame(array[i], list.get(0));
            assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), probes));
            uniqueProbe.setBalanceId(i);
            semiUniqueProbe.setBalanceId(i);
            assertSame(array[i], index.get(uniqueProbe, extractors));
            assertSame(array[i], index.getFromSemiUnique(semiUniqueProbe, extractors));
            assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), probes[0], probes[1]));
        }
        for(int i=MAX;i<MAX*5;i++)
        {
            assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            int stride = (i - MAX) % 4;
            long month = stride *monthTime;
            List list = index.getFromDataForAllDatesAsList(array[i].copy());
            assertEquals(4, list.size());
            assertTrue(list.contains(array[i]));
            Timestamp localFromProbe = new Timestamp(fromProbe.getTime() + month);
            Timestamp[] localProbes = {localFromProbe, inProbe};
            assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), localProbes));
            Timestamp localFrom = new Timestamp(from.getTime() + month);
            TinyBalance localUniqueProbe = new TinyBalance(localFrom, in);
            localUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
            assertSame(array[i], index.get(localUniqueProbe, extractors));
            TinyBalance localSemiUniqueProbe = new TinyBalance(localFromProbe, inProbe);
            localSemiUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
            assertSame(array[i], index.getFromSemiUnique(localSemiUniqueProbe, extractors));
            assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, localProbes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), localProbes[0], localProbes[1]));
        }
    }

    private void prepareIndex(FullSemiUniqueDatedIndex index, Timestamp from, Timestamp in, long monthTime, Timestamp to, TinyBalanceData[] array)
    {
        for (int i=0;i< MAX;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBalanceId(i);
            data.setBusinessDateFrom(from);
            data.setBusinessDateTo(to);
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            array[i] = data;
            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
        }
        for (int i=MAX;i<MAX*5;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBalanceId((i-MAX)/4 + MAX);
            long month = ((i-MAX) % 4)*monthTime;
            data.setBusinessDateFrom(new Timestamp(from.getTime() + month));
            data.setBusinessDateTo(new Timestamp(to.getTime() + month));
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            array[i] = data;
            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
        }
        assertEquals(MAX*5, index.size());
        assertEquals(MAX*2, index.getSemiUniqueSize());
    }

    public void testPutAndGetMultiMixRemove()  throws Exception
    {
        runTestPutAndGetMultiMixRemove(0);
        runTestPutAndGetMultiMixRemove(1);
        runTestPutAndGetMultiMixRemove(2);
        runTestPutAndGetMultiMixRemove(3);
    }

    private void runTestPutAndGetMultiMixRemove(int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        FullSemiUniqueDatedIndex index = new FullSemiUniqueDatedIndex("",
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes());
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
        Timestamp to = new Timestamp(from.getTime() + monthTime);

        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
        TinyBalanceData[] array = new TinyBalanceData[MAX*5];
        prepareIndex(index, from, in, monthTime, to, array);
        TinyBalance uniqueProbe = new TinyBalance(from, in);
        TinyBalance semiUniqueProbe = new TinyBalance(fromProbe, inProbe);
        RelationshipHashStrategy rhs = new TinyBalanceShiftedRhs(shift);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift));
        extractors.add(TinyBalanceFinder.businessDate());
        extractors.add(TinyBalanceFinder.processingDate());

        TinyBalanceData[] removedArray = new TinyBalanceData[MAX*5];


        for(int i=0;i<MAX;i+=5)
        {
            assertSame(array[i], index.remove(array[i].copy()));
            removedArray[i] = array[i];
            array[i] = null;
        }
        for(int i=MAX;i<MAX*3;i+=3)
        {
            assertSame(array[i], index.remove(array[i].copy()));
            removedArray[i] = array[i];
            array[i] = null;
        }

        for(int i=MAX*3;i<MAX*4;i++)
        {
            assertSame(array[i], index.remove(array[i].copy()));
            removedArray[i] = array[i];
            array[i] = null;
            if (i % 4 == 3) i++;
        }

        for(int i=MAX*4;i<MAX*5;i++)
        {
            assertSame(array[i], index.remove(array[i].copy()));
            removedArray[i] = array[i];
            array[i] = null;
        }

        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
    }

    public void testPutAndGetMultiMixRemoveOldEntry()  throws Exception
    {
        runTestPutAndGetMultiMixRemoveOldEntry(0);
        runTestPutAndGetMultiMixRemoveOldEntry(1);
        runTestPutAndGetMultiMixRemoveOldEntry(2);
        runTestPutAndGetMultiMixRemoveOldEntry(3);
    }

    private void runTestPutAndGetMultiMixRemoveOldEntry(int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        FullSemiUniqueDatedIndex index = new FullSemiUniqueDatedIndex("",
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes());
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
        Timestamp to = new Timestamp(from.getTime() + monthTime);

        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
        TinyBalanceData[] array = new TinyBalanceData[MAX*5];
        prepareIndex(index, from, in, monthTime, to, array);
        TinyBalance uniqueProbe = new TinyBalance(from, in);
        TinyBalance semiUniqueProbe = new TinyBalance(fromProbe, inProbe);
        RelationshipHashStrategy rhs = new TinyBalanceShiftedRhs(shift);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift));
        extractors.add(TinyBalanceFinder.businessDate());
        extractors.add(TinyBalanceFinder.processingDate());

        TinyBalanceData[] removedArray = new TinyBalanceData[MAX*5];


        for(int i=0;i<MAX;i+=5)
        {
            assertSame(array[i], index.removeOldEntry(array[i].copy(), probes));
            removedArray[i] = array[i];
            array[i] = null;
        }
        for(int i=MAX;i<MAX*3;i+=3)
        {
            long month = (i - MAX) % 4 *monthTime;
            Timestamp[] localProbes = {new Timestamp(fromProbe.getTime() + month), inProbe};
            assertSame(array[i], index.removeOldEntry(array[i].copy(), localProbes));
            removedArray[i] = array[i];
            array[i] = null;
        }

        for(int i=MAX*3;i<MAX*4;i++)
        {
            long month = (i - MAX) % 4 *monthTime;
            Timestamp[] localProbes = {new Timestamp(fromProbe.getTime() + month), inProbe};
            assertSame(array[i], index.removeOldEntry(array[i].copy(), localProbes));
            removedArray[i] = array[i];
            array[i] = null;
            if (i % 4 == 3) i++;
        }

        for(int i=MAX*4;i<MAX*5;i++)
        {
            long month = (i - MAX) % 4 *monthTime;
            Timestamp[] localProbes = {new Timestamp(fromProbe.getTime() + month), inProbe};
            assertSame(array[i], index.removeOldEntry(array[i].copy(), localProbes));
            removedArray[i] = array[i];
            array[i] = null;
        }

        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
    }

    public void testPutAndGetMultiMixRemoveOldEntryForRange()  throws Exception
    {
        runTestPutAndGetMultiMixRemoveOldEntryForRange(0);
        runTestPutAndGetMultiMixRemoveOldEntryForRange(1);
        runTestPutAndGetMultiMixRemoveOldEntryForRange(2);
        runTestPutAndGetMultiMixRemoveOldEntryForRange(3);
    }

    private void runTestPutAndGetMultiMixRemoveOldEntryForRange(int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        FullSemiUniqueDatedIndex index = new FullSemiUniqueDatedIndex("",
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes());
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
        Timestamp to = new Timestamp(from.getTime() + monthTime);

        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
        TinyBalanceData[] array = new TinyBalanceData[MAX*5];
        prepareIndex(index, from, in, monthTime, to, array);
        TinyBalance uniqueProbe = new TinyBalance(from, in);
        TinyBalance semiUniqueProbe = new TinyBalance(fromProbe, inProbe);
        RelationshipHashStrategy rhs = new TinyBalanceShiftedRhs(shift);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift));
        extractors.add(TinyBalanceFinder.businessDate());
        extractors.add(TinyBalanceFinder.processingDate());

        TinyBalanceData[] removedArray = new TinyBalanceData[MAX*5];


        for(int i=0;i<MAX;i+=5)
        {
            List list = index.removeOldEntryForRange(array[i].copy());
            assertEquals(1, list.size());
            assertTrue(list.contains(array[i]));
            removedArray[i] = array[i];
            array[i] = null;
        }
        for(int i=MAX;i<MAX*3;i+=3)
        {
            List list = index.removeOldEntryForRange(array[i].copy());
            assertEquals(1, list.size());
            assertTrue(list.contains(array[i]));
            removedArray[i] = array[i];
            array[i] = null;
        }

        for(int i=MAX*3;i<MAX*4;i++)
        {
            List list = index.removeOldEntryForRange(array[i].copy());
            assertEquals(1, list.size());
            assertTrue(list.contains(array[i]));
            removedArray[i] = array[i];
            array[i] = null;
            if (i % 4 == 3) i++;
        }

        for(int i=MAX*4;i<MAX*5;i+=4)
        {
            TinyBalanceData data = (TinyBalanceData) array[i].copy();
            data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
            List list = index.removeOldEntryForRange(data);
            assertEquals(4, list.size());
            for(int j=0;j<4;j++)
            {
                assertTrue(list.contains(array[i+j]));
                removedArray[i+j] = array[i+j];
                array[i+j] = null;
            }
        }

        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
    }

    private void checkIndexAfterRemove(Extractor[] shiftedExtractor, FullSemiUniqueDatedIndex index, Timestamp from, Timestamp in, long monthTime, Timestamp fromProbe, Timestamp inProbe, Timestamp[] probes, TinyBalanceData[] array, TinyBalance uniqueProbe, TinyBalance semiUniqueProbe, List extractors, TinyBalanceData[] removedArray, RelationshipHashStrategy rhs)
    {
        for(int i=0;i<MAX;i++)
        {
            if (array[i] != null)
            {
                assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
                List list = index.getFromDataForAllDatesAsList(array[i].copy());
                assertEquals(1, list.size());
                assertSame(array[i], list.get(0));
                assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), probes));
                uniqueProbe.setBalanceId(i);
                semiUniqueProbe.setBalanceId(i);
                assertSame(array[i], index.get(uniqueProbe, extractors));
                assertSame(array[i], index.getFromSemiUnique(semiUniqueProbe, extractors));
                assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
                assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), probes[0], probes[1]));
            }
        }
        for(int i=MAX;i<MAX*5;i++)
        {
            if (array[i] != null)
            {
                assertSame(array[i], index.getFromData(array[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
                int stride = (i - MAX) % 4;
                long month = stride *monthTime;
                List list = index.getFromDataForAllDatesAsList(array[i].copy());
                assertTrue(list.contains(array[i]));
                Timestamp localFromProbe = new Timestamp(fromProbe.getTime() + month);
                Timestamp[] localProbes = {localFromProbe, inProbe};
                assertSame(array[i], index.getSemiUniqueFromData(array[i].copy(), localProbes));
                Timestamp localFrom = new Timestamp(from.getTime() + month);
                TinyBalance localUniqueProbe = new TinyBalance(localFrom, in);
                localUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
                assertSame(array[i], index.get(localUniqueProbe, extractors));
                TinyBalance localSemiUniqueProbe = new TinyBalance(localFromProbe, inProbe);
                localSemiUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
                assertSame(array[i], index.getFromSemiUnique(localSemiUniqueProbe, extractors));
                assertSame(array[i], index.getSemiUniqueAsOneWithDates(array[i].copy(), shiftedExtractor, localProbes, index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
                assertSame(array[i], index.getSemiUniqueAsOne(null, array[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(array[i]), localProbes[0], localProbes[1]));
            }
        }
        for(int i=0;i<MAX;i++)
        {
            if (removedArray[i] != null)
            {
                assertNull(index.getFromData(removedArray[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
                assertEquals(0, index.getFromDataForAllDatesAsList(removedArray[i].copy()).size());
                assertNull(index.getSemiUniqueFromData(removedArray[i].copy(), probes));
                uniqueProbe.setBalanceId(i);
                semiUniqueProbe.setBalanceId(i);
                assertNull(index.get(uniqueProbe, extractors));
                assertNull(index.getFromSemiUnique(semiUniqueProbe, extractors));
                assertNull(index.getSemiUniqueAsOneWithDates(removedArray[i].copy(), shiftedExtractor, probes, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
                assertNull(index.getSemiUniqueAsOne(null, removedArray[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i]), probes[0], probes[1]));
            }
        }
        for(int i=MAX;i<MAX*5;i++)
        {
            if (removedArray[i] != null)
            {
                assertNull(index.getFromData(removedArray[i].copy(), index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
                int stride = (i - MAX) % 4;
                long month = stride *monthTime;
                List list = index.getFromDataForAllDatesAsList(removedArray[i].copy());
                assertTrue(!list.contains(removedArray[i]));
                Timestamp localFromProbe = new Timestamp(fromProbe.getTime() + month);
                Timestamp[] localProbes = {localFromProbe, inProbe};
                assertNull(index.getSemiUniqueFromData(removedArray[i].copy(), localProbes));
                Timestamp localFrom = new Timestamp(from.getTime() + month);
                TinyBalance localUniqueProbe = new TinyBalance(localFrom, in);
                localUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
                assertNull(index.get(localUniqueProbe, extractors));
                TinyBalance localSemiUniqueProbe = new TinyBalance(localFromProbe, inProbe);
                localSemiUniqueProbe.setBalanceId((i-MAX)/4 + MAX);
                assertNull(index.getFromSemiUnique(localSemiUniqueProbe, extractors));
                assertNull(index.getSemiUniqueAsOneWithDates(removedArray[i].copy(), shiftedExtractor, localProbes, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i])));
                assertNull(index.getSemiUniqueAsOne(null, removedArray[i].copy(), rhs, index.getNonDatedPkHashStrategy().computeHashCode(removedArray[i]), localProbes[0], localProbes[1]));
            }
        }
    }

    public void testPutAndGetMultiMixRemoveAllFilter()  throws Exception
    {
        runTestPutAndGetMultiMixRemoveAllFilter(0);
        runTestPutAndGetMultiMixRemoveAllFilter(1);
        runTestPutAndGetMultiMixRemoveAllFilter(2);
        runTestPutAndGetMultiMixRemoveAllFilter(3);
    }

    private void runTestPutAndGetMultiMixRemoveAllFilter(int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        FullSemiUniqueDatedIndex index = new FullSemiUniqueDatedIndex("",
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes());
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
        Timestamp to = new Timestamp(from.getTime() + monthTime);

        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
        TinyBalanceData[] array = new TinyBalanceData[MAX*5];
        for (int i=0;i< MAX;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBalanceId(i);
            data.setBusinessDateFrom(from);
            data.setBusinessDateTo(to);
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            array[i] = data;
            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
        }
        for (int i=MAX;i<MAX*5;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setQuantity(i);
            data.setBalanceId((i-MAX)/4 + MAX);
            long month = ((i-MAX) % 4)*monthTime;
            data.setBusinessDateFrom(new Timestamp(from.getTime() + month));
            data.setBusinessDateTo(new Timestamp(to.getTime() + month));
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            array[i] = data;
            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
        }
        assertEquals(MAX*5, index.size());
        assertEquals(MAX*2, index.getSemiUniqueSize());
        TinyBalance uniqueProbe = new TinyBalance(from, in);
        TinyBalance semiUniqueProbe = new TinyBalance(fromProbe, inProbe);
        RelationshipHashStrategy rhs = new TinyBalanceShiftedRhs(shift);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift));
        extractors.add(TinyBalanceFinder.businessDate());
        extractors.add(TinyBalanceFinder.processingDate());

        TinyBalanceData[] removedArray = new TinyBalanceData[MAX*5];


        for(int i=0;i<MAX;i+=5)
        {
            removedArray[i] = array[i];
            array[i] = null;
        }
        index.removeAll(new Filter()
        {
            public boolean matches(Object o)
            {
                TinyBalanceData data = (TinyBalanceData) o;
                return data.getBalanceId() < MAX && data.getBalanceId() % 5 == 0;
            }
        });

        for(int i=MAX;i<MAX*3;i+=3)
        {
            removedArray[i] = array[i];
            array[i] = null;
        }
        index.removeAll(new Filter()
        {
            public boolean matches(Object o)
            {
                TinyBalanceData data = (TinyBalanceData) o;
                int i = (int) Math.round(data.getQuantity());
                return i >= MAX && i < MAX*3 && (i-MAX) % 3 == 0;
            }
        });

        for(int i=MAX*3;i<MAX*4;i++)
        {
            if (i % 4 != 3)
            {
                removedArray[i] = array[i];
                array[i] = null;
            }
        }
        index.removeAll(new Filter()
        {
            public boolean matches(Object o)
            {
                TinyBalanceData data = (TinyBalanceData) o;
                int i = (int) Math.round(data.getQuantity());
                return i >= MAX*3 && i < MAX*4 && i % 4 != 3;
            }
        });

        for(int i=MAX*4;i<MAX*5;i++)
        {
            removedArray[i] = array[i];
            array[i] = null;
        }
        index.removeAll(new Filter()
        {
            public boolean matches(Object o)
            {
                TinyBalanceData data = (TinyBalanceData) o;
                int i = (int) Math.round(data.getQuantity());
                return i >= MAX*4;
            }
        });

        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
    }

    public void testPutAndGetMultiMixRemoveAllIgnoringDate()  throws Exception
    {
        runTestPutAndGetMultiMixRemoveAllIgnoringDate(0);
        runTestPutAndGetMultiMixRemoveAllIgnoringDate(1);
        runTestPutAndGetMultiMixRemoveAllIgnoringDate(2);
        runTestPutAndGetMultiMixRemoveAllIgnoringDate(3);
    }

    private void runTestPutAndGetMultiMixRemoveAllIgnoringDate(int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        FullSemiUniqueDatedIndex index = new FullSemiUniqueDatedIndex("",
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes());
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        long monthTime = 30 * 24 * 3600 * 1000L; // about a month
        Timestamp to = new Timestamp(from.getTime() + monthTime);

        Timestamp fromProbe = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp inProbe = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp[] probes = new Timestamp[] { fromProbe, inProbe };
        TinyBalanceData[] array = new TinyBalanceData[MAX*5];
        for (int i=0;i< MAX;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBalanceId(i);
            data.setBusinessDateFrom(from);
            data.setBusinessDateTo(to);
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            array[i] = data;
            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
        }
        for (int i=MAX;i<MAX*5;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setQuantity(i);
            data.setBalanceId((i-MAX)/4 + MAX);
            long month = ((i-MAX) % 4)*monthTime;
            data.setBusinessDateFrom(new Timestamp(from.getTime() + month));
            data.setBusinessDateTo(new Timestamp(to.getTime() + month));
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            array[i] = data;
            index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
        }
        assertEquals(MAX*5, index.size());
        assertEquals(MAX*2, index.getSemiUniqueSize());
        TinyBalance uniqueProbe = new TinyBalance(from, in);
        TinyBalance semiUniqueProbe = new TinyBalance(fromProbe, inProbe);
        RelationshipHashStrategy rhs = new TinyBalanceShiftedRhs(shift);
        List extractors = new FastList();
        extractors.add(new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift));
        extractors.add(TinyBalanceFinder.businessDate());
        extractors.add(TinyBalanceFinder.processingDate());

        TinyBalanceData[] removedArray = new TinyBalanceData[MAX*5];

        final int[] count = new int[1];
        DoProcedure proc = new DoProcedure()
        {
            public void execute(Object object)
            {
                count[0]++;
            }
        };

        for(int i=0;i<MAX;i+=5)
        {
            count[0] = 0;
            assertTrue(index.removeAllIgnoringDate(array[i], proc));
            assertEquals(1, count[0]);
            removedArray[i] = array[i];
            array[i] = null;
        }

        for(int i=MAX;i<MAX*5;i+=8)
        {
            count[0] = 0;
            assertTrue(index.removeAllIgnoringDate(array[i], proc));
            assertEquals(4, count[0]);
            for(int j=0;j<4;j++)
            {
                removedArray[i+j] = array[i+j];
                array[i+j] = null;
            }
        }

        checkIndexAfterRemove(shiftedExtractor, index, from, in, monthTime, fromProbe, inProbe, probes, array, uniqueProbe, semiUniqueProbe, extractors, removedArray, rhs);
    }


    private static class ShiftedIntExtractor implements IntExtractor
    {
        private int shift;
        private IntExtractor delegate;

        private ShiftedIntExtractor(IntExtractor delegate, int shift)
        {
            this.delegate = delegate;
            this.shift = shift;
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

    private static class TinyBalanceShiftedRhs implements RelationshipHashStrategy
    {
        private int shift;

        private TinyBalanceShiftedRhs(int shift)
        {
            this.shift = shift;
        }

        public boolean equalsForRelationship(Object _srcObject, Object _srcData, Object _targetData, Timestamp _asOfDate0, Timestamp _asOfDate1)
        {
            TinyBalanceData srcData = (TinyBalanceData) _srcData;
            TinyBalanceData targetData = (TinyBalanceData) _targetData;
            if (srcData.getBalanceId() == targetData.getBalanceId())
            {
                return TinyBalanceFinder.businessDate().dataMatches(targetData, _asOfDate0) &&
                        TinyBalanceFinder.processingDate().dataMatches(targetData, _asOfDate1);
            }
            return false;
        }

        public int computeHashCodeFromRelated(Object srcObject, Object _srcData)
        {
            TinyBalanceData srcData = (TinyBalanceData) _srcData;
            return srcData.getBalanceId() >> shift;
        }

        @Override
        public int computeOffHeapHashCodeFromRelated(Object srcObject, Object srcData)
        {
            return this.computeHashCodeFromRelated(srcObject, srcData);
        }
    }

}
