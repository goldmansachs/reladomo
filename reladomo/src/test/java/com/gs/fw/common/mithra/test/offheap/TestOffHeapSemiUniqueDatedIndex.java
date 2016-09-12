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
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.ReadWriteLock;
import com.gs.fw.common.mithra.cache.offheap.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.extractor.OffHeapableExtractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.bcp.TlewTrialData;
import com.gs.fw.common.mithra.test.domain.bcp.TlewTrialDatabaseObjectAbstract;
import com.gs.fw.common.mithra.util.*;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class TestOffHeapSemiUniqueDatedIndex extends TestCase
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final int MAX = 10000;

    private OffHeapDataStorage dataStorage;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        dataStorage = new FastUnsafeOffHeapDataStorage(TinyBalanceData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        TinyBalanceData.TinyBalanceDataOffHeap.zSetStorage(dataStorage);
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

//    public void testMultipleReadArchive() throws Exception
//    {
//        MithraTestResource mtr1 = new MithraTestResource("xml/mithra/test2/MithraRuntimeConfig.xml");
//        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
//        mtr1.createSingleDatabase(cmForConfig);
//        mtr1.setUp();
//        File ARCHIVE_BASE_DIR = new File("/tmp");
//        String ARCHIVE_OBJECT = "com.gs.fw.glew.domain.lew.deposit.PwmDeposit";
//        String[] DATE_STRINGS = {"2010", "2011"};
//        try
//        {
//            MithraRuntimeCacheController controller = new MithraRuntimeCacheController(PwmDepositFinder.class);
//            for (String dateString : DATE_STRINGS)
//            {
//                File archiveFile = new File(ARCHIVE_BASE_DIR, ARCHIVE_OBJECT+"."+dateString);
//                System.out.println("Loading archive file for " + dateString);
//                FileInputStream fileInputStream = new FileInputStream(archiveFile);
//                controller.readCacheFromArchiveDoNotRemoveLeftOver(fileInputStream);
//                System.out.println("Loaded archive file for " + dateString);
//            }
//
//        }
//        finally
//        {
//            mtr1.tearDown();
//        }
//    }


//    public void testLoadArchive() throws Exception
//    {
//        FastUnsafeOffHeapDataStorage store = new FastUnsafeOffHeapDataStorage(AbstractLewTradeData.OFF_HEAP_DATA_SIZE);
//        AbstractLewTradeData.AbstractLewTradeDataOffHeap.zSetStorage(store);
//
//        NonUniqueOffHeapIndex index = new NonUniqueOffHeapIndex(new Extractor[] {AbstractLewTradeFinder.instrumentId(), AbstractLewTradeFinder.region()}, 16, store);
//        Attribute[] nonDatedPkAttributes = AbstractLewTradeFinder.getPrimaryKeyAttributes();
//        AsOfAttribute[] asOfAttributes = AbstractLewTradeFinder.getAsOfAttributes();
//        Attribute[] primaryKeyAttributes = new Attribute[nonDatedPkAttributes.length + asOfAttributes.length];
//        System.arraycopy(nonDatedPkAttributes, 0, primaryKeyAttributes, 0, nonDatedPkAttributes.length);
//        for (int i = 0; i < asOfAttributes.length; i++)
//        {
//            primaryKeyAttributes[nonDatedPkAttributes.length + i] = asOfAttributes[i].getFromAttribute();
//        }
//        NonUniqueIndex index2 = new NonUniqueIndex("", primaryKeyAttributes, new Extractor[] {AbstractLewTradeFinder.instrumentId(), AbstractLewTradeFinder.region()});
//        try
//        {
//            readArchive(new FileInputStream("h://tmp/com.gs.fw.glew.domain.lew.trade.AbstractLewTrade"), index, index2);
//        }
//        finally
//        {
//            index.destroy();
//            store.destroy();
//        }
//    }
//
//    private void readArchive(InputStream in, NonUniqueOffHeapIndex index, NonUniqueIndex index2)
//            throws IOException, ClassNotFoundException
//    {
//        String className = AbstractLewTrade.class.getName();
//        in = new BufferedInputStream(in, 8092);
//        MithraObjectDeserializer deserializer = new AbstractLewTradeDatabaseObject();
//        ZInputStream zis = null;
//        byte archiveVersion = (byte) in.read();
//        if (archiveVersion > 2)
//        {
//            throw new MithraBusinessException("unknown cache archive version "+archiveVersion);
//        }
//        byte compression = (byte) in.read();
//        if (compression != 1)
//        {
//            throw new MithraBusinessException("unknown cache compression type "+compression);
//        }
//        zis = new ZInputStream(in);
//        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(zis, 8092));
//        String finderClassName = (String) ois.readObject();
//        if (!finderClassName.equals(className))
//        {
//            throw new MithraBusinessException("Wrong cache archive. Expecting "+className+" but got "+
//             finderClassName);
//        }
//        int serialVersion = ois.readInt();
//        if (serialVersion != AbstractLewTradeFinder.getFinderInstance().getSerialVersionId())
//        {
//            throw new MithraBusinessException("Wrong serial version for class "+className+" Expecting "+
//                    AbstractLewTradeFinder.getFinderInstance().getSerialVersionId()+" but got "+serialVersion);
//        }
//        boolean waitForZero = archiveVersion > 1;
//        boolean done = false;
//        do
//        {
//            int size = ois.readInt();
//            done = size == 0;
//            for(int i=0;i<size;i++)
//            {
//                if (i == 100)
//                {
//                    System.out.println("watch out");
//                }
//                MithraDataObject data = deserializer.deserializeFullData(ois);
//
////                if (((AbstractLewTradeData)data).getInstrumentId() == 2018667184)
////                {
////                    System.out.println("where");
////                }
//
//                index.put(data.zCopyOffHeap());
//                index2.put(data);
//                assertEquals(index2.size(), index.size());
//                if (index2.getNonUniqueSize() != index.getNonUniqueSize())
//                {
//                    System.out.println("off");
//                }
//                assertEquals(index2.getNonUniqueSize(), index.getNonUniqueSize());
//            }
//        }
//        while(waitForZero && !done);
//    }

    private void renewStorage()
    {
        dataStorage.destroy();
        dataStorage = new FastUnsafeOffHeapDataStorage(TinyBalanceData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        TinyBalanceData.TinyBalanceDataOffHeap.zSetStorage(dataStorage);
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
    }


    public void testIsNullOffsetWithManyFields()
    {
        FastUnsafeOffHeapDataStorage store = new FastUnsafeOffHeapDataStorage(TlewTrialData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        TlewTrialData.TlewTrialDataOffHeap.zSetStorage(store);
                try
                {
                    TlewTrialData data = TlewTrialDatabaseObjectAbstract.allocateOnHeapData();
                    data.setProcessingDateTo(new Timestamp(0L));

                    data.setOpenIntBase(0.0);

                    data.setMtdIntIncmNull();
                    data.setProdRelHostNull();
                    data.setProdUndrlrPxNull();
                    data.setPositionQuantityNull();
                    data.setPosMktvlBaseNull();
                    data.setPosCostBaseNull();
                    data.setPosAccrIntBaseNull();
                    data.setPosPurchIntBaseNull();
                    data.setPosNotvlBaseNull();
                    data.setContractualNpvNull();
                    data.setUnstldRwPlNull();
                    data.setUnstldFwdPlNull();
                    data.setUnrealFutPerGmiPlNull();
                    data.setFairValAdjNull();
                    data.setAdjTdUnstldFutPlNull();
                    data.setValuationAdjNull();
                    data.setAccrDivNull();
                    data.setOtherTdAdjNull();
                    data.setFxRevalAdjNull();

                    TlewTrialData offHeapData = (TlewTrialData) data.zCopyOffHeap();


                    assertFalse("Confirm value set earlier has not been overridden with Null by another field", offHeapData.isOpenIntBaseNull());
                    assertEquals(0.0, offHeapData.getOpenIntBase());

                }
            finally
            {
                store.destroy();
            }

    }

    public void testBooleanOffHeap()
    {
        FastUnsafeOffHeapDataStorage store = new FastUnsafeOffHeapDataStorage(AuditedUserData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        AuditedUserData.AuditedUserDataOffHeap.zSetStorage(store);
        try
        {
            AuditedUserData data = AuditedUserDatabaseObjectAbstract.allocateOffHeapData();
            data.setActive(true);
            assertTrue(data.isActive());
            assertFalse(data.isActiveNull());
            data.setActive(false);
            assertFalse(data.isActive());
            assertFalse(data.isActiveNull());
            data.setActiveNull();
            assertTrue(data.isActiveNull());

            AuditedUserData d2 = AuditedUserDatabaseObjectAbstract.allocateOnHeapData();
            d2.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
            d2.setProcessingDateTo(new Timestamp(System.currentTimeMillis()));
            d2.setActive(true);
            data = (AuditedUserData) d2.zCopyOffHeap();
            assertEquals(true, data.isActive());
            assertFalse(data.isActiveNull());
            d2.setActive(false);
            data = (AuditedUserData) d2.zCopyOffHeap();
            assertEquals(false, data.isActive());
            assertFalse(data.isActiveNull());
            d2.setActiveNull();
            data = (AuditedUserData) d2.zCopyOffHeap();
            assertTrue(data.isActiveNull());

        }
        finally
        {
            store.destroy();
        }
    }

    public void testBasicData() throws Exception
    {
        for(int i=0;i<10;i++)
        {
            runBasicData();
            renewStorage();
        }
    }

    public void runBasicData() throws Exception
    {
        Timestamp from = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());
        Timestamp in = new Timestamp(timestampFormat.parse("2002-01-01 10:00:00").getTime());
        Timestamp to = new Timestamp(from.getTime() + 30*24*3600*1000L); // about a month

        TimestampPool.getInstance().getOrAddToCache(new ImmutableTimestamp(0), true);

        long start = System.currentTimeMillis();
        for (int i=0;i< MAX;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
            assertNull(data.getAcmapCode());
            assertNull(data.getBusinessDateFrom());
            assertNull(data.getBusinessDateTo());
            assertNull(data.getProcessingDateFrom());
            assertNull(data.getProcessingDateTo());
            data.setAcmapCode("X"+i);
            data.setBalanceId(i);
            data.setBusinessDateFrom(from);
            data.setBusinessDateTo(to);
            data.setProcessingDateFrom(in);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());

            assertEquals("X"+i, data.getAcmapCode());
            assertEquals(i, data.getBalanceId());
            assertEquals(from, data.getBusinessDateFrom());
            assertEquals(to, data.getBusinessDateTo());
            assertEquals(in, data.getProcessingDateFrom());
            assertEquals(TinyBalanceFinder.processingDate().getInfinityDate(), data.getProcessingDateTo());

            assertEquals("X"+i, ((StringAttribute)TinyBalanceFinder.acmapCode()).stringValueOf(data));
            assertEquals(i, ((IntegerAttribute)TinyBalanceFinder.balanceId()).intValueOf(data));
            assertEquals(1009861200000L, ((TimestampAttribute)TinyBalanceFinder.businessDateFrom()).timestampValueOfAsLong(data));
            assertEquals(1012453200000L, ((TimestampAttribute)TinyBalanceFinder.businessDateTo()).timestampValueOfAsLong(data));
            assertEquals(1009897200000L, ((TimestampAttribute)TinyBalanceFinder.processingDateFrom()).timestampValueOfAsLong(data));
            assertEquals(253399726740000L, ((TimestampAttribute)TinyBalanceFinder.processingDateTo()).timestampValueOfAsLong(data));
        }
        long end = System.currentTimeMillis();
        System.out.println("creation took "+(end - start)+" ms");
    }

    protected Date createParaBusinessDate(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 18);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    protected Date addDays(Date d, int days)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.add(Calendar.DATE, days);

        return cal.getTime();
    }

    private Timestamp addDaysAsTimestamp(java.util.Date d, int days)
    {
        return new Timestamp(addDays(d, days).getTime());
    }

    public void testMultiSegmentRemove()
    {
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());

        for(int i=1;i<10;i++)
        {
            OffHeapDataStorage dataStorage = new FastUnsafeOffHeapDataStorage(ParaBalanceData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
            ParaBalanceData.ParaBalanceDataOffHeap.zSetStorage(dataStorage);
            OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                    ParaBalanceFinder.getPrimaryKeyAttributes(),
                    ParaBalanceFinder.getAsOfAttributes(), dataStorage);
            try
            {
                Timestamp[] businessDates = new Timestamp[i];
                Timestamp[] inDates = new Timestamp[i];
                for (int j = 0; j < i; j++)
                {
                    businessDates[j] = addDaysAsTimestamp(paraBusinessDate, j * 10);
                    inDates[j] = new Timestamp(System.currentTimeMillis() + 1000 * j);
                }
                long seed = System.currentTimeMillis();
                Random random = new Random(seed);
                ParaBalanceData[] offHeap = new ParaBalanceData[i*1000];
                for(int k=0;k<1000;k++)
                {
                    int id = random.nextInt();
                    for (int j = 0; j < i - 1; j++)
                    {
                        ParaBalanceData data = createParaBalance(id, businessDates[j], businessDates[j + 1], inDates[j], inDates[j + 1]);
                        offHeap[k*i + j] = data;
                    }
                    offHeap[k*i + i-1] = createParaBalance(id, businessDates[i-1], ParaBalanceFinder.businessDate().getInfinityDate(), inDates[i-1], ParaBalanceFinder.processingDate().getInfinityDate());
                }
                ParaBalanceData[] copy = Arrays.copyOf(offHeap, offHeap.length);
                this.shuffle(offHeap, seed);
                this.shuffle(copy, Long.rotateLeft(seed, 32));
                addRemove(index, offHeap, copy);
            }
            finally
            {
                index.destroy();
                dataStorage.destroy();
            }
        }
    }

    private void addRemove(OffHeapSemiUniqueDatedIndex index, ParaBalanceData[] offHeap, ParaBalanceData[] copy)
    {
        Extractor[] forSemiUnique = new Extractor[] { ParaBalanceFinder.balanceId(), ParaBalanceFinder.acmapCode(), ParaBalanceFinder.businessDateTo(), ParaBalanceFinder.processingDateTo() };
        Extractor[] forUnique = new Extractor[] { ParaBalanceFinder.balanceId(), ParaBalanceFinder.acmapCode(), ParaBalanceFinder.businessDateFrom(), ParaBalanceFinder.processingDateFrom() };
        for(int i=0;i<offHeap.length;i++)
        {
            index.put(offHeap[i], index.getNonDatedPkHashStrategy().computeHashCode(offHeap[i]));
        }
        assertEquals(1000, index.getSemiUniqueSize());
        assertEquals(offHeap.length, index.size());
        for(int i=0;i<copy.length;i++)
        {
            assertSame(copy[i], index.getFromSemiUnique(copy[i], forSemiUnique));
            assertSame(copy[i], index.get(copy[i], forUnique));
            index.remove(copy[i]);
            assertNull(index.getFromSemiUnique(copy[i], forSemiUnique));
            assertNull(index.get(copy[i], forUnique));
        }
        for(int i=0;i<copy.length;i++)
        {
            index.put(copy[i], index.getNonDatedPkHashStrategy().computeHashCode(copy[i]));
        }
        assertEquals(1000, index.getSemiUniqueSize());
        assertEquals(offHeap.length, index.size());
        for(int i=0;i<offHeap.length;i++)
        {
            assertSame(offHeap[i], index.getFromSemiUnique(offHeap[i], forSemiUnique));
            assertSame(offHeap[i], index.get(offHeap[i], forUnique));
            index.remove(offHeap[i]);
            assertNull(index.getFromSemiUnique(offHeap[i], forSemiUnique));
            assertNull(index.get(offHeap[i], forUnique));
        }
    }

    private ParaBalanceData createParaBalance(int id, Timestamp businessDateFrom, Timestamp businessDateTo, Timestamp inDate, Timestamp outDate)
    {
        ParaBalanceData data2 = ParaBalanceDatabaseObject.allocateOffHeapData();
        data2.setBalanceId(id);
        data2.setAcmapCode("A");
        data2.setBusinessDateFrom(businessDateFrom);
        data2.setBusinessDateTo(businessDateTo);
        data2.setProcessingDateFrom(inDate);
        data2.setProcessingDateTo(outDate);
        return data2;
    }

    public void testPutAndRemoveRandomOrder() throws Exception
    {
        OffHeapDataStorage dataStorage = new FastUnsafeOffHeapDataStorage(ParaBalanceData.OFF_HEAP_DATA_SIZE, new ReadWriteLock());
        ParaBalanceData.ParaBalanceDataOffHeap.zSetStorage(dataStorage);

        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, 0);
        Timestamp oldBusinessDate = addDaysAsTimestamp(paraBusinessDate, -10);
        Timestamp start = new Timestamp(System.currentTimeMillis());
        Timestamp later = new Timestamp(System.currentTimeMillis() + 1000);
        ParaBalanceData[] offHeap = new ParaBalanceData[500];
        ParaBalanceData[] onHeap = new ParaBalanceData[500];
        for(int i=2000;i<2500;i++)
        {
            ParaBalanceData data = createParaBalance(5000 - i, oldBusinessDate, ParaBalanceFinder.businessDate().getInfinityDate(), start, ParaBalanceFinder.processingDate().getInfinityDate());
            offHeap[i - 2000] = data;
            onHeap[i - 2000] = (ParaBalanceData) data.copy();
        }
        ParaBalanceData[] offHeapSplitOne = new ParaBalanceData[500];
        for(int i=2000;i<2500;i++)
        {
            ParaBalanceData data = createParaBalance(5000 - i, oldBusinessDate, businessDate, later, ParaBalanceFinder.processingDate().getInfinityDate());
            offHeapSplitOne[i - 2000] = data;
        }

        ParaBalanceData[] offHeapSplitTwo = new ParaBalanceData[500];
        for(int i=2000;i<2500;i++)
        {
            ParaBalanceData data = createParaBalance(5000 - i, businessDate, ParaBalanceFinder.businessDate().getInfinityDate(), later, ParaBalanceFinder.processingDate().getInfinityDate());
            offHeapSplitTwo[i - 2000] = data;
        }

        long seed = System.currentTimeMillis();
        this.shuffle(onHeap, seed);
        shuffle(offHeap, seed);
        shuffle(offHeapSplitOne, seed);
        shuffle(offHeapSplitTwo, seed);

        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                ParaBalanceFinder.getPrimaryKeyAttributes(),
                ParaBalanceFinder.getAsOfAttributes(), dataStorage);
        try
        {
            for(ParaBalanceData data: offHeap)
            {
                index.put(data, index.getNonDatedPkHashStrategy().computeHashCode(data));
            }
            assertEquals(500, index.size());
            assertEquals(500, index.getSemiUniqueSize());
            for(int i=0;i<500;i++)
            {
                MithraOffHeapDataObject removed = (MithraOffHeapDataObject) index.remove(onHeap[i]);
                dataStorage.free(removed.zGetOffset());
                index.put(offHeapSplitOne[i], index.getNonDatedPkHashStrategy().computeHashCode(offHeapSplitOne[i]));
                if (i % 2 == 0)
                {
                    index.put(offHeapSplitTwo[i], index.getNonDatedPkHashStrategy().computeHashCode(offHeapSplitTwo[i]));
                }
            }
            assertEquals(750, index.size());
            assertEquals(500, index.getSemiUniqueSize());
        }
        finally
        {
            index.destroy();
            dataStorage.destroy();
        }

    }

    private void shuffle(ParaBalanceData[] data, long seed)
    {
        Collections.shuffle(Arrays.asList(data), new Random(seed));
    }

    public void testPutAndGet()  throws Exception
    {
        for (int i=0;i<10;i++)
        {
            runTestPutAndGet(0);
            renewStorage();
        }
        runTestPutAndGet(1);
        renewStorage();
        runTestPutAndGet(2);
        renewStorage();
        runTestPutAndGet(3);
    }

    private void runTestPutAndGet(final int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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

    public void testEnsureCapacity() throws Exception
    {
        Extractor[] shiftedExtractor = {TinyBalanceFinder.balanceId()};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
        try
        {
            index.ensureExtraCapacity(100000000);
        }
        finally
        {
            index.destroy();
        }
    }

    private void runTestClear(final int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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

    private void runPutAndGetOnIndex(int shift, Extractor[] shiftedExtractor, OffHeapSemiUniqueDatedIndex index) throws ParseException
    {
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
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
            data.setAcmapCode("X" + i);
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

//                assertSame(array[i], index.getFromData(array[i], index.getNonDatedPkHashStrategy().computeHashCode(array[i])));

            assertSame(array[i], index.getFromData(array[i], index.getNonDatedPkHashStrategy().computeHashCode(array[i])));
            List list = index.getFromDataForAllDatesAsList(array[i]);
            assertEquals(1, list.size());
            assertSame(array[i], list.get(0));
            assertSame(array[i], index.getSemiUniqueFromData(array[i], probes));
            assertSame(array[i], index.get(uniqueProbe, extractors));
            assertTrue(index.contains(uniqueProbe, extractorArray, null));
            assertSame(array[i], index.getFromSemiUnique(semiUniqueProbe, extractors));
            assertTrue(index.containsInSemiUnique(semiUniqueProbe, extractorArray, null));
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

//                assertSame(array[i], index.getFromData(array[i], index.getNonDatedPkHashStrategy().computeHashCode(array[i])));

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
        renewStorage();
        runTestPutAndGetMultiMix(1);
        renewStorage();
        runTestPutAndGetMultiMix(2);
        renewStorage();
        runTestPutAndGetMultiMix(3);
    }

    private void runTestPutAndGetMultiMix(int shift) throws Exception
    {
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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

    private void prepareIndex(OffHeapSemiUniqueDatedIndex index, Timestamp from, Timestamp in, long monthTime, Timestamp to, TinyBalanceData[] array)
    {
        for (int i=0;i< MAX;i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
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
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
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
        renewStorage();
        runTestPutAndGetMultiMixRemove(1);
        renewStorage();
        runTestPutAndGetMultiMixRemove(2);
        renewStorage();
        runTestPutAndGetMultiMixRemove(3);
    }

    private void runTestPutAndGetMultiMixRemove(int shift) throws Exception
    {
        System.out.println(shift);
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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
        renewStorage();
        runTestPutAndGetMultiMixRemoveOldEntry(1);
        renewStorage();
        runTestPutAndGetMultiMixRemoveOldEntry(2);
        renewStorage();
        runTestPutAndGetMultiMixRemoveOldEntry(3);
    }

    private void runTestPutAndGetMultiMixRemoveOldEntry(int shift) throws Exception
    {
        System.out.println(shift);
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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
        renewStorage();
        runTestPutAndGetMultiMixRemoveOldEntryForRange(1);
        renewStorage();
        runTestPutAndGetMultiMixRemoveOldEntryForRange(2);
        renewStorage();
        runTestPutAndGetMultiMixRemoveOldEntryForRange(3);
    }

    private void runTestPutAndGetMultiMixRemoveOldEntryForRange(int shift) throws Exception
    {
        System.out.println(shift);
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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

    private void checkIndexAfterRemove(Extractor[] shiftedExtractor, OffHeapSemiUniqueDatedIndex index, Timestamp from, Timestamp in, long monthTime, Timestamp fromProbe, Timestamp inProbe, Timestamp[] probes, TinyBalanceData[] array, TinyBalance uniqueProbe, TinyBalance semiUniqueProbe, List extractors, TinyBalanceData[] removedArray, RelationshipHashStrategy rhs)
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
        renewStorage();
        runTestPutAndGetMultiMixRemoveAllFilter(1);
        renewStorage();
        runTestPutAndGetMultiMixRemoveAllFilter(2);
        renewStorage();
        runTestPutAndGetMultiMixRemoveAllFilter(3);
    }

    private void runTestPutAndGetMultiMixRemoveAllFilter(int shift) throws Exception
    {
        System.out.println(shift);
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
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
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
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
        renewStorage();
        runTestPutAndGetMultiMixRemoveAllIgnoringDate(1);
        renewStorage();
        runTestPutAndGetMultiMixRemoveAllIgnoringDate(2);
        renewStorage();
        runTestPutAndGetMultiMixRemoveAllIgnoringDate(3);
    }

    private void runTestPutAndGetMultiMixRemoveAllIgnoringDate(int shift) throws Exception
    {
        System.out.println(shift);
        Extractor[] shiftedExtractor = {new ShiftedIntExtractor(TinyBalanceFinder.balanceId(), shift)};
        OffHeapSemiUniqueDatedIndex index = new OffHeapSemiUniqueDatedIndex(
                shiftedExtractor,
                TinyBalanceFinder.getAsOfAttributes(), dataStorage);
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
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
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
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOffHeapData();
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

    private static class ShiftedIntOffHeapExtractor implements OffHeapExtractor
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
            return computeHashCodeFromRelated(srcObject, srcData);
        }
    }

}
