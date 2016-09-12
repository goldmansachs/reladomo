
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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.bcp.*;
import junit.framework.Assert;

import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

public class SybaseBcpTest  extends SybaseBcpTestAbstract
{
    private static final int INSERT_COUNT = 5;
    private static final long MAGIC_TIME_CONSTANT = 1178028171000L;
    private static final Timestamp TIMESTAMP_1 = new Timestamp(MAGIC_TIME_CONSTANT);

    private static final Timestamp TIMESTAMP_2 = new Timestamp(107, 7, 4, 3, 30, 12, 123456789);
    private PrintWriter writer;

    // -----

    public void testBcpRepoTrade()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertRepoTrade();
            }
        });
        this.assertRepoTrade();
    }

    public void testRepoTrade()
    {
        this.insertRepoTrade();
        this.assertRepoTrade();
    }

    public void testNonNullableSetToNull()
    {
        final BcpRepoTrade repoTrade = new BcpRepoTrade(new Timestamp(new Date().getTime()));
        try
        {
            this.runAsBcpTransaction(new Callable()
            {
                public Object call()
                {
                    repoTrade.insert();
                    return null;
                }
            });
            fail();
        }
        catch (MithraBusinessException e)
        {
            // ok
        }

    }

    public void testColumnTooLong()
    {
        final Timestamp timestamp = new Timestamp(new Date().getTime());

        final BcpRepoTradeList list = new BcpRepoTradeList();
        for(int i=100;i<200;i++)
        {
            final BcpRepoTrade repoTrade = new BcpRepoTrade(new Timestamp(new Date().getTime()));

            // use the data object to bypass the length check in the business object
            BcpRepoTradeData repoTradeData = BcpRepoTradeDatabaseObject.allocateOnHeapData();
            repoTradeData.setRepoId(String.valueOf(i));
            repoTradeData.setTradingAcctCode("ABC");
            repoTradeData.setTradeCurrencyCode("xasdfasdfasd");
            repoTradeData.setStartDate(timestamp);
            repoTradeData.setTradeDate(timestamp);
            repoTradeData.setEndDate(timestamp);
            repoTradeData.setSource("S");
            repoTradeData.setAcctAggType("ABCDEFGHIJ");
            repoTrade.zSetCurrentData(repoTradeData);

            list.add(repoTrade);
        }

        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                list.insertAll();
                return null;
            }
        });
    }

    private Object insertRepoTrade()
    {
        BcpRepoTradeList list = new BcpRepoTradeList();
        for (int i = 0; i < 3; i++)
        {
            list.add(newRepoTrade(i));
        }
        list.insertAll();
        return list;
    }

    private BcpRepoTrade newRepoTrade(int i)
    {
        final Timestamp timestamp = new Timestamp(new Date().getTime());

        BcpRepoTrade repoTrade = new BcpRepoTrade(new Timestamp(new Date().getTime()));
        repoTrade.setRepoId(String.valueOf(i));
        repoTrade.setTradingAcctCode("ABC");
        repoTrade.setTradeCurrencyCode("x");
        repoTrade.setStartDate(timestamp);
        repoTrade.setTradeDate(timestamp);
        repoTrade.setEndDate(timestamp);
        repoTrade.setSource("S");
        repoTrade.setAcctAggType("ABCDEFGHIJ");
        return repoTrade;
    }

    private void assertRepoTrade()
    {
        BcpRepoTrade one = BcpRepoTradeFinder.findOneBypassCache(BcpRepoTradeFinder.repoId().eq("1"));

        Assert.assertNotNull(one);
    }

    // -----

    public void testBcpNullableTimestamp2()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertNullableTimestamp();
            }
        });
        this.assertNullableTimestamp();
    }

    public void testNullableTimestamp()
    {
        this.insertNullableTimestamp();
        this.assertNullableTimestamp();
    }

    private Object insertNullableTimestamp()
    {
        BcpNullableTimestampList list = new BcpNullableTimestampList();
        list.add(this.newNullableTimestamp(1, TIMESTAMP_2));
        list.add(this.newNullableTimestamp(2, TIMESTAMP_2));
        list.insertAll();
        return list;
    }

    private void assertNullableTimestamp()
    {
        final BcpNullableTimestamp one = BcpNullableTimestampFinder.findOneBypassCache(BcpNullableTimestampFinder.id().eq(1));

        this.assertTimestampEquals(TIMESTAMP_1, one.getTimestampValue());
        this.assertTimestampEquals(TIMESTAMP_2, one.getNullableTimestampValue());

        final BcpNullableTimestampList list = new BcpNullableTimestampList(BcpNullableTimestampFinder.all());
        list.setBypassCache(true);
        Assert.assertEquals(2, list.size());
    }

    private BcpNullableTimestamp newNullableTimestamp(final int id, final Timestamp timestamp)
    {
        final BcpNullableTimestamp bcpTimestamp = new BcpNullableTimestamp();

        bcpTimestamp.setId(id);
        bcpTimestamp.setTimestampValue(TIMESTAMP_1);
        bcpTimestamp.setNullableTimestampValue(timestamp);

        return bcpTimestamp;
    }

    // -----

    public void testBcpNullableTimestamp()
    {
        final long oneTime = new Date(104, 7, 1).getTime();
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                newTimestamp(1, oneTime).insert();
                BcpAllTypes bcpAllTypes = newTimestamp(2, new Date(107, 6, 4).getTime());
                bcpAllTypes.insert();
                return bcpAllTypes;
            }
        });

        final BcpAllTypes one = BcpAllTypesFinder.findOneBypassCache(BcpAllTypesFinder.id().eq(1));

        final Date actualDate = one.getDateValue();
        Assert.assertEquals("date", 104, actualDate.getYear() );
        Assert.assertEquals("date", 7  , actualDate.getMonth());
        Assert.assertEquals("date", 1  , actualDate.getDate()  );

        this.assertTimestampEquals(new Timestamp(oneTime), one.getTimestampValue());
    }

    // -----

    public void testBcpTimestamp()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertTimestamp();
            }
        });
        this.assertTimestamp();
    }

    public void testTimestamp()
    {
        this.insertTimestamp();
        this.assertTimestamp();
    }

    private List insertTimestamp()
    {
        BcpAllTypesList list = new BcpAllTypesList();
        long time = MAGIC_TIME_CONSTANT;
        for (int i = 0; i < INSERT_COUNT; i++)
        {
            final BcpAllTypes obj = this.newTimestamp(i, time);
            list.add(obj);
            time++;
        }
        list.insertAll();
        return list;
    }

    private void assertTimestamp()
    {
//        final BcpAllTypesList list = new BcpAllTypesList(BcpAllTypesFinder.all());
//        list.setBypassCache(true);
//        Assert.assertTimestampEquals(INSERT_COUNT, list.size());

        long time = MAGIC_TIME_CONSTANT;
        for (int i = 0; i < INSERT_COUNT; i++)
        {
            final BcpAllTypes one = BcpAllTypesFinder.findOneBypassCache(BcpAllTypesFinder.id().eq(i));

            final Date expectedDate = new Date(time);
            final Date actualDate = one.getDateValue();
            Assert.assertEquals("date", expectedDate.getYear() , actualDate.getYear() );
            Assert.assertEquals("date", expectedDate.getMonth(), actualDate.getMonth());
            Assert.assertEquals("date", expectedDate.getDay()  , actualDate.getDay()  );

            final Timestamp expectedTimestamp = new Timestamp(time);

            this.assertTimestampEquals(expectedTimestamp, one.getTimestampValue());

            final Date actualNullableDate = one.getDateValue();
            Assert.assertEquals("nullable date", expectedDate.getYear() , actualNullableDate.getYear() );
            Assert.assertEquals("nullable date", expectedDate.getMonth(), actualNullableDate.getMonth());
            Assert.assertEquals("nullable date", expectedDate.getDay()  , actualNullableDate.getDay()  );

            this.assertTimestampEquals(expectedTimestamp, one.getNullableTimestampValue());

            time++;
        }

        final BcpAllTypesList list = new BcpAllTypesList(BcpAllTypesFinder.all());
        list.setBypassCache(true);
        Assert.assertEquals(INSERT_COUNT, list.size());
    }

    private BcpAllTypes newTimestamp(final int id, final long time)
    {
        final BcpAllTypes allTypes = new BcpAllTypes();

        allTypes.setId(id);

        final Date date = new Date(time);
        final Timestamp timestamp = new Timestamp(time);

        allTypes.setCharValue('m');
        allTypes.setNullableCharValueNull();
        allTypes.setDateValue(date);
        allTypes.setTimestampValue(timestamp);
        allTypes.setStringValue("Once upon a time ...");
        allTypes.setNullableDateValue(date);
        allTypes.setNullableTimestampValue(timestamp);

        return allTypes;
    }

    // -----

    public void testBcpAllTypes()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertAllTypes();
            }
        });
        this.assertAllTypes();
    }

    public void testAllTypes()
    {
        this.insertAllTypes();
        this.assertAllTypes();
    }

    private List insertAllTypes()
    {
        BcpAllTypesList list = new BcpAllTypesList();
        for (int i = 0; i < INSERT_COUNT; i++)
        {
            list.add(newAllTypes(i, i % 2 == 0));
        }
        list.insertAll();
        return list;
    }

    private void assertAllTypes()
    {
        BcpAllTypes one = BcpAllTypesFinder.findOneBypassCache(BcpAllTypesFinder.id().eq(1));
        //Assert.assertFalse(one.isNullableBooleanValueNull());  // todo -- gonzra
        Assert.assertFalse(one.isNullableByteValueNull());
        Assert.assertFalse(one.isNullableShortValueNull());
        Assert.assertFalse(one.isNullableCharValueNull());
        Assert.assertFalse(one.isNullableIntValueNull());
        Assert.assertFalse(one.isNullableLongValueNull());
        Assert.assertFalse(one.isNullableFloatValueNull());
        Assert.assertFalse(one.isNullableDoubleValueNull());
        Assert.assertFalse(one.isNullableDateValueNull());
        Assert.assertFalse(one.isNullableTimestampValueNull());
        Assert.assertFalse(one.isNullableStringValueNull());

        BcpAllTypes two = BcpAllTypesFinder.findOneBypassCache(BcpAllTypesFinder.id().eq(2));
        //Assert.assertTrue(two.isNullableBooleanValueNull());  // todo -- gonzra
        Assert.assertTrue(two.isNullableByteValueNull());
        Assert.assertTrue(two.isNullableShortValueNull());
        Assert.assertTrue(two.isNullableCharValueNull());
        Assert.assertTrue(two.isNullableIntValueNull());
        Assert.assertTrue(two.isNullableLongValueNull());
        Assert.assertTrue(two.isNullableFloatValueNull());
        Assert.assertTrue(two.isNullableDoubleValueNull());
        Assert.assertTrue(two.isNullableDateValueNull());
        Assert.assertTrue(two.isNullableTimestampValueNull());
        Assert.assertTrue(two.isNullableStringValueNull());

        final BcpAllTypesList list = new BcpAllTypesList(BcpAllTypesFinder.all());
        list.setBypassCache(true);
        Assert.assertEquals(INSERT_COUNT, list.size());
    }

    private BcpAllTypes newAllTypes(int id, boolean withNullablesNull)
    {
        final BcpAllTypes allTypes = new BcpAllTypes();

        allTypes.setId(id);

        final long time = System.currentTimeMillis();
        final Date date = new Date(time);
        final Timestamp timestamp = new Timestamp(time);

        allTypes.setBooleanValue(false);
        allTypes.setByteValue((byte) 42);
        allTypes.setShortValue((short) 43);
        allTypes.setCharValue('M');
        allTypes.setIntValue(44);
        allTypes.setLongValue(Long.MAX_VALUE - 42);
        allTypes.setFloatValue((float) Math.PI);
        allTypes.setDoubleValue(Math.E);
        allTypes.setDateValue(date);
        allTypes.setTimestampValue(timestamp);
        allTypes.setStringValue("Once upon a time ...");

        if (withNullablesNull)
        {
            allTypes.setNullablePrimitiveAttributesToNull();
        }
        else
        {
            //allTypes.setNullableBooleanValue(true);  // todo -- gonzra
            allTypes.setNullableByteValue((byte) 22);
            allTypes.setNullableShortValue((short) 23);
            allTypes.setNullableCharValue('m');
            allTypes.setNullableIntValue(24);
            allTypes.setNullableLongValue(Long.MIN_VALUE + 42);
            allTypes.setNullableFloatValue((float) (Math.PI - 2));
            allTypes.setNullableDoubleValue(Math.E + 20);
            allTypes.setNullableDateValue(date);
            allTypes.setNullableTimestampValue(timestamp);
            allTypes.setNullableStringValue(".. and they all lived happily ever after.");
        }
        return allTypes;
    }

    // -----

    public void testBcpSimple()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertSimple();
            }
        });
        this.assertSimple();
    }

    public void testEndSimple()
    {
        this.insertSimple();
        this.assertSimple();
    }

    private List insertSimple()
    {
        ArrayList list = new ArrayList();
        for (int i = 0; i < INSERT_COUNT; i++)
        {
            BcpSimple bcpSimple = new BcpSimple(i, "Simple Name " + i);
            bcpSimple.insert();
            list.add(bcpSimple);
        }
        return list;
    }

    private void assertSimple()
    {
        BcpSimple four = BcpSimpleFinder.findOneBypassCache(BcpSimpleFinder.id().eq(4));
        Assert.assertEquals("Simple Name 4", four.getName());

        final BcpSimpleList list = new BcpSimpleList(BcpSimpleFinder.all());
        list.setBypassCache(true);
        Assert.assertEquals(INSERT_COUNT, list.size());
    }

    // -----

    public void testBcpSimpleWithIdentity()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertSimpleWithIdentity();
            }
        });
        this.assertSimpleWithIdentity();
    }

    public void testEndSimpleWithIdentity()
    {
        this.insertSimpleWithIdentity();
        this.assertSimpleWithIdentity();
    }

    private List insertSimpleWithIdentity()
    {
        ArrayList list = new ArrayList();
        for (int i = 0; i < INSERT_COUNT; i++)
        {
            BcpSimpleWithIdentity bcpSimpleWithIdentity = new BcpSimpleWithIdentity(i, "Simple Name " + i);
            bcpSimpleWithIdentity.insert();
            list.add(bcpSimpleWithIdentity);
        }
        return list;
    }

    private void assertSimpleWithIdentity()
    {
        BcpSimpleWithIdentity four = BcpSimpleWithIdentityFinder.findOneBypassCache(BcpSimpleWithIdentityFinder.id().eq(4));
        Assert.assertEquals("Simple Name 4", four.getName());

        final BcpSimpleWithIdentityList list = new BcpSimpleWithIdentityList(BcpSimpleWithIdentityFinder.all());
        list.setBypassCache(true);
        Assert.assertEquals(INSERT_COUNT, list.size());
    }

    public void testBcpEndBits()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertEndBits();
            }
        });
        this.assertEndBits();
    }

    public void testEndBits()
    {
        this.insertEndBits();
        this.assertEndBits();
    }

    public void testTlewTrial()
    {
        final TlewTrial trial = new TlewTrial(InfinityTimestamp.getParaInfinity());
        trial.setAcctId(411323);
        trial.setInstrument(300079454);
        trial.setPositionType("SD");
        trial.setInstrumentType("S");
        trial.setInstrumentSubType("S");
        trial.setAccountNumber("76091212.01");
        trial.setTrial("026J");
        trial.setLegalEntity(9263331);
        trial.setBaseCurrency("USD");
        trial.setAccountTypeB(131072);
        trial.setAccountTypeC("TRA");
        trial.setPrimaryIncomeFunction("2680");
        trial.setCarryTransferFlag((short)1);
        trial.setAccountDescription("PURS");
        trial.setProdSyn("8780GCMKT");
        trial.setProductDescription("8780GCMKT");
        trial.setContractualNpv(14140731.385797);
        trial.setTotUnrealPl(14140731.385797);
        trial.setPmeTotUnrealPl(14106118.652);
        trial.setProdGlobalClass(1151);
        trial.setProdScrpPlPostClass("FIO");
        trial.setProdScrpBsPostClass("W02");
        trial.setTlewTrialCreate((short)0);
        trial.setBusDate(new Timestamp((System.currentTimeMillis()/60000)*60000));
        trial.setProdType("N");
        trial.setMtdTdUnreal(34612.73379699886);

        final TlewTrial trial2 = trial.getNonPersistentCopy();
        trial2.setAcctId(123456);
        trial2.setInstrument(300079455);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                tx.setBulkInsertThreshold(1);
                trial.insert();
                trial2.insert();
                return null;
            }
        });

        final TlewTrial trial3 = trial.getDetachedCopy();
        final TlewTrial trial4 = trial2.getDetachedCopy();


        TlewTrialFinder.findOneBypassCache(TlewTrialFinder.acctId().eq(411323));
        TlewTrialFinder.findOneBypassCache(TlewTrialFinder.acctId().eq(123456));

        assertEquals(trial.getBusDate().getTime(), trial3.getBusDate().getTime());
        trial3.setBusDate(trial.getBusDate());

        assertEquals(trial2.getBusDate().getTime(), trial4.getBusDate().getTime());
        trial4.setBusDate(trial2.getBusDate());

        assertFalse(trial3.isModifiedSinceDetachment());
        assertFalse(trial4.isModifiedSinceDetachment());
    }

    private Object insertEndBits()
    {
        new BcpEndBits(100, "Snow White", true, false).insert();
        BcpEndBits bcpEndBits = new BcpEndBits(101, "Evil Queen", false, false);
        bcpEndBits.insert();
        return bcpEndBits;
    }

    private void assertEndBits()
    {
        BcpEndBits happy = BcpEndBitsFinder.findOneBypassCache(BcpEndBitsFinder.name().eq("Happy"));
        Assert.assertFalse(happy.isDopey());
        Assert.assertTrue(happy.isHappy());

        BcpEndBits dopey = BcpEndBitsFinder.findOneBypassCache(BcpEndBitsFinder.name().eq("Dopey"));
        Assert.assertTrue(dopey.isDopey());
        Assert.assertFalse(dopey.isHappy());

        BcpEndBits snowWhite = BcpEndBitsFinder.findOneBypassCache(BcpEndBitsFinder.id().eq(100));
        Assert.assertTrue(snowWhite.isDopey());
        Assert.assertFalse(snowWhite.isHappy());

        final BcpEndBitsList list = new BcpEndBitsList(BcpEndBitsFinder.all());
        list.setBypassCache(true);
        Assert.assertEquals(7, list.size());

        BcpEndBits evilQueen = BcpEndBitsFinder.findOneBypassCache(BcpEndBitsFinder.id().eq(101));
        Assert.assertFalse(evilQueen.isDopey());
        Assert.assertFalse(evilQueen.isHappy());
    }

    // -----

    public void testBcpEndNullable()
    {
        this.runAsBcpTransaction(new Callable()
        {
            public Object call()
            {
                return SybaseBcpTest.this.insertEndNullable();
            }
        });
        this.assertEndNullable();
    }

    public void testEndNullable()
    {
        this.insertEndNullable();
        this.assertEndNullable();
    }

    private Object insertEndNullable()
    {
        new BcpEndNullable(100, "Snow White", "A Beautiful young lady").insert();
        BcpEndNullable bcpEndNullable = new BcpEndNullable(101, "Evil Queen", null);
        bcpEndNullable.insert();
        return bcpEndNullable;
    }

    private void assertEndNullable()
    {
        BcpEndNullable bashful = BcpEndNullableFinder.findOneBypassCache(BcpEndNullableFinder.name().eq("Bashful"));
        Assert.assertEquals("Very very shy", bashful.getDescription());

        BcpEndNullable sleepy = BcpEndNullableFinder.findOneBypassCache(BcpEndNullableFinder.name().eq("Sleepy"));
        Assert.assertNull(sleepy.getDescription());

        BcpEndNullable snowWhite = BcpEndNullableFinder.findOneBypassCache(BcpEndNullableFinder.id().eq(100));
        Assert.assertEquals("Snow White", snowWhite.getName());
        Assert.assertEquals("A Beautiful young lady", snowWhite.getDescription());

        final BcpEndNullableList list = new BcpEndNullableList(BcpEndNullableFinder.all());
        list.setBypassCache(true);
        Assert.assertEquals(7, list.size());

        BcpEndNullable evilQueen = BcpEndNullableFinder.findOneBypassCache(BcpEndNullableFinder.id().eq(101));
        Assert.assertEquals("Evil Queen", evilQueen.getName());
        Assert.assertNull(evilQueen.getDescription());
    }

    // -----

    private void assertTimestampEquals(Timestamp expectedTimestamp, Timestamp actualTimestamp)
    {
        boolean timestampsEqual = actualTimestamp.getTime() - expectedTimestamp.getTime() < 10;
        if (!timestampsEqual)
        {
            Assert.fail("Timestamps not equal; expected:<" + expectedTimestamp + ">; actual:<" + actualTimestamp + ">");
        }
    }
}
