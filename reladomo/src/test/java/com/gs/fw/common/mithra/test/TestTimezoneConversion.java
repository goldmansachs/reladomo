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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.glew.BitemporalProductSynonym;
import com.gs.fw.common.mithra.test.glew.BitemporalProductSynonymFinder;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;
import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;



public abstract class TestTimezoneConversion extends MithraTestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final int ONE_HOUR = 60*60*1000;

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TimezoneTest.class,
            BitemporalProductSynonym.class,
            PkTimezoneTest.class
        };
    }

    public void checkBitemporalInfinity(TimeZone tz) throws Exception
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        final Timestamp timestamp = new Timestamp(timestampFormat.parse("2013-01-01 00:00:00.000").getTime());
        Operation bizOp = BitemporalProductSynonymFinder.businessDate().eq(timestamp);
        BitemporalProductSynonym one = BitemporalProductSynonymFinder.findOne(BitemporalProductSynonymFinder.instrumentId().eq(1).and(bizOp));
        assertNotNull(one);
        assertEquals(DefaultInfinityTimestamp.getDefaultInfinity().getTime(), one.getProcessingDateTo().getTime());
        one = BitemporalProductSynonymFinder.findOne(BitemporalProductSynonymFinder.instrumentId().eq(1).and(BitemporalProductSynonymFinder.processingDate().eq(timestamp).and(bizOp)));
        assertNotNull(one);
        assertEquals(DefaultInfinityTimestamp.getDefaultInfinity().getTime(), one.getProcessingDateTo().getTime());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalProductSynonym newSyn = new BitemporalProductSynonym(timestamp, DefaultInfinityTimestamp.getDefaultInfinity());
                newSyn.setInstrumentId(2);
                newSyn.setSynonymType("CUS");
                newSyn.setSynonymValue("DEF");
                newSyn.insert();
                return null;
            }
        });

        one = BitemporalProductSynonymFinder.findOne(BitemporalProductSynonymFinder.instrumentId().eq(2).and(bizOp));
        assertNotNull(one);
        assertEquals(DefaultInfinityTimestamp.getDefaultInfinity().getTime(), one.getProcessingDateTo().getTime());
        one = BitemporalProductSynonymFinder.findOne(BitemporalProductSynonymFinder.instrumentId().eq(2).and(BitemporalProductSynonymFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() + 1000)).and(bizOp)));
        assertNotNull(one);
        assertEquals(DefaultInfinityTimestamp.getDefaultInfinity().getTime(), one.getProcessingDateTo().getTime());

    }

    public void checkFixInfinityPerformance(TimeZone tz)
    {
        Timestamp localInfinity = new Timestamp(DefaultInfinityTimestamp.getDefaultInfinity().getTime());
        MithraTimestamp.convertTimeToLocalTimeZone(tz, localInfinity);

        MithraTimestamp defaultInfinity = DefaultInfinityTimestamp.getDefaultInfinity();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 300000000; i++)
        {
            Timestamp fixedInfinity = MithraTimestamp.zFixInfinity(localInfinity, tz, defaultInfinity);
            assertSame(fixedInfinity, defaultInfinity);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // This test always fails : this is intended as a reminder to disable the test before committing
        fail("Performance test: took " + duration + " msecs");
    }

    public void checkRead(TimeZone tz)
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        TimezoneTest timezoneTest = TimezoneTestFinder.findOne(TimezoneTestFinder.timezoneTestId().eq(1));
        assertNotNull(timezoneTest);

        Timestamp insensitiveTimestamp = timezoneTest.getInsensitiveTimestamp();
        Timestamp databaseTimestamp = timezoneTest.getDatabaseTimestamp();
        Timestamp utcTimestamp = timezoneTest.getUtcTimestamp();

        assertTrue(databaseTimestamp.getTime() != utcTimestamp.getTime());

        TimezoneTest.assertDatesAndTimestamps(timezoneTest.getInsensitiveDate(), timezoneTest.getUtcDate(), timezoneTest.getDatabaseDate(), insensitiveTimestamp, utcTimestamp, databaseTimestamp, tz);

    }

    public void checkReadAggregate(TimeZone tz)
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        AggregateList list = new AggregateList(TimezoneTestFinder.timezoneTestId().eq(1));
        list.addAggregateAttribute("insensitiveDate", TimezoneTestFinder.insensitiveDate().max());
        list.addAggregateAttribute("databaseDate", TimezoneTestFinder.databaseDate().max());
        list.addAggregateAttribute("utcDate", TimezoneTestFinder.utcDate().max());
        list.addAggregateAttribute("insensitiveTimestamp", TimezoneTestFinder.insensitiveTimestamp().max());
        list.addAggregateAttribute("databaseTimestamp", TimezoneTestFinder.databaseTimestamp().max());
        list.addAggregateAttribute("utcTimestamp", TimezoneTestFinder.utcTimestamp().max());

        assertEquals(1, list.size());

        AggregateData data = list.get(0);

        Date insensitiveDate = data.getAttributeAsDate("insensitiveDate");
        Date databaseDate = data.getAttributeAsDate("databaseDate");
        Date utcDate = data.getAttributeAsDate("utcDate");
        Timestamp insensitiveTimestamp = data.getAttributeAsTimestamp("insensitiveTimestamp");
        Timestamp databaseTimestamp = data.getAttributeAsTimestamp("databaseTimestamp");
        Timestamp utcTimestamp = data.getAttributeAsTimestamp("utcTimestamp");

        assertTrue(databaseTimestamp.getTime() != utcTimestamp.getTime());

        TimezoneTest.assertDatesAndTimestamps(insensitiveDate, utcDate, databaseDate, insensitiveTimestamp, utcTimestamp, databaseTimestamp, tz);
    }

    // todo: figure out why the tests failed on 2006-10-28 14:23 NYK time
    public void xtestDaylightSavingsSwitch() throws ParseException
    {
        Calendar c = Calendar.getInstance();
        c.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        c.set(Calendar.YEAR, 2006);
        c.set(Calendar.MONTH, 9);
        c.set(Calendar.DAY_OF_MONTH, 28);
        c.set(Calendar.HOUR_OF_DAY, 14);
        c.set(Calendar.MINUTE, 23);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        long time = c.getTimeInMillis();

        Timestamp toTokyo = new Timestamp(time);
        MithraTimestamp.convertTimeToLocalTimeZone(TimeZone.getTimeZone("Asia/Tokyo"), toTokyo);
    }

    public void checkQuery(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        TimezoneTest timezoneTest = TimezoneTestFinder.findOne(
                TimezoneTestFinder.insensitiveTimestamp().eq(new Timestamp(timestampFormat.parse("2005-10-01 10:12:30.150").getTime())));
        assertNotNull(timezoneTest);
        assertEquals(1, timezoneTest.getTimezoneTestId());

        timezoneTest = TimezoneTestFinder.findOne(
                TimezoneTestFinder.databaseTimestamp().eq(timezoneTest.getDatabaseTimestamp()));
        assertNotNull(timezoneTest);
        assertEquals(1, timezoneTest.getTimezoneTestId());

        timezoneTest = TimezoneTestFinder.findOne(
                TimezoneTestFinder.utcTimestamp().eq(timezoneTest.getUtcTimestamp()));
        assertNotNull(timezoneTest);
        assertEquals(1, timezoneTest.getTimezoneTestId());

        timezoneTest = TimezoneTestFinder.findOne(
                TimezoneTestFinder.utcTimestamp().eq(timezoneTest.getUtcTimestamp()).and(
                        TimezoneTestFinder.databaseTimestamp().eq(timezoneTest.getDatabaseTimestamp())).and(
                        TimezoneTestFinder.insensitiveTimestamp().eq(new Timestamp(timestampFormat.parse("2005-10-01 10:12:30.150").getTime())
                        )));
        assertNotNull(timezoneTest);
        assertEquals(1, timezoneTest.getTimezoneTestId());
    }

    public void checkUpdate(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        TimezoneTest timezoneTest = TimezoneTestFinder.findOne(TimezoneTestFinder.timezoneTestId().eq(1));
        timezoneTest.setDatabaseTimestamp(nextHour(timezoneTest.getDatabaseTimestamp()));
        timezoneTest.setUtcTimestamp(nextHour(timezoneTest.getUtcTimestamp()));

        checkQuery(tz);

        timezoneTest.setInsensitiveTimestamp(nextHour(timezoneTest.getInsensitiveTimestamp()));

        timezoneTest = TimezoneTestFinder.findOne(
                TimezoneTestFinder.insensitiveTimestamp().eq(timezoneTest.getInsensitiveTimestamp()));
        assertNotNull(timezoneTest);
        assertEquals(1, timezoneTest.getTimezoneTestId());
    }

    private Timestamp nextHour(Timestamp timestamp)
    {
        return new Timestamp(timestamp.getTime()+ONE_HOUR);
    }

    public void checkInsert(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        TimezoneTest timezoneTest = new TimezoneTest();
        timezoneTest.setTimezoneTestId(1000);
        timezoneTest.setInsensitiveTimestamp(new Timestamp(timestampFormat.parse("2005-10-01 10:12:30.150").getTime()));
        Timestamp databaseTimestamp = new Timestamp(System.currentTimeMillis());
        Timestamp utcTimestamp = new Timestamp(databaseTimestamp.getTime()+1000);
        timezoneTest.setDatabaseTimestamp(databaseTimestamp);
        timezoneTest.setUtcTimestamp(utcTimestamp);
        timezoneTest.insert();

        timezoneTest = TimezoneTestFinder.findOne(
                TimezoneTestFinder.utcTimestamp().eq(timezoneTest.getUtcTimestamp()).and(
                        TimezoneTestFinder.databaseTimestamp().eq(timezoneTest.getDatabaseTimestamp())).and(
                        TimezoneTestFinder.insensitiveTimestamp().eq(new Timestamp(timestampFormat.parse("2005-10-01 10:12:30.150").getTime())
                        )));
        assertNotNull(timezoneTest);
        assertEquals(databaseTimestamp.getTime(), timezoneTest.getDatabaseTimestamp().getTime());
        assertEquals(utcTimestamp.getTime(), timezoneTest.getUtcTimestamp().getTime());
        assertEquals(1000, timezoneTest.getTimezoneTestId());
    }

    public void checkUpdateWithPk(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        PkTimezoneTest pkTimezoneTest = PkTimezoneTestFinder.findOne(PkTimezoneTestFinder.value().eq(1));
        assertNotNull(pkTimezoneTest);
        pkTimezoneTest.setValue(1000);

        pkTimezoneTest = PkTimezoneTestFinder.findOneBypassCache(PkTimezoneTestFinder.value().eq(1000));
        assertNotNull(pkTimezoneTest);
    }

    public void checkDelete(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        PkTimezoneTest pkTimezoneTest = PkTimezoneTestFinder.findOne(PkTimezoneTestFinder.value().eq(1));
        assertNotNull(pkTimezoneTest);
        pkTimezoneTest.delete();

        pkTimezoneTest = PkTimezoneTestFinder.findOneBypassCache(PkTimezoneTestFinder.value().eq(1));
        assertNull(pkTimezoneTest);
    }

    public void checkRefresh(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        PkTimezoneTest pkTimezoneTest = PkTimezoneTestFinder.findOne(PkTimezoneTestFinder.value().eq(1));
        assertNotNull(pkTimezoneTest);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        pkTimezoneTest.setValue(1000);
        tx.commit();
        pkTimezoneTest = PkTimezoneTestFinder.findOneBypassCache(PkTimezoneTestFinder.value().eq(1000));
        assertNotNull(pkTimezoneTest);
    }

    public void checkBatchInsert(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        long startTime = System.currentTimeMillis();
        for(int i=0;i<10;i++)
        {
            TimezoneTest timezoneTest = new TimezoneTest();
            timezoneTest.setTimezoneTestId(1000+i);
            timezoneTest.setInsensitiveTimestamp(new Timestamp(timestampFormat.parse("2005-10-01 10:12:30.150").getTime()));
            Timestamp databaseTimestamp = new Timestamp(startTime + i*100);
            Timestamp utcTimestamp = new Timestamp(databaseTimestamp.getTime()+1000);
            timezoneTest.setDatabaseTimestamp(databaseTimestamp);
            timezoneTest.setUtcTimestamp(utcTimestamp);
            timezoneTest.insert();
        }
        tx.commit();

        for(int i=0;i<10;i++)
        {
            Timestamp databaseTimestamp = new Timestamp(startTime + i*100);
            Timestamp utcTimestamp = new Timestamp(databaseTimestamp.getTime()+1000);
            TimezoneTest timezoneTest = TimezoneTestFinder.findOne(
                    TimezoneTestFinder.utcTimestamp().eq(utcTimestamp).and(
                            TimezoneTestFinder.databaseTimestamp().eq(databaseTimestamp)).and(
                            TimezoneTestFinder.insensitiveTimestamp().eq(new Timestamp(timestampFormat.parse("2005-10-01 10:12:30.150").getTime())
                            )));
            assertNotNull(timezoneTest);
            assertEquals(databaseTimestamp.getTime(), timezoneTest.getDatabaseTimestamp().getTime());
            assertEquals(utcTimestamp.getTime(), timezoneTest.getUtcTimestamp().getTime());
            assertEquals(1000+i, timezoneTest.getTimezoneTestId());
        }
    }

    public void checkBatchUpdateWithPk(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PkTimezoneTestList list = new PkTimezoneTestList(PkTimezoneTestFinder.value().eq(123));
        int originalSize = list.size();
        assertTrue(originalSize > 1);
        for(int i=0;i<list.size();i++)
        {
            list.getPkTimezoneTestAt(i).setValue(1000);
        }
        tx.commit();
        PkTimezoneTestList newList = new PkTimezoneTestList(PkTimezoneTestFinder.value().eq(1000));
        assertEquals(originalSize, newList.size());
    }

    public void checkBatchDelete(TimeZone tz) throws ParseException
    {
        ConnectionManagerForTests.getInstance().setDatabaseTimeZone(tz);
        PkTimezoneTestList list = new PkTimezoneTestList(PkTimezoneTestFinder.value().eq(123));
        assertTrue(list.size() > 1);
        list.deleteAll();

        list = new PkTimezoneTestList(PkTimezoneTestFinder.value().eq(123));
        assertEquals(0, list.size());
    }

}
