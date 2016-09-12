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

import com.gs.collections.impl.utility.StringIterate;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.TimestampConversion;
import com.gs.fw.common.mithra.test.domain.TimestampConversionFinder;
import com.gs.fw.common.mithra.test.domain.TimestampConversionList;
import com.gs.fw.common.mithra.test.domain.alarm.*;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;

public class TestSybaseTimeTests extends MithraTestAbstract
{
    public void testTransactionalBasicTime()
    {
        AlarmList alarmList = new AlarmList(AlarmFinder.all());
        alarmList.addOrderBy(AlarmFinder.id().ascendingOrderBy());

        Alarm alarm = new Alarm();
        Time time = Time.withMillis(2, 43, 55, 33);
        alarm.setId(1);
        alarm.setDescription("test alarm");
        alarm.setTime(time);

        assertEquals(Time.withMillis(2, 43, 55, 33), alarm.getTime());
        assertEquals(1, alarm.getId());
        assertEquals("test alarm", alarm.getDescription());

        assertEquals(3, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 10), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testTimeTransactionalUpdate()
    {
        AlarmSybaseList alarms = new AlarmSybaseList(AlarmSybaseFinder.all());
        alarms.addOrderBy(AlarmSybaseFinder.id().ascendingOrderBy());

        assertEquals("alarm 1", alarms.get(0).getDescription());
        assertEquals("alarm 2", alarms.get(1).getDescription());
        assertEquals("alarm 3", alarms.get(2).getDescription());

        alarms.setDescription("update alarms");

        assertEquals("update alarms", alarms.get(0).getDescription());
        assertEquals("update alarms", alarms.get(1).getDescription());
        assertEquals("update alarms", alarms.get(2).getDescription());
    }

    public void testTimeTransactionalDelete()
    {
        AlarmSybaseList alarmList = new AlarmSybaseList(AlarmSybaseFinder.all());
        AlarmSybase alarm =  AlarmSybaseFinder.findOne(AlarmSybaseFinder.time().eq(Time.withMillis(10, 30, 59, 10)));
        assertNotNull(alarm);
        alarm.delete();

        assertEquals(26, new AlarmSybaseList(AlarmSybaseFinder.all()).size());
        AlarmSybase alarmAfterDelete = AlarmSybaseFinder.findOne(AlarmSybaseFinder.time().eq(Time.withMillis(10, 30, 59, 10)));
        assertNull(alarmAfterDelete);
    }

    public void testBitemporalUpdateUntil()
    {
        Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));
        final AlarmBitemporalTransactionalList alarms = new AlarmBitemporalTransactionalList(businessDate);
        int size = alarms.size();
        alarms.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                alarms.get(0).setTimeUntil(Time.withMillis(1, 2, 3, 3), Timestamp.valueOf("2013-01-01 23:59:00.0"));
                return null;
            }
        });

        assertEquals(Time.withMillis(1, 2, 3, 3), AlarmBitemporalTransactionalFinder.findOne(AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-12-12 23:59:00.0")).and(AlarmBitemporalTransactionalFinder.id().eq(1))).getTime());

        Operation op = AlarmBitemporalTransactionalFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(AlarmBitemporalTransactionalFinder.id().eq(1));

        assertEquals(Time.withMillis(10, 30, 59, 10), AlarmBitemporalTransactionalFinder.findOne(op).getTime());
    }

    public void testTimeTransactionalToStringWithSybaseAseRounding()
    {
        // In ASE time values are stored with 1/300s precision. So e.g. 11ms is rounded to 10ms.

        AlarmSybaseList alarmList = new AlarmSybaseList(AlarmSybaseFinder.all());
        alarmList.addOrderBy(AlarmSybaseFinder.id().ascendingOrderBy());

        assertEquals("10:30:59.010", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());

        assertEquals("14:59:10.000", alarmList.get(3).getTime().toString());
        assertEquals("14:59:10.000", alarmList.get(4).getTime().toString());

        assertEquals("14:59:10.003", alarmList.get(5).getTime().toString());
        assertEquals("14:59:10.003", alarmList.get(6).getTime().toString());
        assertEquals("14:59:10.003", alarmList.get(7).getTime().toString());

        assertEquals("14:59:10.006", alarmList.get(8).getTime().toString());
        assertEquals("14:59:10.006", alarmList.get(9).getTime().toString());
        assertEquals("14:59:10.006", alarmList.get(10).getTime().toString());
        assertEquals("14:59:10.006", alarmList.get(11).getTime().toString());

        assertEquals("14:59:10.010", alarmList.get(12).getTime().toString());
        assertEquals("14:59:10.010", alarmList.get(13).getTime().toString());
        assertEquals("14:59:10.010", alarmList.get(14).getTime().toString());

        assertEquals("14:59:10.013", alarmList.get(15).getTime().toString());
        assertEquals("14:59:10.013", alarmList.get(16).getTime().toString());
        assertEquals("14:59:10.013", alarmList.get(17).getTime().toString());

        assertEquals("14:59:10.016", alarmList.get(18).getTime().toString());
        assertEquals("14:59:10.016", alarmList.get(19).getTime().toString());
        assertEquals("14:59:10.016", alarmList.get(20).getTime().toString());
        assertEquals("14:59:10.016", alarmList.get(21).getTime().toString());

        assertEquals("14:59:10.020", alarmList.get(22).getTime().toString());
        assertEquals("14:59:10.020", alarmList.get(23).getTime().toString());
        assertEquals("14:59:10.020", alarmList.get(24).getTime().toString());

        assertEquals("14:59:10.200", alarmList.get(25).getTime().toString());
        assertEquals("14:59:10.996", alarmList.get(26).getTime().toString());
    }

    public void testTimeTransactionalToStringWithoutRounding()
    {
        // In IQ (using jConnect 7.0 and later) millisecond values are stored with full precision with no rounding
        // Though AlarmSybase.xml specifies modifyTimePrecisionOnSet="sybase", this does not apply to test data files
        // (nor would this attribute prevent unrounded values from being loaded into the DB by other means)
        // This test is therefore asserting that whatever value is stored in the DB is returned without rounding.

        AlarmSybaseList alarmList = new AlarmSybaseList(AlarmSybaseFinder.all());
        alarmList.addOrderBy(AlarmSybaseFinder.id().ascendingOrderBy());

        assertEquals("10:30:59.010", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());

        assertEquals("14:59:10.000", alarmList.get(3).getTime().toString());
        assertEquals("14:59:10.001", alarmList.get(4).getTime().toString());

        assertEquals("14:59:10.002", alarmList.get(5).getTime().toString());
        assertEquals("14:59:10.003", alarmList.get(6).getTime().toString());
        assertEquals("14:59:10.004", alarmList.get(7).getTime().toString());

        assertEquals("14:59:10.005", alarmList.get(8).getTime().toString());
        assertEquals("14:59:10.006", alarmList.get(9).getTime().toString());
        assertEquals("14:59:10.007", alarmList.get(10).getTime().toString());
        assertEquals("14:59:10.008", alarmList.get(11).getTime().toString());

        assertEquals("14:59:10.009", alarmList.get(12).getTime().toString());
        assertEquals("14:59:10.010", alarmList.get(13).getTime().toString());
        assertEquals("14:59:10.011", alarmList.get(14).getTime().toString());

        assertEquals("14:59:10.012", alarmList.get(15).getTime().toString());
        assertEquals("14:59:10.013", alarmList.get(16).getTime().toString());
        assertEquals("14:59:10.014", alarmList.get(17).getTime().toString());

        assertEquals("14:59:10.015", alarmList.get(18).getTime().toString());
        assertEquals("14:59:10.016", alarmList.get(19).getTime().toString());
        assertEquals("14:59:10.017", alarmList.get(20).getTime().toString());
        assertEquals("14:59:10.018", alarmList.get(21).getTime().toString());

        assertEquals("14:59:10.019", alarmList.get(22).getTime().toString());
        assertEquals("14:59:10.020", alarmList.get(23).getTime().toString());
        assertEquals("14:59:10.021", alarmList.get(24).getTime().toString());

        assertEquals("14:59:10.199", alarmList.get(25).getTime().toString());
        assertEquals("14:59:10.999", alarmList.get(26).getTime().toString());
    }

    public void testModifyTimePrecisionOnSet()
    {
        // AlarmSybase.xml specifies modifyTimePrecisionOnSet="sybase" on the "time" attribute

        AlarmSybaseList toInsert = new AlarmSybaseList();
        for (int millis = 0; millis <= 999; millis++)
        {
            AlarmSybase alarm = new AlarmSybase();
            Time time = Time.withMillis(7, 30, 5, millis);
            alarm.setId(100 + millis);
            alarm.setTime(time);
            alarm.setDescription("dummy");
            toInsert.add(alarm);
        }
        toInsert.insertAll();

        AlarmSybaseList fromRead = AlarmSybaseFinder.findManyBypassCache(AlarmSybaseFinder.id().greaterThanEquals(100));
        fromRead.setOrderBy(AlarmSybaseFinder.id().ascendingOrderBy());
        assertEquals(1000, fromRead.size());

        for (int millis = 0; millis <= 999; millis++)
        {
            AlarmSybase alarm = fromRead.getAlarmSybaseAt(millis);
            int roundedMillis = calculateExpectedMillisecondsWithAseRounding(millis);
            Time expectedTime = Time.withMillis(7, 30, 5, roundedMillis);
            assertEquals("Expected time for millis = " + millis, expectedTime, alarm.getTime());
        }
    }

    private int calculateExpectedMillisecondsWithAseRounding(int millis)
    {
        return millis / 10 * 10 + (millis % 10 < 2 ? 0 : (millis % 10 < 5 ? 3 : (millis % 10 < 9 ? 6 : (millis < 999 ? 10 : 6))));
    }

    public void testTimeGranularityWithSybaseAseRounding()
    {
        // In ASE time values are stored with 1/300s precision. So e.g. 11ms is rounded to 10ms.

        // This test should ensure that we will be alerted if a future jConnect driver version changes the rounding behaviour
        // e.g. jConnect 6.0.5 rounds .999 to .996 whereas jConnect 7.0 and above rounds it to 1.000.

        AlarmList toInsert = new AlarmList();
        for (int millis = 0; millis <= 999; millis++)
        {
            Alarm alarm = new Alarm();
            Time time = Time.withMillis(7, 30, 5, millis);
            alarm.setId(100 + millis);
            alarm.setTime(time);
            alarm.setDescription("dummy");
            toInsert.add(alarm);
        }
        toInsert.insertAll();

        // Note that Alarm.xml does NOT specify modifyTimePrecisionOnSet="sybase" - this is why we chose Alarm for this test.
        // The ASE database itself will perform the rounding.

        // There is some separate transformation logic in SybaseDatabaseType.setTime() to bridge compatibility between jConnect versions.

        AlarmList fromRead = AlarmFinder.findManyBypassCache(AlarmFinder.id().greaterThanEquals(100));
        fromRead.setOrderBy(AlarmFinder.id().ascendingOrderBy());
        assertEquals(1000, fromRead.size());

        for (int millis = 0; millis <= 999; millis++)
        {
            Alarm alarm = fromRead.getAlarmAt(millis);
            int roundedMillis = calculateExpectedMillisecondsWithAseRounding(millis);
            Time expectedTime = Time.withMillis(7, 30, 5, roundedMillis);
            assertEquals("Expected time for millis = " + millis, expectedTime, alarm.getTime());
        }
    }

    public void testTimeGranularityWithoutRounding()
    {
        // In IQ (using jConnect 7.0 and later) millisecond values are stored with full precision with no rounding

        AlarmList toInsert = new AlarmList();
        for (int millis = 0; millis <= 999; millis++)
        {
            Alarm alarm = new Alarm();
            Time time = Time.withMillis(7, 30, 5, millis);
            alarm.setId(100 + millis);
            alarm.setTime(time);
            alarm.setDescription("dummy");
            toInsert.add(alarm);
        }
        toInsert.insertAll();

        // Note that Alarm.xml does NOT specify modifyTimePrecisionOnSet="sybase" - this is why we chose Alarm for this test.

        AlarmList fromRead = AlarmFinder.findManyBypassCache(AlarmFinder.id().greaterThanEquals(100));
        fromRead.setOrderBy(AlarmFinder.id().ascendingOrderBy());
        assertEquals(1000, fromRead.size());

        for (int millis = 0; millis <= 999; millis++)
        {
            Alarm alarm = fromRead.getAlarmAt(millis);
            Time expectedTime = Time.withMillis(7, 30, 5, millis); // assert an exact match with no rounding
            assertEquals("Expected time for millis = " + millis, expectedTime, alarm.getTime());
        }
    }

    public void testTimestampGranularityWithSybaseAseRounding()
    {
        // In ASE timestamp values are stored with 1/300s precision. So e.g. 11ms is rounded to 10ms.

        // This test should ensure that we will be alerted if a future jConnect driver version changes the rounding behaviour
        // e.g. jConnect 6.0.5 rounds .999 to .996 whereas jConnect 7.0 and above rounds it to 1.000.

        TimestampConversionList toInsert = new TimestampConversionList();
        for (int millis = 0; millis <= 999; millis++)
        {
            TimestampConversion obj = new TimestampConversion();
            Timestamp timestamp = Timestamp.valueOf("2015-05-01 07:30:05." + (millis / 100) + "" + ((millis % 100) / 10) + "" + (millis % 10));
            obj.setId(100 + millis);
            obj.setTimestampValueNone(timestamp);
            obj.setTimestampValueDB(new Timestamp(0)); // dummy value for non-nullable field
            obj.setTimestampValueUTC(new Timestamp(0)); // dummy value for non-nullable field
            toInsert.add(obj);
        }
        toInsert.insertAll();

        // Note that TimestampConversion.xml does NOT specify modifyTimePrecisionOnSet="sybase" - this is why we chose TimestampConversion for this test.
        // The ASE database itself will perform the rounding.

        // There is some separate transformation logic in SybaseDatabaseType.setTime() to bridge compatibility between jConnect versions.

        TimestampConversionList fromRead = TimestampConversionFinder.findManyBypassCache(TimestampConversionFinder.id().greaterThanEquals(100));
        fromRead.setOrderBy(TimestampConversionFinder.id().ascendingOrderBy());
        assertEquals(1000, fromRead.size());

        for (int millis = 0; millis <= 999; millis++)
        {
            TimestampConversion obj = fromRead.getTimestampConversionAt(millis);
            int roundedMillis = calculateExpectedMillisecondsWithAseRounding(millis);
            String roundedMillisAsString = "" + (roundedMillis / 100) + "" + ((roundedMillis % 100) / 10) + "" + (roundedMillis % 10); // pad with zeroes e.g .001
            String roundedNanosOfMillisAsString = StringIterate.repeat("" + (roundedMillis % 10), 6); // Sybase rounds to 1/300s precision, which leads to nanosecond values like 013333333
            Timestamp expectedTime = Timestamp.valueOf("2015-05-01 07:30:05." + roundedMillisAsString + roundedNanosOfMillisAsString);

            assertEquals("Expected time for millis = " + millis, expectedTime, obj.getTimestampValueNone());
        }
    }

    public void testTimestampGranularityWithoutRounding()
    {
        // In IQ (using jConnect 7.0 and later) millisecond values are stored with full precision with no rounding

        TimestampConversionList toInsert = new TimestampConversionList();
        for (int millis = 0; millis <= 999; millis++)
        {
            TimestampConversion obj = new TimestampConversion();
            Timestamp timestamp = Timestamp.valueOf("2015-05-01 07:30:05." + (millis / 100) + "" + ((millis % 100) / 10) + "" + (millis % 10) + "123999");
            obj.setId(100 + millis);
            obj.setTimestampValueNone(timestamp);
            obj.setTimestampValueDB(new Timestamp(0)); // dummy value for non-nullable field
            obj.setTimestampValueUTC(new Timestamp(0)); // dummy value for non-nullable field
            toInsert.add(obj);
        }
        toInsert.insertAll();

        // Note that TimestampConversion.xml does NOT specify modifyTimePrecisionOnSet="sybase" - this is why we chose TimestampConversion for this test.

        TimestampConversionList fromRead = TimestampConversionFinder.findManyBypassCache(TimestampConversionFinder.id().greaterThanEquals(100));
        fromRead.setOrderBy(TimestampConversionFinder.id().ascendingOrderBy());
        assertEquals(1000, fromRead.size());

        for (int millis = 0; millis <= 999; millis++)
        {
            TimestampConversion obj = fromRead.getTimestampConversionAt(millis);

            // We expect IQ to truncate the time at microsecond level (nanos are ignored)
            Timestamp expectedTime = Timestamp.valueOf("2015-05-01 07:30:05." + (millis / 100) + "" + ((millis % 100) / 10) + "" + (millis % 10) + "123000");
            assertEquals("Expected time for millis = " + millis, expectedTime, obj.getTimestampValueNone());
        }
    }

    public void testTimeNonTransactionalBasicTime()
    {
        AlarmNonTransactional alarm = new AlarmNonTransactional();
        Time time = Time.withMillis(2, 43, 55, 33);
        alarm.setId(1);
        alarm.setDescription("test alarm");
        alarm.setTime(time);

        assertEquals(Time.withMillis(2, 43, 55, 33), alarm.getTime());
        assertEquals(1, alarm.getId());
        assertEquals("test alarm", alarm.getDescription());

        AlarmNonTransactionalList alarmList = new AlarmNonTransactionalList(AlarmNonTransactionalFinder.all());
        alarmList.addOrderBy(AlarmNonTransactionalFinder.id().ascendingOrderBy());

        assertEquals(4, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());
        assertEquals(4, alarmList.get(3).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());
        assertEquals("null alarm", alarmList.get(3).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 10), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testTimeNonTransactionalToString()
    {
        AlarmNonTransactionalList alarmList = new AlarmNonTransactionalList(AlarmNonTransactionalFinder.all());
        assertEquals("10:30:59.010", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());
    }

    public void testTimeDatedTransactionalBasicTime()
    {
        AlarmDatedTransactional alarm = new AlarmDatedTransactional();
        Time time = Time.withMillis(2, 43, 55, 33);
        alarm.setId(1);
        alarm.setDescription("test alarm");
        alarm.setTime(time);

        assertEquals(Time.withMillis(2, 43, 55, 33), alarm.getTime());
        assertEquals(1, alarm.getId());
        assertEquals("test alarm", alarm.getDescription());

        AlarmDatedTransactionalList alarmList = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        alarmList.addOrderBy(AlarmDatedTransactionalFinder.id().ascendingOrderBy());

        assertEquals(3, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 10), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(03, 11, 23, 00), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testTimeDatedTransactionalToString()
    {
        AlarmDatedTransactionalList alarmList = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        assertEquals("10:30:59.010", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());
    }

    public void testTimeDatedNonTransactionalBasicTime()
    {
        AlarmDatedNonTransactional alarm = new AlarmDatedNonTransactional();
        Time time = Time.withMillis(2, 43, 55, 33);
        alarm.setId(1);
        alarm.setDescription("test alarm");
        alarm.setTime(time);

        assertEquals(Time.withMillis(2, 43, 55, 33), alarm.getTime());
        assertEquals(1, alarm.getId());
        assertEquals("test alarm", alarm.getDescription());

        AlarmDatedNonTransactionalList alarmList = new AlarmDatedNonTransactionalList(AlarmDatedNonTransactionalFinder.all());
        alarmList.addOrderBy(AlarmDatedNonTransactionalFinder.id().ascendingOrderBy());

        assertEquals(4, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 10), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(03, 11, 23, 00), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testTimeDatedNonTransactionalToString()
    {
        AlarmDatedNonTransactionalList alarmList = new AlarmDatedNonTransactionalList(AlarmDatedNonTransactionalFinder.all());
        assertEquals("10:30:59.010", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());
    }

    public void testBitemporalInsertUntil()
    {
        Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));
        final AlarmBitemporalTransactionalList alarms = new AlarmBitemporalTransactionalList(businessDate);
        alarms.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AlarmBitemporalTransactional insertAlarm = new AlarmBitemporalTransactional(Timestamp.valueOf("2012-01-01 23:59:00.0"));
                insertAlarm.setTime(Time.withMillis(1, 1, 1, 1));
                insertAlarm.setId(200);
                insertAlarm.insertUntil(Timestamp.valueOf("2013-01-01 23:59:00.0"));
                return null;
            }
        });

        assertNotNull(AlarmBitemporalTransactionalFinder.findOne(AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-12-12 23:59:00.0")).and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 1, 1, 1)))));

        Operation op = AlarmBitemporalTransactionalFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 1, 1, 1)));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(op));
    }

    public void testTimeDatedTransactionalUpdate()
    {
        final AlarmDatedTransactionalList alarms = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        alarms.addOrderBy(AlarmDatedTransactionalFinder.id().ascendingOrderBy());
        assertEquals("alarm 1", alarms.get(0).getDescription());
        assertEquals("alarm 2", alarms.get(1).getDescription());
        assertEquals("alarm 3", alarms.get(2).getDescription());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                alarms.setTime(Time.withMillis(1, 2, 3, 3));
                return null;
            }
        });

        assertEquals(3, alarms.size());
        assertEquals("01:02:03.003", alarms.get(0).getTime().toString());
        assertEquals("01:02:03.003", alarms.get(1).getTime().toString());
        assertEquals("01:02:03.003", alarms.get(2).getTime().toString());

        Operation eq = AlarmDatedTransactionalFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000)).and(
                AlarmDatedTransactionalFinder.description().eq("alarm 1")
        );
        AlarmDatedTransactional alarm = AlarmDatedTransactionalFinder.findOne(eq);
        assertEquals(Time.withMillis(10, 30, 59, 10), alarm.getTime());
    }

    public void testTimeTransactionalInsert()
    {
        AlarmList originalList = new AlarmList(AlarmFinder.all());
        originalList.addOrderBy(AlarmFinder.id().ascendingOrderBy());

        assertEquals(3, originalList.size());

        Alarm alarm = AlarmFinder.findOne(AlarmFinder.description().eq("alarm 4"));
        assertNull(alarm);

        Alarm newAlarm = new Alarm();
        newAlarm.setTime(Time.withMillis(23, 55, 49, 0));
        newAlarm.setId(1000);
        newAlarm.setDescription("alarm 1000");

        newAlarm.insert();

        AlarmList alarmList = new AlarmList(AlarmFinder.all());
        alarmList.addOrderBy(AlarmFinder.id().ascendingOrderBy());

        assertEquals(originalList.size() + 1, alarmList.size());
        assertEquals(newAlarm, alarmList.get(3));

        Alarm alarm5 = new Alarm();
        alarm5.setTime(Time.withMillis(5, 22, 21, 10));
        alarm5.setId(1001);
        alarm5.setDescription("alarm 1001");

        Alarm alarm6 = new Alarm();
        alarm6.setTime(Time.withMillis(12, 34, 43, 33));
        alarm6.setId(1002);
        alarm6.setDescription("alarm 1002");

        Alarm alarm7 = new Alarm();
        alarm7.setTime(Time.withMillis(6, 11, 37, 43));
        alarm7.setId(1003);
        alarm7.setDescription("alarm 1003");

        Alarm alarm8 = new Alarm();
        alarm8.setTime(Time.withMillis(1, 55, 49, 0));
        alarm8.setId(1004);
        alarm8.setDescription("alarm 1004");

        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(5, 22, 21, 10))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(12, 34, 43, 30))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(6, 11, 37, 43))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(1, 55, 49, 0))));

        AlarmList insertAllList = new AlarmList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmList alarmList1 = new AlarmList(AlarmFinder.all());
        assertEquals(alarmList.size() + 4, alarmList1.size());

        assertEquals(alarm5, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(5, 22, 21, 10))));
        assertEquals(alarm6, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(12, 34, 43, 33))));
        assertEquals(alarm7, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(6, 11, 37, 43))));
        assertEquals(alarm8, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(1, 55, 49, 0))));
    }
}
