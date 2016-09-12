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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmBitemporalTransactional;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmBitemporalTransactionalFinder;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmBitemporalTransactionalList;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;

public class TestTimeBitemporalTransactional extends MithraTestAbstract
{
    public void testBasicTime()
    {
        AlarmBitemporalTransactional alarm = new AlarmBitemporalTransactional(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        Time time = Time.withMillis(2, 43, 55, 33);
        alarm.setId(1);
        alarm.setDescription("test alarm");
        alarm.setTime(time);

        assertEquals(Time.withMillis(2, 43, 55, 33), alarm.getTime());
        assertEquals(1, alarm.getId());
        assertEquals("test alarm", alarm.getDescription());

        AlarmBitemporalTransactionalList alarmList = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        alarmList.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());

        assertEquals(3, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 11), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testToString()
    {
        AlarmBitemporalTransactionalList alarmList = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        alarmList.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());
        assertEquals("10:30:59.011", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());
    }

    public void testInsert()
    {
        Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity());

        AlarmBitemporalTransactionalList originalList = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        int originalSize = originalList.size();

        final AlarmBitemporalTransactional alarm = AlarmBitemporalTransactionalFinder.findOne(businessDate.and(AlarmBitemporalTransactionalFinder.description().eq("alarm 1000")));
        assertNull(alarm);

        final AlarmBitemporalTransactional newAlarmBitemporalTransactional = new AlarmBitemporalTransactional(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        newAlarmBitemporalTransactional.setTime(Time.withMillis(23, 55, 49, 0));
        newAlarmBitemporalTransactional.setId(1000);
        newAlarmBitemporalTransactional.setDescription("alarm 1000");

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newAlarmBitemporalTransactional.insert();
                return null;
            }
        });

        AlarmBitemporalTransactionalList alarmList = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        assertEquals(originalSize + 1, alarmList.size());
        Operation businessDate2 = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        assertEquals(newAlarmBitemporalTransactional,
                AlarmBitemporalTransactionalFinder.findOne(
                        businessDate2.and(
                                AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(23, 55, 49, 0))
                                        .and(AlarmBitemporalTransactionalFinder.id().eq(1000))
                        )
                ));

        AlarmBitemporalTransactional alarm5 = new AlarmBitemporalTransactional(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        alarm5.setTime(Time.withMillis(5, 22, 21, 11));
        alarm5.setId(1001);
        alarm5.setDescription("alarm 1001");

        AlarmBitemporalTransactional alarm6 = new AlarmBitemporalTransactional(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        alarm6.setTime(Time.withMillis(12, 34, 43, 32));
        alarm6.setId(1002);
        alarm6.setDescription("alarm 1002");

        AlarmBitemporalTransactional alarm7 = new AlarmBitemporalTransactional(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        alarm7.setTime(Time.withMillis(6, 11, 37, 44));
        alarm7.setId(1003);
        alarm7.setDescription("alarm 1003");

        AlarmBitemporalTransactional alarm8 = new AlarmBitemporalTransactional(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        alarm8.setTime(Time.withMillis(1, 55, 49, 0));
        alarm8.setId(1004);
        alarm8.setDescription("alarm 1004");

        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 11)))));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 32)))));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 44)))));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0)))));

        AlarmBitemporalTransactionalList insertAllList = new AlarmBitemporalTransactionalList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmBitemporalTransactionalList alarmList1 = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        assertEquals(8, alarmList1.size());

        assertEquals(alarm5, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 11)))));
        assertEquals(alarm6, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 32)))));
        assertEquals(alarm7, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 44)))));
        assertEquals(alarm8, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0)))));
    }

    public void testTerminate()
    {
        final Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));

        AlarmBitemporalTransactionalList originalList = new AlarmBitemporalTransactionalList(businessDate);
        int originalListSize = originalList.size();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AlarmBitemporalTransactional alarm = AlarmBitemporalTransactionalFinder.findOne(businessDate.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(10, 30, 59, 11))));
                alarm.terminate();
                return null;
            }
        });

        AlarmBitemporalTransactionalList newList = new AlarmBitemporalTransactionalList(businessDate);
        assertEquals(originalListSize - 1, newList.size());
        AlarmBitemporalTransactional alarmAfterDelete = AlarmBitemporalTransactionalFinder.findOne(businessDate.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(10, 30, 59, 11))));
        Operation eq = AlarmBitemporalTransactionalFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000)).and(
                AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(10, 30, 59, 11))
        );
        AlarmBitemporalTransactional alarmOld = AlarmBitemporalTransactionalFinder.findOne(businessDate.and(eq));
        assertEquals(Time.withMillis(10, 30, 59, 11), alarmOld.getTime());
        assertNull(alarmAfterDelete);
    }

    public void testUpdate()
    {
        Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));
        final AlarmBitemporalTransactionalList alarms = new AlarmBitemporalTransactionalList(businessDate);
        alarms.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());

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

        Operation eq = AlarmBitemporalTransactionalFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000)).and(
                AlarmBitemporalTransactionalFinder.description().eq("alarm 1")
        );
        AlarmBitemporalTransactional alarm = AlarmBitemporalTransactionalFinder.findOne(businessDate.and(eq));
        assertEquals(Time.withMillis(10, 30, 59, 11), alarm.getTime());
    }

    public void testUpdateUntil()
    {
        Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));
        final AlarmBitemporalTransactionalList alarms = new AlarmBitemporalTransactionalList(businessDate);
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

        assertEquals(Time.withMillis(10, 30, 59, 11), AlarmBitemporalTransactionalFinder.findOne(op).getTime());
    }

    public void testInsertUntil()
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

    public void testTerminateUntil()
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
                insertAlarm.terminateUntil(Timestamp.valueOf("2013-01-01 23:59:00.0"));
                return null;
            }
        });

        assertNull(AlarmBitemporalTransactionalFinder.findOne(AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-12-12 23:59:00.0")).and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 1, 1, 1)))));

        Operation op = AlarmBitemporalTransactionalFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 1, 1, 1)));
        assertNotNull(AlarmBitemporalTransactionalFinder.findOne(op));
    }

    public void testNullTime()
    {
        final AlarmBitemporalTransactional alarm = new AlarmBitemporalTransactional(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("1970-11-11 10:20:30.222"));
        alarm.setTime(null);
        alarm.setDescription("null alarm");
        alarm.setId(9);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                alarm.insert();
                return null;
            }
        });

        assertEquals(alarm, AlarmBitemporalTransactionalFinder.findOne(businessDate.and(AlarmBitemporalTransactionalFinder.time().isNull())));

        Operation businessDate2 = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));
        final AlarmBitemporalTransactional beforeAlarm = AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.id().eq(1)));

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                beforeAlarm.setTime(null);
                return null;
            }
        });

        AlarmBitemporalTransactional afterAlarm = AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.id().eq(1)));
        assertNull(afterAlarm.getTime());
    }
}
