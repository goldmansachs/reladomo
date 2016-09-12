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
import com.gs.fw.common.mithra.test.domain.alarm.*;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;

public class TestTimeDb2 extends MithraTestAbstract
{
    public void testBasicTimeTransactional()
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

        assertEquals(Time.withMillis(10, 30, 59, 0), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 0), alarmList.get(2).getTime());
    }

    public void testToStringTransactional()
    {
        AlarmList alarmList = new AlarmList(AlarmFinder.all());
        alarmList.addOrderBy(AlarmFinder.id().ascendingOrderBy());

        assertEquals("10:30:59.000", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.000", alarmList.get(2).getTime().toString());
    }

    public void testInsertTransactional()
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
        alarm5.setTime(Time.withMillis(5, 22, 21, 11));
        alarm5.setId(1001);
        alarm5.setDescription("alarm 1001");

        Alarm alarm6 = new Alarm();
        alarm6.setTime(Time.withMillis(12, 34, 43, 32));
        alarm6.setId(1002);
        alarm6.setDescription("alarm 1002");

        Alarm alarm7 = new Alarm();
        alarm7.setTime(Time.withMillis(6, 11, 37, 44));
        alarm7.setId(1003);
        alarm7.setDescription("alarm 1003");

        Alarm alarm8 = new Alarm();
        alarm8.setTime(Time.withMillis(1, 55, 49, 0));
        alarm8.setId(1004);
        alarm8.setDescription("alarm 1004");

        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(5, 22, 21, 0))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(12, 34, 43, 0))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(6, 11, 37, 0))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(1, 55, 49, 0))));

        AlarmList insertAllList = new AlarmList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmList alarmList1 = new AlarmList(AlarmFinder.all());
        assertEquals(alarmList.size() + 4, alarmList1.size());

        assertEquals(alarm5, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(5, 22, 21, 0))));
        assertEquals(alarm6, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(12, 34, 43, 0))));
        assertEquals(alarm7, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(6, 11, 37, 0))));
        assertEquals(alarm8, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(1, 55, 49, 0))));
    }

    public void testDeleteTransactional()
    {
        AlarmList alarmList = new AlarmList(AlarmFinder.all());
        Alarm alarm =  AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(10, 30, 59, 0)));
        assertNotNull(alarm);
        alarm.delete();

        assertEquals(2, new AlarmList(AlarmFinder.all()).size());
        Alarm alarmAfterDelete = AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(10, 30, 59, 0)));
        assertNull(alarmAfterDelete);
    }

    public void testUpdateTransactional()
    {
        AlarmList alarms = new AlarmList(AlarmFinder.all());
        alarms.addOrderBy(AlarmFinder.id().ascendingOrderBy());

        assertEquals("10:30:59.000", alarms.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarms.get(1).getTime().toString());
        assertEquals("14:59:10.000", alarms.get(2).getTime().toString());

        alarms.setTime(Time.withMillis(1, 2, 3, 0));

        assertEquals(3, alarms.size());
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        assertEquals("01:02:03.000", alarms.get(1).getTime().toString());
        assertEquals("01:02:03.000", alarms.get(2).getTime().toString());
    }

    public void testNullTimeTransactional()
    {
        Alarm alarm = new Alarm();
        alarm.setTime(null);
        alarm.setDescription("null alarm");
        alarm.setId(1009);
        alarm.insert();
        assertEquals(alarm, AlarmFinder.findOne(AlarmFinder.time().isNull()));

        alarm.delete();

        Alarm nullAlarm = new AlarmList(AlarmFinder.all()).get(0);
        assertNotNull(nullAlarm.getTime());
        nullAlarm.setTime(null);
        assertNull(AlarmFinder.findOne(AlarmFinder.time().isNull()).getTime());
    }

    public void testTwoTimeAttributesTransactional()
    {
        Alarm2 alarm2 = Alarm2Finder.findOne(Alarm2Finder.id().eq(1));
        assertEquals(Time.withMillis(10, 30, 59, 0), alarm2.getTime());
        assertEquals(Time.withMillis(4, 32, 19, 0), alarm2.getTime2());
    }

    //non transactional
    public void testBasicTimeNonTransactional()
    {
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

        assertEquals(Time.withMillis(10, 30, 59, 0), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 0), alarmList.get(2).getTime());
    }

    public void testToStringNonTransactional()
    {
        AlarmNonTransactionalList alarmList = new AlarmNonTransactionalList(AlarmNonTransactionalFinder.all());
        alarmList.addOrderBy(AlarmNonTransactionalFinder.id().ascendingOrderBy());
        assertEquals("10:30:59.000", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.000", alarmList.get(2).getTime().toString());
    }

    public void testNullTimeNonTransactional()
    {
        AlarmNonTransactionalList alarmList = new AlarmNonTransactionalList(AlarmNonTransactionalFinder.all());
        alarmList.addOrderBy(AlarmNonTransactionalFinder.id().ascendingOrderBy());
        assertEquals(alarmList.get(3), AlarmNonTransactionalFinder.findOne(AlarmNonTransactionalFinder.time().isNull()));
    }

    //dated transactional
    public void testBasicTimeDatedTransactional()
    {
        AlarmDatedTransactionalList alarmList = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        alarmList.addOrderBy(AlarmDatedTransactionalFinder.id().ascendingOrderBy());

        assertEquals(3, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 0), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 0), alarmList.get(2).getTime());
    }

    public void testToStringDatedTransactional()
    {
        AlarmDatedTransactionalList alarmList = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        alarmList.addOrderBy(AlarmDatedTransactionalFinder.id().ascendingOrderBy());
        assertEquals("10:30:59.000", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.000", alarmList.get(2).getTime().toString());
    }

    public void testInsertDatedTransactional()
    {
        AlarmDatedTransactionalList originalList = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        int originalSize = originalList.size();

        final AlarmDatedTransactional alarm = AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.description().eq("alarm 4"));
        assertNull(alarm);

        final AlarmDatedTransactional newAlarmDatedTransactional = new AlarmDatedTransactional();
        newAlarmDatedTransactional.setTime(Time.withMillis(23, 55, 49, 0));
        newAlarmDatedTransactional.setId(1000);
        newAlarmDatedTransactional.setDescription("alarm 1000");

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newAlarmDatedTransactional.insert();
                return null;
            }
        });

        AlarmDatedTransactionalList alarmList = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        assertEquals(originalSize + 1, alarmList.size());
        assertEquals(newAlarmDatedTransactional,
                AlarmDatedTransactionalFinder.findOne(
                        AlarmDatedTransactionalFinder.time().eq(Time.withMillis(23, 55, 49, 0))
                                .and(AlarmDatedTransactionalFinder.id().eq(1000))
                ));

        AlarmDatedTransactional alarm5 = new AlarmDatedTransactional();
        alarm5.setTime(Time.withMillis(5, 22, 21, 11));
        alarm5.setId(1001);
        alarm5.setDescription("alarm 1001");

        AlarmDatedTransactional alarm6 = new AlarmDatedTransactional();
        alarm6.setTime(Time.withMillis(12, 34, 43, 32));
        alarm6.setId(1002);
        alarm6.setDescription("alarm 1002");

        AlarmDatedTransactional alarm7 = new AlarmDatedTransactional();
        alarm7.setTime(Time.withMillis(6, 11, 37, 44));
        alarm7.setId(1003);
        alarm7.setDescription("alarm 1003");

        AlarmDatedTransactional alarm8 = new AlarmDatedTransactional();
        alarm8.setTime(Time.withMillis(1, 55, 49, 0));
        alarm8.setId(1004);
        alarm8.setDescription("alarm 1004");

        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 0))));
        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 0))));
        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 0))));
        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0))));

        AlarmDatedTransactionalList insertAllList = new AlarmDatedTransactionalList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmDatedTransactionalList alarmList1 = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        assertEquals(8, alarmList1.size());

        assertEquals(alarm5, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 0))));
        assertEquals(alarm6, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 0))));
        assertEquals(alarm7, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 0))));
        assertEquals(alarm8, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0))));
    }

    public void testTerminateDatedTransactional()
    {
        final AlarmDatedTransactional alarm =  AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.id().eq(1));
        assertNotNull(alarm);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                alarm.terminate();
                return null;
            }
        });

        assertEquals(2, new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all()).size());
        AlarmDatedTransactional alarmAfterDelete = AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.description().eq("alarm 1"));
        Operation eq = AlarmDatedTransactionalFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000)).and(
                AlarmDatedTransactionalFinder.id().eq(1)
        );
        AlarmDatedTransactional alarmOld = AlarmDatedTransactionalFinder.findOne(eq);
        assertEquals(1, alarmOld.getId());
        assertNull(alarmAfterDelete);
    }

    public void testUpdateDatedTransactional()
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
                alarms.setTime(Time.withMillis(1, 2, 3, 0));
                return null;
            }
        });

        assertEquals(3, alarms.size());
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        assertEquals("01:02:03.000", alarms.get(1).getTime().toString());
        assertEquals("01:02:03.000", alarms.get(2).getTime().toString());

        Operation eq = AlarmDatedTransactionalFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000)).and(
                AlarmDatedTransactionalFinder.description().eq("alarm 1")
        );
        AlarmDatedTransactional alarm = AlarmDatedTransactionalFinder.findOne(eq);
        assertEquals(Time.withMillis(10, 30, 59, 0), alarm.getTime());
    }

    public void testNullTimeDatedTransactional()
    {
        final AlarmDatedTransactional alarm = new AlarmDatedTransactional();
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

        assertEquals(alarm, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().isNull()));

        final AlarmDatedTransactional beforeAlarm = AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.id().eq(1));

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                beforeAlarm.setTime(null);
                return null;
            }
        });

        AlarmDatedTransactional afterAlarm = AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.id().eq(1));
        assertNull(afterAlarm.getTime());
    }

    //dated non transactional
    public void testBasicTimeDatedNonTransactional()
    {
        AlarmDatedNonTransactionalList alarmList = new AlarmDatedNonTransactionalList(AlarmDatedNonTransactionalFinder.all());
        alarmList.addOrderBy(AlarmDatedNonTransactionalFinder.id().ascendingOrderBy());

        assertEquals(4, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 0), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 0), alarmList.get(2).getTime());
    }

    public void testToStringDatedNonTransactional()
    {
        AlarmDatedNonTransactionalList alarmList = new AlarmDatedNonTransactionalList(AlarmDatedNonTransactionalFinder.all());
        assertEquals("10:30:59.000", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.000", alarmList.get(2).getTime().toString());
    }

    public void testNullTimeDatedNonTransactional()
    {
        AlarmDatedNonTransactional nullAlarm = AlarmDatedNonTransactionalFinder.findOne(AlarmDatedNonTransactionalFinder.time().isNull());
        assertNull(nullAlarm.getTime());
        assertEquals(4, nullAlarm.getId());
    }

    //bitemporal transactional
    public void testBasicTimeBitemporalTransactional()
    {
        AlarmBitemporalTransactionalList alarmList = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        alarmList.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());

        assertEquals(3, alarmList.size());
        assertEquals(1, alarmList.get(0).getId());
        assertEquals(2, alarmList.get(1).getId());
        assertEquals(3, alarmList.get(2).getId());

        assertEquals("alarm 1", alarmList.get(0).getDescription());
        assertEquals("alarm 2", alarmList.get(1).getDescription());
        assertEquals("alarm 3", alarmList.get(2).getDescription());

        assertEquals(Time.withMillis(10, 30, 59, 0), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 0), alarmList.get(2).getTime());
    }

    public void testToStringBitemporalTransactional()
    {
        AlarmBitemporalTransactionalList alarmList = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        alarmList.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());
        assertEquals("10:30:59.000", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.000", alarmList.get(2).getTime().toString());
    }

    public void testInsertBitemporalTransactional()
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

        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 0)))));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 0)))));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 0)))));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0)))));

        AlarmBitemporalTransactionalList insertAllList = new AlarmBitemporalTransactionalList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmBitemporalTransactionalList alarmList1 = new AlarmBitemporalTransactionalList(AlarmBitemporalTransactionalFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        assertEquals(8, alarmList1.size());

        assertEquals(alarm5, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 0)))));
        assertEquals(alarm6, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 0)))));
        assertEquals(alarm7, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 0)))));
        assertEquals(alarm8, AlarmBitemporalTransactionalFinder.findOne(businessDate2.and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0)))));
    }

    public void testTerminateBitemporalTransactional()
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
        assertEquals(Time.withMillis(10, 30, 59, 0), alarmOld.getTime());
        assertNull(alarmAfterDelete);
    }

    public void testUpdateBitemporalTransactional()
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
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        assertEquals("01:02:03.000", alarms.get(1).getTime().toString());
        assertEquals("01:02:03.000", alarms.get(2).getTime().toString());

        Operation eq = AlarmBitemporalTransactionalFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000)).and(
                AlarmBitemporalTransactionalFinder.description().eq("alarm 1")
        );
        AlarmBitemporalTransactional alarm = AlarmBitemporalTransactionalFinder.findOne(businessDate.and(eq));
        assertEquals(Time.withMillis(10, 30, 59, 0), alarm.getTime());
    }

    public void testUpdateUntilBitemporalTransactional()
    {
        Operation businessDate = AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));
        final AlarmBitemporalTransactionalList alarms = new AlarmBitemporalTransactionalList(businessDate);
        alarms.addOrderBy(AlarmBitemporalTransactionalFinder.id().ascendingOrderBy());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                alarms.get(0).setTimeUntil(Time.withMillis(1, 2, 3, 0), Timestamp.valueOf("2013-01-01 23:59:00.0"));
                return null;
            }
        });

        assertEquals(Time.withMillis(1, 2, 3, 0), AlarmBitemporalTransactionalFinder.findOne(AlarmBitemporalTransactionalFinder.businessDate().eq(Timestamp.valueOf("2012-12-12 23:59:00.0")).and(AlarmBitemporalTransactionalFinder.id().eq(1))).getTime());

        Operation op = AlarmBitemporalTransactionalFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(AlarmBitemporalTransactionalFinder.id().eq(1));

        assertEquals(Time.withMillis(10, 30, 59, 0), AlarmBitemporalTransactionalFinder.findOne(op).getTime());
    }

    public void testInsertUntilBitemporalTransactional()
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

        Operation op = AlarmBitemporalTransactionalFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 1, 1, 0)));
        assertNull(AlarmBitemporalTransactionalFinder.findOne(op));
    }

    public void testTerminateUntilBitemporalTransactional()
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

        Operation op = AlarmBitemporalTransactionalFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(AlarmBitemporalTransactionalFinder.time().eq(Time.withMillis(1, 1, 1, 0)));
        assertNotNull(AlarmBitemporalTransactionalFinder.findOne(op));
    }

    public void testNullTimeBitemporalTransactional()
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
