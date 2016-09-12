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
import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedTransactional;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedTransactionalFinder;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedTransactionalList;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;

public class TestTimeDatedTransactional extends MithraTestAbstract
{
    public void testBasicTime()
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

        assertEquals(Time.withMillis(10, 30, 59, 11), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testToString()
    {
        AlarmDatedTransactionalList alarmList = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        assertEquals("10:30:59.011", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());
        assertEquals("04:03:02.001", Time.withMillis(4, 3, 2, 1).toString());
        assertEquals("04:03:02.000", Time.withNanos(4, 3, 2, 1).toString());
    }

    public void testInsert()
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

        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 11))));
        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 32))));
        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 44))));
        assertNull(AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0))));

        AlarmDatedTransactionalList insertAllList = new AlarmDatedTransactionalList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmDatedTransactionalList alarmList1 = new AlarmDatedTransactionalList(AlarmDatedTransactionalFinder.all());
        assertEquals(8, alarmList1.size());

        assertEquals(alarm5, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(5, 22, 21, 11))));
        assertEquals(alarm6, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(12, 34, 43, 32))));
        assertEquals(alarm7, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(6, 11, 37, 44))));
        assertEquals(alarm8, AlarmDatedTransactionalFinder.findOne(AlarmDatedTransactionalFinder.time().eq(Time.withMillis(1, 55, 49, 0))));
    }

    public void testTerminate()
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

    public void testUpdate()
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
        assertEquals(Time.withMillis(10, 30, 59, 11), alarm.getTime());
    }

    public void testNullTime()
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
}
