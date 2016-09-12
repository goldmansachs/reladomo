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

import com.gs.fw.common.mithra.test.domain.alarm.*;
import com.gs.fw.common.mithra.util.Time;

public class TestTimeTransactional extends MithraTestAbstract
{
    public void testBasicTime()
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

        assertEquals(Time.withMillis(10, 30, 59, 11), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testToString()
    {
        AlarmList alarmList = new AlarmList(AlarmFinder.all());
        alarmList.addOrderBy(AlarmFinder.id().ascendingOrderBy());

        assertEquals("10:30:59.011", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());
        assertEquals("04:03:02.001", Time.withMillis(4, 3, 2, 1).toString());
        assertEquals("04:03:02.000", Time.withNanos(4, 3, 2, 1).toString());
    }

    public void testInsert()
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

        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(5, 22, 21, 11))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(12, 34, 43, 32))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(6, 11, 37, 44))));
        assertNull(AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(1, 55, 49, 0))));

        AlarmList insertAllList = new AlarmList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmList alarmList1 = new AlarmList(AlarmFinder.all());
        assertEquals(alarmList.size() + 4, alarmList1.size());

        assertEquals(alarm5, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(5, 22, 21, 11))));
        assertEquals(alarm6, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(12, 34, 43, 32))));
        assertEquals(alarm7, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(6, 11, 37, 44))));
        assertEquals(alarm8, AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(1, 55, 49, 0))));
    }

    public void testDelete()
    {
        AlarmList alarmList = new AlarmList(AlarmFinder.all());
        Alarm alarm =  AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(10, 30, 59, 11)));
        assertNotNull(alarm);
        alarm.delete();

        assertEquals(2, new AlarmList(AlarmFinder.all()).size());
        Alarm alarmAfterDelete = AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(10, 30, 59, 11)));
        assertNull(alarmAfterDelete);
    }

    public void testUpdate()
    {
        AlarmList alarms = new AlarmList(AlarmFinder.all());
        alarms.addOrderBy(AlarmFinder.id().ascendingOrderBy());

        assertEquals("10:30:59.011", alarms.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarms.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarms.get(2).getTime().toString());

        alarms.setTime(Time.withMillis(1, 2, 3, 3));

        assertEquals(3, alarms.size());
        assertEquals("01:02:03.003", alarms.get(0).getTime().toString());
        assertEquals("01:02:03.003", alarms.get(1).getTime().toString());
        assertEquals("01:02:03.003", alarms.get(2).getTime().toString());
    }

    public void testNullTime()
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

    public void testTwoTimeAttributes()
    {
        Alarm2 alarm2 = Alarm2Finder.findOne(Alarm2Finder.id().eq(1));
        assertEquals(Time.withMillis(10, 30, 59, 10), alarm2.getTime());
        assertEquals(Time.withMillis(4, 32, 19, 0), alarm2.getTime2());
    }
}
