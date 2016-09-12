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

import com.gs.fw.common.mithra.test.domain.alarm.AlarmTenMillis;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmTenMillisFinder;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmTenMillisList;
import com.gs.fw.common.mithra.util.Time;

public class TestTimeTenMillis extends MithraTestAbstract
{
    public void testToString()
    {
        AlarmTenMillisList AlarmTenMillisList = new AlarmTenMillisList(AlarmTenMillisFinder.all());
        AlarmTenMillisList.addOrderBy(AlarmTenMillisFinder.id().ascendingOrderBy());

        assertEquals("10:30:59.010", AlarmTenMillisList.get(0).getTime().toString());
        assertEquals("03:11:23.000", AlarmTenMillisList.get(1).getTime().toString());
        assertEquals("14:59:10.043", AlarmTenMillisList.get(2).getTime().toString());
    }

    public void testInsert()
    {
        AlarmTenMillisList originalList = new AlarmTenMillisList(AlarmTenMillisFinder.all());
        originalList.addOrderBy(AlarmTenMillisFinder.id().ascendingOrderBy());
        int originalListSize = originalList.size();

        AlarmTenMillis alarm = AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.description().eq("alarm 4"));
        assertNull(alarm);

        AlarmTenMillis newAlarm = new AlarmTenMillis();
        newAlarm.setTime(Time.withMillis(23, 55, 49, 0));
        newAlarm.setId(1000);
        newAlarm.setDescription("alarm 1000");

        newAlarm.insert();

        AlarmTenMillisList AlarmTenMillisList = new AlarmTenMillisList(AlarmTenMillisFinder.all());
        AlarmTenMillisList.addOrderBy(AlarmTenMillisFinder.id().ascendingOrderBy());

        assertEquals(originalList.size() + 1, AlarmTenMillisList.size());
        assertEquals(newAlarm, AlarmTenMillisList.get(AlarmTenMillisList.size() - 1));

        AlarmTenMillis alarm5 = new AlarmTenMillis();
        alarm5.setTime(Time.withMillis(5, 22, 21, 11));
        alarm5.setId(1001);
        alarm5.setDescription("alarm 1001");

        AlarmTenMillis alarm6 = new AlarmTenMillis();
        alarm6.setTime(Time.withMillis(12, 34, 43, 32));
        alarm6.setId(1002);
        alarm6.setDescription("alarm 1002");

        AlarmTenMillis alarm7 = new AlarmTenMillis();
        alarm7.setTime(Time.withMillis(6, 11, 37, 44));
        alarm7.setId(1003);
        alarm7.setDescription("alarm 1003");

        AlarmTenMillis alarm8 = new AlarmTenMillis();
        alarm8.setTime(Time.withMillis(1, 55, 49, 0));
        alarm8.setId(1004);
        alarm8.setDescription("alarm 1004");

        assertNull(AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(5, 22, 21, 10))));
        assertNull(AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(12, 34, 43, 30))));
        assertNull(AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(6, 11, 37, 40))));
        assertNull(AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(1, 55, 49, 0))));

        AlarmTenMillisList insertAllList = new AlarmTenMillisList();

        insertAllList.add(alarm5);
        insertAllList.add(alarm6);
        insertAllList.add(alarm7);
        insertAllList.add(alarm8);

        insertAllList.insertAll();

        AlarmTenMillisList AlarmTenMillisList1 = new AlarmTenMillisList(AlarmTenMillisFinder.all());
        assertEquals(AlarmTenMillisList.size() + 4, AlarmTenMillisList1.size());

        assertEquals(alarm5, AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(5, 22, 21, 10))));
        assertEquals(alarm6, AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(12, 34, 43, 30))));
        assertEquals(alarm7, AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(6, 11, 37, 40))));
        assertEquals(alarm8, AlarmTenMillisFinder.findOne(AlarmTenMillisFinder.time().eq(Time.withMillis(1, 55, 49, 0))));
    }

    public void testUpdate()
    {
        AlarmTenMillisList alarms = new AlarmTenMillisList(AlarmTenMillisFinder.all());
        alarms.addOrderBy(AlarmTenMillisFinder.id().ascendingOrderBy());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 4));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 0));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 1));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 2));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 3));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 4));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 5));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 6));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 7));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 8));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 9));
        assertEquals("01:02:03.000", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 10));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 11));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 12));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 13));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 14));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 15));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 16));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 17));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 18));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 19));
        assertEquals("01:02:03.010", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 20));
        assertEquals("01:02:03.020", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 999));
        assertEquals("01:02:03.990", alarms.get(0).getTime().toString());
        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 996));
        assertEquals("01:02:03.990", alarms.get(0).getTime().toString());

        alarms.get(0).setTime(Time.withMillis(1, 2, 3, 998));
        assertEquals("01:02:03.990", alarms.get(0).getTime().toString());
    }
}
