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

public class TestTimeDatedNonTransactional extends MithraTestAbstract
{
    public void testBasicTime()
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

        assertEquals(Time.withMillis(10, 30, 59, 11), alarmList.get(0).getTime());
        assertEquals(Time.withMillis(3, 11, 23, 0), alarmList.get(1).getTime());
        assertEquals(Time.withMillis(14, 59, 10, 43), alarmList.get(2).getTime());
    }

    public void testToString()
    {
        AlarmDatedNonTransactionalList alarmList = new AlarmDatedNonTransactionalList(AlarmDatedNonTransactionalFinder.all());
        assertEquals("10:30:59.011", alarmList.get(0).getTime().toString());
        assertEquals("03:11:23.000", alarmList.get(1).getTime().toString());
        assertEquals("14:59:10.043", alarmList.get(2).getTime().toString());
        assertEquals("04:03:02.001", Time.withMillis(4, 3, 2, 1).toString());
        assertEquals("04:03:02.000", Time.withNanos(4, 3, 2, 1).toString());
    }

    public void testNullTime()
    {
        AlarmDatedNonTransactional nullAlarm = AlarmDatedNonTransactionalFinder.findOne(AlarmDatedNonTransactionalFinder.time().isNull());
        assertNotNull(nullAlarm);
        assertNull(nullAlarm.getTime());
        assertEquals(4, nullAlarm.getId());
    }
}
