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

import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmPrimaryKey;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmPrimaryKeyFinder;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmPrimaryKeyList;
import com.gs.fw.common.mithra.util.Time;

public class TestTimePrimaryKey extends MithraTestAbstract
{
    public void testPrimaryKeyInsert()
    {
        AlarmPrimaryKeyList list = new AlarmPrimaryKeyList(AlarmPrimaryKeyFinder.all());
        assertEquals(2, list.size());

        AlarmPrimaryKey alarmPrimaryKey = new AlarmPrimaryKey();
        alarmPrimaryKey.setId(300);
        alarmPrimaryKey.setDescription("new alarm");
        alarmPrimaryKey.setTime(Time.withMillis(10, 30, 59, 11));

        try
        {
            alarmPrimaryKey.insert();
            fail("violates primary key");
        }
        catch (MithraException e)
        {
            assertEquals(2, list.size());
        }

        alarmPrimaryKey.setTime(Time.withMillis(20, 40, 59, 10));

        alarmPrimaryKey.insert();
        AlarmPrimaryKeyList newList = new AlarmPrimaryKeyList(AlarmPrimaryKeyFinder.all());
        assertEquals(list.size() + 1, newList.size());
    }

    public void testDelete()
    {
        AlarmPrimaryKey alarm = AlarmPrimaryKeyFinder.findOne(AlarmPrimaryKeyFinder.time().eq(Time.withMillis(10, 30, 59, 11)));
        alarm.delete();

        assertNull(AlarmPrimaryKeyFinder.findOne(AlarmPrimaryKeyFinder.time().eq(Time.withMillis(10, 30, 59, 11))));
    }

    public void testUpdate()
    {
        AlarmPrimaryKey alarm = AlarmPrimaryKeyFinder.findOne(AlarmPrimaryKeyFinder.time().eq(Time.withMillis(10, 30, 59, 11)));
        assertNotNull(alarm);

        try
        {
            alarm.setTime(Time.withMillis(3, 11, 23, 0));
            fail("violates primary key");
        }
        catch (MithraException e)
        {
            assertEquals(Time.withMillis(10, 30, 59, 11), alarm.getTime());
        }

        alarm.setTime(Time.withMillis(1, 1, 1, 1));

        assertEquals(Time.withMillis(1, 1, 1, 1), alarm.getTime());
    }

    public void testMutablePk()
    {
        AlarmPrimaryKey alarm = AlarmPrimaryKeyFinder.findOne(AlarmPrimaryKeyFinder.id().eq(1));
        assertEquals(Time.withMillis(10, 30, 59, 11), alarm.getTime());

        alarm.setTime(Time.withMillis(1, 1, 1, 1));

        assertEquals(Time.withMillis(1, 1, 1, 1), alarm.getTime());
    }
}
