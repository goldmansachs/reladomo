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

public class TestTimeRelationship extends MithraTestAbstract
{
    public void testRelationship()
    {
        Cellphone phone = CellphoneFinder.findOne(CellphoneFinder.id().eq(1));
        assertNotNull(phone);

        Alarm alarm = phone.getAlarm();

        assertEquals(AlarmFinder.findOne(AlarmFinder.id().eq(phone.getId())), alarm);

        Cellphone phone2 = CellphoneFinder.findOne(CellphoneFinder.id().eq(2));
        assertNotNull(phone2);

        Alarm alarm2 = phone2.getAlarm();
        assertEquals(AlarmFinder.findOne(AlarmFinder.id().eq(phone2.getId())), alarm2);
    }

    public void testTimeRelationship()
    {
        Cellphone2 cellphone = Cellphone2Finder.findOne(Cellphone2Finder.id().eq(1));
        AlarmList times = cellphone.getAlarms();

        Alarm alarm = times.get(0);
        assertEquals(Time.withMillis(10, 30, 59, 11), alarm.getTime());

        Cellphone2 cellphone2 = Cellphone2Finder.findOne(Cellphone2Finder.id().eq(2));
        AlarmList times2 = cellphone2.getAlarms();

        Alarm alarm2 = times2.get(0);
        assertEquals(Time.withMillis(3, 11, 23, 0), alarm2.getTime());
    }

}
