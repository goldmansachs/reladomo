
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

package com.gs.fw.common.mithra.test.bulkloader;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import com.gs.fw.common.mithra.test.domain.PkTimezoneTestFinder;
import junit.framework.TestCase;

import com.gs.fw.common.mithra.bulkloader.TimeZoneTimestampFormatter;


public class TimeZoneTimestampFormatterTest extends TestCase
{

    public void testFormatter() throws Exception
    {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.set(2006, Calendar.MARCH, 15, 10, 6, 24);
        calendar.set(Calendar.MILLISECOND, 235);

        TimeZoneTimestampFormatter formatter = new TimeZoneTimestampFormatter("MMM dd yyyy HH:mm:ss.SSS", TimeZone.getTimeZone("America/New_York"), PkTimezoneTestFinder.databaseDate());
        String formattedValue = formatter.format(new Timestamp(calendar.getTimeInMillis()));

        assertEquals("Wrong formatted value.", "Mar 15 2006 05:06:24.235", formattedValue);
    }
}
