
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

import junit.framework.TestCase;

import com.gs.fw.common.mithra.bulkloader.TimestampFormatter;


public class TimestampFormatterTest extends TestCase
{

    public void testFormatter() throws Exception
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2006, Calendar.MARCH, 15, 10, 6, 24);
        calendar.set(Calendar.MILLISECOND, 235);

        TimestampFormatter formatter = new TimestampFormatter("MMM dd yyyy HH:mm:ss.SSS");
        String formattedValue = formatter.format(new Timestamp(calendar.getTimeInMillis()));

        assertEquals("Wrong formatted value.", "Mar 15 2006 10:06:24.235", formattedValue);
    }
}
