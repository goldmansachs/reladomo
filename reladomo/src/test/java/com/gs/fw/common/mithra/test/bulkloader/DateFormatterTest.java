
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

import java.util.Calendar;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.bulkloader.DateFormatter;


public class DateFormatterTest extends TestCase
{

    public void testDateFormatter() throws Exception
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2006, Calendar.MARCH, 15, 9, 15);

        DateFormatter formatter = new DateFormatter("MMM dd yyyy");
        String formattedValue = formatter.format(calendar.getTime());

        assertEquals("Formatter returned the wrong value", "Mar 15 2006", formattedValue);
    }
}
