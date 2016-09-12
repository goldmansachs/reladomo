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

import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.sql.Timestamp;

import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;



public class TestMithraTimestamp extends TestCase
{

    public void testConversionFromUtc()
    {
        Calendar c = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
        c.set(Calendar.MILLISECOND, 150);
        c.set(2005, 9, 6, 10, 20, 30); // Oct 6th, 2005, 10 am
        Timestamp time = new Timestamp(c.getTimeInMillis());
        MithraTimestamp.convertTimeFromUtc(time);
        c = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
        c.set(Calendar.MILLISECOND, 150);
        c.set(2005, 9, 6, 6, 20, 30); // Oct 6th, 2005, 6 am
        assertEquals(c.getTimeInMillis(), time.getTime());
//        assertEquals(1128594030150L, time.getTime());
    }

    public void testConversionFromUtcFor1900()
    {
        MithraTimestamp utcJodaDawnOfTime = new MithraTimestamp(new DateTime(1900, 1, 1, 0, 0, 0).getMillis() , false);
        this.checkConversion(utcJodaDawnOfTime, "1900-01-01 00:00:00.000");
    }

    public void testConversionFromUtcFor2005()
    {
        MithraTimestamp utcJodaDawnOfTime = new MithraTimestamp(new DateTime(2005, 9, 5, 0, 0, 0).getMillis() , false);
        this.checkConversion(utcJodaDawnOfTime, "2005-09-05 00:00:00.000");
    }

    public void testConversionFromCalUtcFor2005()
    {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2005);
        cal.set(Calendar.MONTH, 8);
        cal.set(Calendar.DAY_OF_MONTH, 5);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        MithraTimestamp utcJodaDawnOfTime = new MithraTimestamp(cal.getTime().getTime(), false);
        this.checkConversion(utcJodaDawnOfTime, "2005-09-05 00:00:00.000");

        this.checkConversion(new MithraTimestamp(cal.getTime().getTime()), "2005-09-05 00:00:00.000");

        cal.set(Calendar.HOUR_OF_DAY, 10);
        cal.set(Calendar.MINUTE, 20);
        cal.set(Calendar.SECOND, 30);
        this.checkConversion(new MithraTimestamp(cal.getTime().getTime()), "2005-09-05 10:20:30.000");
    }

    private void checkConversion(MithraTimestamp timestamp, final String expectedReadInString)
    {
        Timestamp timeForWrite = MithraTimestamp.zConvertTimeForWritingWithUtcCalendar(timestamp, TimeZone.getDefault());
        Timestamp timeForRead = MithraTimestamp.zConvertTimeForReadingWithUtcCalendar(timeForWrite, TimeZone.getDefault());

        assertTrue(timestamp.equals(timeForRead));
        final String timeForReadInString = PrintablePreparedStatement.timestampFormat.print(timeForRead.getTime()).replace("'", "");
        assertTrue("timeForReadInString is wrong:" + timeForReadInString, expectedReadInString.equals(timeForReadInString));
    }

    public void testConversionFromDatabaseTime()
    {
        Calendar c = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
        c.set(Calendar.MILLISECOND, 150);
        c.set(2005, 9, 6, 10, 20, 30); // Oct 6th, 2005, 10 am
        Timestamp time = new Timestamp(c.getTimeInMillis());
        MithraTimestamp.convertTimeToLocalTimeZone(TimeZone.getTimeZone("Europe/London"), time);
        c = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
        c.set(Calendar.MILLISECOND, 150);
        c.set(2005, 9, 6, 5, 20, 30); // Oct 6th, 2005, 5 am
        assertEquals(c.getTimeInMillis(), time.getTime());
//        assertEquals(1128590430150L, time.getTime());
    }
}
