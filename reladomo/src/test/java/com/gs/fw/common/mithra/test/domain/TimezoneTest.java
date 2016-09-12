
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

package com.gs.fw.common.mithra.test.domain;

import java.sql.*;
import java.util.*;
import java.util.Date;

import junit.framework.*;

public class TimezoneTest extends TimezoneTestAbstract
{
    public static void assertDatesAndTimestamps(Date insensitiveDate, Date utcDate, Date databaseDate, Timestamp insensitiveTimestamp, Timestamp utcTimestamp, Timestamp databaseTimestamp, TimeZone databaseTimeZone)
    {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(insensitiveTimestamp.getTime());
        Assert.assertEquals(2005, c.get(Calendar.YEAR));
        Assert.assertEquals(9, c.get(Calendar.MONTH));
        Assert.assertEquals(1, c.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(10, c.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(12, c.get(Calendar.MINUTE));
        Assert.assertEquals(30, c.get(Calendar.SECOND));
        Assert.assertEquals(150, c.get(Calendar.MILLISECOND));

        Timestamp dateForTimezone = getTestDateInTimezone(databaseTimeZone);
        Assert.assertEquals(dateForTimezone.getTime(), databaseTimestamp.getTime());

        Assert.assertEquals(getTestDateInUtc().getTime(), utcTimestamp.getTime());
    }

    public static Timestamp getTestDateInTimezone(TimeZone tz)
    {
        Calendar c = Calendar.getInstance();
        c.setTimeZone(tz);
        c.set(Calendar.YEAR, 2005);
        c.set(Calendar.MONTH, 9);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR_OF_DAY, 10);
        c.set(Calendar.MINUTE, 12);
        c.set(Calendar.SECOND, 30);
        c.set(Calendar.MILLISECOND, 150);

        return new Timestamp(c.getTimeInMillis());
    }

    public static Timestamp getTestDateInUtc()
    {
        return getTestDateInTimezone(TimeZone.getTimeZone("UTC"));
    }
}
