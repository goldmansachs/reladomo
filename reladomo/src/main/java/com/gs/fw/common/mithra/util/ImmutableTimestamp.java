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

package com.gs.fw.common.mithra.util;

import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;

import java.sql.Timestamp;
import java.io.ObjectStreamException;


public class ImmutableTimestamp extends Timestamp
{
    public static final DateTimeFormatter timestampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.");

    public static final ImmutableTimestamp ZERO = new ImmutableTimestamp(0);

    public ImmutableTimestamp(long time)
    {
        super(time);
    }

    public ImmutableTimestamp(long time, int nanos)
    {
        super(time);
        super.setNanos(nanos);
    }

    public ImmutableTimestamp(Timestamp other)
    {
        super(other.getTime());
        super.setNanos(other.getNanos());
    }

    public void setTime(long time)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    public void setNanos(int n)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    @Deprecated
    public void setDate(int date)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    @Deprecated
    public void setHours(int hours)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    @Deprecated
    public void setMinutes(int minutes)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    @Deprecated
    public void setMonth(int month)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    @Deprecated
    public void setSeconds(int seconds)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    @Deprecated
    public void setYear(int year)
    {
        throw new RuntimeException("ImmutableTimestamp must not be modified");
    }

    @Deprecated
    public int getDate()
    {
        return new DateTime(this.getTime()).getDayOfMonth();
    }

    @Deprecated
    public int getDay()
    {
        int day = new DateTime(this.getTime()).getDayOfWeek();
        return day == 7 ? 0 : day;
    }

    @Deprecated
    public int getHours()
    {
        return new DateTime(this.getTime()).getHourOfDay();
    }

    @Deprecated
    public int getMinutes()
    {
        return new DateTime(this.getTime()).getMinuteOfHour();
    }

    @Deprecated
    public int getMonth()
    {
        return new DateTime(this.getTime()).getMonthOfYear() - 1;
    }

    @Deprecated
    public int getSeconds()
    {
        return new DateTime(this.getTime()).getSecondOfMinute();
    }

    @Deprecated
    public int getTimezoneOffset()
    {
        return -ISOChronology.getInstance().getZone().getOffset(this.getTime())/60000;
    }

    @Deprecated
    public int getYear()
    {
        return new DateTime(this.getTime()).getYear() - 1900;
    }

    public boolean equals(Object ts)
    {
        return this == ts || super.equals(ts);
    }

    public String toString()
    {
        String result = timestampFormat.print(this.getTime());
        int nanos = getNanos();
        String nanosString = "0";
        String zeros = "000000000";
        if (nanos != 0)
        {
            nanosString = Integer.toString(nanos);

            // Add leading zeros
            nanosString = zeros.substring(0, (9-nanosString.length())) +
            nanosString;

            // Truncate trailing zeros
            char[] nanosChar = new char[nanosString.length()];
            nanosString.getChars(0, nanosString.length(), nanosChar, 0);
            int truncIndex = 8;
            while (nanosChar[truncIndex] == '0') {
            truncIndex--;
            }

            nanosString = new String(nanosChar, 0, truncIndex + 1);
        }

        return result+nanosString;
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        Timestamp timestamp = new Timestamp(this.getTime());
        timestamp.setNanos(this.getNanos());
        return timestamp;
    }

}
