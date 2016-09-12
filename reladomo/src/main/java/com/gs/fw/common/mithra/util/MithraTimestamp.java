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

import com.gs.collections.api.block.function.Function0;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;



public class MithraTimestamp extends Timestamp
{
    private static final long serialVersionUID = 2097501108058515685L;

    public final static TimeZone UtcTimeZone = TimeZone.getTimeZone("UTC");
    public final static TimeZone DefaultTimeZone = TimeZone.getDefault();
    private static final int DAY_IN_MILLISECONDS = 24 * 3600 * 1000;
    private boolean timezoneSensitive = true;
    private static DateTimeZone DEFAULT_TIMEZONE = DateTimeZone.forTimeZone(TimeZone.getDefault());
    private static long TIME_FOR_1950_01_01 = new DateTime(1950, 1, 1, 0, 0, 0).getMillis();  // due to historical GMT offset

    private static final byte IS_NULL = 0x10;
    private static final byte IS_INFINITY = 0x20;
    private static final byte IS_NOT_NULL_AND_NOT_INFINITY = 0x30;
    private static final byte IS_NOT_NULL_AND_NOT_INFINITY_ZERO_NANOS = 0x40;

    private static final ConcurrentHashMap<String,ConcurrentHashMap<Timestamp, Integer>> infinityTimezoneOffsetCache = new ConcurrentHashMap<String,ConcurrentHashMap<Timestamp, Integer>>();

    public MithraTimestamp(long time)
    {
        this(time, true);
    }

    public MithraTimestamp(long time, boolean timezoneSensitive)
    {
        super(time);
        this.timezoneSensitive = timezoneSensitive;
    }

    protected static int getOffsetFromTimeZoneToUtc(TimeZone tz, Date date)
    {
        if (date.getTime() <= TIME_FOR_1950_01_01)
        {
            return DateTimeZone.forTimeZone(tz).getOffset(date.getTime());
        }
        else
        {
            int offset = tz.getRawOffset();
            if (tz.inDaylightTime(date))
            {
                offset += tz.getDSTSavings();
            }
            return offset;
        }
    }

    public MithraTimestamp(Timestamp timestamp, boolean timezoneSensitive, TimeZone originalTimeZone)
    {
        super(timestamp.getTime());
        this.timezoneSensitive = timezoneSensitive;
        if (this.timezoneSensitive)
        {
            int offset = getOffsetFromTimeZoneToDefault(originalTimeZone, this);
            this.setTime(timestamp.getTime() + offset);
        }
        this.setNanos(timestamp.getNanos());
    }

    public MithraTimestamp convertToTimeZone(TimeZone targetTimeZone)
    {
        if (!this.timezoneSensitive) return this;

        int offset = getOffsetFromTimeZoneToUtc(targetTimeZone, this) - getDefaultOffset(this);
        MithraTimestamp result = new MithraTimestamp(this.getTime() + offset, false);
        result.setNanos(this.getNanos());
        return result;
    }

    private void writeObject(java.io.ObjectOutputStream out)
     throws IOException
    {
        out.writeBoolean(this.timezoneSensitive);
        long time = getSerializableTime(this.timezoneSensitive, this.getTime());
        out.writeLong(time);
        out.writeInt(this.getNanos());
    }

    public static long getSerializableTime(boolean sensitive, long time)
    {
        if (!sensitive)
        {
            // we'll write the time in UTC
            int offset = getDefaultOffsetForTime(time);
            time += offset;
        }
        return time;
    }

    private void readObject(java.io.ObjectInputStream in)
     throws IOException, ClassNotFoundException
    {
        this.timezoneSensitive = in.readBoolean();
        long time = in.readLong();
        if (!this.timezoneSensitive)
        {
            // convert the time from UTC to local
            int offset = getDefaultOffset(this);
            time -= offset;
        }
        this.setTime(time);
        this.setNanos(in.readInt());
    }

    public static void convertTimeToLocalTimeZone(TimeZone fromTimeZone, Timestamp timestamp)
    {
        if (timestamp == null) return;
        int offset = getOffsetFromTimeZoneToDefault(fromTimeZone, timestamp);
        if (offset != 0)
        {
            int nanos = timestamp.getNanos();
            timestamp.setTime(timestamp.getTime() + offset);
            timestamp.setNanos(nanos);
        }
    }

    private static int getOffsetFromTimeZoneToDefault(TimeZone fromTimeZone, Timestamp timestamp)
    {
        return getDefaultOffset(timestamp) - getOffsetFromTimeZoneToUtc(fromTimeZone, timestamp);
    }

    private static int getDefaultOffset(java.util.Date timestamp)
    {
        return getDefaultOffsetForTime(timestamp.getTime());
    }

    public static int getDefaultOffsetForTime(long time)
    {
        return DEFAULT_TIMEZONE.getOffset(time);
    }

    private static int getDefaultOffset(long time)
    {
        return getDefaultOffsetForTime(time);
    }

    public static void convertTimeFromUtc(Timestamp timestamp)
    {
        if (timestamp == null) return;
        long utcTime = timestamp.getTime();
        int offset = getDefaultOffset(utcTime);
        if (offset != 0)
        {
            int nanos = timestamp.getNanos();
            timestamp.setTime(utcTime + offset);
            timestamp.setNanos(nanos);
            int realOffset = getDefaultOffset(timestamp.getTime());
            if (realOffset != offset)
            {
                nanos = timestamp.getNanos();
                timestamp.setTime(utcTime + realOffset);
                timestamp.setNanos(nanos);
            }
        }
    }

    private static long fromLocalToUtc(long localtime)
    {
        int offset = DEFAULT_TIMEZONE.getOffset(localtime);
        return localtime - offset;
    }

    public static Timestamp createUtcTime(Timestamp timestamp)
    {
        if (timestamp == null) return null;
        Timestamp result = timestamp;
        long utcTime = fromLocalToUtc(timestamp.getTime());
        if (utcTime != timestamp.getTime())
        {
            result = new Timestamp(utcTime);
            result.setNanos(timestamp.getNanos());
        }
        return result;
    }

    public static Timestamp createDatabaseTime(Timestamp timestamp, TimeZone databaseTimezone)
    {
        if (timestamp == null) return null;
        if (databaseTimezone == UtcTimeZone)
        {
            return createUtcTime(timestamp);
        }
        int offset = - getOffsetFromTimeZoneToDefault(databaseTimezone, timestamp);
        return createTimestampFromOffset(timestamp, offset);
    }

    private static Timestamp createTimestampFromOffset(Timestamp timestamp, int offset)
    {
        Timestamp result = timestamp;
        if (offset != 0)
        {
            result = new Timestamp(timestamp.getTime() + offset);
            result.setNanos(timestamp.getNanos());
        }
        return result;
    }

    private static void writeNormalTimestamp(DataOutput out, long time, int nanos)
            throws IOException
    {
        int nanosWithoutMillis = nanos % 1000000;
        if (nanosWithoutMillis == 0)
        {
            out.writeByte(IS_NOT_NULL_AND_NOT_INFINITY_ZERO_NANOS);
            out.writeLong(time);
        }
        else
        {
            out.writeByte(IS_NOT_NULL_AND_NOT_INFINITY);
            out.writeLong(time);
            out.writeInt(nanos);
        }
    }

    public static void writeTimestamp(DataOutput out, Timestamp timestamp) throws IOException
    {
        if (timestamp == null)
        {
            out.writeByte(IS_NULL);
            return;
        }
        writeNormalTimestamp(out, timestamp.getTime(), timestamp.getNanos());
    }

    public static void writeTimestamp(DataOutput out, long timestamp) throws IOException
    {
        if (timestamp == TimestampPool.OFF_HEAP_NULL)
        {
            out.writeByte(IS_NULL);
            return;
        }
        writeNormalTimestamp(out, timestamp, 0);
    }

    public static Timestamp readTimestamp(ObjectInput in) throws IOException
    {
        byte serialType = in.readByte();
        if (serialType == IS_NULL) return null;

        long time = in.readLong();
        return createImmutableTimestamp(in, serialType, time);
    }

    private static Timestamp createImmutableTimestamp(ObjectInput in, byte serialType, long time)
            throws IOException
    {
        if (serialType == IS_NOT_NULL_AND_NOT_INFINITY)
        {
            int nanos = in.readInt();
            return new ImmutableTimestamp(time, nanos);
        }
        else
        {
            return new ImmutableTimestamp(time);
        }
    }

    public static void writeTimestampWithInfinity(DataOutput out, Timestamp timestamp, Timestamp infinity) throws IOException
    {
        if (timestamp == null)
        {
            out.writeByte(IS_NULL);
            return;
        }
        if (timestamp.getTime() == infinity.getTime())
        {
            out.writeByte(IS_INFINITY);
            return;
        }
        writeNormalTimestamp(out, timestamp.getTime(), timestamp.getNanos());
    }

    public static void writeTimestampWithInfinity(DataOutput out, long timestamp, Timestamp infinity) throws IOException
    {
        if (timestamp == TimestampPool.OFF_HEAP_NULL)
        {
            out.writeByte(IS_NULL);
            return;
        }
        if (timestamp == infinity.getTime())
        {
            out.writeByte(IS_INFINITY);
            return;
        }
        writeNormalTimestamp(out, timestamp, 0);
    }

    public static Timestamp readTimestampWithInfinity(ObjectInput in, Timestamp infinity) throws IOException
    {
        byte serialType = in.readByte();
        if (serialType == IS_NULL) return null;
        if (serialType == IS_INFINITY) return infinity;

        long time = in.readLong();
        return createImmutableTimestamp(in, serialType, time);
    }

    public static void writeTimezoneInsensitiveTimestamp(DataOutput out, Timestamp timestamp) throws IOException
    {
        if (timestamp == null)
        {
            out.writeByte(IS_NULL);
            return;
        }
        long time = timestamp.getTime();
        // we'll write the time in UTC
        int offset = getDefaultOffset(timestamp);
        time += offset;

        writeNormalTimestamp(out, time, timestamp.getNanos());
    }

    public static void writeTimezoneInsensitiveTimestamp(DataOutput out, long timestamp) throws IOException
    {
        if (timestamp == TimestampPool.OFF_HEAP_NULL)
        {
            out.writeByte(IS_NULL);
            return;
        }
        // we'll write the time in UTC
        int offset = getDefaultOffset(timestamp);
        timestamp += offset;

        writeNormalTimestamp(out, timestamp, 0);
    }

    public static Timestamp readTimezoneInsensitiveTimestamp(ObjectInput in) throws IOException
    {
        byte serialType = in.readByte();
        if (serialType == IS_NULL) return null;

        long time = in.readLong();

        time -= getDefaultOffset(time);
        return createImmutableTimestamp(in, serialType, time);
    }

    public static void writeTimezoneInsensitiveTimestampWithInfinity(DataOutput out,
            Timestamp timestamp, Timestamp infinity) throws IOException
    {
        if (timestamp == null)
        {
            out.writeByte(IS_NULL);
            return;
        }
        long time = timestamp.getTime();
        if (time == infinity.getTime())
        {
            out.writeByte(IS_INFINITY);
            return;
        }
        // we'll write the time in UTC
        time += getDefaultOffset(timestamp);

        writeNormalTimestamp(out, time, timestamp.getNanos());
    }

    public static void writeTimezoneInsensitiveTimestampWithInfinity(DataOutput out,
            long timestamp, Timestamp infinity) throws IOException
    {
        if (timestamp == TimestampPool.OFF_HEAP_NULL)
        {
            out.writeByte(IS_NULL);
            return;
        }
        if (timestamp == infinity.getTime())
        {
            out.writeByte(IS_INFINITY);
            return;
        }
        // we'll write the time in UTC
        timestamp += getDefaultOffset(timestamp);

        writeNormalTimestamp(out, timestamp, 0);
    }

    public static Timestamp readTimezoneInsensitiveTimestampWithInfinity(ObjectInput in, Timestamp infinity) throws IOException
    {
        byte serialType = in.readByte();
        if (serialType == IS_NULL) return null;
        if (serialType == IS_INFINITY) return infinity;

        long time = in.readLong();

        time -= getDefaultOffset(time);
        return createImmutableTimestamp(in, serialType, time);
    }

    public static void writeTimezoneInsensitiveDate(ObjectOutput out, Date timestamp) throws IOException
    {
        if (timestamp == null)
        {
            out.writeByte(IS_NULL);
            return;
        }
        out.writeByte(IS_NOT_NULL_AND_NOT_INFINITY_ZERO_NANOS);
        long time = timestamp.getTime();
        // we'll write the time in UTC
        int offset = getDefaultOffset(timestamp);
        time += offset;

        out.writeLong(time);
    }

    public static Date readTimezoneInsensitiveDate(ObjectInput in) throws IOException
    {
        byte isNullOrInfinity = in.readByte();
        if (isNullOrInfinity == IS_NULL) return null;

        long time = in.readLong();


        time -= getDefaultOffset(time);
        return  new java.sql.Date(time);
    }

    @Deprecated
    public synchronized void setDate(int date)
    {
        super.setDate(date);
        this.setTime(this.getTime());
    }

    @Deprecated
    public synchronized void setHours(int hours)
    {
        super.setHours(hours);
        this.setTime(this.getTime());
    }

    @Deprecated
    public synchronized void setMinutes(int minutes)
    {
        super.setMinutes(minutes);
        this.setTime(this.getTime());
    }

    @Deprecated
    public synchronized void setMonth(int month)
    {
        super.setMonth(month);
        this.setTime(this.getTime());
    }

    @Deprecated
    public synchronized void setSeconds(int seconds)
    {
        super.setSeconds(seconds);
        this.setTime(this.getTime());
    }

    @Deprecated
    public synchronized void setYear(int year)
    {
        super.setYear(year);
        this.setTime(this.getTime());
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
        String result = ImmutableTimestamp.timestampFormat.print(this.getTime());
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

    public static void zSetDefaultTimezone(TimeZone timezone)
    {
        DEFAULT_TIMEZONE = DateTimeZone.forTimeZone(timezone);
    }

    public static Timestamp zConvertTimeForWritingWithUtcCalendar(Timestamp timestamp, TimeZone timeZone)
    {
        if (timestamp == null) return null;
        if (timeZone == UtcTimeZone) return timestamp;
        if (timeZone == DefaultTimeZone)
        {
            int offset = getDefaultOffset(timestamp);
            return createTimestampFromOffset(timestamp, offset);
        }
        return createTimestampFromOffset(timestamp, getOffsetFromTimeZoneToUtc(timeZone, timestamp));
    }

    public static Timestamp zFixInfinity(Timestamp toFix, TimeZone timeZone, Timestamp infinity)
    {
        if (toFix == null) return null;
        if (Math.abs(infinity.getTime() - toFix.getTime() ) < DAY_IN_MILLISECONDS)
        {
            int offset = getInfinityTimeZoneOffsetWithMemoisation(timeZone, infinity);
            if (toFix.getTime() == infinity.getTime() + offset)
            {
                toFix = infinity;
            }
        }
        return toFix;
    }

    private static final Function0<ConcurrentHashMap<Timestamp, Integer>> MAP_CONSTRUCTOR = new Function0<ConcurrentHashMap<Timestamp, Integer>>()
    {
        @Override
        public ConcurrentHashMap<Timestamp, Integer> value()
        {
            return ConcurrentHashMap.newMap();
        }
    };

    private static int getInfinityTimeZoneOffsetWithMemoisation(TimeZone timeZone, Timestamp infinity)
    {
        // There are not many distinct infinity timestamps or timezones so this map isn't going to be very large and we don't need any kind of purging algorithm
        String timeZoneID = timeZone.getID();

        ConcurrentHashMap<Timestamp, Integer> timeToOffset = infinityTimezoneOffsetCache.getIfAbsentPut(timeZoneID, MAP_CONSTRUCTOR);

        Integer offset = timeToOffset.get(infinity);
        if (offset == null)
        {
            offset = getOffsetFromTimeZoneToDefault(timeZone, infinity);
            timeToOffset.put(infinity, offset);
        }

        return offset;
    }

    public static Timestamp zConvertTimeForReadingWithUtcCalendar(Timestamp timestamp, TimeZone timeZone)
    {
        if (timestamp == null) return null;
        if (timeZone == UtcTimeZone) return timestamp;
        long utcTime = timestamp.getTime();
        if (timeZone == DefaultTimeZone || timeZone.equals(DefaultTimeZone))
        {
            int offset = getDefaultOffset(utcTime);
            if (offset != 0)
            {
                int nanos = timestamp.getNanos();
                timestamp.setTime(utcTime - offset);
                timestamp.setNanos(nanos);
                int realOffset = getDefaultOffset(timestamp.getTime());
                if (realOffset != offset)
                {
                    nanos = timestamp.getNanos();
                    timestamp.setTime(utcTime - realOffset);
                    timestamp.setNanos(nanos);
                }
            }
            return timestamp;
        }
        int offset = getOffsetFromTimeZoneToUtc(timeZone, timestamp);
        if (offset != 0)
        {
            int nanos = timestamp.getNanos();
            timestamp = new Timestamp(utcTime - offset);
            timestamp.setNanos(nanos);
            int realOffset = getOffsetFromTimeZoneToUtc(timeZone, timestamp);
            if (realOffset != offset)
            {
                nanos = timestamp.getNanos();
                timestamp.setTime(utcTime - realOffset);
                timestamp.setNanos(nanos);
            }
        }
        return timestamp;

    }
}
