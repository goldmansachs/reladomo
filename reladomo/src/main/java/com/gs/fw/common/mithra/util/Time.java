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

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.TimeZone;

public class Time implements Comparable<Time>, Serializable
{
    private static final long serialVersionUID = 0L;

    private static Logger logger;
    private byte hour;
    private byte minute;
    private byte second;
    private int nano;

    protected Time(int hour, int minute, int second, int millisecond)
    {
        this.hour = (byte) hour;
        this.minute = (byte) minute;
        this.second = (byte) second;
        this.nano = millisecond * 1000000;
    }

    protected Time(int hour, int minute, int second, int nanos, boolean dummy)
    {
        this.hour = (byte) hour;
        this.minute = (byte) minute;
        this.second = (byte) second;
        this.nano = nanos;
    }

    public static Time withNanos(int hour, int minute, int second, int nanos)
    {
        return new Time(hour, minute, second, nanos, true);
    }

    public static Time withMillis(int hour, int minute, int second, int millis)
    {
        return new Time(hour, minute, second, millis);
    }

    public static Time withSqlTime(java.sql.Time sqlTime)
    {
        if (sqlTime == null) return null;

        LocalDateTime localDateTime = new LocalDateTime(sqlTime.getTime());
        return Time.withMillis(localDateTime.getHourOfDay(), localDateTime.getMinuteOfHour(), localDateTime.getSecondOfMinute(), localDateTime.getMillisOfSecond());
    }

    public static Time offHeap(long offHeapValue)
    {
        if (offHeapValue == TimestampPool.OFF_HEAP_NULL) return null;
        return new Time((int)(offHeapValue >> 48), (int) ((offHeapValue >> 40) & 0xFF), (int) ((offHeapValue >> 32) & 0xFF), (int) (offHeapValue & 0xFFFFFFFF), true);
    }

    public long getOffHeapTime()
    {
        return (((long)hour) << 48) | (((long) minute) << 40) | (((long)second) << 32) | nano;
    }

    public byte getHour()
    {
        return this.hour;
    }

    public byte getMinute()
    {
        return this.minute;
    }

    public byte getSecond()
    {
        return this.second;
    }

    public int getMillisecond()
    {
        return this.nano / 1000000;
    }

    public int getNano()
    {
        return this.nano;
    }

    @Override
    public int compareTo(Time time)
    {
        return (int) (this.getTime() - time.getTime());
    }

    public long getTime()
    {
        return this.getTimeInMillis();
    }

    public java.sql.Time convertToSql()
    {
        return new java.sql.Time(getTime());
    }

    public Time createOrReturnTimeWithSybaseMillis()
    {
        int milliseconds = this.getMillisecond();
        int newMillis = 0;
        //996 or greater, 996
        if (milliseconds > 996)
        {
            newMillis = 996;
        }
        else
        {
            int mod = milliseconds % 10;

            if (mod == 0 || mod == 3 || mod == 6)
            {
                return this;
            }
            else
            {
                if (mod == 2 || mod == 5 || mod == 9)
                {
                    newMillis = milliseconds + 1;
                }
                else if (mod == 1 || mod == 4)
                {
                    newMillis = milliseconds - 1;
                }
                else
                {
                    int difference = mod - 6;
                    newMillis = milliseconds - difference;
                }
            }
        }
        return Time.withMillis(this.hour, this.minute, this.second, newMillis);
    }

    public Time createOrReturnTimeWithRoundingForSybaseJConnectCompatibility()
    {
        int milliseconds = this.getMillisecond();
        int newMillis = 0;
        if (milliseconds >= 996)
        {
            // This logic is to shield applications from a bug observed in jConnect 7.07 which causes 999 milliseconds to round up to the nearest second (1.000).
            // The behaviour in jConnect 6 and in Sybase itself (when it converts quoted time literals) is to round everything >= 996 millis down to 996 millis.
            newMillis = 996;
        }
        else
        {
            return this;
        }
        return Time.withMillis(this.hour, this.minute, this.second, newMillis);
    }

    public Time createOrReturnTimeTenMillisecond()
    {
        int milliseconds = this.getMillisecond();

        if(milliseconds % 10 == 0)
            return this;
        return Time.withMillis(this.hour, this.minute, this.second, (this.getMillisecond() / 10) * 10);
    }

    private void readObject(java.io.ObjectInputStream s)
            throws java.io.IOException, ClassNotFoundException
    {
        this.hour = s.readByte();
        this.minute = s.readByte();
        this.second = s.readByte();
        this.nano = s.readInt();
    }

    private void writeObject(java.io.ObjectOutputStream s)
            throws java.io.IOException
    {
        s.writeByte(this.hour);
        s.writeByte(this.minute);
        s.writeByte(this.second);
        s.writeInt(this.nano);
    }

    public static Time readFromStream(ObjectInput in) throws IOException
    {
        byte isNull = in.readByte();
        if (isNull == 0)
        {
            return Time.withNanos(in.readByte(), in.readByte(), in.readByte(), in.readInt());
        }
        return null;
    }

    public static void writeToStream(ObjectOutput out, Time time) throws IOException
    {
        if (time == null)
        {
            out.writeByte(1);
        }
        else
        {
            out.writeByte(0);
            out.writeByte(time.getHour());
            out.writeByte(time.getMinute());
            out.writeByte(time.getSecond());
            out.writeInt(time.getNano());
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Time)) return false;

        Time time = (Time) o;

        if (this.hour != time.hour) return false;
        if (this.minute != time.minute) return false;
        if (this.second != time.second) return false;
        if (this.nano != time.nano) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return (this.hour * 3600000 + this.minute * 60000 + this.second * 1000) ^ this.nano;
    }

    protected static final ISOChronology ISO_CHRONOLOGY_LOCAL = ISOChronology.getInstance(DateTimeZone.forTimeZone(TimeZone.getDefault()));

    private long getTimeInMillis()
    {
        try
        {
            return ISO_CHRONOLOGY_LOCAL.getDateTimeMillis(1970, 1, 1, this.hour, this.minute, this.second, this.getMillisecond());
        }
        catch (IllegalArgumentException e)
        {
            getLogger().warn(e.getMessage());
            return ISO_CHRONOLOGY_LOCAL.getDateTimeMillis(1970, 1, 1, this.hour, this.minute, this.second, this.getMillisecond());
        }
    }

    private static Logger getLogger()
    {
        if (logger == null)
        {
            logger = LoggerFactory.getLogger(Time.class.getName());
        }
        return logger;
    }

    @Override
    public String toString()
    {
        StringBuffer buffer = new StringBuffer(12);
        String hour = this.hour + "";
        String minute = this.minute + "";
        String second = this.second + "";
        int millis = this.nano / 1000000;
        String milliString = millis + "";

        if (this.hour < 10)
        {
            hour = "0" + this.hour;
        }
        if (this.minute < 10)
        {
            minute = "0" + this.minute;
        }
        if (this.second < 10)
        {
            second = "0" + this.second;
        }
        if (millis < 100)
        {
            if (millis < 10)
                milliString = "00" + millis;
            else
                milliString = "0" + millis;
        }
        buffer.append(hour).append(':').append(minute).append(':').append(second).append('.').append(milliString);
        return buffer.toString();
    }
}
