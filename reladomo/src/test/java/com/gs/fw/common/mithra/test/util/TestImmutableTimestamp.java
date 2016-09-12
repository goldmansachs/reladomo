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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import junit.framework.TestCase;
import junit.framework.Assert;
import org.joda.time.DateTimeZone;

import java.sql.Timestamp;
import java.io.*;
import java.util.Date;
import java.util.TimeZone;


public class TestImmutableTimestamp extends TestCase
{

    public void testImmutableTimestamp()
    {
        long now = System.currentTimeMillis();
        for(int i=0;i<10000;i++) // 10K is a bit more than a year
        {
            checkTimestamp(now + i * 3600 * 1000);
        }
    }

    public void testImmutableSerialization()
    {
        ImmutableTimestamp ts = new ImmutableTimestamp(System.currentTimeMillis());

        Timestamp ts2 = serializeAndDeserialize(ts);
        assertEquals(ts.getTime(), ts2.getTime());
        assertEquals(ts.getNanos(), ts2.getNanos());

        ts = new ImmutableTimestamp(System.currentTimeMillis(), 123456789);
        ts2 = serializeAndDeserialize(ts);
        assertEquals(ts.getTime(), ts2.getTime());
        assertEquals(ts.getNanos(), ts2.getNanos());
    }

    private void checkTimestamp(long time)
    {
        Timestamp timestamp = new Timestamp(time);
        ImmutableTimestamp it = new ImmutableTimestamp(time);
        assertEquals(timestamp.getDate(), it.getDate());
        assertEquals(timestamp.getDay(), it.getDay());
        assertEquals(timestamp.getHours(), it.getHours());
        assertEquals(timestamp.getMinutes(), it.getMinutes());
        assertEquals(timestamp.getMonth(), it.getMonth());
        assertEquals(timestamp.getSeconds(), it.getSeconds());
        assertEquals(timestamp.getTimezoneOffset(), it.getTimezoneOffset());
        assertEquals(timestamp.getYear(), it.getYear());
        assertEquals(timestamp.getTimezoneOffset(), it.getTimezoneOffset());
        assertEquals(timestamp.toString(), it.toString());
    }

    private Timestamp serializeAndDeserialize(Timestamp original)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos);
            MithraTimestamp.writeTimestamp(objectOutputStream, original);
            objectOutputStream.flush();
            objectOutputStream.close();
            baos.close();
        }
        catch (IOException e)
        {
            MithraTestAbstract.getLogger().error("could not serialize object", e);
            Assert.fail("could not serialize object");
        }

        byte[] pileOfBytes = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(pileOfBytes);
        Timestamp deserialized = null;

        try
        {
            ObjectInputStream objectStream = new ObjectInputStream(bais);
            deserialized = MithraTimestamp.readTimestamp(objectStream);
        }
        catch (Exception e)
        {
            MithraTestAbstract.getLogger().error("could not deserialize object", e);
            Assert.fail("could not deserialize object");
        }

        return deserialized;
    }

    public void testJodaTimezoneOffset()
    {
        long now = System.currentTimeMillis();
        long twoYears = 1000L * 60 * 60 * 24* 365 * 2;
        TimeZone javaTimeZone = TimeZone.getDefault();
        DateTimeZone jodaTimeZone = DateTimeZone.getDefault();

        assertEquals(getOffsetFromTimeZone(javaTimeZone, new Date(1135090501017L)), jodaTimeZone.getOffset(1135090501017L));
        assertEquals(getOffsetFromTimeZone(javaTimeZone, new Date(1135040101017L)), jodaTimeZone.getOffset(1135040101017L));

        for(long i=0; i<twoYears;i+=500)
        {
            long testTime = now - i;
            assertEquals(getOffsetFromTimeZone(javaTimeZone, new Date(testTime)), jodaTimeZone.getOffset(testTime));
        }
    }

    protected static int getOffsetFromTimeZone(TimeZone tz, Date date)
    {
        int offset = tz.getRawOffset();
        if (tz.inDaylightTime(date))
        {
            offset += tz.getDSTSavings();
        }
        return offset;
    }

}
