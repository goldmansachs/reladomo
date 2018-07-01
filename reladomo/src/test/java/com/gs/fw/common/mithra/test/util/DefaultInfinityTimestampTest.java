package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import junit.framework.TestCase;

public class DefaultInfinityTimestampTest extends TestCase
{
    public void testDefaultIQInfinityTimestmap ()
    {
        MithraTimestamp t = DefaultInfinityTimestamp.getSybaseIqInfinity ();
        assertEquals("9999-12-01 00:00:00.0", t.toString ());
    }
    public void testIqSmalldateInfinity  ()
    {
        MithraTimestamp t = DefaultInfinityTimestamp.getIqSmalldateInfinity ();
        assertEquals("9999-12-30 12:00:00.0", t.toString ());
    }

    public void testDefaultInfinity ()
    {
        MithraTimestamp t = DefaultInfinityTimestamp.getDefaultInfinity ();
        assertEquals("9999-12-01 23:59:00.0", t.toString ());
    }
    public void testDefaultSmalldateInfinity ()
    {
        MithraTimestamp t = DefaultInfinityTimestamp.getDefaultSmalldateInfinity ();
        assertEquals("2079-06-06 23:59:00.0", t.toString ());
    }
}
