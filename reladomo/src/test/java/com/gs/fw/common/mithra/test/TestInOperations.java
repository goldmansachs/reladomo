

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

import com.gs.collections.impl.set.mutable.primitive.*;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskList;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class TestInOperations
extends TestCase
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected Date getDawnOfTime()
    {
        try
        {
            return timestampFormat.parse("1900-01-01 00:00:00");
        }
        catch (ParseException e)
        {
            //never happens
        }
        return null;
    }

    public void testBooleanInOperatons()
    {
        final Boolean[] testBools = { true };

        final BooleanHashSet boolSet = new BooleanHashSet();
        boolSet.add(true);

        final Operation setOp = ParaDeskFinder.activeBoolean().in(boolSet);

        this.genericInOperationEqualsTest(testBools, ParaDeskFinder.activeBoolean(), setOp);
    }

    public void testByteInOperatons()
    {
        final Byte[] testBytes = {
                (byte) 10,
                (byte) 20,
                (byte) 30,
                (byte) 40,
                (byte) 50,
                (byte) 127,
                (byte) -127,
                (byte) -128,
                (byte) 127
        };

        final ByteHashSet byteSet = new ByteHashSet();
        for (byte b : testBytes)
        {
            byteSet.add(b);
        }
        
        final Operation setOp = ParaDeskFinder.locationByte().in(byteSet);

        this.genericInOperationEqualsTest(testBytes, ParaDeskFinder.locationByte(), setOp);
    }

    public void testCharacterInOperatons()
    {
        final Character[] testChars = { 'O', 'P', 'G', 'T' };

        final CharHashSet charSet = new CharHashSet();
        for (char c : testChars)
        {
            charSet.add(c);
        }

        final Operation setOp = ParaDeskFinder.statusChar().in(charSet);

        this.genericInOperationEqualsTest(testChars, ParaDeskFinder.statusChar(), setOp);
    }

    public void testDoubleInOperatons()
    {
        final Double[] testDoubles = { 4000000000.0d, 677673542.3d };

        final DoubleHashSet doubleSet = new DoubleHashSet();
        for (double dbl : testDoubles)
        {
            doubleSet.add(dbl);
        }

        final Operation setOp = ParaDeskFinder.sizeDouble().in(doubleSet);

        this.genericInOperationEqualsTest(testDoubles, ParaDeskFinder.sizeDouble(), setOp);
    }

    public void testFloatInOperatons()
    {
        final Float[] testFloats = { 4000000000.0f, 677673542.3f };

        final FloatHashSet floatSet = new FloatHashSet();
        for (float flt : testFloats)
        {
            floatSet.add(flt);
        }

        final Operation setOp = ParaDeskFinder.maxFloat().in(floatSet);

        this.genericInOperationEqualsTest(testFloats, ParaDeskFinder.maxFloat(), setOp);
    }

    public void testIntegerInOperatons()
    {
        final Integer[] testIntegers = { 100, 200, 300, 400, 500, 600, 700, 800, 900 };

        final IntHashSet IntHashSet = new IntHashSet();
        for (int i : testIntegers)
        {
            IntHashSet.add(i);
        }

        final Operation setOp = ParaDeskFinder.tagInt().in(IntHashSet);

        this.genericInOperationEqualsTest(testIntegers, ParaDeskFinder.tagInt(), setOp);
    }

    public void testLongInOperatons()
    {
        final Long[] testLongs = { 1000000L, 2000000L };

        final LongHashSet longSet = new LongHashSet();
        for (long lng : testLongs)
        {
            longSet.add(lng);
        }

        final Operation setOp = ParaDeskFinder.connectionLong().in(longSet);

        this.genericInOperationEqualsTest(testLongs, ParaDeskFinder.connectionLong(), setOp);
    }

    public void testShortInOperatons()
    {
        final Short[] testShorts = { 1000, 2000 };

        final ShortHashSet shortSet = new ShortHashSet();
        for (short shrt : testShorts)
        {
            shortSet.add(shrt);
        }

        final Operation setOp = ParaDeskFinder.minShort().in(shortSet);

        this.genericInOperationEqualsTest(testShorts, ParaDeskFinder.minShort(), setOp);
    }

    public void testStringInOperatons()
    {
        final String[] testStrings = { "rnd", "cap", "lsd", "zzz" };

        final Set<String> stringSet = new HashSet(Arrays.asList(testStrings));

        final Operation setOp = ParaDeskFinder.deskIdString().in(stringSet);

        this.genericInOperationEqualsTest(testStrings, ParaDeskFinder.deskIdString(), setOp);
    }

    public void testTimestampInOperatonsDawnOfTime()
    {
        final Timestamp[] testTimestamps = { new Timestamp(getDawnOfTime().getTime()), new Timestamp(System.currentTimeMillis()) };

        final Set<Timestamp> timestampSet = new HashSet(Arrays.asList(testTimestamps));

        final Operation setOp = ParaDeskFinder.createTimestamp().in(timestampSet);

        this.genericInOperationEqualsTest(testTimestamps, ParaDeskFinder.createTimestamp(), setOp);
    }

    public void testTimestampInOperatonsParaInfinityDate()
    {
        final Timestamp[] testTimestamps = { InfinityTimestamp.getParaInfinity(), new Timestamp(System.currentTimeMillis()) };

        final Set<Timestamp> timestampSet = new HashSet(Arrays.asList(testTimestamps));

        final Operation setOp = ParaDeskFinder.createTimestamp().in(timestampSet);

        this.genericInOperationEqualsTest(testTimestamps, ParaDeskFinder.createTimestamp(), setOp);
    }

    public void testDateInOperatons()
    {
        final Date[] testDates = { getDawnOfTime(), new Date() };

        final Set<Date> dateSet = new HashSet(Arrays.asList(testDates));

        final Operation setOp = ParaDeskFinder.closedDate().in(dateSet);

        this.genericInOperationEqualsTest(testDates, ParaDeskFinder.closedDate(), setOp);
    }


    private <T> void genericInOperationEqualsTest(final T[] testvalues, final Attribute<ParaDesk, T> attribute, final Operation setOp)
    {
        final ParaDeskList obsList = new ParaDeskList();
        for (T value : testvalues)
        {
            final ParaDesk paraDesk = new ParaDesk();
            attribute.setValue(paraDesk, value);
            obsList.add(paraDesk);
        }

        final Operation listOp = attribute.in(obsList, attribute);

        final Iterable<ParaDesk> objsIterable = new HashSet(obsList);
        final Operation iterableOp = attribute.in(objsIterable, attribute);

        assertEquals(setOp, listOp);
        assertEquals(setOp, iterableOp);
        assertEquals(listOp, iterableOp);
    }

}
