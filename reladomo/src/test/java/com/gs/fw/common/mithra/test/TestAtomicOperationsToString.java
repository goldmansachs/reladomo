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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.test.domain.DatedAllTypes;
import com.gs.fw.common.mithra.test.domain.DatedAllTypesFinder;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.ByteHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.CharHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.DoubleHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.FloatHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.ShortHashSet;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;


public class TestAtomicOperationsToString extends MithraTestAbstract
{
    private static final Format TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    private static final Timestamp BUSINESS_TIMESTAMP = Timestamp.valueOf("2010-12-31 23:59:00.0");
    private static final Date BUSINESS_DATE = new Date(Timestamp.valueOf("2010-12-31 00:00:00.0").getTime());
    private static final String BUSINESS_DATE_UNQUOTED_STRING = "Fri Dec 31 00:00:00 EST 2010";
    private static final String BUSINESS_DATE_QUOTED_STRING = quote(BUSINESS_DATE_UNQUOTED_STRING);
    private static final String BUSINESS_TIMESTAMP_UNQUOTED_STRING = TIMESTAMP_FORMAT.format(BUSINESS_TIMESTAMP);
    private static final String BUSINESS_TIMESTAMP_QUOTED_STRING = quote(BUSINESS_TIMESTAMP_UNQUOTED_STRING);

    private static final Timestamp BUSINESS_TIMESTAMP2 = Timestamp.valueOf("2011-01-01 23:59:00.0");
    private static final Date BUSINESS_DATE2 = new Date(Timestamp.valueOf("2011-01-01 00:00:00.0").getTime());
    private static final String BUSINESS_DATE2_UNQUOTED_STRING = BUSINESS_DATE2.toString();
    private static final String BUSINESS_DATE2_QUOTED_STRING = quote(BUSINESS_DATE2_UNQUOTED_STRING);
    private static final String BUSINESS_TIMESTAMP2_UNQUOTED_STRING = TIMESTAMP_FORMAT.format(BUSINESS_TIMESTAMP2);
    private static final String BUSINESS_TIMESTAMP2_QUOTED_STRING = quote(BUSINESS_TIMESTAMP2_UNQUOTED_STRING);

    private static final String VALUE_STRING = "Value";
    private static final String VALUE2_STRING = "Value2";
    private static final String VALUE_QUOTED_STRING = quote(VALUE_STRING);
    private static final String VALUE2_QUOTED_STRING = quote(VALUE2_STRING);
    private static final String WILDCARD_STRING = "Value?";
    private static final String WILDCARD_QUOTED_STRING = quote(WILDCARD_STRING);

    private static String quote(String in)
    {
        return '"' + in + '"';
    }

    public void testIsNullOperation()
    {
        Operation nullableStringValueEquals = DatedAllTypesFinder.nullableStringValue().isNull();
        assertEquals("DatedAllTypes.nullableStringValue is null", nullableStringValueEquals.toString());
    }
    
    public void testIsNotNullOperation()
    {
        Operation nullableStringValueEquals = DatedAllTypesFinder.nullableStringValue().isNotNull();
        assertEquals("DatedAllTypes.nullableStringValue is not null", nullableStringValueEquals.toString());
    }
    
    public void testEqualsEdgePointOperation()
    {
        Operation asOfEqualsEdgePoint = DatedAllTypesFinder.businessDate().equalsEdgePoint();
        assertEquals("DatedAllTypes.businessDate equalsEdgePoint", asOfEqualsEdgePoint.toString());
    }

    public void testEqualsOperation()
    {
        Operation booleanValueEquals = DatedAllTypesFinder.booleanValue().eq(true);
        assertEquals("DatedAllTypes.booleanValue = true", booleanValueEquals.toString());

        Operation byteValueEquals = DatedAllTypesFinder.byteValue().eq(toByte(2));
        assertEquals("DatedAllTypes.byteValue = 2", byteValueEquals.toString());

        Operation shortValueEquals = DatedAllTypesFinder.shortValue().eq((short)3);
        assertEquals("DatedAllTypes.shortValue = 3", shortValueEquals.toString());

        Operation charValueEquals = DatedAllTypesFinder.charValue().eq('A');
        assertEquals("DatedAllTypes.charValue = A", charValueEquals.toString());

        Operation intValueEquals = DatedAllTypesFinder.intValue().eq(4);
        assertEquals("DatedAllTypes.intValue = 4", intValueEquals.toString());

        Operation longValueEquals = DatedAllTypesFinder.longValue().eq(5L);
        assertEquals("DatedAllTypes.longValue = 5", longValueEquals.toString());

        Operation floatValueEquals = DatedAllTypesFinder.floatValue().eq((float)6.6);
        assertEquals("DatedAllTypes.floatValue = 6.6", floatValueEquals.toString());

        Operation doubleValueEquals = DatedAllTypesFinder.doubleValue().eq(7.7);
        assertEquals("DatedAllTypes.doubleValue = 7.7", doubleValueEquals.toString());

        Operation dateValueEquals = DatedAllTypesFinder.dateValue().eq(BUSINESS_DATE);
        assertEquals("DatedAllTypes.dateValue = " + BUSINESS_DATE_QUOTED_STRING, dateValueEquals.toString());

        Operation timestampValueEquals = DatedAllTypesFinder.timestampValue().eq(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.timestampValue = " + BUSINESS_TIMESTAMP_QUOTED_STRING, timestampValueEquals.toString());

        Operation stringValueEquals = DatedAllTypesFinder.stringValue().eq(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue = " + VALUE_QUOTED_STRING, stringValueEquals.toString());

        Operation byteArrayValueEquals = DatedAllTypesFinder.byteArrayValue().eq("Value".getBytes());
        assertEquals("DatedAllTypes.byteArrayValue = [86, 97, 108, 117, 101]", byteArrayValueEquals.toString());

        Operation asOfEqualsDate = DatedAllTypesFinder.businessDate().eq(BUSINESS_DATE);
        assertEquals("DatedAllTypes.businessDate = " + quote("2010-12-31 00:00:00.0"), asOfEqualsDate.toString());
        
        Operation asOfEqualsTimestamp = DatedAllTypesFinder.businessDate().eq(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.businessDate = " + BUSINESS_TIMESTAMP_QUOTED_STRING, asOfEqualsTimestamp.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().eq(1234.56789);
        assertEquals("DatedAllTypes.bigDecimalValue = 1234.56789", bigDecimalValueEquals.toString());
    }

    public void testNonPrimitiveEqualsOperation()
    {
        Operation booleanValueEquals = DatedAllTypesFinder.booleanValue().nonPrimitiveEq(Boolean.TRUE);
        assertEquals("DatedAllTypes.booleanValue = true", booleanValueEquals.toString());

        Operation byteValueEquals = DatedAllTypesFinder.byteValue().nonPrimitiveEq(Byte.valueOf((byte)2));
        assertEquals("DatedAllTypes.byteValue = 2", byteValueEquals.toString());

        Operation shortValueEquals = DatedAllTypesFinder.shortValue().nonPrimitiveEq(Short.valueOf((short)3));
        assertEquals("DatedAllTypes.shortValue = 3", shortValueEquals.toString());

        Operation charValueEquals = DatedAllTypesFinder.charValue().nonPrimitiveEq(Character.valueOf('A'));
        assertEquals("DatedAllTypes.charValue = A", charValueEquals.toString());

        Operation intValueEquals = DatedAllTypesFinder.intValue().nonPrimitiveEq(Integer.valueOf(4));
        assertEquals("DatedAllTypes.intValue = 4", intValueEquals.toString());

        Operation longValueEquals = DatedAllTypesFinder.longValue().nonPrimitiveEq(Long.valueOf(5L));
        assertEquals("DatedAllTypes.longValue = 5", longValueEquals.toString());

        Operation floatValueEquals = DatedAllTypesFinder.floatValue().nonPrimitiveEq(Float.valueOf((float)6.6));
        assertEquals("DatedAllTypes.floatValue = 6.6", floatValueEquals.toString());

        Operation doubleValueEquals = DatedAllTypesFinder.doubleValue().nonPrimitiveEq(Double.valueOf(7.7));
        assertEquals("DatedAllTypes.doubleValue = 7.7", doubleValueEquals.toString());

        Operation dateValueEquals = DatedAllTypesFinder.dateValue().nonPrimitiveEq(BUSINESS_DATE);
        assertEquals("DatedAllTypes.dateValue = " + BUSINESS_DATE_QUOTED_STRING, dateValueEquals.toString());

        Operation timestampValueEquals = DatedAllTypesFinder.timestampValue().nonPrimitiveEq(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.timestampValue = " + BUSINESS_TIMESTAMP_QUOTED_STRING, timestampValueEquals.toString());

        Operation stringValueEquals = DatedAllTypesFinder.stringValue().nonPrimitiveEq(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue = " + VALUE_QUOTED_STRING, stringValueEquals.toString());

        Operation byteArrayValueEquals = DatedAllTypesFinder.byteArrayValue().nonPrimitiveEq("Value".getBytes());
        assertEquals("DatedAllTypes.byteArrayValue = [86, 97, 108, 117, 101]", byteArrayValueEquals.toString());

        Operation asOfEqualsTimestamp = DatedAllTypesFinder.businessDate().nonPrimitiveEq(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.businessDate = " + BUSINESS_TIMESTAMP_QUOTED_STRING, asOfEqualsTimestamp.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().nonPrimitiveEq(new BigDecimal("1234.56789"));
        assertEquals("DatedAllTypes.bigDecimalValue = 1234.56789", bigDecimalValueEquals.toString());
    }

    public void testNotEqualsOperation()
    {
        Operation booleanValueNotEquals = DatedAllTypesFinder.booleanValue().notEq(true);
        assertEquals("DatedAllTypes.booleanValue != true", booleanValueNotEquals.toString());

        Operation byteValueNotEquals = DatedAllTypesFinder.byteValue().notEq(toByte(2));
        assertEquals("DatedAllTypes.byteValue != 2", byteValueNotEquals.toString());

        Operation shortValueNotEquals = DatedAllTypesFinder.shortValue().notEq((short)3);
        assertEquals("DatedAllTypes.shortValue != 3", shortValueNotEquals.toString());

        Operation charValueNotEquals = DatedAllTypesFinder.charValue().notEq('A');
        assertEquals("DatedAllTypes.charValue != A", charValueNotEquals.toString());

        Operation intValueNotEquals = DatedAllTypesFinder.intValue().notEq(4);
        assertEquals("DatedAllTypes.intValue != 4", intValueNotEquals.toString());

        Operation longValueNotEquals = DatedAllTypesFinder.longValue().notEq(5L);
        assertEquals("DatedAllTypes.longValue != 5", longValueNotEquals.toString());

        Operation floatValueNotEquals = DatedAllTypesFinder.floatValue().notEq((float)6.6);
        assertEquals("DatedAllTypes.floatValue != 6.6", floatValueNotEquals.toString());

        Operation doubleValueNotEquals = DatedAllTypesFinder.doubleValue().notEq(7.7);
        assertEquals("DatedAllTypes.doubleValue != 7.7", doubleValueNotEquals.toString());

        Operation dateValueNotEquals = DatedAllTypesFinder.dateValue().notEq(BUSINESS_DATE);
        assertEquals("DatedAllTypes.dateValue != " + BUSINESS_DATE_QUOTED_STRING, dateValueNotEquals.toString());

        Operation timestampValueNotEquals = DatedAllTypesFinder.timestampValue().notEq(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.timestampValue != " + BUSINESS_TIMESTAMP_QUOTED_STRING, timestampValueNotEquals.toString());

        Operation stringValueNotEquals = DatedAllTypesFinder.stringValue().notEq(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue != " + VALUE_QUOTED_STRING, stringValueNotEquals.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().notEq(1234.56789);
        assertEquals("DatedAllTypes.bigDecimalValue != 1234.56789", bigDecimalValueEquals.toString());
    }

    public void testGreaterThanOperation()
    {
        Operation byteValueGreaterTran = DatedAllTypesFinder.byteValue().greaterThan(toByte(2));
        assertEquals("DatedAllTypes.byteValue > 2", byteValueGreaterTran.toString());

        Operation shortValueGreaterTran = DatedAllTypesFinder.shortValue().greaterThan((short)3);
        assertEquals("DatedAllTypes.shortValue > 3", shortValueGreaterTran.toString());

        Operation charValueGreaterTran = DatedAllTypesFinder.charValue().greaterThan('A');
        assertEquals("DatedAllTypes.charValue > A", charValueGreaterTran.toString());

        Operation intValueGreaterTran = DatedAllTypesFinder.intValue().greaterThan(4);
        assertEquals("DatedAllTypes.intValue > 4", intValueGreaterTran.toString());

        Operation longValueGreaterTran = DatedAllTypesFinder.longValue().greaterThan(5L);
        assertEquals("DatedAllTypes.longValue > 5", longValueGreaterTran.toString());

        Operation floatValueGreaterTran = DatedAllTypesFinder.floatValue().greaterThan((float)6.6);
        assertEquals("DatedAllTypes.floatValue > 6.6", floatValueGreaterTran.toString());

        Operation doubleValueGreaterTran = DatedAllTypesFinder.doubleValue().greaterThan(7.7);
        assertEquals("DatedAllTypes.doubleValue > 7.7", doubleValueGreaterTran.toString());

        Operation dateValueGreaterTran = DatedAllTypesFinder.dateValue().greaterThan(BUSINESS_DATE);
        assertEquals("DatedAllTypes.dateValue > " + BUSINESS_DATE_QUOTED_STRING, dateValueGreaterTran.toString());

        Operation timestampValueGreaterTran = DatedAllTypesFinder.timestampValue().greaterThan(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.timestampValue > " + BUSINESS_TIMESTAMP_QUOTED_STRING, timestampValueGreaterTran.toString());

        Operation stringValueGreaterTran = DatedAllTypesFinder.stringValue().greaterThan(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue > " + VALUE_QUOTED_STRING, stringValueGreaterTran.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().greaterThan(1234.56789);
        assertEquals("DatedAllTypes.bigDecimalValue > 1234.56789", bigDecimalValueEquals.toString());
    }

    public void testGreaterThanEqualsOperation()
    {
        Operation byteValueGreaterTranEquals = DatedAllTypesFinder.byteValue().greaterThanEquals(toByte(2));
        assertEquals("DatedAllTypes.byteValue >= 2", byteValueGreaterTranEquals.toString());

        Operation shortValueGreaterTranEquals = DatedAllTypesFinder.shortValue().greaterThanEquals((short)3);
        assertEquals("DatedAllTypes.shortValue >= 3", shortValueGreaterTranEquals.toString());

        Operation charValueGreaterTranEquals = DatedAllTypesFinder.charValue().greaterThanEquals('A');
        assertEquals("DatedAllTypes.charValue >= A", charValueGreaterTranEquals.toString());

        Operation intValueGreaterTranEquals = DatedAllTypesFinder.intValue().greaterThanEquals(4);
        assertEquals("DatedAllTypes.intValue >= 4", intValueGreaterTranEquals.toString());

        Operation longValueGreaterTranEquals = DatedAllTypesFinder.longValue().greaterThanEquals(5L);
        assertEquals("DatedAllTypes.longValue >= 5", longValueGreaterTranEquals.toString());

        Operation floatValueGreaterTranEquals = DatedAllTypesFinder.floatValue().greaterThanEquals((float)6.6);
        assertEquals("DatedAllTypes.floatValue >= 6.6", floatValueGreaterTranEquals.toString());

        Operation doubleValueGreaterTranEquals = DatedAllTypesFinder.doubleValue().greaterThanEquals(7.7);
        assertEquals("DatedAllTypes.doubleValue >= 7.7", doubleValueGreaterTranEquals.toString());

        Operation dateValueGreaterTranEquals = DatedAllTypesFinder.dateValue().greaterThanEquals(BUSINESS_DATE);
        assertEquals("DatedAllTypes.dateValue >= " + BUSINESS_DATE_QUOTED_STRING, dateValueGreaterTranEquals.toString());

        Operation timestampValueGreaterTranEquals = DatedAllTypesFinder.timestampValue().greaterThanEquals(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.timestampValue >= " + BUSINESS_TIMESTAMP_QUOTED_STRING, timestampValueGreaterTranEquals.toString());

        Operation stringValueGreaterTranEquals = DatedAllTypesFinder.stringValue().greaterThanEquals(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue >= " + VALUE_QUOTED_STRING, stringValueGreaterTranEquals.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().greaterThanEquals(1234.56789);
        assertEquals("DatedAllTypes.bigDecimalValue >= 1234.56789", bigDecimalValueEquals.toString());
    }

    public void testLessThanOperation()
    {
        Operation byteValueLessThan = DatedAllTypesFinder.byteValue().lessThan(toByte(2));
        assertEquals("DatedAllTypes.byteValue < 2", byteValueLessThan.toString());

        Operation shortValueLessThan = DatedAllTypesFinder.shortValue().lessThan((short)3);
        assertEquals("DatedAllTypes.shortValue < 3", shortValueLessThan.toString());

        Operation charValueLessThan = DatedAllTypesFinder.charValue().lessThan('A');
        assertEquals("DatedAllTypes.charValue < A", charValueLessThan.toString());

        Operation intValueLessThan = DatedAllTypesFinder.intValue().lessThan(4);
        assertEquals("DatedAllTypes.intValue < 4", intValueLessThan.toString());

        Operation longValueLessThan = DatedAllTypesFinder.longValue().lessThan(5L);
        assertEquals("DatedAllTypes.longValue < 5", longValueLessThan.toString());

        Operation floatValueLessThan = DatedAllTypesFinder.floatValue().lessThan((float)6.6);
        assertEquals("DatedAllTypes.floatValue < 6.6", floatValueLessThan.toString());

        Operation doubleValueLessThan = DatedAllTypesFinder.doubleValue().lessThan(7.7);
        assertEquals("DatedAllTypes.doubleValue < 7.7", doubleValueLessThan.toString());

        Operation dateValueLessThan = DatedAllTypesFinder.dateValue().lessThan(BUSINESS_DATE);
        assertEquals("DatedAllTypes.dateValue < " + BUSINESS_DATE_QUOTED_STRING, dateValueLessThan.toString());

        Operation timestampValueLessThan = DatedAllTypesFinder.timestampValue().lessThan(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.timestampValue < " + BUSINESS_TIMESTAMP_QUOTED_STRING, timestampValueLessThan.toString());

        Operation stringValueLessThan = DatedAllTypesFinder.stringValue().lessThan(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue < " + VALUE_QUOTED_STRING, stringValueLessThan.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().lessThan(1234.56789);
        assertEquals("DatedAllTypes.bigDecimalValue < 1234.56789", bigDecimalValueEquals.toString());
    }

    public void testLessThanEqualsOperation()
    {
        Operation byteValueLessThanEquals = DatedAllTypesFinder.byteValue().lessThanEquals(toByte(2));
        assertEquals("DatedAllTypes.byteValue <= 2", byteValueLessThanEquals.toString());

        Operation shortValueLessThanEquals = DatedAllTypesFinder.shortValue().lessThanEquals((short)3);
        assertEquals("DatedAllTypes.shortValue <= 3", shortValueLessThanEquals.toString());

        Operation charValueLessThanEquals = DatedAllTypesFinder.charValue().lessThanEquals('A');
        assertEquals("DatedAllTypes.charValue <= A", charValueLessThanEquals.toString());

        Operation intValueLessThanEquals = DatedAllTypesFinder.intValue().lessThanEquals(4);
        assertEquals("DatedAllTypes.intValue <= 4", intValueLessThanEquals.toString());

        Operation longValueLessThanEquals = DatedAllTypesFinder.longValue().lessThanEquals(5L);
        assertEquals("DatedAllTypes.longValue <= 5", longValueLessThanEquals.toString());

        Operation floatValueLessThanEquals = DatedAllTypesFinder.floatValue().lessThanEquals((float)6.6);
        assertEquals("DatedAllTypes.floatValue <= 6.6", floatValueLessThanEquals.toString());

        Operation doubleValueLessThanEquals = DatedAllTypesFinder.doubleValue().lessThanEquals(7.7);
        assertEquals("DatedAllTypes.doubleValue <= 7.7", doubleValueLessThanEquals.toString());

        Operation dateValueLessThanEquals = DatedAllTypesFinder.dateValue().lessThanEquals(BUSINESS_DATE);
        assertEquals("DatedAllTypes.dateValue <= " + BUSINESS_DATE_QUOTED_STRING, dateValueLessThanEquals.toString());

        Operation timestampValueLessThanEquals = DatedAllTypesFinder.timestampValue().lessThanEquals(BUSINESS_TIMESTAMP);
        assertEquals("DatedAllTypes.timestampValue <= " + BUSINESS_TIMESTAMP_QUOTED_STRING, timestampValueLessThanEquals.toString());

        Operation stringValueLessThanEquals = DatedAllTypesFinder.stringValue().lessThanEquals(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue <= " + VALUE_QUOTED_STRING, stringValueLessThanEquals.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().lessThanEquals(1234.56789);
        assertEquals("DatedAllTypes.bigDecimalValue <= 1234.56789", bigDecimalValueEquals.toString());
    }

    public void testStringLikeOperations()
    {
        Operation stringValueEndsWith = DatedAllTypesFinder.stringValue().endsWith(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue endsWith " + VALUE_QUOTED_STRING, stringValueEndsWith.toString());

        Operation stringValueContains = DatedAllTypesFinder.stringValue().contains(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue contains " + VALUE_QUOTED_STRING, stringValueContains.toString());

        Operation stringValueStartsWith = DatedAllTypesFinder.stringValue().startsWith(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue startsWith " + VALUE_QUOTED_STRING, stringValueStartsWith.toString());

        Operation stringValueWildcardEquals = DatedAllTypesFinder.stringValue().wildCardEq(WILDCARD_STRING);
        assertEquals("DatedAllTypes.stringValue wildCardEquals " + WILDCARD_QUOTED_STRING, stringValueWildcardEquals.toString());
    }

    public void testStringNotLikeOperations()
    {
        Operation stringValueNotEndsWith = DatedAllTypesFinder.stringValue().notEndsWith(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue not endsWith " + VALUE_QUOTED_STRING, stringValueNotEndsWith.toString());

        Operation stringValueNotContains = DatedAllTypesFinder.stringValue().notContains(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue not contains " + VALUE_QUOTED_STRING, stringValueNotContains.toString());

        Operation stringValueNotStartsWith = DatedAllTypesFinder.stringValue().notStartsWith(VALUE_STRING);
        assertEquals("DatedAllTypes.stringValue not startsWith " + VALUE_QUOTED_STRING, stringValueNotStartsWith.toString());

        Operation stringValueWildcardNotEquals = DatedAllTypesFinder.stringValue().wildCardNotEq(WILDCARD_STRING);
        assertEquals("DatedAllTypes.stringValue not wildCardEquals " + WILDCARD_QUOTED_STRING, stringValueWildcardNotEquals.toString());
    }

    public void testInOperation()
    {
        ByteHashSet gscByteSet = ByteHashSet.newSetWith(new byte[] { (byte) 2, (byte) 3 });
        Operation byteValueIn = DatedAllTypesFinder.byteValue().in(gscByteSet);
        assertEqualsEither(
                "DatedAllTypes.byteValue in [2, 3]",
                "DatedAllTypes.byteValue in [3, 2]",
                byteValueIn.toString());

        ShortHashSet gscShortSet = ShortHashSet.newSetWith(new short[] { (short) 3, (short) 4 });
        Operation shortValueIn = DatedAllTypesFinder.shortValue().in(gscShortSet);
        assertEqualsEither(
                "DatedAllTypes.shortValue in [3, 4]",
                "DatedAllTypes.shortValue in [4, 3]",
                shortValueIn.toString());

        CharHashSet gscCharSet = CharHashSet.newSetWith(new char[] { 'A', 'B' });
        Operation charValueIn = DatedAllTypesFinder.charValue().in(gscCharSet);
        assertEqualsEither(
                "DatedAllTypes.charValue in [A, B]",
                "DatedAllTypes.charValue in [B, A]",
                charValueIn.toString());

        IntHashSet gscIntHashSet = IntHashSet.newSetWith(new int[] { 4, 5 });
        Operation intValueIn = DatedAllTypesFinder.intValue().in(gscIntHashSet);
        assertEqualsEither(
                "DatedAllTypes.intValue in [4, 5]",
                "DatedAllTypes.intValue in [5, 4]",
                intValueIn.toString());

        LongHashSet gscLongSet = LongHashSet.newSetWith(new long[] { 5L, 6L });
        Operation longValueIn = DatedAllTypesFinder.longValue().in(gscLongSet);
        assertEqualsEither(
                "DatedAllTypes.longValue in [5, 6]",
                "DatedAllTypes.longValue in [6, 5]",
                longValueIn.toString());

        FloatHashSet gscFloatSet = FloatHashSet.newSetWith(new float[] { (float) 6.6, (float) 7.7 });
        Operation floatValueIn = DatedAllTypesFinder.floatValue().in(gscFloatSet);
        assertEqualsEither(
                "DatedAllTypes.floatValue in [6.6, 7.7]",
                "DatedAllTypes.floatValue in [7.7, 6.6]",
                floatValueIn.toString());

        DoubleHashSet gscDoubleSet = DoubleHashSet.newSetWith(new double[] { 7.7, 8.8 });
        Operation doubleValueIn = DatedAllTypesFinder.doubleValue().in(gscDoubleSet);
        assertEqualsEither(
                "DatedAllTypes.doubleValue in [7.7, 8.8]",
                "DatedAllTypes.doubleValue in [8.8, 7.7]",
                doubleValueIn.toString());

        Set<Date> dateSet = new TreeSet<Date>(UnifiedSet.newSetWith(BUSINESS_DATE, BUSINESS_DATE2));
        Operation dateValueIn = DatedAllTypesFinder.dateValue().in(dateSet);
        assertEquals("DatedAllTypes.dateValue in [" + BUSINESS_DATE_QUOTED_STRING + ", " + BUSINESS_DATE2_QUOTED_STRING + "]", dateValueIn.toString());

        Set<Timestamp> timestampSet = new TreeSet<Timestamp>(UnifiedSet.newSetWith(BUSINESS_TIMESTAMP, BUSINESS_TIMESTAMP2));
        Operation timestampValueIn = DatedAllTypesFinder.timestampValue().in(timestampSet);
        assertEquals("DatedAllTypes.timestampValue in [" + BUSINESS_TIMESTAMP_QUOTED_STRING + ", " + BUSINESS_TIMESTAMP2_QUOTED_STRING + "]", timestampValueIn.toString());

        Set<String> stringSet = new TreeSet<String>(UnifiedSet.newSetWith(VALUE_STRING, VALUE2_STRING));
        Operation stringValueIn = DatedAllTypesFinder.stringValue().in(stringSet);
        assertEquals("DatedAllTypes.stringValue in [" + VALUE_QUOTED_STRING + ", " + VALUE2_QUOTED_STRING + "]", stringValueIn.toString());

        ByteArraySet byteArraySet = new ByteArraySet();
        byteArraySet.add(new byte[] {2,3});
        byteArraySet.add(new byte[] {4,5});
        //TODO: How to ensure order?
        Operation byteArrayValueIn = DatedAllTypesFinder.byteArrayValue().in(byteArraySet);
        assertEquals("DatedAllTypes.byteArrayValue in [[2, 3], [4, 5]]", byteArrayValueIn.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().in(gscDoubleSet);
        assertEquals("DatedAllTypes.bigDecimalValue in [7.70000, 8.80000]", bigDecimalValueEquals.toString());
    }

    public void testNotInOperation()
    {
        ByteHashSet gscByteSet = ByteHashSet.newSetWith(new byte[] { 2, 3 });
        Operation byteValueNotIn = DatedAllTypesFinder.byteValue().notIn(gscByteSet);
        assertEqualsEither(
                "DatedAllTypes.byteValue not in [2, 3]",
                "DatedAllTypes.byteValue not in [3, 2]",
                byteValueNotIn.toString());

        ShortHashSet gscShortSet = ShortHashSet.newSetWith(new short[] { 3, 4 });
        Operation shortValueNotIn = DatedAllTypesFinder.shortValue().notIn(gscShortSet);
        assertEqualsEither(
                "DatedAllTypes.shortValue not in [3, 4]",
                "DatedAllTypes.shortValue not in [4, 3]",
                shortValueNotIn.toString());

        CharHashSet gscCharSet = CharHashSet.newSetWith(new char[] { 'A', 'B' });
        Operation charValueNotIn = DatedAllTypesFinder.charValue().notIn(gscCharSet);
        assertEqualsEither(
                "DatedAllTypes.charValue not in [A, B]",
                "DatedAllTypes.charValue not in [B, A]",
                charValueNotIn.toString());

        IntHashSet gscIntHashSet = IntHashSet.newSetWith(new int[] { 4, 5 });
        Operation intValueNotIn = DatedAllTypesFinder.intValue().notIn(gscIntHashSet);
        assertEqualsEither(
                "DatedAllTypes.intValue not in [4, 5]",
                "DatedAllTypes.intValue not in [5, 4]",
                intValueNotIn.toString());

        LongHashSet gscLongSet = LongHashSet.newSetWith(new long[] { 5L, 6L });
        Operation longValueNotIn = DatedAllTypesFinder.longValue().notIn(gscLongSet);
        assertEqualsEither(
                "DatedAllTypes.longValue not in [5, 6]",
                "DatedAllTypes.longValue not in [6, 5]",
                longValueNotIn.toString());

        FloatHashSet gscFloatSet = FloatHashSet.newSetWith(new float[] { (float) 6.6, (float) 7.7 });
        Operation floatValueNotIn = DatedAllTypesFinder.floatValue().notIn(gscFloatSet);
        assertEqualsEither(
                "DatedAllTypes.floatValue not in [6.6, 7.7]",
                "DatedAllTypes.floatValue not in [7.7, 6.6]",
                floatValueNotIn.toString());

        DoubleHashSet gscDoubleSet = DoubleHashSet.newSetWith(new double[] { 7.7, 8.8 });
        Operation doubleValueNotIn = DatedAllTypesFinder.doubleValue().notIn(gscDoubleSet);
        assertEqualsEither(
                "DatedAllTypes.doubleValue not in [7.7, 8.8]",
                "DatedAllTypes.doubleValue not in [8.8, 7.7]",
                doubleValueNotIn.toString());

        Set<Date> dateSet = new TreeSet<Date>(UnifiedSet.newSetWith(BUSINESS_DATE, BUSINESS_DATE2));
        Operation dateValueNotIn = DatedAllTypesFinder.dateValue().notIn(dateSet);
        assertEquals("DatedAllTypes.dateValue not in [" + BUSINESS_DATE_QUOTED_STRING + ", " + BUSINESS_DATE2_QUOTED_STRING + "]", dateValueNotIn.toString());

        Set<Timestamp> timestampSet = new TreeSet<Timestamp>(UnifiedSet.newSetWith(BUSINESS_TIMESTAMP, BUSINESS_TIMESTAMP2));
        Operation timestampValueNotIn = DatedAllTypesFinder.timestampValue().notIn(timestampSet);
        assertEquals("DatedAllTypes.timestampValue not in [" + BUSINESS_TIMESTAMP_QUOTED_STRING + ", " + BUSINESS_TIMESTAMP2_QUOTED_STRING + "]", timestampValueNotIn.toString());

        Set<String> stringSet = new TreeSet<String>(UnifiedSet.newSetWith(VALUE_STRING, VALUE2_STRING));
        Operation stringValueNotIn = DatedAllTypesFinder.stringValue().notIn(stringSet);
        assertEquals("DatedAllTypes.stringValue not in [" + VALUE_QUOTED_STRING + ", " + VALUE2_QUOTED_STRING + "]", stringValueNotIn.toString());

        ByteArraySet byteArraySet = new ByteArraySet();
        byteArraySet.add(new byte[] {2, 3});
        byteArraySet.add(new byte[] {4, 5});
        //TODO: How to ensure order?
        Operation byteArrayValueNotIn = DatedAllTypesFinder.byteArrayValue().notIn(byteArraySet);
        assertEquals("DatedAllTypes.byteArrayValue not in [[2, 3], [4, 5]]", byteArrayValueNotIn.toString());

        Operation bigDecimalValueEquals = DatedAllTypesFinder.bigDecimalValue().notIn(gscDoubleSet);
        assertEquals("DatedAllTypes.bigDecimalValue not in [7.70000, 8.80000]", bigDecimalValueEquals.toString());
    }

    private void assertEqualsEither(String expected1, String expected2, String actual)
    {
        if (!actual.equals(expected1) && !actual.equals(expected2))
        {
            fail("Actual string was not expected1 or expected2: actual=" + actual);
        }
    }
}

