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

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.desk.aggregate.AggregateTrialCurrencyExposure;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public class TestNullRetrieval
extends MithraTestAbstract
{
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            NullTest.class, TestPositionPrice.class
        };
    }

    public void testAllNotNull()
    throws Exception
    {
        NullTestList nulltests = new NullTestList(NullTestFinder.notNullString().eq("AllNotNull"));
        assertEquals(1, nulltests.size());
        NullTest nullTest = nulltests.getNullTestAt(0);

        assertFalse(nullTest.isNoDefaultNullBooleanNull());
        assertFalse(nullTest.isNoDefaultNullByteNull());
        assertFalse(nullTest.isNoDefaultNullCharNull());
        assertFalse(nullTest.isNoDefaultNullDateNull());
        assertFalse(nullTest.isNoDefaultNullDoubleNull());
        assertFalse(nullTest.isNoDefaultNullFloatNull());
        assertFalse(nullTest.isNoDefaultNullIntNull());
        assertFalse(nullTest.isNoDefaultNullLongNull());
        assertFalse(nullTest.isNoDefaultNullShortNull());
        assertFalse(nullTest.isNoDefaultNullStringNull());
        assertFalse(nullTest.isNoDefaultNullTimestampNull());
        assertFalse(nullTest.isNotNullBooleanNull());
        assertFalse(nullTest.isNotNullByteNull());
        assertFalse(nullTest.isNotNullCharNull());
        assertFalse(nullTest.isNotNullDateNull());
        assertFalse(nullTest.isNotNullDoubleNull());
        assertFalse(nullTest.isNotNullFloatNull());
        assertFalse(nullTest.isNotNullIntNull());
        assertFalse(nullTest.isNotNullLongNull());
        assertFalse(nullTest.isNotNullShortNull());
        assertFalse(nullTest.isNotNullStringNull());
        assertFalse(nullTest.isNotNullTimestampNull());
        assertFalse(nullTest.isNullableBooleanNull());
        assertFalse(nullTest.isNullableByteNull());
        assertFalse(nullTest.isNullableCharNull());
        assertFalse(nullTest.isNullableDateNull());
        assertFalse(nullTest.isNullableDoubleNull());
        assertFalse(nullTest.isNullableFloatNull());
        assertFalse(nullTest.isNullableIntNull());
        assertFalse(nullTest.isNullableLongNull());
        assertFalse(nullTest.isNullableShortNull());
        assertFalse(nullTest.isNullableStringNull());
        assertFalse(nullTest.isNullableTimestampNull());
    }

    public void testAllNullableNull()
    throws Exception
    {
        NullTestList nulltests = new NullTestList(NullTestFinder.notNullString().eq("AllNullableNull"));
        assertEquals(1, nulltests.size());
        NullTest nullTest = nulltests.getNullTestAt(0);

        assertTrue(nullTest.isNoDefaultNullBooleanNull());
        assertTrue(nullTest.isNoDefaultNullByteNull());
        assertTrue(nullTest.isNoDefaultNullCharNull());
        assertTrue(nullTest.isNoDefaultNullDateNull());
        assertTrue(nullTest.isNoDefaultNullDoubleNull());
        assertTrue(nullTest.isNoDefaultNullFloatNull());
        assertTrue(nullTest.isNoDefaultNullIntNull());
        assertTrue(nullTest.isNoDefaultNullLongNull());
        assertTrue(nullTest.isNoDefaultNullShortNull());
        assertTrue(nullTest.isNoDefaultNullStringNull());
        assertTrue(nullTest.isNoDefaultNullTimestampNull());
        assertFalse(nullTest.isNotNullBooleanNull());
        assertFalse(nullTest.isNotNullByteNull());
        assertFalse(nullTest.isNotNullCharNull());
        assertFalse(nullTest.isNotNullDateNull());
        assertFalse(nullTest.isNotNullDoubleNull());
        assertFalse(nullTest.isNotNullFloatNull());
        assertFalse(nullTest.isNotNullIntNull());
        assertFalse(nullTest.isNotNullLongNull());
        assertFalse(nullTest.isNotNullShortNull());
        assertFalse(nullTest.isNotNullStringNull());
        assertFalse(nullTest.isNotNullTimestampNull());
        assertTrue(nullTest.isNullableBooleanNull());
        assertTrue(nullTest.isNullableByteNull());
        assertTrue(nullTest.isNullableCharNull());
        assertTrue(nullTest.isNullableDateNull());
        assertTrue(nullTest.isNullableDoubleNull());
        assertTrue(nullTest.isNullableFloatNull());
        assertTrue(nullTest.isNullableIntNull());
        assertTrue(nullTest.isNullableLongNull());
        assertTrue(nullTest.isNullableShortNull());
        assertTrue(nullTest.isNullableStringNull());
        assertTrue(nullTest.isNullableTimestampNull());

        assertEquals(12, nullTest.getNullableDouble(), 0);
        assertEquals(12, nullTest.getNullableFloat(), 0);
        assertEquals(12, nullTest.getNullableInt());
        assertEquals(12, nullTest.getNullableByte());
        assertEquals(' ', nullTest.getNullableChar());
        assertEquals(12, nullTest.getNullableShort());
        assertEquals(12, nullTest.getNullableLong());
    }

    public void testNullBits()
    {
        AggregateTrialCurrencyExposure obj1 = new AggregateTrialCurrencyExposure();
        obj1.setTradingNull();
        assertFalse(obj1.isNpvAjustmentNull());
        assertFalse(obj1.isModelAdjustmentNull());
        assertFalse(obj1.isBidOfferAdjustmentNull());
        
        AggregateTrialCurrencyExposure obj2 = new AggregateTrialCurrencyExposure();
        obj2.setDividendPaidReceivedNull();
        assertFalse(obj2.isNpvAjustmentNull());
        assertFalse(obj2.isModelAdjustmentNull());
        assertFalse(obj2.isBidOfferAdjustmentNull());

        TestObj32Nullables obj3 = new TestObj32Nullables();
        obj3.setNullableField10Null();
        assertFalse(obj3.isNullableField30Null());
        assertFalse(obj3.isNullableField31Null());

        TestObj32Nullables obj4 = new TestObj32Nullables();
        obj4.setNullableField30Null();
        assertTrue(obj4.isNullableField30Null());
        assertFalse(obj4.isNullableField31Null());

        TestObj32Nullables obj5 = new TestObj32Nullables();
        obj5.setNullableField31Null();
        assertFalse(obj5.isNullableField30Null());
        assertTrue(obj5.isNullableField31Null());


        TestObj16Nullables obj6 = new TestObj16Nullables();
        obj6.setNullableField10Null();
        assertTrue(obj6.isNullableField10Null());
        assertFalse(obj6.isNullableField14Null());
        assertFalse(obj6.isNullableField15Null());

        TestObj16Nullables obj7 = new TestObj16Nullables();
        assertFalse(obj7.isNullableField15Null());
        obj7.setNullableField15Null();
        assertTrue(obj7.isNullableField15Null());

        TestObj8Nullables obj8 = new TestObj8Nullables();
        obj8.setNullableField5Null();
        assertTrue(obj8.isNullableField5Null());
        assertFalse(obj8.isNullableField6Null());
        assertFalse(obj8.isNullableField7Null());

        TestObj8Nullables obj9 = new TestObj8Nullables();
        assertFalse(obj9.isNullableField7Null());
        obj9.setNullableField7Null();
        assertTrue(obj9.isNullableField7Null());
    }

    public void testNoDefaultNull()
    {
        String sql;
        List nulltests;

        sql = "select * from NULL_TEST where NOT_NULL_STRING = 'NoDefaultNull'";
        nulltests = new NullTestList(NullTestFinder.noDefaultNullString().eq("NoDefaultNull"));

        try
        {
            this.genericRetrievalTest(sql, nulltests, 0);
        }
        catch (SQLException e)
        {
            assertTrue(e.getMessage().matches("attribute*is null in database and a default is not specified in mithra xml"));
        }
    }

    public void testIsNull()
    {
        NullTest allNullable = NullTestFinder.findOne(NullTestFinder.notNullString().eq("AllNullableNull"));
        NullTest nullTest = NullTestFinder.findOne(NullTestFinder.nullableBoolean().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableByte().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableChar().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableDate().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableDouble().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableFloat().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableInt().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableLong().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableShort().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableString().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableTimestamp().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullBoolean().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullByte().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullChar().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullDate().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullDouble().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullFloat().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullInt().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullLong().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullShort().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullString().isNull());
        assertSame(allNullable, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullTimestamp().isNull());
        assertSame(allNullable, nullTest);
    }

    public void testIsNotNull()
    {
        NullTest notNull = NullTestFinder.findOne(NullTestFinder.notNullString().eq("AllNotNull"));
        NullTest nullTest = NullTestFinder.findOne(NullTestFinder.nullableBoolean().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableByte().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableChar().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableDate().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableDouble().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableFloat().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableInt().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableLong().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableShort().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableString().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.nullableTimestamp().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullBoolean().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullByte().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullChar().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullDate().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullDouble().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullFloat().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullInt().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullLong().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullShort().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullString().isNotNull());
        assertSame(notNull, nullTest);
        nullTest = NullTestFinder.findOne(NullTestFinder.noDefaultNullTimestamp().isNotNull());
        assertSame(notNull, nullTest);
    }

    public void testDatedObjectWithNull()
    {
        TestPositionPrice price = TestPositionPriceFinder.findOne(TestPositionPriceFinder.accountId().eq("7880003907").
                and(TestPositionPriceFinder.businessDate().equalsEdgePoint()).
                and(TestPositionPriceFinder.acmapCode().eq("A")));
        assertNotNull(price);
        assertEquals(12, price.getSourceId());
        assertEquals(17, price.getPriceType());
        assertTrue(price.isSourceIdNull());
        assertTrue(price.isPriceTypeNull());
    }

    public void testDefaultValueForNull()
    {
        NullTest nullTest = new NullTest();
        nullTest.setNullableDoubleNull();
        assertEquals(12.0, nullTest.getNullableDouble(), 0.0);
    }

    public void testDefaultValueForNullAfterInsert()
    {
        NullTest nullTest = new NullTest();
        nullTest.setNotNullString("A");
        nullTest.setNotNullBoolean(true);
        nullTest.setNotNullDouble(1.0);
        nullTest.setNotNullLong(1);
        nullTest.setNotNullInt(1);
        nullTest.setNotNullChar('A');
        nullTest.setNotNullTimestamp(new Timestamp(System.currentTimeMillis()));
        nullTest.setNotNullByte((byte)1);
        nullTest.setNotNullDate(new Date());
        nullTest.setNotNullFloat((float)1.0);
        nullTest.setNotNullShort((short)1);
        nullTest.setNullableDoubleNull();
        nullTest.insert();
        assertEquals(12.0, nullTest.getNullableDouble(), 0.0);
    }

    public void testDefaultValueForNullAfterInsertWithFind()
    {
        NullTest nullTest = new NullTest();
        nullTest.setNotNullString("A");
        nullTest.setNotNullBoolean(true);
        nullTest.setNotNullDouble(1.0);
        nullTest.setNotNullLong(1);
        nullTest.setNotNullInt(1);
        nullTest.setNotNullChar('A');
        nullTest.setNotNullTimestamp(new Timestamp(System.currentTimeMillis()));
        nullTest.setNotNullByte((byte)1);
        nullTest.setNotNullDate(new Date());
        nullTest.setNotNullFloat((float)1.0);
        nullTest.setNotNullShort((short)1);
        nullTest.setNullableDoubleNull();
        nullTest.insert();
        assertEquals(12.0, NullTestFinder.findOne(NullTestFinder.notNullString().eq("A")).getNullableDouble(), 0.0);
    }

    public void testDatedObjectDefaultValueForNull()
    {
        TestPositionPrice price  = new TestPositionPrice(new Timestamp(System.currentTimeMillis()));
        price.setPriceTypeNull();
        assertEquals(17, price.getPriceType());
    }

    public void testDatedObjectDefaultValueForNullAfterInsert()
    {
        final TestPositionPrice price  = new TestPositionPrice(new Timestamp(System.currentTimeMillis()));
        price.setAcmapCode("A");
        price.setAccountId("123456789");
        price.setProductId(1);
        price.setCurrency("USD");
        price.setPositionType(1);
        price.setBalanceType(1);
        price.setPriceTypeNull();
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand()
        {

            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                price.insert();
                return null;
            }
        });
        assertEquals(17, price.getPriceType());
    }
    public void testDatedObjectDefaultValueForNullAfterInsertWithFind()
    {
        final TestPositionPrice price  = new TestPositionPrice(new Timestamp(System.currentTimeMillis()));
        price.setAcmapCode("A");
        price.setAccountId("123456789");
        price.setProductId(1);
        price.setCurrency("USD");
        price.setPositionType(1);
        price.setBalanceType(1);
        price.setPriceTypeNull();
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand()
        {

            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                price.insert();
                return null;
            }
        });

        TestPositionPrice result = TestPositionPriceFinder.findOne(TestPositionPriceFinder.acmapCode().eq("A")
                                                                           .and(TestPositionPriceFinder.productId().eq(1))
                                                                           .and(TestPositionPriceFinder.accountId().eq("123456789"))
                                                                           .and(TestPositionPriceFinder.businessDate().equalsEdgePoint()));

        assertEquals(17, result.getPriceType());
    }
}
