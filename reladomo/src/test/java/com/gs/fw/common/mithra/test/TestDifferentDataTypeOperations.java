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
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrial;

import java.sql.SQLException;


public class TestDifferentDataTypeOperations extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            DifferentDataTypes.class,
            MappedDifferentDataTypes.class,
            TamsMithraAccount.class,
            TestTamsMithraTrial.class,
            VariousTypes.class
        };
    }

    public void setUp()
            throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new DifferentDataTypesResultSetComparator());
    }

    public void testBooleanEqOperation()
            throws SQLException
    {
        // test Eq Operation
        Operation op = DifferentDataTypesFinder.booleanColumn().eq(true);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN = 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByBooleanColumn().booleanColumn().eq(true);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.BOOLEAN_COLUMN = D.BOOLEAN_COLUMN and V.BOOLEAN_COLUMN = 1";
        this.genericRetrievalTest(sql, list);

        // test Not Eq Operation
        op = DifferentDataTypesFinder.booleanColumn().notEq(true);
        list = new DifferentDataTypesList(op);

        sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN != 1";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.booleanColumn().notEq(true)));
        this.genericRetrievalTest(sql, list);

        // test Mapped Attribute
        op = DifferentDataTypesFinder.variousByBooleanColumn().booleanColumn().eq(true);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where D.BOOLEAN_COLUMN = V.BOOLEAN_COLUMN and V.BOOLEAN_COLUMN = 1";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.variousByBooleanColumn().booleanColumn().eq(true)));
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where D.BOOLEAN_COLUMN = V.BOOLEAN_COLUMN and V.BOOLEAN_COLUMN = 1";
        this.genericRetrievalTest(sql, list);
    }

    public void testBooleanInOperation()
            throws SQLException
    {
        // test In Operation
        BooleanHashSet booleanSet = new BooleanHashSet();
        booleanSet.add(false);
        booleanSet.add(true);

        Operation op = DifferentDataTypesFinder.booleanColumn().in(booleanSet);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN in (1,0)";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByBooleanColumn().booleanColumn().in(booleanSet);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.BOOLEAN_COLUMN = D.BOOLEAN_COLUMN and V.BOOLEAN_COLUMN in (0, 1)";
        this.genericRetrievalTest(sql, list);

        // test Not In Operation
        booleanSet.remove(true);
        op = DifferentDataTypesFinder.booleanColumn().notIn(booleanSet);
        list = new DifferentDataTypesList(op);

        sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN not in (0)";
        this.genericRetrievalTest(sql, list);

        BooleanHashSet booleanSet1 = new BooleanHashSet();
        booleanSet1.add(false);
        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.booleanColumn().notIn(booleanSet1)).and(DifferentDataTypesFinder.booleanColumn().notIn(booleanSet)));
        this.genericRetrievalTest(sql, list);
    }

    public void testByteEqLessGreaterOperation()
            throws SQLException
    {
        // test Eq Operation
        Operation op = DifferentDataTypesFinder.byteColumn().eq((byte)0);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN = 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByByteColumn().byteColumn().eq((byte) 0);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.BYTE_COLUMN = D.BYTE_COLUMN and V.BYTE_COLUMN = 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().eq((byte) 1).and(DifferentDataTypesFinder.longColumn().eq(235));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN = 1 and LONG_COLUMN = 235";
        this.genericRetrievalTest(sql, list);

        // test LessThanEquals Operation
        op = DifferentDataTypesFinder.byteColumn().lessThanEquals((byte)5).and(DifferentDataTypesFinder.byteColumn().eq((byte)1))
                .and(DifferentDataTypesFinder.longColumn().greaterThan(10));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN <= 5 and BYTE_COLUMN = 1 and LONG_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().lessThanEquals((byte)5).and(DifferentDataTypesFinder.byteColumn().greaterThan((byte)1))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN <= 5 and BYTE_COLUMN > 1 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().lessThanEquals((byte)5).and(DifferentDataTypesFinder.byteColumn().greaterThanEquals((byte)1))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN <= 5 and BYTE_COLUMN >= 1 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().lessThanEquals((byte)5).and(DifferentDataTypesFinder.byteColumn().lessThanEquals((byte)100))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN <= 5 and BYTE_COLUMN <= 100 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().lessThanEquals((byte)5).and(DifferentDataTypesFinder.byteColumn().lessThan((byte)100))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN <= 5 and BYTE_COLUMN < 100 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        // test LessThan Operation
        op = DifferentDataTypesFinder.byteColumn().lessThan((byte)5).and(DifferentDataTypesFinder.byteColumn().greaterThan((byte)1))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN < 5 and BYTE_COLUMN > 1 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().lessThan((byte)5).and(DifferentDataTypesFinder.byteColumn().greaterThanEquals((byte)1))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN < 5 and BYTE_COLUMN >= 1 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        // test GreaterThanEquals Operation
        op = DifferentDataTypesFinder.byteColumn().greaterThanEquals((byte)0).and(DifferentDataTypesFinder.byteColumn().greaterThan((byte)1))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN >= 0 and BYTE_COLUMN > 1 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().greaterThanEquals((byte)0).and(DifferentDataTypesFinder.byteColumn().eq((byte)1))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN >= 0 and BYTE_COLUMN = 1 and FLOAT_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().greaterThanEquals((byte)0).and(DifferentDataTypesFinder.byteColumn().greaterThanEquals((byte)-2));
        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.byteColumn().greaterThanEquals((byte) -1)));
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN >= 0 and BYTE_COLUMN >= -1 and BYTE_COLUMN >= -2";
        this.genericRetrievalTest(sql, list);

        // test Not Eq Operation
        op = DifferentDataTypesFinder.byteColumn().notEq((byte)0);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN != 0";
        this.genericRetrievalTest(sql, list, false);
        int databaseQuery = MithraManager.getInstance().getDatabaseRetrieveCount();

        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN != 0";
        this.genericRetrievalTest(sql, list);
        assertEquals("Check Database Retrieve Count", databaseQuery, MithraManager.getInstance().getDatabaseRetrieveCount());

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.byteColumn().notEq((byte)3)));
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN != 0 and BYTE_COLUMN != 3";
        this.genericRetrievalTest(sql, list);
    }

    public void testByteInOperation()
            throws SQLException
    {
        // test In Operation
        ByteHashSet byteSet = new ByteHashSet();
        byteSet.add((byte)1);
        byteSet.add((byte)3);

        Operation op = DifferentDataTypesFinder.byteColumn().in(byteSet);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN in (1, 3)";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.byteColumn().in(byteSet).and(DifferentDataTypesFinder.byteColumn().greaterThan(((byte)2)));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN in (1, 3) and BYTE_COLUMN > 2";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByByteColumn().byteColumn().in(byteSet);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.BYTE_COLUMN = D.BYTE_COLUMN and V.BYTE_COLUMN in (1, 3)";
        this.genericRetrievalTest(sql, list);

        byteSet.add((byte)2);
        byteSet.add((byte)4);
        byteSet.add((byte)5);
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short)120);
        shortSet.add((short)100);

        op = DifferentDataTypesFinder.byteColumn().in(byteSet).and(DifferentDataTypesFinder.shortColumn().in(shortSet));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN in (1, 3, 2, 4, 5) and SHORT_COLUMN in (120, 100)";
        this.genericRetrievalTest(sql, list);

        // test Not In Operation
        byteSet.remove((byte)1);
        op = DifferentDataTypesFinder.byteColumn().notIn(byteSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN not in (3, 2, 4, 5)";
        this.genericRetrievalTest(sql, list);

        ByteHashSet byteSet1 = new ByteHashSet();
        byteSet1.add((byte)6);
        byteSet1.add((byte)7);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.byteColumn().notIn(byteSet)).and(DifferentDataTypesFinder.byteColumn().notIn(byteSet1)));
        sql = "select * from DIFFERENT_DATA_TYPES where BYTE_COLUMN not in (3, 2, 4, 5) and BYTE_COLUMN not in (6, 7)";
        this.genericRetrievalTest(sql, list);
    }

    public void testDoubleEqLessGreaterOperation()
            throws SQLException
    {
        // test Equals Operation
        Operation op = DifferentDataTypesFinder.doubleColumn().eq(1000.0);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN = 1000.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByDoubleColumn().doubleColumn().eq(100.0);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.DOUBLE_COLUMN = D.DOUBLE_COLUMN and V.DOUBLE_COLUMN = 100.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.doubleColumn().eq(1110.0).and(DifferentDataTypesFinder.longColumn().eq(235));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN = 1110.0 and LONG_COLUMN = 235";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.doubleColumn().eq(1110.0);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN = 1110.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.doubleColumn().eq(1110.0)));
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN = 1110.0";
        this.genericRetrievalTest(sql, list);

        // test LessThanEquals Operation
        op = DifferentDataTypesFinder.doubleColumn().lessThanEquals(500.0).and(DifferentDataTypesFinder.doubleColumn().greaterThan(0.0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN <= 500.0 and DOUBLE_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.doubleColumn().lessThanEquals(500.0).and(DifferentDataTypesFinder.doubleColumn().greaterThanEquals(0.0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN <= 500.0 and DOUBLE_COLUMN >= 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.doubleColumn().lessThanEquals(500.0).and(DifferentDataTypesFinder.doubleColumn().lessThan(1000.0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN <= 500.0 and DOUBLE_COLUMN < 1000.0";
        this.genericRetrievalTest(sql, list);

        // test LessThan Operation
        op = DifferentDataTypesFinder.doubleColumn().lessThan(500.0).and(DifferentDataTypesFinder.doubleColumn().greaterThan(0.0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN < 500.0 and DOUBLE_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.doubleColumn().lessThan(500.0).and(DifferentDataTypesFinder.doubleColumn().greaterThanEquals(0.0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN < 500.0 and DOUBLE_COLUMN >= 0";
        this.genericRetrievalTest(sql, list);

        // test GreaterThanEquals Operation
        op = DifferentDataTypesFinder.doubleColumn().greaterThanEquals(500.0).and(DifferentDataTypesFinder.doubleColumn().greaterThan(0.0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN >= 500.0 and DOUBLE_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        // test Not Eq Operation
        op = DifferentDataTypesFinder.doubleColumn().notEq(5000.0);
        list = new DifferentDataTypesList(op);

        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN != 5000.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.doubleColumn().notEq(5000.0)).and(DifferentDataTypesFinder.doubleColumn().notEq(1000.0)));
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN != 5000.0 and DOUBLE_COLUMN != 1000.0";
        this.genericRetrievalTest(sql, list);

    }

    public void testDoubleInOperation()
            throws SQLException
    {
        // test In Operation
        DoubleHashSet doubleSet = new DoubleHashSet();
        doubleSet.add(1110.0);
        doubleSet.add(100.0);
        doubleSet.add(10.0);

        Operation op = DifferentDataTypesFinder.doubleColumn().in(doubleSet);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN in (1110.0, 100.0, 10.0)";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.doubleColumn().in(doubleSet).and(DifferentDataTypesFinder.byteColumn().greaterThanEquals(((byte)1)));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN in (1110.0, 100.0, 10.0) and BYTE_COLUMN >= 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByDoubleColumn().doubleColumn().in(doubleSet);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.DOUBLE_COLUMN = D.DOUBLE_COLUMN and V.DOUBLE_COLUMN in (1110.0, 100.0, 10.0)";
        this.genericRetrievalTest(sql, list);

        doubleSet.add(125.0);
        doubleSet.add(235.0);
        doubleSet.add(435.0);
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short)120);
        shortSet.add((short)100);

        op = DifferentDataTypesFinder.doubleColumn().in(doubleSet).and(DifferentDataTypesFinder.shortColumn().in(shortSet));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN in (1110.0, 100.0, 10.0, 125.0, 235.0, 435.0) and SHORT_COLUMN in (120, 100)";
        this.genericRetrievalTest(sql, list);

        // test Not In Operation
        op = DifferentDataTypesFinder.doubleColumn().notIn(doubleSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN not in (1110.0, 100.0, 10.0, 125.0, 235.0, 435.0)";
        this.genericRetrievalTest(sql, list);

        DoubleHashSet doubleSet1 = new DoubleHashSet();
        doubleSet1.add(11100.0);
        doubleSet1.add(1001.0);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.doubleColumn().notIn(doubleSet)).and(DifferentDataTypesFinder.doubleColumn().notIn(doubleSet1)));
        sql = "select * from DIFFERENT_DATA_TYPES where DOUBLE_COLUMN not in (1110.0, 100.0, 10.0, 125.0, 235.0, 435.0) and DOUBLE_COLUMN not in (11100.0, 1001.0)";
        this.genericRetrievalTest(sql, list);
    }

    public void testFloatEqLessGreaterOperation()
            throws SQLException
    {
        // test Eq Operation
        Operation op = DifferentDataTypesFinder.floatColumn().eq(15.0f);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN = 15.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByFloatColumn().floatColumn().eq(15.0f);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.FLOAT_COLUMN = D.FLOAT_COLUMN and V.FLOAT_COLUMN = 15.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().eq(15.0f).and(DifferentDataTypesFinder.longColumn().eq(45));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN = 15.0 and LONG_COLUMN = 45";
        this.genericRetrievalTest(sql, list);

        // test Less Than Equals Operation
        op = DifferentDataTypesFinder.floatColumn().lessThanEquals(500.0f).and(DifferentDataTypesFinder.floatColumn().eq(15.0f))
                .and(DifferentDataTypesFinder.longColumn().greaterThan(10));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN <= 500.0 and FLOAT_COLUMN = 15.0 and LONG_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().lessThanEquals(500.0f).and(DifferentDataTypesFinder.floatColumn().greaterThan(0.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN <= 500.0 and FLOAT_COLUMN > 0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().lessThanEquals(500.0f).and(DifferentDataTypesFinder.floatColumn().greaterThanEquals(0.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN <= 500.0 and FLOAT_COLUMN >= 0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().lessThanEquals(500.0f).and(DifferentDataTypesFinder.floatColumn().lessThanEquals(1000.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN <= 500.0 and FLOAT_COLUMN <= 1000.0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().lessThanEquals(500.0f).and(DifferentDataTypesFinder.floatColumn().lessThan(1000.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN <= 500.0 and FLOAT_COLUMN < 1000.0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        // test Greater Than Equals Operation
        op = DifferentDataTypesFinder.floatColumn().greaterThanEquals(5.0f).and(DifferentDataTypesFinder.floatColumn().eq(15.0f))
                .and(DifferentDataTypesFinder.longColumn().greaterThan(10));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN >= 5.0 and FLOAT_COLUMN = 15.0 and LONG_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().greaterThanEquals(5.0f).and(DifferentDataTypesFinder.floatColumn().greaterThan(0.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN >= 5.0 and FLOAT_COLUMN > 0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().greaterThanEquals(5.0f).and(DifferentDataTypesFinder.floatColumn().greaterThanEquals(0.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN >= 5.0 and FLOAT_COLUMN >= 0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().greaterThanEquals(5.0f).and(DifferentDataTypesFinder.floatColumn().lessThanEquals(1000.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN >= 5.0 and FLOAT_COLUMN <= 1000.0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().greaterThanEquals(5.0f).and(DifferentDataTypesFinder.floatColumn().lessThan(1000.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN >= 5.0 and FLOAT_COLUMN < 1000.0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().greaterThanEquals(5.0f);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN >= 5.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.floatColumn().greaterThanEquals(10.0f)).and(DifferentDataTypesFinder.floatColumn().greaterThanEquals(5.0f)));
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN >= 5.0 and FLOAT_COLUMN >= 10.0";
        this.genericRetrievalTest(sql, list);

        // test Less Than Operation
        op = DifferentDataTypesFinder.floatColumn().lessThan(5000.0f).and(DifferentDataTypesFinder.floatColumn().lessThan(1000.0f))
                .and(DifferentDataTypesFinder.floatColumn().greaterThan(10.0f));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN < 5000.0 and FLOAT_COLUMN < 1000.0 and FLOAT_COLUMN > 10.0";
        this.genericRetrievalTest(sql, list);

        // test Not Eq Operation
        op = DifferentDataTypesFinder.floatColumn().notEq(5000.0f);
        list = new DifferentDataTypesList(op);

        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN != 5000.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.floatColumn().notEq(5000.0f)).and(DifferentDataTypesFinder.floatColumn().notEq(50.0f)));
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN != 5000.0 and FLOAT_COLUMN != 50.0";
        this.genericRetrievalTest(sql, list);
    }

    public void testFloatInOperation()
            throws SQLException
    {
        // test In Operation
        FloatHashSet floatSet = new FloatHashSet();
        floatSet.add(15.0f);
        floatSet.add(25.0f);
        floatSet.add(45.0f);

        Operation op = DifferentDataTypesFinder.floatColumn().in(floatSet);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN in (15.0, 25.0, 45.0)";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.floatColumn().in(floatSet).and(DifferentDataTypesFinder.floatColumn().greaterThanEquals(1));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN in (15.0, 25.0, 45.0) and FLOAT_COLUMN >= 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByFloatColumn().floatColumn().in(floatSet);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.FLOAT_COLUMN = D.FLOAT_COLUMN and V.FLOAT_COLUMN in (15.0, 25.0, 45.0)";
        this.genericRetrievalTest(sql, list);

        floatSet.add(125.0f);
        floatSet.add(235.0f);
        floatSet.add(435.0f);
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short)120);
        shortSet.add((short)100);

        op = DifferentDataTypesFinder.floatColumn().in(floatSet).and(DifferentDataTypesFinder.shortColumn().in(shortSet));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN in (15.0, 25.0, 45.0, 125.0, 235.0, 435.0) and SHORT_COLUMN in (120, 100)";
        this.genericRetrievalTest(sql, list);

        floatSet.remove(15.0f);
        floatSet.remove(25.0f);
        floatSet.remove(45.0f);
        op = DifferentDataTypesFinder.floatColumn().notIn(floatSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where FLOAT_COLUMN not in (125.0, 235.0, 435.0)";
        this.genericRetrievalTest(sql, list);
    }

    public void testShortEqLessGreaterOperation()
            throws SQLException
    {
        // test Equals Operation
        Operation op = DifferentDataTypesFinder.shortColumn().eq((short)110);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN = 110";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().lessThanEquals((short)1000).and(DifferentDataTypesFinder.mappedName().id().greaterThanEquals(1));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES D, MAPPED_DIFFERENT_DATA_TYPES M where D.SHORT_COLUMN <= 1000 and M.ID = D.ID and M.ID >= 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByShortColumn().shortColumn().eq((short)100);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.SHORT_COLUMN = D.SHORT_COLUMN and V.SHORT_COLUMN = 100.0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().eq((short)200).and(DifferentDataTypesFinder.longColumn().eq(15));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN = 200 and LONG_COLUMN = 15";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().eq((short)100);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN = 100";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.shortColumn().eq((short)100)));
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN = 100";
        this.genericRetrievalTest(sql, list);

        // test LessThanEquals Operation
        op = DifferentDataTypesFinder.shortColumn().lessThanEquals((short)500).and(DifferentDataTypesFinder.shortColumn().greaterThan((short)0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN <= 500 and SHORT_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().lessThanEquals((short)500).and(DifferentDataTypesFinder.shortColumn().greaterThanEquals((short)0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN <= 500 and SHORT_COLUMN >= 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().lessThanEquals((short)500).and(DifferentDataTypesFinder.shortColumn().lessThan((short)1000));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN <= 500 and SHORT_COLUMN < 1000";
        this.genericRetrievalTest(sql, list);

        // test LessThan Operation
        op = DifferentDataTypesFinder.shortColumn().lessThan((short)500).and(DifferentDataTypesFinder.shortColumn().greaterThan((short)0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN < 500 and SHORT_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().lessThan((short)500).and(DifferentDataTypesFinder.shortColumn().greaterThanEquals((short)0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN < 500 and SHORT_COLUMN >= 0";
        this.genericRetrievalTest(sql, list);

        // test GreaterThanEquals Operation
        op = DifferentDataTypesFinder.shortColumn().greaterThanEquals((short)50).and(DifferentDataTypesFinder.shortColumn().greaterThan((short)0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN >= 50 and SHORT_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().greaterThanEquals((short)50);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN >= 50";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.shortColumn().greaterThanEquals((short)10)).and(DifferentDataTypesFinder.shortColumn().greaterThanEquals((short)50)));
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN >= 50 and SHORT_COLUMN >= 10";
        this.genericRetrievalTest(sql, list);

        // test Not Eq Operation
        op = DifferentDataTypesFinder.shortColumn().notEq((short)120);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN != 120";
        this.genericRetrievalTest(sql, list, false);
        int databaseQuery = MithraManager.getInstance().getDatabaseRetrieveCount();

        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN != 120";
        this.genericRetrievalTest(sql, list);
        assertEquals("Check Database Retrieve Count", databaseQuery, MithraManager.getInstance().getDatabaseRetrieveCount());

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.shortColumn().notEq((short)100)).and(DifferentDataTypesFinder.shortColumn().notEq((short)120)));
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN != 120 and SHORT_COLUMN != 100";
        this.genericRetrievalTest(sql, list);

    }

    public void testShortInOperation()
            throws SQLException
    {
        // test In Operation
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short)100);
        shortSet.add((short)120);

        LongHashSet longSet = new LongHashSet();
        longSet.add(45);
        longSet.add(235);

        Operation op = DifferentDataTypesFinder.shortColumn().in(shortSet);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN in (100, 120)";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.shortColumn().in(shortSet).and(DifferentDataTypesFinder.floatColumn().greaterThanEquals(1));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN in (100, 120) and FLOAT_COLUMN >= 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByShortColumn().shortColumn().in(shortSet);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.SHORT_COLUMN = D.SHORT_COLUMN and V.SHORT_COLUMN in (100, 120)";
        this.genericRetrievalTest(sql, list);

        shortSet.add((short)1200);
        shortSet.add((short)10);
        shortSet.add((short)1120);
        shortSet.add((short)1210);

        op = DifferentDataTypesFinder.shortColumn().in(shortSet).and(DifferentDataTypesFinder.longColumn().in(longSet));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN in (100, 120, 1200, 10, 1120, 1210) and LONG_COLUMN in (45, 235)";
        this.genericRetrievalTest(sql, list);

        // test Not In Operation
        op = DifferentDataTypesFinder.shortColumn().notIn(shortSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN not in (100, 120, 1200, 10, 1120, 1210)";
        this.genericRetrievalTest(sql, list);

        ShortHashSet shortSet1 = new ShortHashSet();
        shortSet1.add((short)500);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.shortColumn().notIn(shortSet1)).and(DifferentDataTypesFinder.shortColumn().notIn(shortSet)));
        sql = "select * from DIFFERENT_DATA_TYPES where SHORT_COLUMN not in (100, 120, 1200, 10, 1120, 1210) and SHORT_COLUMN not in (500)";
        this.genericRetrievalTest(sql, list);
    }

    public void testLongEqLessGreaterOperation()
            throws SQLException
    {
        // test Equals Operation
        Operation op = DifferentDataTypesFinder.longColumn().eq(15);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN = 15";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.longColumn().lessThanEquals(1000).and(DifferentDataTypesFinder.mappedName().id().greaterThanEquals(1));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES D, MAPPED_DIFFERENT_DATA_TYPES M where D.LONG_COLUMN <= 1000 and M.ID = D.ID and M.ID >= 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByLongColumn().longColumn().eq(15);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.LONG_COLUMN = D.LONG_COLUMN and V.LONG_COLUMN = 15";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.longColumn().eq(235).and(DifferentDataTypesFinder.id().eq(3));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN = 235 and ID = 3";
        this.genericRetrievalTest(sql, list);

        // test LessThanEquals Operation
        op = DifferentDataTypesFinder.longColumn().lessThanEquals(500).and(DifferentDataTypesFinder.longColumn().greaterThan(0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN <= 500 and LONG_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.longColumn().lessThanEquals(500).and(DifferentDataTypesFinder.longColumn().greaterThanEquals(0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN <= 500 and LONG_COLUMN >= 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.longColumn().lessThanEquals(500).and(DifferentDataTypesFinder.longColumn().lessThan(1000));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN <= 500 and LONG_COLUMN < 1000";
        this.genericRetrievalTest(sql, list);

        // test LessThan Operation
        op = DifferentDataTypesFinder.longColumn().lessThan(500).and(DifferentDataTypesFinder.longColumn().greaterThan(0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN < 500 and LONG_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.longColumn().lessThan(500).and(DifferentDataTypesFinder.longColumn().greaterThanEquals(0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN < 500 and LONG_COLUMN >= 0";
        this.genericRetrievalTest(sql, list);

        // test GreaterThanEquals Operation
        op = DifferentDataTypesFinder.longColumn().greaterThanEquals(50).and(DifferentDataTypesFinder.longColumn().greaterThan(0));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN >= 50 and LONG_COLUMN > 0";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.longColumn().greaterThanEquals(50);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN >= 50";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.longColumn().greaterThanEquals(10)).and(DifferentDataTypesFinder.longColumn().greaterThanEquals(50)));
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN >= 50 and LONG_COLUMN >= 10";
        this.genericRetrievalTest(sql, list);

        // test Not Equals Operation
        op = DifferentDataTypesFinder.longColumn().notEq(50);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN != 50";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.longColumn().notEq(10)).and(DifferentDataTypesFinder.longColumn().notEq(50)));
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN != 50 and LONG_COLUMN != 10";
        this.genericRetrievalTest(sql, list);
    }

    public void testLongInOperation()
            throws SQLException
    {
        // test In Operation
        LongHashSet longSet = new LongHashSet();
        longSet.add(45);
        longSet.add(235);

        Operation op = DifferentDataTypesFinder.longColumn().in(longSet);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN in (45, 235)";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.longColumn().in(longSet).and(DifferentDataTypesFinder.floatColumn().greaterThanEquals(1));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN in (45, 235) and FLOAT_COLUMN >= 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByLongColumn().longColumn().in(longSet);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.LONG_COLUMN = D.LONG_COLUMN and V.LONG_COLUMN in (45, 235)";
        this.genericRetrievalTest(sql, list);

        longSet.add(245);
        longSet.add(215);
        longSet.add(15);
        longSet.add(5);

        CharHashSet charSet = new CharHashSet();
        charSet.add('A');
        charSet.add('C');

        op = DifferentDataTypesFinder.longColumn().in(longSet).and(DifferentDataTypesFinder.charColumn().in(charSet));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN in (45, 235, 245, 215, 15, 5) and CHAR_COLUMN in ('A', 'C')";
        this.genericRetrievalTest(sql, list);

        // test Not In Operation
        longSet.remove(45);
        longSet.remove(235);

        op = DifferentDataTypesFinder.longColumn().notIn(longSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN not in (245, 215, 15, 5)";
        this.genericRetrievalTest(sql, list);

        LongHashSet longSet1 = new LongHashSet();
        longSet1.add(45);
        longSet1.add(450);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.longColumn().notIn(longSet1)).and(DifferentDataTypesFinder.longColumn().notIn(longSet)));
        sql = "select * from DIFFERENT_DATA_TYPES where LONG_COLUMN not in (245, 215, 15, 5) and LONG_COLUMN not in (45, 450)";
        this.genericRetrievalTest(sql, list);
    }

    public void testCharEqLessGreaterOperation()
            throws SQLException
    {
        // test Equals Operation
        Operation op = DifferentDataTypesFinder.charColumn().eq('C');
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN = 'C'";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByCharColumn().charColumn().eq('A');
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.CHAR_COLUMN = D.CHAR_COLUMN and V.CHAR_COLUMN = 'A'";
        this.genericRetrievalTest(sql, list);

        // test GreaterThanEquals Operation
        op = DifferentDataTypesFinder.charColumn().greaterThanEquals('A').and(DifferentDataTypesFinder.charColumn().greaterThan('B'));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN >= 'A' and CHAR_COLUMN > 'B'";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.charColumn().eq('B').and(DifferentDataTypesFinder.longColumn().eq(15));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN = 'B' and LONG_COLUMN = 15";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.charColumn().greaterThanEquals('A');
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN >= 'A'";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.charColumn().greaterThanEquals('B')).and(DifferentDataTypesFinder.charColumn().greaterThanEquals('A')));
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN >= 'A' and CHAR_COLUMN >= 'B'";
        this.genericRetrievalTest(sql, list);

        // test LessThanEquals Operation
        op = DifferentDataTypesFinder.charColumn().lessThanEquals('Z').and(DifferentDataTypesFinder.charColumn().greaterThan('B'));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN <= 'Z' and CHAR_COLUMN > 'B'";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.charColumn().lessThanEquals('Z').and(DifferentDataTypesFinder.charColumn().greaterThanEquals('B'));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN <= 'Z' and CHAR_COLUMN >= 'B'";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.charColumn().lessThanEquals('Z').and(DifferentDataTypesFinder.charColumn().lessThan('B'));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN <= 'Z' and CHAR_COLUMN < 'B'";
        this.genericRetrievalTest(sql, list);

        // test LessThan Operation
        op = DifferentDataTypesFinder.charColumn().lessThan('Z').and(DifferentDataTypesFinder.charColumn().greaterThan('B'));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN < 'Z' and CHAR_COLUMN > 'B'";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.charColumn().lessThan('Z').and(DifferentDataTypesFinder.charColumn().greaterThanEquals('B'));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN < 'Z' and CHAR_COLUMN >= 'B'";
        this.genericRetrievalTest(sql, list);

        // test Not Eq Operation
        op = DifferentDataTypesFinder.charColumn().notEq('Z');
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN != 'Z'";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.charColumn().notEq('Z')).and(DifferentDataTypesFinder.charColumn().notEq('A')));
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN != 'Z' and CHAR_COLUMN != 'A'";
        this.genericRetrievalTest(sql, list);
    }

    public void testCharInOperation()
            throws SQLException
    {
        // test In Operation
        CharHashSet charSet = new CharHashSet();
        charSet.add('A');
        charSet.add('C');

        Operation op = DifferentDataTypesFinder.charColumn().in(charSet);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN in ('A', 'C')";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.variousByCharColumn().charColumn().in(charSet);
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.CHAR_COLUMN = D.CHAR_COLUMN and V.CHAR_COLUMN in ('A', 'C')";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.charColumn().in(charSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN in ('A', 'C')";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.charColumn().in(charSet)));
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN in ('A', 'C')";
        this.genericRetrievalTest(sql, list);

        charSet.add('B');
        charSet.add('D');
        charSet.add('E');
        charSet.add('F');
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short)100);
        shortSet.add((short)120);

        op = DifferentDataTypesFinder.charColumn().in(charSet).and(DifferentDataTypesFinder.shortColumn().in(shortSet));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN in ('A', 'C', 'B', 'D', 'E', 'F') and SHORT_COLUMN in (100, 120)";
        this.genericRetrievalTest(sql, list);

        // test Not In Operation
        charSet.remove('A');
        CharHashSet charSet1 = new CharHashSet();
        charSet1.add('Z');
        charSet1.add('Y');

        op = DifferentDataTypesFinder.charColumn().notIn(charSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN not in ('C', 'B', 'D', 'E', 'F')";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.charColumn().notIn(charSet)).and(DifferentDataTypesFinder.charColumn().notIn(charSet1)));
        sql = "select * from DIFFERENT_DATA_TYPES where CHAR_COLUMN not in ('C', 'B', 'D', 'E', 'F') and CHAR_COLUMN not in ('Z', 'Y')";
        this.genericRetrievalTest(sql, list);
    }

    public void testIntegerEqLessGreaterInOperation()
            throws SQLException
    {
        // test Less Than Equals Operation
        Operation op = DifferentDataTypesFinder.id().lessThanEquals(15);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where ID <= 15";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.id().lessThanEquals(15).and(DifferentDataTypesFinder.id().greaterThan(1));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where ID <= 15 and ID > 1";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.id().lessThanEquals(15).and(DifferentDataTypesFinder.id().lessThan(10));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where ID <= 15 and ID < 10";
        this.genericRetrievalTest(sql, list);

        // test Greater Than Equals Operation
        op = DifferentDataTypesFinder.id().greaterThanEquals(1);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where ID >= 1";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.id().greaterThanEquals(0)).and(DifferentDataTypesFinder.id().greaterThanEquals(1)));
        sql = "select * from DIFFERENT_DATA_TYPES where ID >= 1 and ID >= 0";
        this.genericRetrievalTest(sql, list);

        // test Not Eq Operation
        op = DifferentDataTypesFinder.id().notEq(1);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where ID != 1";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.id().notEq(0)).and(DifferentDataTypesFinder.id().notEq(1)));
        sql = "select * from DIFFERENT_DATA_TYPES where ID != 1 and ID != 0";
        this.genericRetrievalTest(sql, list);

        // test Not In Operation
        IntHashSet IntHashSet = new IntHashSet();
        IntHashSet.add(5);
        IntHashSet.add(7);

        op = DifferentDataTypesFinder.id().notIn(IntHashSet);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where ID not in (5, 7)";
        this.genericRetrievalTest(sql, list);

        IntHashSet IntHashSet1 = new IntHashSet();
        IntHashSet1.add(9);
        IntHashSet1.add(8);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.id().notIn(IntHashSet)).and(DifferentDataTypesFinder.id().notIn(IntHashSet1)));
        sql = "select * from DIFFERENT_DATA_TYPES where ID not in (5, 7) and ID not in (9, 8)";
        this.genericRetrievalTest(sql, list);
    }

    public void testNotNullOperation()
            throws SQLException
    {
        // test NotNull Operation
        Operation op = DifferentDataTypesFinder.booleanColumn().isNotNull().and(DifferentDataTypesFinder.doubleColumn().isNotNull());
        DifferentDataTypesList list = new DifferentDataTypesList(op);
        String sql = "Select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN is not null and DOUBLE_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.booleanColumn().isNotNull().and(DifferentDataTypesFinder.longColumn().greaterThanEquals(10));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN is not null and LONG_COLUMN >= 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.booleanColumn().isNotNull().and(DifferentDataTypesFinder.longColumn().greaterThan(10));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN is not null and LONG_COLUMN > 10";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.booleanColumn().isNotNull().and(DifferentDataTypesFinder.longColumn().lessThan(1000));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN is not null and LONG_COLUMN < 1000";
        this.genericRetrievalTest(sql, list);

        op = DifferentDataTypesFinder.booleanColumn().isNotNull().and(DifferentDataTypesFinder.longColumn().lessThanEquals(1000));
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where BOOLEAN_COLUMN is not null and LONG_COLUMN <= 1000";
        this.genericRetrievalTest(sql, list);
    }

    public void testCalculatedDoubleEqLessGreaterOperation()
            throws SQLException
    {
        // test Equals Operation
        Operation op = DifferentDataTypesFinder.doubleColumn().absoluteValue().eq(1000.0);
        DifferentDataTypesList list = new DifferentDataTypesList(op);

        String sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) = 1000.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.doubleColumn().absoluteValue().eq(1000.0)));
        sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) = 1000.0";
        this.genericRetrievalTest(sql, list);

        // test Greater Than Equals Operation
        op = DifferentDataTypesFinder.doubleColumn().absoluteValue().greaterThanEquals(10.0);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) >= 10.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.doubleColumn().absoluteValue().greaterThanEquals(5.0)).and(DifferentDataTypesFinder.doubleColumn().absoluteValue().greaterThanEquals(10.0)));
        sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) >= 10.0 and abs(DOUBLE_COLUMN) >= 5.0";
        this.genericRetrievalTest(sql, list);

        // test Less Than Equals Operation
        op = DifferentDataTypesFinder.doubleColumn().absoluteValue().lessThanEquals(10000.0);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) <= 10000.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.doubleColumn().absoluteValue().lessThanEquals(5100.0)).and(DifferentDataTypesFinder.doubleColumn().absoluteValue().lessThanEquals(10000.0)));
        sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) <= 10000.0 and abs(DOUBLE_COLUMN) <= 5100.0";
        this.genericRetrievalTest(sql, list);

        // test Less Than Operation
        op = DifferentDataTypesFinder.doubleColumn().absoluteValue().lessThan(10000.0);
        list = new DifferentDataTypesList(op);
        sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) < 10000.0";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op.and(DifferentDataTypesFinder.doubleColumn().absoluteValue().lessThan(5100.0)).and(DifferentDataTypesFinder.doubleColumn().absoluteValue().lessThan(10000.0)));
        sql = "select * from DIFFERENT_DATA_TYPES where abs(DOUBLE_COLUMN) < 10000.0 and abs(DOUBLE_COLUMN) < 5100.0";
        this.genericRetrievalTest(sql, list);
    }

    public void testVariousMappedAttributes()
            throws SQLException
    {
        // test Mapped Boolean Attribute
        Operation op = DifferentDataTypesFinder.variousById().booleanColumn().eq(DifferentDataTypesFinder.variousByBooleanColumn().booleanColumn());
        DifferentDataTypesList list = new DifferentDataTypesList(op);
        String sql = "select  t0.* from DIFFERENT_DATA_TYPES t0, VARIOUS_TYPES t1, VARIOUS_TYPES t2 where  t0.ID = t1.ID and t0.BOOLEAN_COLUMN = t2.BOOLEAN_COLUMN and t1.BOOLEAN_COLUMN = t2.BOOLEAN_COLUMN";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test Mapped Byte Attribute
        op = DifferentDataTypesFinder.variousById().byteColumn().eq(DifferentDataTypesFinder.variousByByteColumn().byteColumn());
        list = new DifferentDataTypesList(op);
        sql = "select t0.* from DIFFERENT_DATA_TYPES t0, VARIOUS_TYPES t1, VARIOUS_TYPES t2 where  t0.ID = t1.ID and t0.BYTE_COLUMN = t2.BYTE_COLUMN and t1.BYTE_COLUMN = t2.BYTE_COLUMN";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test Mapped Float Attribute
        op = DifferentDataTypesFinder.variousById().floatColumn().eq(DifferentDataTypesFinder.variousByFloatColumn().floatColumn());
        list = new DifferentDataTypesList(op);
        sql = "select t0.* from DIFFERENT_DATA_TYPES t0, VARIOUS_TYPES t1, VARIOUS_TYPES t2 where  t0.ID = t1.ID and t0.FLOAT_COLUMN = t2.FLOAT_COLUMN and t1.FLOAT_COLUMN = t2.FLOAT_COLUMN";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test Mapped Short Attribute
        op = DifferentDataTypesFinder.variousById().shortColumn().eq(DifferentDataTypesFinder.variousByShortColumn().shortColumn());
        list = new DifferentDataTypesList(op);
        sql = "select t0.* from DIFFERENT_DATA_TYPES t0, VARIOUS_TYPES t2, VARIOUS_TYPES t1 where  t0.ID = t1.ID and t0.SHORT_COLUMN = t2.SHORT_COLUMN and t1.SHORT_COLUMN = t2.SHORT_COLUMN";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test Mapped Long Attribute
        op = DifferentDataTypesFinder.variousById().longColumn().eq(DifferentDataTypesFinder.variousByLongColumn().longColumn());
        list = new DifferentDataTypesList(op);
        sql = "select t0.* from DIFFERENT_DATA_TYPES t0, VARIOUS_TYPES t2, VARIOUS_TYPES t1 where  t0.ID = t1.ID and t0.LONG_COLUMN = t2.LONG_COLUMN and t1.LONG_COLUMN = t2.LONG_COLUMN";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test Mapped Char Attribute
        op = DifferentDataTypesFinder.variousById().charColumn().eq(DifferentDataTypesFinder.variousByCharColumn().charColumn());
        list = new DifferentDataTypesList(op);
        sql = "select t0.* from DIFFERENT_DATA_TYPES t0, VARIOUS_TYPES t1, VARIOUS_TYPES t2 where  t0.ID = t1.ID and t0.CHAR_COLUMN = t2.CHAR_COLUMN and t1.CHAR_COLUMN = t2.CHAR_COLUMN";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test Mapped Integer Attribute
        op = DifferentDataTypesFinder.variousById().id().eq(DifferentDataTypesFinder.variousById().id());
        list = new DifferentDataTypesList(op);
        sql = "select D.* from DIFFERENT_DATA_TYPES D, VARIOUS_TYPES V where V.ID = D.ID and D.ID = V.ID";
        this.genericRetrievalTest(sql, list);

        list = new DifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        assertEquals(DifferentDataTypesFinder.variousById().doubleColumn().absoluteValue(), DifferentDataTypesFinder.variousById().doubleColumn().absoluteValue());
        assertEquals(DifferentDataTypesFinder.variousById().doubleColumn().absoluteValue().hashCode(), DifferentDataTypesFinder.variousById().doubleColumn().absoluteValue().hashCode());
    }

    public void testRelationshipByIndex()
    {
        DifferentDataTypes types = DifferentDataTypesFinder.findOne(DifferentDataTypesFinder.id().eq(1));
        VariousTypes variousTypes = VariousTypesFinder.findOne(VariousTypesFinder.id().eq(1));
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertSame(variousTypes, types.getVariousByByteColumn());
        assertSame(variousTypes, types.getVariousByCharColumn());
        assertSame(variousTypes, types.getVariousByDoubleColumn());
        assertSame(variousTypes, types.getVariousByFloatColumn());
        assertSame(variousTypes, types.getVariousById());
        assertSame(variousTypes, types.getVariousByLongColumn());
        assertSame(variousTypes, types.getVariousByShortColumn());

        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        assertSame(variousTypes, types.getVariousByBooleanColumn());
    }

    public void testToOneDateByIndexRelationship()
    {
        TamsMithraAccount acct = TamsMithraAccountFinder.findOne(TamsMithraAccountFinder.accountNumber().eq("71234567"));
        assertNotNull(acct);
        assertNotNull(acct.getTrial());
        assertEquals(1, acct.getTrial().getTrialNodeId());
    }
}
