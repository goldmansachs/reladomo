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
import com.gs.fw.common.mithra.test.domain.DifferentDataTypes;
import com.gs.fw.common.mithra.test.domain.MappedDifferentDataTypes;
import com.gs.fw.common.mithra.test.domain.MappedDifferentDataTypesFinder;
import com.gs.fw.common.mithra.test.domain.MappedDifferentDataTypesList;
import com.gs.fw.common.mithra.test.domain.TestBinaryArray;
import org.eclipse.collections.impl.set.mutable.primitive.BooleanHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.ByteHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.CharHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.DoubleHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.FloatHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.ShortHashSet;

import java.sql.SQLException;


public class TestMappedAttributes extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            DifferentDataTypes.class,
            MappedDifferentDataTypes.class,
            TestBinaryArray.class,
        };
    }

    public void setUp()
            throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new MappedDifferentDataTypesResultSetComparator());
    }

    public void testBooleanMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().booleanColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BOOLEAN_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setBooleanColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().booleanColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BOOLEAN_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setBooleanColumn(true);

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setBooleanColumn(true);
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BOOLEAN_COLUMN = TRUE";
        op = MappedDifferentDataTypesFinder.datatypes().booleanColumn().eq(true);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().booleanColumn().notEq(true);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BOOLEAN_COLUMN <> TRUE";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        BooleanHashSet booleanSet = new BooleanHashSet();
        booleanSet.add(false);
        booleanSet.add(true);

        op = MappedDifferentDataTypesFinder.datatypes().booleanColumn().in(booleanSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BOOLEAN_COLUMN in (FALSE, TRUE)";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.datatypes().booleanColumn().eq(true);
        list = new MappedDifferentDataTypesList(op);
        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BOOLEAN_COLUMN = TRUE";
        this.genericRetrievalTest(sql, list);

        list = new MappedDifferentDataTypesList(op.and(MappedDifferentDataTypesFinder.datatypes().booleanColumn().eq(true)));
        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BOOLEAN_COLUMN = TRUE";
        this.genericRetrievalTest(sql, list);
    }

    public void testByteMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().byteColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setByteColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setByteColumn((byte)0);

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setByteColumn((byte)2);
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN = 2";
        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().eq((byte)2);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().notEq((byte)5);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN <> 5";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        ByteHashSet byteSet = new ByteHashSet();
        byteSet.add((byte)1);
        byteSet.add((byte)3);

        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().in(byteSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN in (1, 3)";
        this.genericRetrievalTest(sql, list);

        // test NOT IN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().notIn(byteSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN not in (1, 3)";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().greaterThan((byte)1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN > 1";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().greaterThanEquals((byte)1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN >= 1";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().lessThan((byte)1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN < 1";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().byteColumn().lessThanEquals((byte)1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.BYTE_COLUMN <= 1";
        this.genericRetrievalTest(sql, list);
    }

    public void testDoubleMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setDoubleColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setDoubleColumn(10.0);

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setDoubleColumn(110.0);
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN = 110.0";
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().eq(110.0);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().notEq(5.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN <> 5.0";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        DoubleHashSet doubleSet = new DoubleHashSet();
        doubleSet.add(1110.0);
        doubleSet.add(100.0);
        doubleSet.add(10.0);

        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().in(doubleSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN in (1110.0, 100.0, 10.0)";
        this.genericRetrievalTest(sql, list);

        // test NOT IN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().notIn(doubleSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN not in (1110.0, 100.0, 10.0)";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().greaterThan(100.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN > 100.0";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().greaterThanEquals(100.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN >= 100.0";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().lessThan(1000.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN < 1000.0";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().lessThanEquals(1000.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.DOUBLE_COLUMN <= 1000.0";
        this.genericRetrievalTest(sql, list);
    }

    public void testFloatMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().floatColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setFloatColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setFloatColumn(15.0f);

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setFloatColumn(20.0f);
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN = 20.0";
        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().eq(20.0f);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().notEq(50.0f);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN <> 50.0";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        FloatHashSet floatSet = new FloatHashSet();
        floatSet.add(15.0f);
        floatSet.add(25.0f);
        floatSet.add(45.0f);

        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().in(floatSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN in (15.0, 25.0, 45.0)";
        this.genericRetrievalTest(sql, list);

        // test NOT IN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().notIn(floatSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN not in (15.0, 25.0, 45.0)";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().greaterThan(25.0f);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN > 25.0";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().greaterThanEquals(25.0f);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN >= 25.0";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().lessThan(30.0f);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN < 30.0";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().floatColumn().lessThanEquals(30.0f);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.FLOAT_COLUMN <= 30.0";
        this.genericRetrievalTest(sql, list);
    }

    public void testShortMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().shortColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setShortColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setShortColumn((short)100);

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setShortColumn((short)20);
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN = 20";
        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().eq((short)20);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().notEq((short)50);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN <> 50";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short)100);
        shortSet.add((short)120);

        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().in(shortSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN in (100, 120)";
        this.genericRetrievalTest(sql, list);

        // test NOT IN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().notIn(shortSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN not in (100, 120)";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().greaterThan((short)25);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN > 25";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().greaterThanEquals((short)25);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN >= 25";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().lessThan((short)200);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN < 200";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().shortColumn().lessThanEquals((short)200);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.SHORT_COLUMN <= 200";
        this.genericRetrievalTest(sql, list);
    }

    public void testLongMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().longColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setLongColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().longColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setLongColumn(45);

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setLongColumn(20);
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN = 20";
        op = MappedDifferentDataTypesFinder.datatypes().longColumn().eq(20);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().longColumn().notEq(50);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN <> 50";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        LongHashSet longSet = new LongHashSet();
        longSet.add(45);
        longSet.add(235);

        op = MappedDifferentDataTypesFinder.datatypes().longColumn().in(longSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN in (45, 235)";
        this.genericRetrievalTest(sql, list);

        // test NOT IN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().longColumn().notIn(longSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN not in (45, 235)";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().longColumn().greaterThan(25);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN > 25";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().longColumn().greaterThanEquals(25);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN >= 25";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().longColumn().lessThan(100);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN < 100";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().longColumn().lessThanEquals(100);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.LONG_COLUMN <= 100";
        this.genericRetrievalTest(sql, list);
    }

    public void testCharMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().charColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setCharColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().charColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setCharColumn('A');

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setCharColumn('D');
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN = 'D'";
        op = MappedDifferentDataTypesFinder.datatypes().charColumn().eq('D');
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().charColumn().notEq('D');
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN <> 'D'";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        CharHashSet charSet = new CharHashSet();
        charSet.add('A');
        charSet.add('C');

        op = MappedDifferentDataTypesFinder.datatypes().charColumn().in(charSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN in ('A', 'C')";
        this.genericRetrievalTest(sql, list);

        // test NOT IN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().charColumn().notIn(charSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN not in ('A', 'C')";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().charColumn().greaterThan('A');
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN > 'A'";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().charColumn().greaterThanEquals('A');
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN >= 'A'";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().charColumn().lessThan('C');
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN < 'C'";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().charColumn().lessThanEquals('C');
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.CHAR_COLUMN <= 'C'";
        this.genericRetrievalTest(sql, list);
    }

    public void testIntegerMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.datatypes().intColumn().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN is not null";
        this.genericRetrievalTest(sql, list);

        // test NULL UPDATE + NULL for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(1);
        MappedDifferentDataTypes mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setIntColumnNull();

        op = MappedDifferentDataTypesFinder.datatypes().intColumn().isNull();
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN is null";
        this.genericRetrievalTest(sql, list);

        op = MappedDifferentDataTypesFinder.id().eq(1);
        mappedDifferentDataType = MappedDifferentDataTypesFinder.findOne(op);
        mappedDifferentDataType.getDatatypes().setIntColumn(0);

        // test EQUALS + UPDATE for mapped attribute
        op = MappedDifferentDataTypesFinder.id().eq(2);
        list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setIntColumn(2);
        }

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN = 2";
        op = MappedDifferentDataTypesFinder.datatypes().intColumn().eq(2);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test NOT EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().intColumn().notEq(5);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN <> 5";
        this.genericRetrievalTest(sql, list);

        // test IN for mapped attribute
        IntHashSet IntHashSet = new IntHashSet();
        IntHashSet.add(1);
        IntHashSet.add(3);

        op = MappedDifferentDataTypesFinder.datatypes().intColumn().in(IntHashSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN in (1, 3)";
        this.genericRetrievalTest(sql, list);

        // test NOT IN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().intColumn().notIn(IntHashSet);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN not in (1, 3)";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().intColumn().greaterThan(1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN > 1";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().intColumn().greaterThanEquals(1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN >= 1";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().intColumn().lessThan(1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN < 1";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().intColumn().lessThanEquals(1);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and D.INT_COLUMN <= 1";
        this.genericRetrievalTest(sql, list);
    }


    public void testByteArrayMappedAttribute()
            throws SQLException
    {
        // test NOT NULL for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.byteArrayObj().pictureData().isNotNull();
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, TEST_BINARY_ARRAY T where M.ID = T.TEST_BINARY_ARRAY_ID and T.BINARY_ARRAY is not null";
        this.genericRetrievalTest(sql, list);

//        // test EQUALS for mapped attribute
//        byte[] byteArrayData = new byte[4];
//        byteArrayData[0] = toByte(0xFF);
//        byteArrayData[1] = toByte(0x01);
//        byteArrayData[2] = toByte(0x02);
//        byteArrayData[3] = 0;
//
//        op = MappedDifferentDataTypesFinder.byteArrayObj().pictureData().eq(byteArrayData);
//        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, TEST_BINARY_ARRAY T where M.ID = T.TEST_BINARY_ARRAY_ID and T.BINARY_ARRAY = 0xFF010200";
//        list = new MappedDifferentDataTypesList(op);
//        this.genericRetrievalTest(sql, list);
//
//        // test IN for mapped attribute
//        byte[] newData1 = new byte[5];
//        newData1[0] = toByte(0xAA);
//        newData1[1] = toByte(0xBB);
//        newData1[2] = toByte(0x99);
//        newData1[3] = toByte(0x11);
//        newData1[4] = 0;
//
//        Set set = new HashSet();
//        set.add(byteArrayData);
//        set.add(newData1);
//        op = MappedDifferentDataTypesFinder.byteArrayObj().pictureData().in(set);
//        list = new MappedDifferentDataTypesList(op);
//
//        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, TEST_BINARY_ARRAY T where M.ID = T.TEST_BINARY_ARRAY_ID and T.BINARY_ARRAY IN (0xFF010200, 0xAABB991100)";
//        this.genericRetrievalTest(sql, list);
//
//        // test NOT IN for mapped attribute
//        byte[] newData2 = new byte[5];
//        newData2[0] = toByte(0xAA);
//        newData2[1] = toByte(0xBB);
//        newData2[2] = toByte(0x88);
//        newData2[3] = toByte(0x11);
//        newData2[4] = 0;
//
//        set.add(newData2);
//        set.remove(byteArrayData);
//        op = MappedDifferentDataTypesFinder.byteArrayObj().pictureData().notIn(set);
//        list = new MappedDifferentDataTypesList(op);
//
//        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, TEST_BINARY_ARRAY T where M.ID = T.TEST_BINARY_ARRAY_ID and T.BINARY_ARRAY IN (0xAABB881100, 0xAABB991100)";
//        this.genericRetrievalTest(sql, list);
    }
//
//    private byte toByte(int i)
//    {
//        if (i > 127) i-=256;
//
//        return (byte) (i & 0xFF);
//    }

    public void testCalculatedDoubleMappedAttribute()
            throws SQLException
    {
        // test EQUALS + UPDATE for mapped attribute
        Operation op = MappedDifferentDataTypesFinder.id().eq(2);
        MappedDifferentDataTypesList list = new MappedDifferentDataTypesList(op);
        for (int i = 0; i < list.size(); i++)
        {
            MappedDifferentDataTypes obj =  list.getMappedDifferentDataTypesAt(i);
            obj.getDatatypes().setDoubleColumn(110.0);
        }

        String sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and abs(D.DOUBLE_COLUMN) = 110.0";
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().absoluteValue().eq(110.0);
        list = new MappedDifferentDataTypesList(op);
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().absoluteValue().greaterThan(100.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and abs(D.DOUBLE_COLUMN) > 100.0";
        this.genericRetrievalTest(sql, list);

        // test GREATER THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().absoluteValue().greaterThanEquals(100.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and abs(D.DOUBLE_COLUMN) >= 100.0";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().absoluteValue().lessThan(1000.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and abs(D.DOUBLE_COLUMN) < 1000.0";
        this.genericRetrievalTest(sql, list);

        // test LESS THAN EQUALS for mapped attribute
        op = MappedDifferentDataTypesFinder.datatypes().doubleColumn().absoluteValue().lessThanEquals(1000.0);
        list = new MappedDifferentDataTypesList(op);

        sql = "select M.* from MAPPED_DIFFERENT_DATA_TYPES M, DIFFERENT_DATA_TYPES D where M.ID = D.ID and abs(D.DOUBLE_COLUMN) <= 1000.0";
        this.genericRetrievalTest(sql, list);
    }

}
