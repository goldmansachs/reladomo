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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.DifferentDataTypes;
import com.gs.fw.common.mithra.test.domain.DifferentDataTypesFinder;
import com.gs.fw.common.mithra.test.domain.DifferentDataTypesList;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.bcp.TlewTrial;

import java.sql.SQLException;
import java.util.List;


public class TestAttributeUpdateWrapper extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            DifferentDataTypes.class,
        };
    }

    public void setUp()
            throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new DifferentDataTypesResultSetComparator());
    }

    public void testBooleanUpdateWrapper()
            throws SQLException
    {
        Operation op = DifferentDataTypesFinder.id().eq(1);
        DifferentDataTypes differentDataTypes = DifferentDataTypesFinder.findOne(op);
        differentDataTypes.setBooleanColumn(false);

        String sql = "select * from DIFFERENT_DATA_TYPES where id = 1";
        List differentDataTypesList = new DifferentDataTypesList(DifferentDataTypesFinder.id().eq(1));
        this.genericRetrievalTest(sql, differentDataTypesList);
    }

    public void testByteUpdateWrapper()
            throws SQLException
    {
        Operation op = DifferentDataTypesFinder.id().eq(1);
        DifferentDataTypes differentDataTypes = DifferentDataTypesFinder.findOne(op);
        differentDataTypes.setByteColumn((byte)1);

        String sql = "select * from DIFFERENT_DATA_TYPES where id = 1";
        List differentDataTypesList = new DifferentDataTypesList(DifferentDataTypesFinder.id().eq(1));
        this.genericRetrievalTest(sql, differentDataTypesList);
    }

    public void testFloatUpdateWrapper()
            throws SQLException
    {
        Operation op = DifferentDataTypesFinder.id().eq(1);
        DifferentDataTypes differentDataTypes = DifferentDataTypesFinder.findOne(op);
        differentDataTypes.setFloatColumn(150.0f);

        String sql = "select * from DIFFERENT_DATA_TYPES where id = 1";
        List differentDataTypesList = new DifferentDataTypesList(DifferentDataTypesFinder.id().eq(1));
        this.genericRetrievalTest(sql, differentDataTypesList);
    }

    public void testShortUpdateWrapper()
            throws SQLException
    {
        Operation op = DifferentDataTypesFinder.id().eq(1);
        DifferentDataTypes differentDataTypes = DifferentDataTypesFinder.findOne(op);
        differentDataTypes.setShortColumn((short)1000);

        String sql = "select * from DIFFERENT_DATA_TYPES where id = 1";
        List differentDataTypesList = new DifferentDataTypesList(DifferentDataTypesFinder.id().eq(1));
        this.genericRetrievalTest(sql, differentDataTypesList);
    }

    public void testCharUpdateWrapper()
            throws SQLException
    {
        Operation op = DifferentDataTypesFinder.id().eq(1);
        DifferentDataTypes differentDataTypes = DifferentDataTypesFinder.findOne(op);
        differentDataTypes.setCharColumn('D');

        String sql = "select * from DIFFERENT_DATA_TYPES where id = 1";
        List differentDataTypesList = new DifferentDataTypesList(DifferentDataTypesFinder.id().eq(1));
        this.genericRetrievalTest(sql, differentDataTypesList);
    }

    public void testBooleanNullUpdateWrapper()
            throws SQLException
    {
        Operation op = DifferentDataTypesFinder.id().eq(1);
        DifferentDataTypes differentDataTypes = DifferentDataTypesFinder.findOne(op);
        differentDataTypes.setBooleanColumnNull();
        differentDataTypes.setByteColumnNull();
        differentDataTypes.setFloatColumnNull();
        differentDataTypes.setShortColumnNull();
        differentDataTypes.setLongColumnNull();
        differentDataTypes.setCharColumnNull();

        DifferentDataTypesFinder.clearQueryCache();

        differentDataTypes = DifferentDataTypesFinder.findOne(op);
        assertEquals("Check Boolean Column is null", true, differentDataTypes.isBooleanColumnNull());
        assertEquals("Check Byte Column is null", true, differentDataTypes.isByteColumnNull());
        assertEquals("Check Float Column is null", true, differentDataTypes.isFloatColumnNull());
        assertEquals("Check Short Column is null", true, differentDataTypes.isShortColumnNull());
        assertEquals("Check Long Column is null", true, differentDataTypes.isLongColumnNull());
        assertEquals("Check Char Column is null", true, differentDataTypes.isCharColumnNull());
    }

    public void testBooleanNullUpdateWrapper2()
            throws SQLException
    {
        Operation op = DifferentDataTypesFinder.id().eq(1);
        DifferentDataTypes differentDataTypes = DifferentDataTypesFinder.findOne(op);
        differentDataTypes.setNullablePrimitiveAttributesToNull();

        DifferentDataTypesFinder.clearQueryCache();

        differentDataTypes = DifferentDataTypesFinder.findOne(op);
        assertEquals("Check Boolean Column is null", true, differentDataTypes.isBooleanColumnNull());
        assertEquals("Check Byte Column is null", true, differentDataTypes.isByteColumnNull());
        assertEquals("Check Float Column is null", true, differentDataTypes.isFloatColumnNull());
        assertEquals("Check Short Column is null", true, differentDataTypes.isShortColumnNull());
        assertEquals("Check Long Column is null", true, differentDataTypes.isLongColumnNull());
        assertEquals("Check Char Column is null", true, differentDataTypes.isCharColumnNull());
    }

    public void testManyNullableFields()
    {
        TlewTrial trial = new TlewTrial(InfinityTimestamp.getParaInfinity());
        trial.setNullablePrimitiveAttributesToNull();
        assertTrue(trial.isPosMktvlBaseNull());
        assertTrue(trial.isPosAccrIntBaseNull());
        assertTrue(trial.isUnrealFutPerGmiPlNull());
        assertTrue(trial.isFairValAdjNull());
        assertTrue(trial.isAdjTdUnstldFutPlNull());
        assertTrue(trial.isValuationAdjNull());
        assertTrue(trial.isFxPerStockRecNull());
        assertTrue(trial.isMtdDivIncmNull());
        assertTrue(trial.isMtdTransferTaxNull());
        assertTrue(trial.isRegCostBaseNull());
    }
}
