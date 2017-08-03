
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

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.OracleDatabaseType;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.AllTypes;
import com.gs.fw.common.mithra.test.domain.AllTypesList;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductList;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class MithraOracleTestAbstract extends MithraTestAbstract
{
    private MithraTestResource mithraTestResource;
    private String testDataFileName = "testdata/vendor/mithraOracleTestData.txt";

    public void setTestDataFileName(String testDataFileName)
    {
        this.testDataFileName = testDataFileName;
    }

    public String getTestDataFileName()
    {
        return testDataFileName;
    }

    protected void setUp()
    throws Exception
    {
        setMithraTestObjectToResultSetComparator(new AllTypesOracleResultSetComparator());
        mithraTestResource = new MithraTestResource("MithraOracleTestConfig.xml", OracleDatabaseType.getInstance());
        mithraTestResource.setRestrictedClassList(getRestrictedClassList());

        OracleTestConnectionManager connectionManager = OracleTestConnectionManager.getInstance();
        connectionManager.setDefaultSource("mithra_qa");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());

        mithraTestResource.createSingleDatabase(connectionManager, "mithra_qa", getTestDataFileName());
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        if (!OracleTestConnectionManager.getInstance().ensureAllConnectionsReturnedToPool())
        {
            fail("Connections were not returned to pool");
        }
    }

    protected DatabaseType getDatabaseType()
    {
        return this.mithraTestResource.getDatabaseType();
    }

    protected void validateMithraResult(Operation op, String sql, int minSize)
    {
        this.validateMithraResult(new AllTypesList(op), sql, minSize);
    }

    protected void validateMithraResult(MithraList list, String sql, int minSize)
      {
          try
          {
              list.setBypassCache(true);
              Connection con = OracleTestConnectionManager.getInstance().getConnection();
              PreparedStatement ps = con.prepareStatement(sql);
              this.genericRetrievalTest(ps, list, con, minSize);
          }
          catch(SQLException e)
          {
              getLogger().error("SQLException on MithraOracleTestAbstract.validateMithraResult()",e);
              throw new RuntimeException("SQLException ",e);
          }
      }


    @Override
    public Connection getConnection()
    {
        return OracleTestConnectionManager.getInstance().getConnection();
    }

    protected void validateMithraResult(Operation op, String sql)
    {
        validateMithraResult(op, sql, 1);
    }

    protected AllTypesList createNewAllTypesList(int firstId, long count)
    {
        AllTypesList list = new AllTypesList();
        for(int i = firstId; i < (firstId + count); i++)
        {
            AllTypes obj = this.createNewAllTypes(i, true);
            list.add(obj);
        }
        return list;
    }

    protected ProductList createNewProductList(int firstId, long count)
    {
        ProductList list = new ProductList();
        for(int i = firstId; i < (firstId + count); i++)
        {
            Product obj = new Product();
            obj.setProductId(i);
            obj.setProductCode("ABC"+i);
            obj.setProductDescription("Product "+i);
            obj.setManufacturerId(1);
            obj.setDailyProductionRate(100.25f);
            list.add(obj);
        }
        return list;
    }

    protected AllTypes createNewAllTypes(int id, boolean withNullablesNull)
    {
        AllTypes allTypesObj = new AllTypes();
        Date date = new Date();
        Timestamp timestamp = new Timestamp(date.getTime());
        byte[] newData = new byte[5];
        newData[0] = toByte(0xAA);
        newData[1] = toByte(0xBB);
        newData[2] = toByte(0x99);
        newData[3] = toByte(0x11);
        newData[4] = 0;

        if(withNullablesNull)
        {
            allTypesObj.setNullablePrimitiveAttributesToNull();
        }
        else
        {
            allTypesObj.setNullableByteValue((byte)100);
            allTypesObj.setNullableShortValue((short) 30000);
            allTypesObj.setNullableCharValue('a');
            allTypesObj.setNullableIntValue(2000000000);
            allTypesObj.setNullableLongValue(9000000000000000000L);
            allTypesObj.setNullableFloatValue(100.99f);
            allTypesObj.setNullableDoubleValue(100.99998888777);
            allTypesObj.setNullableDateValue(date);
            allTypesObj.setNullableTimeValue(Time.withMillis(1, 1, 1, 0));
            allTypesObj.setNullableTimestampValue(timestamp);
            allTypesObj.setNullableStringValue("This is a test");
            allTypesObj.setNullableByteArrayValue(newData);

        }
        allTypesObj.setId(id);
        allTypesObj.setBooleanValue(true);
        allTypesObj.setByteValue((byte)100);
        allTypesObj.setShortValue((short) 30000);
        allTypesObj.setCharValue('a');
        allTypesObj.setIntValue(2000000000);
        allTypesObj.setLongValue(9000000000000000000L);
        allTypesObj.setFloatValue(100.99f);
        allTypesObj.setDoubleValue(100.99998888777);
        allTypesObj.setDateValue(date);
        allTypesObj.setTimeValue(Time.withMillis(1, 1, 1, 0));
        allTypesObj.setTimestampValue(timestamp);
        allTypesObj.setStringValue("This is a test");
        allTypesObj.setByteArrayValue(newData);

        return allTypesObj;
    }
}
