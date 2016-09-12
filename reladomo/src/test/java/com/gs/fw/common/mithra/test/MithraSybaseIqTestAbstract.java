
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
import com.gs.fw.common.mithra.databasetype.SybaseIqDatabaseType;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.*;
import java.util.Calendar;
import java.util.Date;

public class MithraSybaseIqTestAbstract extends MithraTestAbstract
{
    private MithraTestResource mithraTestResource;
    private String testDataFileName = "testdata/vendor/mithraSybaseIqTestData.txt";

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
        TestInfinityTimestamp.fixForSybaseIq();
        setMithraTestObjectToResultSetComparator(new AllTypesIqResultSetComparator());
        mithraTestResource = new MithraTestResource("MithraSybaseIqTestConfig.xml", SybaseIqDatabaseType.getInstance());
        mithraTestResource.setRestrictedClassList(getRestrictedClassList());

        SybaseIqTestConnectionManager connectionManager = SybaseIqTestConnectionManager.getInstance();
        connectionManager.setDefaultSource("DVDB");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());

        mithraTestResource.createSingleDatabase(connectionManager, "DVDB", getTestDataFileName());
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "A", "sybase/mithraSybaseIqSourceATestData.txt");
        mithraTestResource.setUp();
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        if (!SybaseIqTestConnectionManager.getInstance().ensureAllConnectionsReturnedToPool())
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
        AllTypesIqList list = new AllTypesIqList(op);
        list.forceResolve();
        this.validateMithraResult(list, sql, minSize);
    }

    protected void validateMithraResult(MithraList list, String sql, int minSize)
      {
          try
          {
              list.setBypassCache(true);
              Connection con = SybaseIqTestConnectionManager.getInstance().getConnection();
              PreparedStatement ps = con.prepareStatement(sql);
              this.genericRetrievalTest(ps, list, con, minSize);
          }
          catch(SQLException e)
          {
              getLogger().error("SQLException on MithraSybaseTestAbstract.validateMithraResult()",e);
              throw new RuntimeException("SQLException ",e);
          }
      }


    protected void validateMithraResult(Operation op, String sql)
    {
        validateMithraResult(op, sql, 1);
    }

    protected AllTypesIqList createNewAllTypesIqList(int firstId, long count)
    {
        AllTypesIqList list = new AllTypesIqList();
        for(int i = firstId; i < (firstId + count); i++)
        {
            AllTypesIq obj = this.createNewAllTypesIq(i, true);
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

    protected AllTypesIq createNewAllTypesIq(int id, boolean withNullablesNull)
    {
        AllTypesIq allTypesIqObj = new AllTypesIq();
        long time = System.currentTimeMillis() / 10 * 10;
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.AM_PM, Calendar.AM);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date date = new Date(cal.getTimeInMillis());
        Timestamp timestamp = new Timestamp(time);
        byte[] newData = new byte[5];
        newData[0] = toByte(0xAA);
        newData[1] = toByte(0xBB);
        newData[2] = toByte(0x99);
        newData[3] = toByte(0x11);
        newData[4] = 0;

        if(withNullablesNull)
        {
            allTypesIqObj.setNullablePrimitiveAttributesToNull();
        }
        else
        {
            allTypesIqObj.setNullableByteValue((byte)100);
            allTypesIqObj.setNullableShortValue((short) 30000);
            allTypesIqObj.setNullableCharValue('a');
            allTypesIqObj.setNullableIntValue(2000000000);
            allTypesIqObj.setNullableLongValue(9000000000000000000L);
            allTypesIqObj.setNullableFloatValue(100.99f);
            allTypesIqObj.setNullableDoubleValue(100.99998888777);
            allTypesIqObj.setNullableDateValue(date);
            allTypesIqObj.setNullableTimestampValue(timestamp);
            allTypesIqObj.setNullableStringValue("This is a test");

        }
        allTypesIqObj.setId(id);
        allTypesIqObj.setBooleanValue(true);
        allTypesIqObj.setByteValue((byte)100);
        allTypesIqObj.setShortValue((short) 30000);
        allTypesIqObj.setCharValue('a');
        allTypesIqObj.setIntValue(2000000000);
        allTypesIqObj.setLongValue(9000000000000000000L);
        allTypesIqObj.setFloatValue(100.99f);
        allTypesIqObj.setDoubleValue(100.99998888777);
        allTypesIqObj.setDateValue(date);
        allTypesIqObj.setTimestampValue(timestamp);
        allTypesIqObj.setStringValue("This is a test");

        return allTypesIqObj;
    }

    public void testLocalTempTable() throws Exception
    {
        Connection con = SybaseIqTestConnectionManager.getInstance().getConnection();
        Statement stm = con.createStatement();
        stm.executeUpdate("create local temporary table TAAAAWMCEAHLGOOLFPCAOrderDriv (c0 integer not null)  on commit preserve rows");
        ResultSet rs = stm.executeQuery("select 1 from TAAAAWMCEAHLGOOLFPCAOrderDriv where 0 = 1");
        ResultSetMetaData metaData = rs.getMetaData();
        System.out.println("meta data");
        con.close();

    }
}