
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TestDatedBitemporalOptimisticLockingNull extends TestDatedBitemporalOptimisticLocking implements TestDatedBitemporalDatabaseChecker
{

    private MithraTestResource mithraTestResource;

    protected void setUp()
    throws Exception
    {
        String xmlFile = System.getProperty("mithra.xml.config");

        mithraTestResource = new MithraTestResource(xmlFile);
        mithraTestResource.setRestrictedClassList(getRestrictedClassList());

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "A", "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceANull.txt");
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 0, "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceA.txt");
        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "B", "B", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceBNull.txt");
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 1, "B", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceB.txt");
        mithraTestResource.createSingleDatabase(connectionManager, "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataDefaultSourceNull.txt");
        mithraTestResource.addTestDataForPureObjects(MITHRA_TEST_DATA_FILE_PATH + "mithraTestPure.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
    }

    public void checkDatedBitemporalInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE where BALANCE_ID = ? and " +
                "OUT_Z is null and THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }


}
