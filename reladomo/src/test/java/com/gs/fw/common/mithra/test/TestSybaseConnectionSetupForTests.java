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

import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatabaseObject;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmFinder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestSybaseConnectionSetupForTests extends MithraTestAbstract
{
    public void testToCheckRepeatReadIsDisabled() throws SQLException
    {
        // This test is not testing Mithra functionality itself. It is checking that the REPEAT_READ property has been set to false on the connection manager used for testing.
        // This ensures that we are testing Mithra against the constraints imposed by REPEAT_READ, as otherwise a typo in the property name could go unnoticed.

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try
        {
            conn = ((AlarmDatabaseObject) AlarmFinder.getMithraObjectPortal().getDatabaseObject()).getConnectionGenericSource(null);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select ID, TIME_COL, DESCRIPTION from ALARM where ID = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            try
            {
                // When REPEAT_READ property is false, the jConnect driver mandates that columns must be read in strict order - in exchange for a performance boost.
                // Therefore we test that REPEAT_READ truly has been disabled by attempting to read a column out of order. If REPEAT_READ is disabled then the driver should complain.
                rs.getString(3);
                rs.getTime(2); // this column number is out of sequence and should trigger an exception
                assertTrue("Please ensure that REPEAT_READ property is set to false in SybaseTestConnectionManager/SybaseIqTestConnectionManager. Either the property has not been set or it is not working as expected.", false);
            }
            catch (SQLException e)
            {
                assertTrue(e.getMessage().contains("JZ0R3")); // this corresponds to the "Column is DEAD" message which we expect if we read columns out of order when REPEAT_READ=false
            }
        }
        finally
        {
            try
            {
                if (rs != null)
                {
                    rs.close();
                }
            }
            catch (SQLException e)
            {
                // Ignore
            }
            try
            {
                if (stmt != null)
                {
                    stmt.close();
                }
            }
            catch (SQLException e)
            {
                // Ignore
            }
            try
            {
                if (conn != null)
                {
                    conn.close();
                }
            }
            catch (SQLException e)
            {
                // Ignore
            }
        }
    }
}
