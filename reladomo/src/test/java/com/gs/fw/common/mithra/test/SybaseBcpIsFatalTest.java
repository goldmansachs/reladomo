
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

import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.test.domain.bcp.BcpDefaultColumn;
import com.gs.fw.common.mithra.test.domain.bcp.BcpDefaultColumnList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class SybaseBcpIsFatalTest extends SybaseBcpTestAbstract
{

    private static final int INSERT_COUNT = 5;

    protected void tearDown() throws Exception
    {
//        JtdsBcpBulkLoader.zClearBulkLoaderPools();
        super.tearDown();
    }

    public void testBcpDefaultColumnsIsFatalTrue()
    {
        SybaseTestConnectionManager.getInstance().setDataModelMismatchIsFatal(true);
        addColumnsToBcpDefaultColumnTable();

        try
        {
            runAsBcpTransaction(new Callable()
            {
                public Object call()
                {
                    return insertBcpDefaultColumn();
                }
            });
        }
        catch (Exception e)
        {
            assertTrue("Unexpected exception: " + e.getMessage(), e instanceof MithraDatabaseException);
            return;
        }
        finally
        {
            dropColumnsFromBcpDefaultColumnTable();
        }

        fail("Expected to throw MithraDatabaseException");
    }


    public void testBcpDefaultColumnsIsFatalFalse()
    {
        addColumnsToBcpDefaultColumnTable();
        SybaseTestConnectionManager.getInstance().setDataModelMismatchIsFatal(false);
        try
        {
            runAsBcpTransaction(new Callable()
            {
                public Object call()
                {
                    return insertBcpDefaultColumn();
                }
            });

            assertBcpDefaultColumnsIsFatalFalse();
        }
        finally
        {
            SybaseTestConnectionManager.getInstance().setDataModelMismatchIsFatal(true);
            dropColumnsFromBcpDefaultColumnTable();
        }
    }

    private BcpDefaultColumnList insertBcpDefaultColumn()
    {
        BcpDefaultColumnList list = new BcpDefaultColumnList();
        for (int i = 0; i < INSERT_COUNT; i++)
        {
            list.add(new BcpDefaultColumn(i, "Simple Name" + i));
        }
        list.insertAll();
        return list;
    }

    private void assertBcpDefaultColumnsIsFatalFalse()
    {
        ResultSet resultSet = selectAllFromBcpDefaultColumnTable();
        int i = 0;
        try
        {
            while (resultSet.next())
            {
                int id = resultSet.getInt(1);
                String stringColumn = resultSet.getString(2);
                int defaultIntColumn = resultSet.getInt(3);
                String nonDefaultIntColumn = resultSet.getString(4);
                String nonDefaultStringColumn = resultSet.getString(5);
                boolean nonDefaultBit = resultSet.getBoolean(6);
                short defaultShort = resultSet.getShort(7);

                assertEquals(i, id);
                assertEquals("Simple Name" + i, stringColumn);
                assertEquals(0, defaultIntColumn);
                assertNull(nonDefaultIntColumn);
                assertNull(nonDefaultStringColumn);
                assertFalse(nonDefaultBit);
                assertEquals(0, defaultShort);

                i++;
            }
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }

        assertEquals(INSERT_COUNT, i);
    }

    public void addColumnsToBcpDefaultColumnTable()
    {
        executeQuery("ALTER TABLE BCP_DEFAULT_COLUMN ADD DEFAULT_INT_COL INT DEFAULT 1, " +
                "NON_DEFAULT_INT_COL INT NULL, NON_DEFAULT_STRING_COL VARCHAR(50) NULL, DEFAULT_BIT_COL BIT DEFAULT 1," +
                "DEFAULT_SHORT_COL SMALLINT DEFAULT 2", true);
    }

    public ResultSet selectAllFromBcpDefaultColumnTable()
    {
        return executeQuery("select * from BCP_DEFAULT_COLUMN", false);
    }

    public void dropColumnsFromBcpDefaultColumnTable()
    {
        executeQuery("ALTER TABLE BCP_DEFAULT_COLUMN DROP DEFAULT_INT_COL, " +
                "NON_DEFAULT_INT_COL, NON_DEFAULT_STRING_COL, DEFAULT_BIT_COL, DEFAULT_SHORT_COL ", true);
    }

    private ResultSet executeQuery(String sql, boolean isUpdate)
    {
        Connection connection = SybaseTestConnectionManager.getInstance().getConnection();
        ResultSet rs = null;
        try
        {
            if (isUpdate)
            {
                connection.createStatement().executeUpdate(sql);
            }
            else
            {
                rs = connection.createStatement().executeQuery(sql);
            }
        }
        catch (SQLException e)
        {
            throw new RuntimeException("caught unexpected SqlException, throwing to fail test", e);
        }
        finally
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                //do nothing
            }
        }
        return rs;
    }
}
