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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.transaction.TransactionStyle;

import java.sql.Connection;
import java.sql.SQLException;


public class TestH2RetryAfterTimeout
extends MithraTestAbstract
{
    private static final int RETRY_COUNT = 2;
    private static final int TIMEOUT_TIME = 5;
    private static int counter = 0;

    protected Class[] getRestrictedClassList()
    {
        return new Class[]{Employee.class};
    }

    public TransactionalCommand getTransactionalCommandWithSetLockNoWait()
    {
        return new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx)
                throws Throwable
                {
                    Connection con = ConnectionManagerForTests.getInstance().getConnection();
                    con.createStatement().execute("SET LOCK_TIMEOUT 0");
                    con.close();
                    counter++;
                    EmployeeList list = new EmployeeList(EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().eq(1)));
                    list.setPhone("090-4427-6408");
                    return null;
                }
            };
    }

    public void testRetryAfterTimeout() throws SQLException
    {
        counter = 0;
        Connection con = ConnectionManagerForTests.getInstance().getConnection();
        con.createStatement().execute("SET AUTOCOMMIT OFF");
        con.createStatement().execute("update EMPLOYEE set PHONE = '03-5496-0544' where ID = 1");

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(this.getTransactionalCommandWithSetLockNoWait(),
                                                                            new TransactionStyle(TIMEOUT_TIME, RETRY_COUNT, true));
        }
        catch(MithraBusinessException e)
        {
            getLogger().warn("Expected exception: " + e);
        }
        finally
        {
            if (con != null)
            {
                con.rollback();
                con.close();
            }
        }
        assertEquals("Number of times retried doesn't match", RETRY_COUNT, counter -1);
    }

    public void testNoRetryIfNotSet() throws SQLException
    {
        counter = 0;
        Connection con = ConnectionManagerForTests.getInstance().getConnection();
        con.createStatement().execute("SET AUTOCOMMIT OFF");
        con.createStatement().execute("update EMPLOYEE set PHONE = '03-5496-0544' where ID = 1");

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(this.getTransactionalCommandWithSetLockNoWait(),
                                                                            new TransactionStyle(TIMEOUT_TIME, RETRY_COUNT, false));
        }
        catch(MithraBusinessException e)
        {
            getLogger().warn("Expected exception: " + e);
        }
        finally
        {
            if (con != null)
            {
                con.rollback();
                con.close();
            }
        }
        assertEquals("Should not have retried due to timeout", 0, counter -1);
    }
}
