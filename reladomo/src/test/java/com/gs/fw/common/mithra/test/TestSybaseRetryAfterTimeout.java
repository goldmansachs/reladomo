
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
import com.gs.fw.common.mithra.test.domain.ProductFinder;
import com.gs.fw.common.mithra.test.domain.ProductList;
import com.gs.fw.common.mithra.transaction.TransactionStyle;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *  Test for retry after timeout
 */
public class TestSybaseRetryAfterTimeout
extends MithraSybaseTestAbstract
{
    
    private static final int RETRY_COUNT = 2;
    private static final int TIMEOUT_TIME = 5;
    private int counter = 0;

    public TransactionalCommand getTransactionalCommandWithSetLockNoWait()
    {
        return new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx)
                throws Throwable
                {
                    Connection con = SybaseTestConnectionManager.getInstance().getConnection();
                    con.createStatement().execute("set lock nowait");
                    con.close();
                    counter++;
                    ProductList list = new ProductList(ProductFinder.all());
                    list.setDailyProductionRate(827);
                    return null;
                }
            };
    }

    public void testNoRetryIfNotSet() throws SQLException
    {
        Connection con = this.resetCounterAndLockTableAndGetConnection();
        this.executeTransactionalCommand(con, false);
        assertEquals("Should not have retried due to timeout", 0, counter-1);            
    }

    public void testRetryAfterTimeout() throws SQLException
    {
        Connection con = this.resetCounterAndLockTableAndGetConnection();
        this.executeTransactionalCommand(con, true);
        assertEquals("Number of times retried doesn't match", RETRY_COUNT, counter-1);
    }
    
    public void testNoTimeoutScenario() throws SQLException
    {
        this.counter = 0;        

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(this.getTransactionalCommandWithSetLockNoWait(), 
                                                                            new TransactionStyle(TIMEOUT_TIME, RETRY_COUNT, true));
        }
        catch(MithraBusinessException e)
        {
            getLogger().warn("Expected exception: " + e);
        } 
        assertEquals("There shouldn't have been any retries", 0, counter-1);            
    }
    
    private Connection resetCounterAndLockTableAndGetConnection()
        throws SQLException
    {
        this.counter = 0;
        Connection con = SybaseTestConnectionManager.getInstance().getConnection();
        con.createStatement().execute("begin tran " +
            "update PRODUCT set DAILY_PRODUCTION_RATE = 999");
        return con;
    }
    
    private void executeTransactionalCommand(Connection con, boolean retriableAfterTimeout)
        throws SQLException
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(this.getTransactionalCommandWithSetLockNoWait(),
                                                                            new TransactionStyle(TIMEOUT_TIME, RETRY_COUNT, retriableAfterTimeout));
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
    }    
}
