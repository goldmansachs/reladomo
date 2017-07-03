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

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.*;
import java.util.concurrent.Exchanger;


public class TestSimulatedSequence extends MithraTestAbstract
{
    
    public void setUp() throws Exception
    {
        super.setUp();
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AccountDescendingKey.class,
            AccountTransactionException.class,
            AccountTransaction.class,
            AccountTransactionNoSrcAttr.class,
            SpecialAccountTransactionException.class,
            SpecialAccountTransaction.class,
            MithraTestSequence.class,
            RiskValueTestAccount.class,
            RiskDateTestAccount.class,
            Order.class,
            OrderItem.class,
            Product.class,
            DatedWithSequence.class,
            DatedWithDescendingSequence.class
        };
    }

    public void testTwoThreadsHoldingLocks()
    {
        final Exchanger rendezvous = new Exchanger();

        Runnable runnable1 = new Runnable()
        {
            public void run()
            {

                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                        new TransactionalCommand()
                        {

                            public Object executeTransaction(MithraTransaction tx) throws Throwable
                            {
                                AccountTransaction one = AccountTransactionFinder.findByPrimaryKey(1, "B");
                                one.setTransactionDescription("something different");
                                tx.executeBufferedOperations(); // we should now have a lock on this table.
                                waitForOtherThread(rendezvous);
                                sleep(1000); // wait for the other thread to lock the simulated sequence. this can't be coordinated.
                                AccountTransaction newTran = new AccountTransaction();
                                newTran.setTransactionDescription("new one 1");
                                newTran.setDeskId("B");
                                newTran.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                                newTran.insert();
                                return null;
                            }
                        });
                waitForOtherThread(rendezvous);
            }


        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                        new TransactionalCommand()
                        {

                            public Object executeTransaction(MithraTransaction tx) throws Throwable
                            {
                                Connection con = ConnectionManagerForTests.getInstance().getConnection("B");
                                con.createStatement().execute("set lock_timeout 10000");
                                con.close();
                                AccountTransaction newTran = new AccountTransaction();
                                newTran.setTransactionDescription("new one 2");
                                newTran.setDeskId("B");
                                newTran.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                                waitForOtherThread(rendezvous); // wait for the other thread to lock the table
                                newTran.insert(); // this should try to do a max from the table
                                con = ConnectionManagerForTests.getInstance().getConnection("B");
                                con.createStatement().execute("set lock_timeout 2000");
                                con.close();
                                return null;
                            }
                        });
                waitForOtherThread(rendezvous);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testSequenceInitialization()  throws Exception
    {
        new RiskValueTestAccountList(RiskValueTestAccountFinder.all()).deleteAll();
        new RiskDateTestAccountList(RiskDateTestAccountFinder.all()).deleteAll();
        RiskValueTestAccount obj = new RiskValueTestAccount();
        obj.setId(1);
        obj.setNullablePrimitiveAttributesToNull();
        obj.insert();

        RiskValueTestAccount obj2 = new RiskValueTestAccount();
        assertEquals(2, obj2.generateAndSetId());
    }

    public void testTwoProcessesAscending() throws Exception
    {
        new AccountTransactionList(AccountTransactionFinder.deskId().eq("B")).deleteAll();

        AccountTransactionList list0 = new AccountTransactionList();
        AccountTransaction accountTransaction0 = null;
        for (int i = 0; i < 3; i++)
        {
            accountTransaction0 = new AccountTransaction();
            accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
            accountTransaction0.setTransactionDescription("Transaction failure: "+i);
            accountTransaction0.setDeskId("A");
            list0.add(accountTransaction0);
        }
        list0.insertAll();

        String sql = "INSERT INTO ACCOUNT_TRANSACTION (TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_DESCRIPTION) VALUES(?,?,?)";
        Connection con = this.getConnection("B");
        con.setAutoCommit(false);
        PreparedStatement ps = null;
        ps = con.prepareStatement(sql);
        for(int i = 0; i < 5; i++)
        {
            ps.setInt(1, 1011 + i);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, "Account Transaction");
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        ps.close();
        con.close();

        con = this.getConnection("B");

        sql = "UPDATE MITHRA_TEST_SEQUENCE SET NEXT_VALUE = ? WHERE SEQUENCE_NAME = ?";

        ps = con.prepareStatement(sql);
        con.setAutoCommit(true);

        ps.setLong(1, 1021);
        ps.setString(2, "AccountTransactionSequence");
        int count = ps.executeUpdate();

        ps.close();
        con.close();
        assertEquals(count, 1);

        AccountTransactionList list1 = new AccountTransactionList();
        AccountTransaction accountTransaction1 = null;
        for (int i = 0; i < 10; i++)
        {
            accountTransaction1 = new AccountTransaction();
            accountTransaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
            accountTransaction1.setTransactionDescription("Transaction failure: "+i);
            accountTransaction1.setDeskId("B");
            list1.add(accountTransaction1);
        }

        list1.insertAll();
        for(int i=0;i<list1.size();i++)
        {
            int id = list1.getAccountTransactionAt(i).getTransactionId();
            assertTrue(id < 1032);
            assertTrue(id > 1020);
        }
    }

    public void testTwoProcessesDescending() throws Exception
    {
        AccountDescendingKeyList list0 = new AccountDescendingKeyList();
        AccountDescendingKey account0 = null;
        for (int i = 0; i < 6; i++)
        {
            account0 = new AccountDescendingKey();
            account0.setAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
            account0.setAccountDescription("Account: "+i);
            account0.setDeskId("A");
            list0.add(account0);
        }
        list0.insertAll();
        for(int i=0;i<list0.size();i++)
        {
            assertTrue(list0.getAccountDescendingKeyAt(i).getAccountId() < 1 || list0.getAccountDescendingKeyAt(i).getAccountId() > -20);
        }

        String sql = "INSERT INTO ACCT_DSC_KEY (ACCOUNT_ID, ACCOUNT_OPENING_DATE, ACCOUNT_DESCRIPTION) VALUES(?,?,?)";
        Connection con = this.getConnection("B");
        con.setAutoCommit(false);
        PreparedStatement ps = null;
        ps = con.prepareStatement(sql);
        for(int i = 0; i < 30; i++)
        {
            ps.setLong(1, (-62 - (3*i)));
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, "New Account");
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        ps.close();
        con.close();

        con = this.getConnection("B");

        sql = "UPDATE MITHRA_TEST_SEQUENCE SET NEXT_VALUE = ? WHERE SEQUENCE_NAME = ?";

        ps = con.prepareStatement(sql);
        con.setAutoCommit(true);

        ps.setLong(1, -152);
        ps.setString(2, "DecrementingSequence");
        int count = ps.executeUpdate();

        ps.close();
        con.close();
        assertEquals(count, 1);

        AccountDescendingKeyList list1 = new AccountDescendingKeyList();
        AccountDescendingKey account1 = null;
        for (int i = 0; i < 20; i++)
        {
            account1 = new AccountDescendingKey();
            account1.setAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
            account1.setAccountDescription("Transaction failure: "+i);
            account1.setDeskId("B");
            list1.add(account1);
        }

        list1.insertAll();
        for(int i=0;i<list1.size();i++)
        {
            assertTrue((list1.getAccountDescendingKeyAt(i).getAccountId() < -17 || list1.getAccountDescendingKeyAt(i).getAccountId() > -62)||(list1.getAccountDescendingKeyAt(i).getAccountId() < -119 || list1.getAccountDescendingKeyAt(i).getAccountId() > -140));
        }
    }

    public void testUseMultiUpdate()
    {
        assertFalse(AccountTransactionExceptionFinder.getMithraObjectPortal().useMultiUpdate());
        assertTrue(OrderFinder.getMithraObjectPortal().useMultiUpdate());
    }

    public void testInsertDatedObject()
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        DatedWithSequence balance = new DatedWithSequence(businessDate, InfinityTimestamp.getParaInfinity());
        balance.setAcmapCode("A");
        long balanceId = balance.generateAndSetBalanceId();
        balance.setQuantity(10.0);
        balance.insert();
        assertEquals(1000, balanceId);
        tx.commit();
    }

    public void testInsertDatedWithDecrementingSequence()
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        DatedWithDescendingSequence balance0 = new DatedWithDescendingSequence(businessDate, InfinityTimestamp.getParaInfinity());
        balance0.setAcmapCode("A");
        balance0.setQuantity(10.0);
        long balanceId0 = balance0.generateAndSetBalanceId();
        balance0.insert();
        assertEquals(1000, balanceId0);

        DatedWithDescendingSequence balance1 = new DatedWithDescendingSequence(businessDate, InfinityTimestamp.getParaInfinity());
        balance1.setAcmapCode("A");
        balance1.setQuantity(10.0);
        long balanceId1 = balance1.generateAndSetBalanceId();
        balance1.insert();
        assertEquals(999, balanceId1);
        tx.commit();

    }

    public void testInsertOneObject()
    {
        AccountTransactionException transactionException = new AccountTransactionException();
        transactionException.setDeskId("A");
        transactionException.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        transactionException.setExceptionDescription("Transaction failure");
        long exceptionId = transactionException.generateAndSetExceptionId();

        assertEquals(1005, exceptionId);

    }

   public void testSimulatedSequenceExceptionHandling() throws SQLException
   {
       String sourceAttribute = "B";
       String dropTableSql = "DROP TABLE MITHRA_TEST_SEQUENCE";
       Connection con = this.getConnection(sourceAttribute);
       PreparedStatement ps = null;

       ps = con.prepareStatement(dropTableSql);
       ps.execute();

       AccountTransaction at = new AccountTransaction();
       at.setDeskId("A");
       at.setTransactionDescription("Description");
       try
       {
          at.insert();
          fail("Should not got here....");
       }
       catch(Exception e)
       {
          assertNotNull(e);
       }
       finally
       {
           ps.close();
           con.close();
           MithraTestSequenceDatabaseObject dboa = (MithraTestSequenceDatabaseObject) MithraTestSequenceFinder.getMithraObjectPortal().getDatabaseObject();
           dboa.createTestTable(sourceAttribute);
       }
   }

    public void testInterlacing() throws SQLException
    {
        String sql = "INSERT INTO ACCT_DSC_KEY (ACCOUNT_ID, ACCOUNT_OPENING_DATE, ACCOUNT_DESCRIPTION) VALUES(?,?,?)";
        Connection con = this.getConnection("A");
        con.setAutoCommit(false);
        PreparedStatement ps = null;
        ps = con.prepareStatement(sql);
        for(int i = 0; i < 5; i++)
        {
            ps.setLong(1, (-1 - (3*i)));
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, "New Account");
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        ps.close();
        con.close();

        sql = "INSERT INTO ACCT_DSC_KEY (ACCOUNT_ID, ACCOUNT_OPENING_DATE, ACCOUNT_DESCRIPTION) VALUES(?,?,?)";
        con = this.getConnection("A");
        con.setAutoCommit(false);
        ps = null;
        ps = con.prepareStatement(sql);
        for(int i = 0; i < 5; i++)
        {
            ps.setLong(1, (-3 - (3*i)));
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, "New Account");
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        ps.close();
        con.close();

        AccountDescendingKeyList list1 = new  AccountDescendingKeyList();
        AccountDescendingKey account = null;
        long accountId = 0;
        for(int i = 0; i < 5; i++)
        {
            account = new AccountDescendingKey();
            account.setDeskId("A");
            account.setAccountDescription("Account "+i);
            account.setAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
            accountId = account.generateAndSetAccountId();
            assertEquals((-17 +(i * -3)), accountId);
            list1.add(account);
        }
        list1.insertAll();
        for(int i=0;i<list1.size();i++)
        {
            assertTrue(list1.getAccountDescendingKeyAt(i).getAccountId() <= -14 + (-3 * i));
        }
    }

    public void testSequenceWithNegativeIncrement()
    {
        AccountDescendingKeyList list1 = new  AccountDescendingKeyList();
        AccountDescendingKey account = null;
        long accountId = 0;
        for(int i = 0; i < 20; i++)
        {
            account = new AccountDescendingKey();
            account.setDeskId("A");
            account.setAccountDescription("Account "+i);
            account.setAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
            accountId = account.generateAndSetAccountId();
            assertEquals((-2 +(i * -3)), accountId);
            list1.add(account);
        }
        list1.insertAll();

        for(int i=0;i<list1.size();i++)
        {
            assertTrue(list1.getAccountDescendingKeyAt(i).getAccountId() < 1 || list1.getAccountDescendingKeyAt(i).getAccountId() > -62);
        }

    }

    public void testMultipleBorrowersWithNegativeIncrement()
    throws Exception
    {
        AccountDescendingKeyList list0 = new AccountDescendingKeyList();
        AccountDescendingKey account0 = null;
        for (int i = 0; i < 6; i++)
        {
            account0 = new AccountDescendingKey();
            account0.setAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
            account0.setAccountDescription("Account: "+i);
            account0.setDeskId("A");
            list0.add(account0);
        }
        list0.insertAll();
        for(int i=0;i<list0.size();i++)
        {
            assertTrue(list0.getAccountDescendingKeyAt(i).getAccountId() < 1 || list0.getAccountDescendingKeyAt(i).getAccountId() > -20);
        }

        String sql = "INSERT INTO ACCT_DSC_KEY (ACCOUNT_ID, ACCOUNT_OPENING_DATE, ACCOUNT_DESCRIPTION) VALUES(?,?,?)";
        Connection con = this.getConnection("A");
        con.setAutoCommit(false);
        PreparedStatement ps = null;
        ps = con.prepareStatement(sql);
        for(int i = 0; i < 30; i++)
        {
            ps.setLong(1, (-62 - (3*i)));
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, "New Account");
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        ps.close();
        con.close();

        con = this.getConnection("B");

        sql = "UPDATE MITHRA_TEST_SEQUENCE SET NEXT_VALUE = ? WHERE SEQUENCE_NAME = ?";

        ps = con.prepareStatement(sql);
        con.setAutoCommit(true);

        ps.setLong(1, -152);
        ps.setString(2, "DecrementingSequence");
        int count = ps.executeUpdate();

        ps.close();
        con.close();
        assertEquals(count, 1);

        AccountDescendingKeyList list1 = new AccountDescendingKeyList();
        AccountDescendingKey account1 = null;
        for (int i = 0; i < 20; i++)
        {
            account1 = new AccountDescendingKey();
            account1.setAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
            account1.setAccountDescription("Transaction failure: "+i);
            account1.setDeskId("A");
            list1.add(account1);
        }

        list1.insertAll();
        for(int i=0;i<list1.size();i++)
        {
            assertTrue((list1.getAccountDescendingKeyAt(i).getAccountId() < -17 || list1.getAccountDescendingKeyAt(i).getAccountId() > -62)||(list1.getAccountDescendingKeyAt(i).getAccountId() < -119 || list1.getAccountDescendingKeyAt(i).getAccountId() > -140));
        }

    }

    public void testInsertAllWithMultipleSourceAttributesInOneList()
    {
        AccountTransactionExceptionList list1 = new AccountTransactionExceptionList();
        AccountTransactionException accountTransactionException1 = null;
        String deskId;
        for (int i = 0; i < 10; i++)
        {
            deskId = (i%2 == 0?"A":"B");
            accountTransactionException1 = new AccountTransactionException();
            accountTransactionException1.setExceptionDate(new Timestamp(System.currentTimeMillis()));
            accountTransactionException1.setExceptionDescription("Transaction failure: "+i);
            accountTransactionException1.setDeskId(deskId);
            list1.add(accountTransactionException1);
        }
        list1.insertAll();
        for(int i=0;i<list1.size();i++)
        {
            if(list1.getAccountTransactionExceptionAt(i).getDeskId().equals("A"))
                assertTrue(list1.getAccountTransactionExceptionAt(i).getExceptionId() > 1002 && list1.getAccountTransactionExceptionAt(i).getExceptionId() < 1033);
            else
                assertTrue(list1.getAccountTransactionExceptionAt(i).getExceptionId() > 100002 && list1.getAccountTransactionExceptionAt(i).getExceptionId() < 100033);

        }
    }

    public void testMultipleBorrowers() throws SQLException
    {
        AccountTransactionList list0 = new AccountTransactionList();
        AccountTransaction accountTransaction0 = null;
        for (int i = 0; i < 3; i++)
        {
            accountTransaction0 = new AccountTransaction();
            accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
            accountTransaction0.setTransactionDescription("Transaction failure: "+i);
            accountTransaction0.setDeskId("A");
            list0.add(accountTransaction0);
        }
        list0.insertAll();

        String sql = "INSERT INTO ACCOUNT_TRANSACTION (TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_DESCRIPTION) VALUES(?,?,?)";
        Connection con = this.getConnection("A");
        con.setAutoCommit(false);
        PreparedStatement ps = null;
        ps = con.prepareStatement(sql);
        for(int i = 0; i < 10; i++)
        {
            ps.setInt(1, 1011 + i);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, "Account Transaction");
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        ps.close();
        con.close();

        con = this.getConnection("B");

        sql = "UPDATE MITHRA_TEST_SEQUENCE SET NEXT_VALUE = ? WHERE SEQUENCE_NAME = ?";

        ps = con.prepareStatement(sql);
        con.setAutoCommit(true);

        ps.setLong(1, 1021);
        ps.setString(2, "AccountTransactionSequence");
        int count = ps.executeUpdate();

        ps.close();
        con.close();
        assertEquals(count, 1);

        AccountTransactionList list1 = new AccountTransactionList();
        AccountTransaction accountTransaction1 = null;
        for (int i = 0; i < 10; i++)
        {
            accountTransaction1 = new AccountTransaction();
            accountTransaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
            accountTransaction1.setTransactionDescription("Transaction failure: "+i);
            accountTransaction1.setDeskId("A");
            list1.add(accountTransaction1);
        }

        list1.insertAll();
        for(int i=0;i<list1.size();i++)
        {
            assertTrue(list1.getAccountTransactionAt(i).getTransactionId() < 1011 || list1.getAccountTransactionAt(i).getTransactionId() > 1020);
        }

    }

    public void testMultipleBatchNeedsInOneTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (int i = 0; i < 50; i++)
                {
                    AccountTransaction accountTransaction = new AccountTransaction();
                    accountTransaction.setDeskId("A");
                    accountTransaction.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                    accountTransaction.setTransactionDescription("desc "+i);
                    accountTransaction.insert();
                }
                return null;
            }
        });

    }

    public void testTwoMithraObjectsWithNoSourceAttributeUsingSameSequenceHasNoSourceAttribute()
    {
        RiskDateTestAccount dateTestAccount0 = new RiskDateTestAccount();
        dateTestAccount0.setAccountNumber("987654321");
        dateTestAccount0.setRiskDate(new Date(System.currentTimeMillis()));
        int dId0 = dateTestAccount0.generateAndSetId();
        assertEquals(1103, dId0);

        RiskValueTestAccount valueTestAccount0 = new RiskValueTestAccount();
        valueTestAccount0.setRiskDateAccountId(1000);
        valueTestAccount0.setRiskPointId(10000);
        valueTestAccount0.setRiskValue(100000);
        int vId0 = valueTestAccount0.generateAndSetId();
        assertEquals(1104, vId0);

        RiskValueTestAccount valueTestAccount1 = new RiskValueTestAccount();
        valueTestAccount1.setRiskDateAccountId(1000);
        valueTestAccount1.setRiskPointId(10000);
        valueTestAccount1.setRiskValue(100000);
        int vId1 = valueTestAccount1.generateAndSetId();
        assertEquals(1105, vId1);

        RiskDateTestAccount dateTestAccount1 = new RiskDateTestAccount();
        dateTestAccount1.setAccountNumber("987654321");
        dateTestAccount1.setRiskDate(new Date(System.currentTimeMillis()));
        int dId1 = dateTestAccount0.generateAndSetId();
        assertEquals(1106, dId1);

        dateTestAccount0.insert();
        dateTestAccount1.insert();
        valueTestAccount0.insert();
        valueTestAccount1.insert();
    }

    public void testTwoMithraObjectsWithSameSourceAttributeUsingSameSequenceHasNoSourceAttribute()
    {

        for(int i = 0; i < 10; i ++)
        {
            AccountTransaction accountTransaction0 = new AccountTransaction();
            accountTransaction0.setDeskId("A");
            accountTransaction0.setTransactionDescription("Account Transaction 0");
            accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
            accountTransaction0.generateAndSetTransactionId();
            accountTransaction0.insert();

            SpecialAccountTransaction specialAccountTransaction0 = new SpecialAccountTransaction();
            specialAccountTransaction0.setDeskId("A");
            specialAccountTransaction0.setSpecialTransactionDescription("Account Transaction 0");
            specialAccountTransaction0.setSpecialTransactionDate(new Timestamp(System.currentTimeMillis()));
            specialAccountTransaction0.generateAndSetSpecialTransactionId();
            specialAccountTransaction0.insert();

            assertEquals(accountTransaction0.getTransactionId() + 1, specialAccountTransaction0.getSpecialTransactionId());
        }
    }

    public void testTwoMithraObjectsWithSameSourceAttributeUsingSameSequenceHasSourceAttribute()
    {

        AccountTransactionException accountTransactionException0 = new AccountTransactionException();
        accountTransactionException0.setDeskId("A");
        accountTransactionException0.setExceptionDescription("Exception 0");
        accountTransactionException0.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId0 = accountTransactionException0.generateAndSetExceptionId();
        accountTransactionException0.insert();
        assertEquals(1005, exceptionId0);

        SpecialAccountTransactionException specialAccountTransactionException0 = new SpecialAccountTransactionException();
        specialAccountTransactionException0.setDeskId("A");
        specialAccountTransactionException0.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException0.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId0 = specialAccountTransactionException0.generateAndSetSpecialExceptionId();
        specialAccountTransactionException0.insert();
        assertEquals(1008, specialExceptionId0);

        AccountTransactionException accountTransactionException1 = new AccountTransactionException();
        accountTransactionException1.setDeskId("A");
        accountTransactionException1.setExceptionDescription("Exception 0");
        accountTransactionException1.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId1 = accountTransactionException1.generateAndSetExceptionId();
        accountTransactionException1.insert();
        assertEquals(1011, exceptionId1);

        SpecialAccountTransactionException specialAccountTransactionException1 = new SpecialAccountTransactionException();
        specialAccountTransactionException1.setDeskId("A");
        specialAccountTransactionException1.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException1.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId1 = specialAccountTransactionException1.generateAndSetSpecialExceptionId();
        specialAccountTransactionException1.insert();
        assertEquals(1014, specialExceptionId1);

        AccountTransactionException accountTransactionException2 = new AccountTransactionException();
        accountTransactionException2.setDeskId("A");
        accountTransactionException2.setExceptionDescription("Exception 0");
        accountTransactionException2.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId2 = accountTransactionException2.generateAndSetExceptionId();
        accountTransactionException2.insert();
        assertEquals(1017, exceptionId2);

        SpecialAccountTransactionException specialAccountTransactionException2 = new SpecialAccountTransactionException();
        specialAccountTransactionException2.setDeskId("A");
        specialAccountTransactionException2.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException2.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId2 = specialAccountTransactionException2.generateAndSetSpecialExceptionId();
        specialAccountTransactionException2.insert();
        assertEquals(1020, specialExceptionId2);

        AccountTransactionException accountTransactionException3 = new AccountTransactionException();
        accountTransactionException3.setDeskId("A");
        accountTransactionException3.setExceptionDescription("Exception 0");
        accountTransactionException3.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId3 = accountTransactionException3.generateAndSetExceptionId();
        accountTransactionException3.insert();
        assertEquals(1023, exceptionId3);

        SpecialAccountTransactionException specialAccountTransactionException3 = new SpecialAccountTransactionException();
        specialAccountTransactionException3.setDeskId("A");
        specialAccountTransactionException3.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException3.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId3 = specialAccountTransactionException3.generateAndSetSpecialExceptionId();
        specialAccountTransactionException3.insert();
        assertEquals(1026, specialExceptionId3);
    }

    public void testTwoMithraObjectsWithDifferentSourceAttributeUsingSameSequenceHasNoSourceAttribute()
    {

        AccountTransaction accountTransaction0 = new AccountTransaction();
        accountTransaction0.setDeskId("A");
        accountTransaction0.setTransactionDescription("Account Transaction 0");
        accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        long transactionId0 = accountTransaction0.generateAndSetTransactionId();
        accountTransaction0.insert();
        assertEquals(1001, transactionId0);

        SpecialAccountTransaction specialAccountTransaction0 = new SpecialAccountTransaction();
        specialAccountTransaction0.setDeskId("B");
        specialAccountTransaction0.setSpecialTransactionDescription("Account Transaction 0");
        specialAccountTransaction0.setSpecialTransactionDate(new Timestamp(System.currentTimeMillis()));
        long specialTransactionId0 = specialAccountTransaction0.generateAndSetSpecialTransactionId();
        specialAccountTransaction0.insert();
        assertEquals(100001, specialTransactionId0);

        AccountTransaction accountTransaction1 = new AccountTransaction();
        accountTransaction1.setDeskId("A");
        accountTransaction1.setTransactionDescription("Account Transaction 0");
        accountTransaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        long transactionId1 = accountTransaction1.generateAndSetTransactionId();
        accountTransaction1.insert();
        assertEquals(100002, transactionId1);

        SpecialAccountTransaction specialAccountTransaction1 = new SpecialAccountTransaction();
        specialAccountTransaction1.setDeskId("B");
        specialAccountTransaction1.setSpecialTransactionDescription("Account Transaction 0");
        specialAccountTransaction1.setSpecialTransactionDate(new Timestamp(System.currentTimeMillis()));
        long specialTransactionId1 = specialAccountTransaction1.generateAndSetSpecialTransactionId();
        specialAccountTransaction1.insert();
        assertEquals(100003, specialTransactionId1);

        AccountTransaction accountTransaction2 = new AccountTransaction();
        accountTransaction2.setDeskId("A");
        accountTransaction2.setTransactionDescription("Account Transaction 0");
        accountTransaction2.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        long transactionId2 = accountTransaction2.generateAndSetTransactionId();
        accountTransaction2.insert();
        assertEquals(100004, transactionId2);

        SpecialAccountTransaction specialAccountTransaction2 = new SpecialAccountTransaction();
        specialAccountTransaction2.setDeskId("B");
        specialAccountTransaction2.setSpecialTransactionDescription("Account Transaction 0");
        specialAccountTransaction2.setSpecialTransactionDate(new Timestamp(System.currentTimeMillis()));
        long specialTransactionId2 = specialAccountTransaction2.generateAndSetSpecialTransactionId();
        specialAccountTransaction2.insert();
        assertEquals(100005, specialTransactionId2);

        AccountTransaction accountTransaction3 = new AccountTransaction();
        accountTransaction3.setDeskId("A");
        accountTransaction3.setTransactionDescription("Account Transaction 0");
        accountTransaction3.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        long transactionId3 = accountTransaction3.generateAndSetTransactionId();
        accountTransaction3.insert();
        assertEquals(100006, transactionId3);

        SpecialAccountTransaction specialAccountTransaction3 = new SpecialAccountTransaction();
        specialAccountTransaction3.setDeskId("B");
        specialAccountTransaction3.setSpecialTransactionDescription("Account Transaction 0");
        specialAccountTransaction3.setSpecialTransactionDate(new Timestamp(System.currentTimeMillis()));
        long specialTransactionId3 = specialAccountTransaction3.generateAndSetSpecialTransactionId();
        specialAccountTransaction3.insert();
        assertEquals(100007, specialTransactionId3);
    }

    public void testTwoMithraObjectsWithDifferentSourceAttributeUsingSameSequenceHasSourceAttribute()
    {

        AccountTransactionException accountTransactionException0 = new AccountTransactionException();
        accountTransactionException0.setDeskId("A");
        accountTransactionException0.setExceptionDescription("Exception 0");
        accountTransactionException0.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId0 = accountTransactionException0.generateAndSetExceptionId();
        accountTransactionException0.insert();
        assertEquals(1005, exceptionId0);

        SpecialAccountTransactionException specialAccountTransactionException0 = new SpecialAccountTransactionException();
        specialAccountTransactionException0.setDeskId("B");
        specialAccountTransactionException0.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException0.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId0 = specialAccountTransactionException0.generateAndSetSpecialExceptionId();
        specialAccountTransactionException0.insert();
        assertEquals(100005, specialExceptionId0);

        AccountTransactionException accountTransactionException1 = new AccountTransactionException();
        accountTransactionException1.setDeskId("A");
        accountTransactionException1.setExceptionDescription("Exception 0");
        accountTransactionException1.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId1 = accountTransactionException1.generateAndSetExceptionId();
        accountTransactionException1.insert();
        assertEquals(1008, exceptionId1);

        SpecialAccountTransactionException specialAccountTransactionException1 = new SpecialAccountTransactionException();
        specialAccountTransactionException1.setDeskId("B");
        specialAccountTransactionException1.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException1.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId1 = specialAccountTransactionException1.generateAndSetSpecialExceptionId();
        specialAccountTransactionException1.insert();
        assertEquals(100008, specialExceptionId1);

        AccountTransactionException accountTransactionException2 = new AccountTransactionException();
        accountTransactionException2.setDeskId("A");
        accountTransactionException2.setExceptionDescription("Exception 0");
        accountTransactionException2.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId2 = accountTransactionException2.generateAndSetExceptionId();
        accountTransactionException2.insert();
        assertEquals(1011, exceptionId2);

        SpecialAccountTransactionException specialAccountTransactionException2 = new SpecialAccountTransactionException();
        specialAccountTransactionException2.setDeskId("B");
        specialAccountTransactionException2.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException2.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId2 = specialAccountTransactionException2.generateAndSetSpecialExceptionId();
        specialAccountTransactionException2.insert();
        assertEquals(100011, specialExceptionId2);

        AccountTransactionException accountTransactionException3 = new AccountTransactionException();
        accountTransactionException3.setDeskId("A");
        accountTransactionException3.setExceptionDescription("Exception 0");
        accountTransactionException3.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        long exceptionId3 = accountTransactionException3.generateAndSetExceptionId();
        accountTransactionException3.insert();
        assertEquals(1014, exceptionId3);

        SpecialAccountTransactionException specialAccountTransactionException3 = new SpecialAccountTransactionException();
        specialAccountTransactionException3.setDeskId("B");
        specialAccountTransactionException3.setSpecialExceptionDescription("Account Transaction 0");
        specialAccountTransactionException3.setSpecialExceptionDate(new Timestamp(System.currentTimeMillis()));
        long specialExceptionId3 = specialAccountTransactionException3.generateAndSetSpecialExceptionId();
        specialAccountTransactionException3.insert();
        assertEquals(100014, specialExceptionId3);
    }



    public void testInsertingList()
    {
        AccountTransactionExceptionList list0 = new AccountTransactionExceptionList();
        AccountTransactionException accountTransactionException0 = null;
        for (int i = 0; i < 10; i++)
        {
            accountTransactionException0 = new AccountTransactionException();
            accountTransactionException0.setExceptionDate(new Timestamp(System.currentTimeMillis()));
            accountTransactionException0.setExceptionDescription("Transaction failure: "+i);
            accountTransactionException0.setDeskId("A");
            list0.add(accountTransactionException0);
        }
        list0.insertAll();

        AccountTransactionExceptionList list1 = new AccountTransactionExceptionList();
        AccountTransactionException accountTransactionException1 = null;
        for (int i = 0; i < 10; i++)
        {
            accountTransactionException1 = new AccountTransactionException();
            accountTransactionException1.setExceptionDate(new Timestamp(System.currentTimeMillis()));
            accountTransactionException1.setExceptionDescription("Transaction failure: "+i);
            accountTransactionException1.setDeskId("A");
            list1.add(accountTransactionException1);
        }
        list1.insertAll();

        for(int i = 0; i < list1.size(); i++)
        {
            accountTransactionException1 = (AccountTransactionException)list1.get(i);
            assertTrue(accountTransactionException1.getExceptionId() > 1032 && accountTransactionException1.getExceptionId() < 1063);
        }
        for(int i = 0; i < list0.size(); i++)
        {
            accountTransactionException0 = (AccountTransactionException)list0.get(i);
            assertTrue(accountTransactionException0.getExceptionId() > 1002 && accountTransactionException0.getExceptionId() < 1033);
        }
    }

    public void testInsertingListGeneratingPrimaryKeysWhileCreatingList()
    {
        AccountTransactionExceptionList list0 = new AccountTransactionExceptionList();
        AccountTransactionException accountTransactionException0 = null;
        for (int i = 0; i < 10; i++)
        {
            accountTransactionException0 = new AccountTransactionException();
            accountTransactionException0.setExceptionDate(new Timestamp(System.currentTimeMillis()));
            accountTransactionException0.setExceptionDescription("Transaction failure: "+i);
            accountTransactionException0.setDeskId("A");
            accountTransactionException0.generateAndSetExceptionId();
            list0.add(accountTransactionException0);
        }
        list0.insertAll();

        AccountTransactionExceptionList list1 = new AccountTransactionExceptionList();
        AccountTransactionException accountTransactionException1 = null;
        for (int i = 0; i < 10; i++)
        {
            accountTransactionException1 = new AccountTransactionException();
            accountTransactionException1.setExceptionDate(new Timestamp(System.currentTimeMillis()));
            accountTransactionException1.setExceptionDescription("Transaction failure: "+i);
            accountTransactionException1.setDeskId("A");
            accountTransactionException1.generateAndSetExceptionId();
            list1.add(accountTransactionException1);
        }
        list1.insertAll();

        for(int i = 0; i < list1.size(); i++)
        {
            accountTransactionException1 = (AccountTransactionException)list1.get(i);
            assertTrue(accountTransactionException1.getExceptionId() > 1032 && accountTransactionException1.getExceptionId() < 1063);
        }
        for(int i = 0; i < list0.size(); i++)
        {
            accountTransactionException0 = (AccountTransactionException)list0.get(i);
            assertTrue(accountTransactionException0.getExceptionId() > 1002 && accountTransactionException0.getExceptionId() < 1033);
        }
    }

    public void testInsertListsInSeparateThreads()
    {
        final Exchanger rendezvous = new Exchanger();


        Runnable accountExceptionList0 = new Runnable()
        {
            public boolean success;
            public void run()
            {
                AccountTransactionExceptionList list0 = new AccountTransactionExceptionList();
                AccountTransactionException accountTransactionException0 = null;
                for (int i = 0; i < 10; i++)
                {
                    accountTransactionException0 = new AccountTransactionException();
                    accountTransactionException0.setExceptionDate(new Timestamp(System.currentTimeMillis()));
                    accountTransactionException0.setExceptionDescription("Transaction failure: "+i);
                    accountTransactionException0.setDeskId("A");
                    list0.add(accountTransactionException0);
                }
                waitForOtherThread(rendezvous);
                list0.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(400);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                AccountTransactionExceptionList list1 = new AccountTransactionExceptionList();
                AccountTransactionException accountTransactionException1 = null;
                for (int i = 0; i < 10; i++)
                {
                    accountTransactionException1 = new AccountTransactionException();
                    accountTransactionException1.setExceptionDate(new Timestamp(System.currentTimeMillis()));
                    accountTransactionException1.setExceptionDescription("Transaction failure: "+i);
                    accountTransactionException1.setDeskId("A");
                    list1.add(accountTransactionException1);
                }
                waitForOtherThread(rendezvous);
                list1.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                this.success = true;
            }

            public boolean success()
            {
                return success;
            }
        };

        Runnable accountExceptionList1 = new Runnable()
        {
            public boolean success;

            public void run()
            {
                AccountTransactionExceptionList list0 = new AccountTransactionExceptionList();
                AccountTransactionException accountTransactionException0 = null;
                for (int i = 0; i < 10; i++)
                {
                    accountTransactionException0 = new AccountTransactionException();
                    accountTransactionException0.setExceptionDate(new Timestamp(System.currentTimeMillis()));
                    accountTransactionException0.setExceptionDescription("Transaction failure: "+i);
                    accountTransactionException0.setDeskId("A");
                    list0.add(accountTransactionException0);
                }
                waitForOtherThread(rendezvous);
                list0.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(600);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                AccountTransactionExceptionList list1 = new AccountTransactionExceptionList();
                AccountTransactionException accountTransactionException1 = null;
                for (int i = 0; i < 10; i++)
                {
                    accountTransactionException1 = new AccountTransactionException();
                    accountTransactionException1.setExceptionDate(new Timestamp(System.currentTimeMillis()));
                    accountTransactionException1.setExceptionDescription("Transaction failure: "+i);
                    accountTransactionException1.setDeskId("A");
                    list1.add(accountTransactionException1);
                }
                waitForOtherThread(rendezvous);
                list1.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(300);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                this.success = true;
            }

            public boolean success()
            {
                return success;
            }
        };

        this.runMultithreadedTest(accountExceptionList0, accountExceptionList1);

    }

    //Inserting 2 different Mithra
    public void testInsertListsInSeparateThreads2()
    {
        final Exchanger rendezvous = new Exchanger();


        Runnable riskValueAccountRunnable = new Runnable()
        {
            public boolean success;
            public void run()
            {
                RiskValueTestAccountList list0 = new RiskValueTestAccountList();
                RiskValueTestAccount riskValueTestAccount0 = null;
                for (int i = 0; i < 200; i++)
                {
                    riskValueTestAccount0 = new RiskValueTestAccount();
                    riskValueTestAccount0.setRiskDateAccountId(i);
                    riskValueTestAccount0.setRiskPointId(i);
                    riskValueTestAccount0.setRiskValue(i);
                    list0.add(riskValueTestAccount0);
                }
                waitForOtherThread(rendezvous);
                list0.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(400);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                RiskValueTestAccountList list1 = new RiskValueTestAccountList();
                RiskValueTestAccount riskValueTestAccount1 = null;
                for (int i = 0; i < 200; i++)
                {
                    riskValueTestAccount1 = new RiskValueTestAccount();
                    riskValueTestAccount1.setRiskDateAccountId(i);
                    riskValueTestAccount1.setRiskPointId(i);
                    riskValueTestAccount1.setRiskValue(i);
                    list1.add(riskValueTestAccount1);
                }
                waitForOtherThread(rendezvous);
                list1.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                this.success = true;
            }

            public boolean success()
            {
                return success;
            }
        };

        Runnable riskDateAccountRunnable = new Runnable()
        {
            public boolean success;
            public void run()
            {
                RiskDateTestAccountList list0 = new RiskDateTestAccountList();
                RiskDateTestAccount riskValueTestAccount0 = null;
                for (int i = 0; i < 200; i++)
                {
                    riskValueTestAccount0 = new RiskDateTestAccount();
                    riskValueTestAccount0.setAccountNumber("Acct"+i);
                    riskValueTestAccount0.setRiskDate(new Date(System.currentTimeMillis()));
                    list0.add(riskValueTestAccount0);
                }
                waitForOtherThread(rendezvous);
                list0.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(400);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                RiskDateTestAccountList list1 = new RiskDateTestAccountList();
                RiskDateTestAccount riskValueTestAccount1 = null;
                for (int i = 0; i < 200; i++)
                {
                    riskValueTestAccount1 = new RiskDateTestAccount();
                    riskValueTestAccount1.setAccountNumber("Acct"+i);
                    riskValueTestAccount1.setRiskDate(new Date(System.currentTimeMillis()));
                    list1.add(riskValueTestAccount1);
                }
                waitForOtherThread(rendezvous);
                list1.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                this.success = true;
            }

            public boolean success()
            {
                return success;
            }
        };

        this.runMultithreadedTest(riskValueAccountRunnable, riskDateAccountRunnable);

    }

    public void testInsertListsInSeparateThreads3()
    {
        final Exchanger rendezvous = new Exchanger();


        Runnable AccountTxRunnable = new Runnable()
        {
            public boolean success;
            public void run()
            {
                AccountTransactionList list0 = new AccountTransactionList();
                AccountTransaction accountTransaction0 = null;
                for (int i = 0; i < 200; i++)
                {
                    accountTransaction0 = new AccountTransaction();
                    accountTransaction0.setDeskId("A");
                    accountTransaction0.setTransactionDescription("Exception 0");
                    accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                    list0.add(accountTransaction0);
                }
                waitForOtherThread(rendezvous);
                list0.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(400);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                AccountTransactionList list1 = new AccountTransactionList();
                AccountTransaction accountTransaction1 = null;
                for (int i = 0; i < 200; i++)
                {
                    accountTransaction1 = new AccountTransaction();
                    accountTransaction1.setDeskId("A");
                    accountTransaction1.setTransactionDescription("Exception 0");
                    accountTransaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                    list1.add(accountTransaction1);
                }
                waitForOtherThread(rendezvous);
                list1.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                this.success = true;
            }

            public boolean success()
            {
                return success;
            }
        };

        Runnable specialAccountTxRunnable = new Runnable()
        {
            public boolean success;
            public void run()
            {
                SpecialAccountTransactionList list0 = new SpecialAccountTransactionList();
                SpecialAccountTransaction specialAccountTransaction0 = null;
                for (int i = 0; i < 200; i++)
                {
                    specialAccountTransaction0 = new SpecialAccountTransaction();
                    specialAccountTransaction0.setDeskId("B");
                    specialAccountTransaction0.setSpecialTransactionDescription("Account Transaction 0");
                    specialAccountTransaction0.setSpecialTransactionDate(new Timestamp(System.currentTimeMillis()));
                    list0.add(specialAccountTransaction0);
                }
                waitForOtherThread(rendezvous);
                list0.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(400);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                SpecialAccountTransactionList list1 = new SpecialAccountTransactionList();
                SpecialAccountTransaction specialAccountTransaction1 = null;
                for (int i = 0; i < 200; i++)
                {
                    specialAccountTransaction1 = new SpecialAccountTransaction();
                    specialAccountTransaction1.setDeskId("B");
                    specialAccountTransaction1.setSpecialTransactionDescription("Account Transaction 0");
                    specialAccountTransaction1.setSpecialTransactionDate(new Timestamp(System.currentTimeMillis()));
                    list1.add(specialAccountTransaction1);
                }
                waitForOtherThread(rendezvous);
                list1.insertAll();
                waitForOtherThread(rendezvous);
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                this.success = true;
            }

            public boolean success()
            {
                return success;
            }
        };

        this.runMultithreadedTest(AccountTxRunnable, specialAccountTxRunnable);

    }



    public void testBatchWithNonZeroInitialCapacity()
    {

        AccountTransaction accountTransaction0 = new AccountTransaction();
        accountTransaction0.setDeskId("A");
        accountTransaction0.setTransactionDescription("Account Transaction 0");
        accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        accountTransaction0.generateAndSetTransactionId();
        accountTransaction0.insert();
        assertEquals(1001, accountTransaction0.getTransactionId());

        AccountTransaction accountTransaction1 = new AccountTransaction();
        accountTransaction1.setDeskId("A");
        accountTransaction1.setTransactionDescription("Account Transaction 0");
        accountTransaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        accountTransaction1.generateAndSetTransactionId();
        accountTransaction1.insert();
        assertEquals(1002, accountTransaction1.getTransactionId());

        AccountTransactionList list1 = new AccountTransactionList();
        AccountTransaction accountTransaction2 = null;
        for (int i = 0; i < 25; i++)
        {
            accountTransaction2 = new AccountTransaction();
            accountTransaction2.setTransactionDate(new Timestamp(System.currentTimeMillis()));
            accountTransaction2.setTransactionDescription("Transaction failure: "+i);
            accountTransaction2.setDeskId("A");
            list1.add(accountTransaction2);
        }
        list1.insertAll();
        for(int i = 0; i < list1.size(); i++)
        {
            assertTrue(list1.getAccountTransactionAt(i).getTransactionId() > 1002 && list1.getAccountTransactionAt(i).getTransactionId() < 1028);
        }

    }

    public void testTransactionRollback()
    {
        AccountTransaction accountTransaction0 = new AccountTransaction();

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();

        accountTransaction0.setDeskId("A");
        accountTransaction0.setTransactionDescription("Account Transaction 0");
        accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        int accountTransactionId = accountTransaction0.generateAndSetTransactionId();
        accountTransaction0.insert();

        tx.rollback();
        assertTrue(accountTransactionId != accountTransaction0.getTransactionId());

        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        accountTransaction0.setDeskId("A");
        accountTransaction0.setTransactionDescription("Account Transaction 0");
        accountTransaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        accountTransaction0.insert();
        tx.commit();
        assertEquals(1002, accountTransaction0.getTransactionId());
    }

}
