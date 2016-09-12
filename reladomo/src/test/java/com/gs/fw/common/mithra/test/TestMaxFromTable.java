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
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;



public class TestMaxFromTable extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AccountTransactionMax.class,
            DatedWithMax.class,
            DbsTestEntity.class
        };
    }

    public void setUp() throws Exception
    {
        super.setUp();
    }

    public void testInsertObjectWithNoSourceAttribute()
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        DbsTestEntity dbsTestEntity = new DbsTestEntity();
        int id = dbsTestEntity.generateAndSetId();
        dbsTestEntity.setDescription("DbsTestEntity"+id);
        dbsTestEntity.setName("DbsTestEntity");
        dbsTestEntity.setAccountId("ParaTamsAccount "+id);
        dbsTestEntity.setGroupId(1234);
        tx.commit();
        assertEquals(1, id);
    }

    public void testInsertListWithNoSourceAttribute()
    {
        DbsTestEntityList list = new DbsTestEntityList();
        DbsTestEntity entity = null;
        for(int i = 0; i < 10; i++)
        {
            entity = new DbsTestEntity();
            entity.setDescription("DbsTestEntity"+i);
            entity.setName("DbsTestEntity"+i);
            entity.setAccountId("ParaTamsAccount "+i);
            entity.setGroupId(1000+i);
            list.add(entity);
        }
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        list.insertAll();
        tx.commit();
        for(int i = 0; i < list.size(); i++)
        {
            assertTrue(list.getDbsTestEntityAt(i).getId() > 0 && list.getDbsTestEntityAt(i).getId() < 11 );
        }
    }

    public void testInsertingDatedObject()
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        DatedWithMax balance = new DatedWithMax(businessDate, InfinityTimestamp.getParaInfinity());
        balance.setAcmapCode("A");
        balance.setQuantity(10.0);
        long balanceId = balance.generateAndSetBalanceId();
        balance.insert();
        assertEquals(1, balanceId);
        tx.commit();

    }
    public void testInsertingOneObjectWithinTx()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        AccountTransactionMax transaction0 = new AccountTransactionMax();
                        transaction0.setTransactionDescription("New Account");
                        transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                        transaction0.setDeskId("A");
                        long accountId = 0;

                        accountId = transaction0.generateAndSetTransactionId();
                        transaction0.insert();

                        AccountTransactionMax data0 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId).and(AccountTransactionMaxFinder.deskId().eq("A")));
                        assertSame(transaction0, data0);
                        assertEquals(103, accountId);
                        return null;
                    }
                });
    }

    public void testInsertingMultipleObjectsWithinTx()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        AccountTransactionMax transaction0 = new AccountTransactionMax();
                        transaction0.setTransactionDescription("New Account");
                        transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                        transaction0.setDeskId("A");
                        long accountId = 0;
                        accountId = transaction0.generateAndSetTransactionId();
                        assertEquals(103, accountId);

                        AccountTransactionMax transaction1 = new AccountTransactionMax();
                        transaction1.setTransactionDescription("New Account");
                        transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                        transaction1.setDeskId("A");
                        long accountId1 = 0;
                        accountId1 = transaction1.generateAndSetTransactionId();
                        assertEquals(104, accountId1);

                        transaction0.insert();
                        transaction1.insert();

                        AccountTransactionMax data0 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId).and(AccountTransactionMaxFinder.deskId().eq("A")));
                        assertSame(transaction0, data0);

                        AccountTransactionMax data1 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId1).and(AccountTransactionMaxFinder.deskId().eq("A")));
                        assertSame(transaction1, data1);
                        return null;
                    }
                });
    }

    public void testInsertingObjectWithNoSourceAttribute()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        
        tx.commit();
    }
    public void testInsertingOneObject()
    {
        try
        {
            AccountTransactionMax transaction0 = new AccountTransactionMax();
            transaction0.setTransactionDescription("New Account");
            transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
            transaction0.setDeskId("A");
            long accountId = 0;

            accountId = transaction0.generateAndSetTransactionId();
            assertEquals(103, accountId);

            transaction0.insert();
            fail("Should not get to this point");
            AccountTransactionMax data0 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId).and(AccountTransactionMaxFinder.deskId().eq("A")));
            assertSame(transaction0, data0);
        }
        catch(Exception e)
        {
           //This is expected
        }

    }

    public void testInsertingObjectsInSeparateTx()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
            new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    AccountTransactionMax transaction0 = new AccountTransactionMax();
                    transaction0.setTransactionDescription("New Account");
                    transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                    transaction0.setDeskId("A");
                    long accountId = 0;
                    accountId = transaction0.generateAndSetTransactionId();
                    assertEquals(103, accountId);

                    AccountTransactionMax transaction1 = new AccountTransactionMax();
                    transaction1.setTransactionDescription("New Account");
                    transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                    transaction1.setDeskId("A");
                    long accountId1 = 0;
                    accountId1 = transaction1.generateAndSetTransactionId();
                    assertEquals(104, accountId1);

                    transaction0.insert();
                    transaction1.insert();

                    AccountTransactionMax data0 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId).and(AccountTransactionMaxFinder.deskId().eq("A")));
                    assertSame(transaction0, data0);

                    AccountTransactionMax data1 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId1).and(AccountTransactionMaxFinder.deskId().eq("A")));
                    assertSame(transaction1, data1);
                    return null;
                }
            });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
            new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    AccountTransactionMax transaction0 = new AccountTransactionMax();
                    transaction0.setTransactionDescription("New Account");
                    transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                    transaction0.setDeskId("A");
                    long accountId = 0;
                    accountId = transaction0.generateAndSetTransactionId();
                    assertEquals(105, accountId);

                    AccountTransactionMax transaction1 = new AccountTransactionMax();
                    transaction1.setTransactionDescription("New Account");
                    transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                    transaction1.setDeskId("A");
                    long accountId1 = 0;
                    accountId1 = transaction1.generateAndSetTransactionId();
                    assertEquals(106, accountId1);

                    transaction0.insert();
                    transaction1.insert();

                    AccountTransactionMax data0 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId).and(AccountTransactionMaxFinder.deskId().eq("A")));
                    assertSame(transaction0, data0);

                    AccountTransactionMax data1 =AccountTransactionMaxFinder.findOne(AccountTransactionMaxFinder.transactionId().eq(accountId1).and(AccountTransactionMaxFinder.deskId().eq("A")));
                    assertSame(transaction1, data1);
                    return null;
                }
            });
    }


    public void testInsertList()
    {

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
            new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    AccountTransactionMaxList list0 = new AccountTransactionMaxList();
                    AccountTransactionMax transaction0 = null;

                    for (int i = 0; i < 20; i++)
                    {
                        transaction0 = new AccountTransactionMax();
                        transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                        transaction0.setTransactionDescription("Transaction failure: "+i);
                        transaction0.setDeskId("A");
                        list0.add(transaction0);
                    }
                    list0.insertAll();
                    for(int i = 0; i < list0.size(); i++)
                    {
                        assertTrue(list0.getAccountTransactionMaxAt(i).getTransactionId() > 102 && list0.getAccountTransactionMaxAt(i).getTransactionId() < 123 );
                    }

                    AccountTransactionMaxList list1 = new AccountTransactionMaxList();
                    AccountTransactionMax transaction1 = null;

                    for (int i = 0; i < 20; i++)
                    {
                        transaction1 = new AccountTransactionMax();
                        transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                        transaction1.setTransactionDescription("Transaction failure: "+i);
                        transaction1.setDeskId("A");
                        list1.add(transaction1);
                    }
                    list1.insertAll();
                    for(int i = 0; i < list1.size(); i++)
                    {
                        assertTrue(list1.getAccountTransactionMaxAt(i).getTransactionId() > 122 && list1.getAccountTransactionMaxAt(i).getTransactionId() < 143 );
                    }
                    return null;
                }
            });
    }

    public void testInsertListWithDifferenSourceAttributes()
    {

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
            new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    AccountTransactionMaxList list0 = new AccountTransactionMaxList();
                    AccountTransactionMax transaction0 = null;
                    String deskId = null;
                    for (int i = 0; i < 20; i++)
                    {
                        deskId = (i%2 == 0?"A":"B");
                        transaction0 = new AccountTransactionMax();
                        transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                        transaction0.setTransactionDescription("Transaction failure: "+i);
                        transaction0.setDeskId(deskId);
                        list0.add(transaction0);
                    }
                    list0.insertAll();

                    for(int i = 0; i < list0.size(); i++)
                    {
                      assertTrue(list0.getAccountTransactionMaxAt(i).getTransactionId() > 0 || list0.getAccountTransactionMaxAt(i).getTransactionId() < 11);
                    }
                    return null;
                }
            });
    }

    public void testInsertingObjectsInSeparateThreads() throws Exception
    {

       Runnable accountTransaction0 = new Runnable()
       {
           public void run()
           {
               MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                    new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            AccountTransactionMax transaction0 = new AccountTransactionMax();
                            transaction0.setTransactionDescription("New Account");
                            transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                            transaction0.setDeskId("A");
                            transaction0.generateAndSetTransactionId();
                            transaction0.insert();
                            Thread.sleep(500);

                            AccountTransactionMax transaction1 = new AccountTransactionMax();
                            transaction1.setTransactionDescription("New Account");
                            transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                            transaction1.setDeskId("A");
                            transaction1.generateAndSetTransactionId();
                            transaction1.insert();
                            Thread.sleep(300);
                            return null;
                                }
                            });
           }
       };

       Runnable accountTransaction1 = new Runnable()
       {
           public void run()
           {
               MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                    new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            AccountTransactionMax transaction0 = new AccountTransactionMax();
                            transaction0.setTransactionDescription("New Account");
                            transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                            transaction0.setDeskId("A");
                            transaction0.generateAndSetTransactionId();
                            transaction0.insert();
                            Thread.sleep(400);

                            AccountTransactionMax transaction1 = new AccountTransactionMax();
                            transaction1.setTransactionDescription("New Account");
                            transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                            transaction1.setDeskId("A");
                            transaction1.generateAndSetTransactionId();
                            transaction1.insert();
                            Thread.sleep(600);
                            return null;
                        }
                    });
           }
       };

        this.runMultithreadedTest(accountTransaction0, accountTransaction1);
    }

    public void testInsertListsInSeparateThreads()
    {
        Runnable accountTransactionList0 = new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                   new TransactionalCommand()
                   {
                       public Object executeTransaction(MithraTransaction tx) throws Throwable
                       {
                           AccountTransactionMaxList list0 = new AccountTransactionMaxList();
                           AccountTransactionMax transaction0 = null;

                           for (int i = 0; i < 10; i++)
                           {
                               transaction0 = new AccountTransactionMax();
                               transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                               transaction0.setTransactionDescription("Transaction failure: "+i);
                               transaction0.setDeskId("A");
                               list0.add(transaction0);
                           }
                           list0.insertAll();
                           Thread.sleep(500);

                           AccountTransactionMaxList list1 = new AccountTransactionMaxList();
                            AccountTransactionMax transaction1 = null;

                            for (int i = 0; i < 10; i++)
                            {
                                transaction1 = new AccountTransactionMax();
                                transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                                transaction1.setTransactionDescription("Transaction failure: "+i);
                                transaction1.setDeskId("A");
                                list1.add(transaction1);
                            }
                            list1.insertAll();
                            Thread.sleep(300);
                          return null;
                       }
                   });
            }
        };

        Runnable accountTransactionList1 = new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                   new TransactionalCommand()
                   {
                       public Object executeTransaction(MithraTransaction tx) throws Throwable
                       {
                           AccountTransactionMaxList list0 = new AccountTransactionMaxList();
                           AccountTransactionMax transaction0 = null;

                           for (int i = 0; i < 10; i++)
                           {
                               transaction0 = new AccountTransactionMax();
                               transaction0.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                               transaction0.setTransactionDescription("Transaction failure: "+i);
                               transaction0.setDeskId("A");
                               list0.add(transaction0);
                           }
                           list0.insertAll();
                           Thread.sleep(400);

                           AccountTransactionMaxList list1 = new AccountTransactionMaxList();
                            AccountTransactionMax transaction1 = null;

                            for (int i = 0; i < 10; i++)
                            {
                                transaction1 = new AccountTransactionMax();
                                transaction1.setTransactionDate(new Timestamp(System.currentTimeMillis()));
                                transaction1.setTransactionDescription("Transaction failure: "+i);
                                transaction1.setDeskId("A");
                                list1.add(transaction1);
                            }
                            list1.insertAll();
                            Thread.sleep(600);
                          return null;
                       }
                   });
            }
        };


        this.runMultithreadedTest(accountTransactionList0, accountTransactionList1);
    }

}
