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
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrialList;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrialFinder;

import java.util.concurrent.Exchanger;


public class TestReadOnlyTransactionParticipation extends MithraTestAbstract
{

    public void testReadOnlyObjectDoesNotConsumeConnection()
    {
        final int connectionsBefore = ConnectionManagerForTests.getInstance().getNumberOfIdleConnection();

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        assertNotNull(ParaDeskFinder.findOne(ParaDeskFinder.deskIdString().eq("rnd")));
                        waitForOtherThread(rendezvous); // 1
                        waitForOtherThread(rendezvous); // 2
                        assertEquals(connectionsBefore, ConnectionManagerForTests.getInstance().getNumberOfIdleConnection());
                        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                        order.setUserId(order.getUserId() + 100);
                        tx.executeBufferedOperations();
                        assertNotNull(order);
                        assertEquals(1, ConnectionManagerForTests.getInstance().getNumberOfActiveConnections());
                        assertNotNull(ParaDeskFinder.findOne(ParaDeskFinder.deskIdString().eq("cap")));
                        assertEquals(1, ConnectionManagerForTests.getInstance().getNumberOfActiveConnections());
                        assertEquals(connectionsBefore - 1, ConnectionManagerForTests.getInstance().getNumberOfIdleConnection());
                        waitForOtherThread(rendezvous); // 3
                        waitForOtherThread(rendezvous); // 4
                        return null;
                    }
                });

            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                assertNotNull(ParaDeskFinder.findOneBypassCache(ParaDeskFinder.deskIdString().eq("rnd")));
                waitForOtherThread(rendezvous); // 2
                waitForOtherThread(rendezvous); // 3
                assertNotNull(ParaDeskFinder.findOneBypassCache(ParaDeskFinder.deskIdString().eq("cap")));

                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        setLockTimeout(100);
                        assertNotNull(ParaDeskFinder.findOneBypassCache(ParaDeskFinder.deskIdString().eq("rnd")));
                        try
                        {
                            assertNotNull(OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1)));
                            fail("did not timeout");
                        }
                        catch(MithraDatabaseException e)
                        {
                            // ok, this should timeout
                        }
                        setLockTimeout(1000);
                        return null;
                    }
                });
                waitForOtherThread(rendezvous); // 4
            }
        };
        assertTrue(this.runMultithreadedTest(runnable1, runnable2));
    }

    public void testReadCacheUpdateRefreshesWithExistingReference()
    {
        final Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderFinder.setTransactionModeReadCacheUpdateCausesRefreshAndLock(tx);
                order.getDescription();
                assertFalse(order.zIsParticipatingInTransaction(tx));
                Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                assertSame(order, order2);
                assertFalse(order.zIsParticipatingInTransaction(tx));
                order2 = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1));
                assertSame(order, order2);
                assertFalse(order.zIsParticipatingInTransaction(tx));
                order2 = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(2));
                assertFalse(order2.zIsParticipatingInTransaction(tx));
                order2.setUserId(1245);
                assertTrue(order2.zIsParticipatingInTransaction(tx));
                return null;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.getDescription();
                assertTrue(order.zIsParticipatingInTransaction(tx));
                Order order2 = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(2));
                assertNotNull(order2);
                assertTrue(order2.zIsParticipatingInTransaction(tx));
                return null;
            }
        });
    }

    public void testReadOnlyObjectQueryCacheInTransaction()
    {
        final Operation operation = TestTamsMithraTrialFinder.trialId().greaterThan("2").and(
                TestTamsMithraTrialFinder.businessDate().eq(InfinityTimestamp.getTamsInfinity()));
        TestTamsMithraTrialList list = new TestTamsMithraTrialList(operation);
        assertEquals(0, list.size());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TestTamsMithraTrialList list = new TestTamsMithraTrialList(operation);
                list.forceResolve();
                return null;
            }
        });
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }
}
