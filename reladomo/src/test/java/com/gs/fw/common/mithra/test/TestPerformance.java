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

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.FullSemiUniqueDatedIndex;
import com.gs.fw.common.mithra.cache.MithraReferenceThread;
import com.gs.fw.common.mithra.cache.bean.I3O3L3;
import com.gs.fw.common.mithra.cache.offheap.OffHeapCleaner;
import com.gs.fw.common.mithra.cache.offheap.OffHeapMemoryReference;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.cache.ReadWriteLock;
import com.gs.fw.common.mithra.finder.AtomicOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.glew.LewContract;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import junit.framework.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestPerformance
extends MithraTestAbstract
{

    private int max = 10;
    private static final int LOCK_RUN_COUNT = 10000000;


    public void setMax(int max)
    {
        this.max = max;
    }

    public void xtestByteArray()
    {
        byte[] a = new byte[10];
        byte[] b = new byte[10];

        for(int i=0;i<10; i++)
        {
            a[0] = (byte)(i+10);
            b[0] = (byte)(i+10);
        }

        Object x = a;
        Object y = b;

        assertEquals(x, y);
    }

    public void testSetupAndTearDown() throws Exception
    {
        this.tearDown();
        for(int i=0;i<100;i++)
        {
            long now = System.currentTimeMillis();
            this.setUp();
            this.tearDown();
            System.out.println("took "+(System.currentTimeMillis() - now));
        }
        this.setUp();
    }

    public void testReadOnlyAttribute()
    {
        User[] users = new User[10000];
        for(int i=0;i<10000;i++)
        {
            users[i] = new User();
            users[i].setId(i);
        }
        sleep(100);
        int n = 1000000;
        int max = 10;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            int sum = sumUsers(users, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("read-only: "+((double)runTimes)/n + " ns per attribute getter");
        }
    }

    private int sumUsers(User[] users, int n)
    {
        int sum = 0;
        int pos = 0;
        for(int i = 0; i < n; i++,pos++)
        {
            if (pos == 10000) pos = 0;
            sum += users[pos].getId();
        }
        return sum;
    }

    public void xtestCascadeInsert()
    {
        OrderList list = new OrderList();
        OrderItemList allItemlist = new OrderItemList();
        for(int i=0;i<100000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+1000);
            OrderItemList itemList = new OrderItemList();
            for(int j=0;j<10;j++)
            {
                OrderItem item = new OrderItem();
                item.setId(1000+i*10+j);
                itemList.add(item);
                allItemlist.add(item);
            }
            order.setItems(itemList);
            list.add(order);
        }
        long start = System.nanoTime();
        list.cascadeInsertAll();
//        list.insertAll();
//        allItemlist.insertAll();
        System.out.println("took "+(System.nanoTime() - start)/1000000+" ms");
    }

    public void testGetTransactionalBehavior()
    {
        sleep(100);
        Order order = new Order();
        order.setOrderId(7);
        int n = 1000000;
        for(int i = 0; i < n; i++)
        {
            PersistenceState.getTransactionalBehaviorForNoTransactionWithWaitIfNecessary(null, PersistenceState.IN_MEMORY, null);
        }
        sleep(100);
        long startTime = System.currentTimeMillis();
        for(int i = 0; i < n; i++)
        {
            PersistenceState.getTransactionalBehaviorForNoTransactionWithWaitIfNecessary(null, PersistenceState.IN_MEMORY, null);
        }
        System.out.println("get transactional behavior: "+((double)(System.currentTimeMillis() - startTime))* 1000000 /n + " ns per attribute getter");
    }

    public void testTransactionalAttribute()
    {
        Order[] orders = new Order[10000];
        for(int i=0;i<10000;i++)
        {
            orders[i] = new Order();
            orders[i].setOrderId(i);
        }
        sleep(100);
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            int sum = sumOrders(orders, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("transactional: "+((double)runTimes)/n + " ns per attribute getter");
        }
    }

    private int sumOrders(Order[] orders, int n)
    {
        int sum = 0;
        int pos = 0;
        for(int i = 0; i < n; i++,pos++)
        {
            if (pos == 10000) pos = 0;
            sum += orders[pos].getOrderId();
        }
        return sum;
    }

    public void testTransactionalAttributeAsReadOnly()
    {
        OrderAsTxReadOnly[] orders = new OrderAsTxReadOnly[10000];
        for(int i=0;i<10000;i++)
        {
            orders[i] = new OrderAsTxReadOnly();
            orders[i].setOrderId(i);
            orders[i].zSetNonTxPersistenceState(PersistenceState.PERSISTED_NON_TRANSACTIONAL);
        }
        sleep(100);
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            int sum = sumOrders(orders, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("transactional: "+((double)runTimes)/n + " ns per attribute getter");
        }
    }

    private int sumOrders(OrderAsTxReadOnly[] orders, int n)
    {
        int sum = 0;
        int pos = 0;
        for(int i = 0; i < n; i++,pos++)
        {
            if (pos == 10000) pos = 0;
            sum += orders[pos].getOrderId();
        }
        return sum;
    }

    public void testSimpleManyToOnePerformanceWithSourceAttribute()
    {
        sleep(100);
        User user = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        user.getProfile();
        sleep(100);
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runGetProfile(user, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("simple many to one: "+((double)runTimes)/n + " ns per relationship resolution");
        }
    }

    private void runGetProfile(User user, int n)
    {
        for(int i = 0; i < n; i++)
        {
            user.getProfile();
        }
    }

    public void testOneToOneDatedWithSourceAttributePerformance()
    {
        sleep(100);
        LewContract contract = LewContractFinder.findOne(LewContractFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(LewContractFinder.instrumentId().eq(1).and(LewContractFinder.region().eq("A"))));
        contract.getLewTransaction();
        sleep(100);
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runGetLewTransaction(contract, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("one to one date with source: "+((double)runTimes)/n + " ns per relationship resolution");
        }
    }

    private void runGetLewTransaction(LewContract contract, int n)
    {
        for(int i = 0; i < n; i++)
        {
            contract.getLewTransaction();
        }
    }

    public void xtestQueryCache()
    {
        int n = 10000000;
        for(int i=0;i<n;i++)
        {
            Operation op = OrderFinder.orderId().eq(-i); // make sure we don't get any hits
            op = op.and(OrderFinder.description().eq("blah blah"));
            op = op.and(OrderFinder.orderDate().isNull());

            OrderFinder.findOne(op);
            if (i % 100000 == 0)
            {
                System.out.println("looking at "+i);
            }
        }
    }

    public void testTransactionalSimpleManyToOnePerformanceWithSourceAttribute()
    {
        sleep(100);
        Player player = PlayerFinder.findOne(PlayerFinder.sourceId().eq("A").and(PlayerFinder.id().eq(100)));
        player.getTeam();
        sleep(100);
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runGetTeam(player, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("simple transactional many to one: "+((double)runTimes)/n + " ns per relationship resolution");
        }
    }

    private void runGetTeam(Player player, int n)
    {
        for(int i = 0; i < n; i++)
        {
            player.getTeam();
        }
    }

    public void testTransactionalSimplestManyToOnePerformanceWithSourceAttribute()
    {
        sleep(100);
        OrderItem item = OrderItemFinder.findOne(OrderItemFinder.id().eq(1));
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runGetOrder(item, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("simplest transactional many to one: "+((double)runTimes)/n + " ns per relationship resolution");
        }
    }

    public void testRelationships()
    {
        testTransactionalSimplestManyToOnePerformanceWithSourceAttribute();
        testTransactionalSimpleManyToOnePerformanceWithSourceAttribute();
        testSimpleManyToOnePerformanceWithSourceAttribute();
        testTransactionalSimplestManyToOnePerformanceWithSourceAttribute();
        testTransactionalSimpleManyToOnePerformanceWithSourceAttribute();
        testSimpleManyToOnePerformanceWithSourceAttribute();
        testTransactionalSimplestManyToOnePerformanceWithSourceAttribute();
        testTransactionalSimpleManyToOnePerformanceWithSourceAttribute();
        testSimpleManyToOnePerformanceWithSourceAttribute();
        testTransactionalSimplestManyToOnePerformanceWithSourceAttribute();
        testTransactionalSimpleManyToOnePerformanceWithSourceAttribute();
        testSimpleManyToOnePerformanceWithSourceAttribute();
    }

    private void runGetOrder(OrderItem item, int n)
    {
        for(int i = 0; i < n; i++)
        {
            item.getOrder();
        }
    }

    public void testSetter()
    {
        sleep(100);

        int n = 1000000;
        Order order = new Order();
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runSetter(order, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("generated setter: "+((double)runTimes)/n/2 + " ns per setter");
        }
    }

    private void runSetter(Order order, int n)
    {
        for(int i = 0; i < n; i++)
        {
            order.setOrderId(i);
            order.setUserId(i);
        }
    }

    public void testRawCache()
    {
        sleep(100);
        OrderItem item = OrderItemFinder.findOne(OrderItemFinder.id().eq(1));
        Object data = item.zGetCurrentData();
        List fororder = Arrays.asList(new Object[] { OrderItemFinder.orderId() });

        OrderFinder.getMithraObjectPortal().getCache().getAsOne(data, fororder);
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runGetAsOne(data, fororder, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("raw cache: "+((double)runTimes)/n + " ns per relationship resolution");
        }
    }

    private void runGetAsOne(Object data, List fororder, int n)
    {
        for(int i = 0; i < n; i++)
        {
            OrderFinder.getMithraObjectPortal().getCache().getAsOne(data, fororder);
        }
    }

    public void testRawCache2()
    {
        sleep(100);
        OrderItem item = OrderItemFinder.findOne(OrderItemFinder.id().eq(1));
        Object data = item.zGetCurrentData();
        Extractor[] fororder = new Extractor[] { OrderItemFinder.orderId() };

        OrderFinder.getMithraObjectPortal().getCache().getAsOne(data, fororder);
        int n = 1000000;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runGetAsOne(data, fororder, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("raw cache: "+((double)runTimes)/n + " ns per relationship resolution");
        }
    }

    private void runGetAsOne(Object data, Extractor[] fororder, int n)
    {
        for(int i = 0; i < n; i++)
        {
            OrderFinder.getMithraObjectPortal().getCache().getAsOne(data, fororder);
        }
    }

    public void xtestPerformanceOfSet()
    {
        runTestForOps(new Operation[] { OrderFinder.orderId().eq(12), OrderFinder.description().eq("blah blah") });
        runTestForOps(new Operation[] { OrderFinder.orderId().eq(12), OrderFinder.description().eq("blah blah"),
            OrderFinder.orderDate().eq(new Timestamp(System.currentTimeMillis())), OrderFinder.state().eq("123"),
            OrderFinder.trackingId().eq("abc"), OrderFinder.userId().eq(4444)});
        runTestForOps(new Operation[] {
                NullTestFinder.noDefaultNullBoolean().eq(true),
                NullTestFinder.noDefaultNullByte().eq((byte)12),
                NullTestFinder.noDefaultNullChar().eq('a'),
                NullTestFinder.noDefaultNullDouble().eq(17.6),
                NullTestFinder.noDefaultNullString().eq("very very long text of stuff that's a string"),
                NullTestFinder.noDefaultNullTimestamp().eq(new Timestamp(System.currentTimeMillis())),
                NullTestFinder.notNullString().eq("also long but not short in any century is a man of mystery"),
                NullTestFinder.notNullLong().eq(12344352342342L),
                NullTestFinder.notNullInt().eq(1234),
                NullTestFinder.notNullShort().eq((short)122),
                NullTestFinder.notNullByte().eq((byte)34),
                NullTestFinder.nullableLong().eq(34567890),
                NullTestFinder.nullableString().eq("some other long long piece of text of stuff"),
                NullTestFinder.nullableFloat().eq((float)12.3),
                NullTestFinder.nullableChar().eq('b'),
                NullTestFinder.nullableDouble().eq(34.6),
//                NullTestFinder.nullableTimestamp().eq(new Timestamp(System.currentTimeMillis()))
        }

        );
    }

    private void runTestForOps(Operation[] operations)
    {
        AtomicOperation[] ops = new AtomicOperation[operations.length];
        for(int i=0;i<operations.length;i++) ops[i] = (AtomicOperation) operations[i];
        int n = 1000000;
        for(int i=0;i<n;i++)
        {
            calculateFalseHoodWithSet(ops);
        }
        for(int i=0;i<n;i++)
        {
            calculateFalseHoodWithLoop(ops);
        }
        sleep(100);
        n = 1000000;
        System.out.println("start");
        long startTime = System.currentTimeMillis();
        for(int i = 0; i < n; i++)
        {
            calculateFalseHoodWithLoop(ops);
        }
        double totalTime = System.currentTimeMillis() - startTime;
        System.out.println("operations: "+ops.length+" with loop "+totalTime/1000 +"total time. ");

        sleep(100);
        System.out.println("start");
        startTime = System.currentTimeMillis();
        for(int i = 0; i < n; i++)
        {
            calculateFalseHoodWithSet(ops);
        }
        totalTime = System.currentTimeMillis() - startTime;
        System.out.println("operations: "+ops.length+" with set "+totalTime/1000 +"total time. ");

    }

    private synchronized void calculateFalseHoodWithSet(AtomicOperation[] atomicOperations)
    {
        boolean isClearlyFalse = false;
        Set attrSet = new UnifiedSet(atomicOperations.length);
        int duplicateCount = 0;
        for(int i=0;i<atomicOperations.length;i++)
        {
            AtomicOperation aop = atomicOperations[i];
            Attribute attribute = aop.getAttribute();
            if (!isClearlyFalse && attrSet.contains(attribute))
            {
                for(int j=0;j<i;j++)
                {
                    AtomicOperation first = atomicOperations[j];
                    if (first.getAttribute().equals(attribute))
                    {
                        if (first.equals(aop))
                        {
                            atomicOperations[i] = null;// mark to remove duplicate
                            duplicateCount++;
                        }
                        else
                        {
                            isClearlyFalse = true;
                        }
                    }

                }
            }
            attrSet.add(attribute);
        }
        if (duplicateCount > 0)
        {
            AtomicOperation[] newOps = new AtomicOperation[atomicOperations.length - duplicateCount];
            int count = 0;
            for(int i=0;i < atomicOperations.length; i++)
            {
                if (atomicOperations[i] != null)
                {
                    newOps[count] = atomicOperations[i];
                    count++;
                }
            }
        }
    }

    private synchronized void calculateFalseHoodWithLoop(AtomicOperation[] atomicOperations)
    {
        boolean isClearlyFalse = false;
        int duplicateCount = 0;
        for(int i=0;i<atomicOperations.length;i++)
        {
            AtomicOperation aop = atomicOperations[i];
            Attribute attribute = aop.getAttribute();
            if (!isClearlyFalse)
            {
                for(int j=0;j<i;j++)
                {
                    AtomicOperation first = atomicOperations[j];
                    if (first.getAttribute().equals(attribute))
                    {
                        if (first.equals(aop))
                        {
                            atomicOperations[i] = null;// mark to remove duplicate
                            duplicateCount++;
                        }
                        else
                        {
                            isClearlyFalse = true;
                        }
                    }

                }
            }
        }
        if (duplicateCount > 0)
        {
            AtomicOperation[] newOps = new AtomicOperation[atomicOperations.length - duplicateCount];
            int count = 0;
            for(int i=0;i < atomicOperations.length; i++)
            {
                if (atomicOperations[i] != null)
                {
                    newOps[count] = atomicOperations[i];
                    count++;
                }
            }
        }
    }

    public void testMithraReadWritePerformance()
    {
        ReadWriteLock mithraLock = new ReadWriteLock();

        runMithraReadLock(mithraLock);

        long now = System.nanoTime();
        runMithraReadLock(mithraLock);
        System.out.println("single reader took "+((double)(System.nanoTime() - now))/LOCK_RUN_COUNT+" ns");

        runMithraWriteLock(mithraLock);
        now = System.nanoTime();
        runMithraWriteLock(mithraLock);
        System.out.println("single writer took "+((double)(System.nanoTime() - now))/LOCK_RUN_COUNT+" ns");

        runMithraUpgradeLock(mithraLock);
        now = System.nanoTime();
        runMithraUpgradeLock(mithraLock);
        System.out.println("single upgrader took "+((double)(System.nanoTime() - now))/LOCK_RUN_COUNT+" ns");

    }

    private void runMithraUpgradeLock(ReadWriteLock mithraLock)
    {
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            mithraLock.acquireReadLock();
            mithraLock.upgradeToWriteLock();
            this.doSomethingUnSynchronized(i);
            mithraLock.release();
        }
    }

    private void runMithraWriteLock(ReadWriteLock mithraLock)
    {
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            mithraLock.acquireWriteLock();
            this.doSomethingUnSynchronized(i);
            mithraLock.release();
        }
    }

    private void runMithraReadLock(ReadWriteLock mithraLock)
    {
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            mithraLock.acquireReadLock();
            this.doSomethingUnSynchronized(i);
            mithraLock.release();
        }
    }


    public void testJdkReadWritePerformance()
    {
        java.util.concurrent.locks.ReentrantReadWriteLock jdkLock = new java.util.concurrent.locks.ReentrantReadWriteLock();

        runJdkReadLock(jdkLock);

        long now = System.nanoTime();
        runJdkReadLock(jdkLock);
        System.out.println("single reader took "+((double)(System.nanoTime() - now))/LOCK_RUN_COUNT+" ns");

        runJdkWriteLock(jdkLock);
        now = System.nanoTime();
        runJdkWriteLock(jdkLock);
        System.out.println("single writer took "+((double)(System.nanoTime() - now))/LOCK_RUN_COUNT+" ns");

        runJdkUpgradeLock(jdkLock);
        now = System.nanoTime();
        runJdkUpgradeLock(jdkLock);
        System.out.println("single upgrader took "+((double)(System.nanoTime() - now))/LOCK_RUN_COUNT+" ns");

    }

    private void runJdkUpgradeLock(java.util.concurrent.locks.ReentrantReadWriteLock jdkLock)
    {
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            jdkLock.readLock().lock();
            jdkLock.readLock().unlock();
            jdkLock.writeLock().lock();
            this.doSomethingUnSynchronized(i);
            jdkLock.writeLock().unlock();
        }
    }

    private void runJdkWriteLock(java.util.concurrent.locks.ReentrantReadWriteLock jdkLock)
    {
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            jdkLock.writeLock().lock();
            this.doSomethingUnSynchronized(i);
            jdkLock.writeLock().unlock();
        }
    }

    private void runJdkReadLock(java.util.concurrent.locks.ReentrantReadWriteLock jdkLock)
    {
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            jdkLock.readLock().lock();
            this.doSomethingUnSynchronized(i);
            jdkLock.readLock().unlock();
        }
    }

    public void testSynchronized()
    {
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            this.doSomethingSynchronized(i);
        }

        long now = System.currentTimeMillis();
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            this.doSomethingSynchronized(i);
        }
        System.out.println("synchronized took "+(System.currentTimeMillis() - now)+" ms");

        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            this.doSomethingUnSynchronized(i);
        }
        now = System.currentTimeMillis();
        for (int i=0;i< LOCK_RUN_COUNT;i++)
        {
            this.doSomethingUnSynchronized(i);
        }
        System.out.println("unsynchronized took "+(System.currentTimeMillis() - now)+" ms");
    }

    public synchronized int doSomethingSynchronized(int count)
    {
        return count+1;
    }

    public int doSomethingUnSynchronized(int count)
    {
        return count+1;
    }

    public void xtestHashCombine()
    {
        final int MAX_COUNT = 10000;
        int[] first = new int[MAX_COUNT];
        int[] second = new int[MAX_COUNT];
        long now = System.currentTimeMillis();

        Random rand = new Random(167776191048573L);

        for(int i=0;i<MAX_COUNT;i++)
        {
            first[i] = rand.nextInt();
        }

        for(int i=0;i<MAX_COUNT;i++)
        {
            second[i] = rand.nextInt();
        }

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashes(first[i], second[j]);
            }
        }

        now = System.currentTimeMillis();
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashes(first[i], second[j]);
            }
        }
        System.out.println("combineHashes took "+(System.currentTimeMillis() - now)+" ms");

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashesOld(first[i], second[j]);
            }
        }

        now = System.currentTimeMillis();
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashesOld(first[i], second[j]);
            }
        }
        System.out.println("combineHashesOld took "+(System.currentTimeMillis() - now)+" ms");
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashesHsieh(first[i], second[j]);
            }
        }

        now = System.currentTimeMillis();
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashesHsieh(first[i], second[j]);
            }
        }
        System.out.println("combineHashesHsieh took "+(System.currentTimeMillis() - now)+" ms");

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashes(first[i], second[j]);
            }
        }

        now = System.currentTimeMillis();
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashes(first[i], second[j]);
            }
        }
        System.out.println("combineHashes took "+(System.currentTimeMillis() - now)+" ms");

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashesBad(first[i], second[j]);
            }
        }

        now = System.currentTimeMillis();
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                HashUtil.combineHashesBad(first[i], second[j]);
            }
        }
        System.out.println("combineHashesBad took "+(System.currentTimeMillis() - now)+" ms");
    }

    public void xtestHashCombineUniqueness()
    {
        final int MAX_COUNT = 2000;
        int[] first = new int[MAX_COUNT];
        int[] second = new int[MAX_COUNT];
        IntHashSet unique = new IntHashSet(MAX_COUNT);

        Random rand = new Random(167776191048573L);

        for(int i=0;i<MAX_COUNT;i++)
        {
//            first[i] = rand.nextInt();
            first[i] = i;
        }

        for(int i=0;i<MAX_COUNT;i++)
        {
//            second[i] = rand.nextInt();
            second[i] = i+MAX_COUNT;
        }

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add((HashUtil.combineHashes(first[i], second[j])& 0x7fffffff) % 4591721);
            }
        }
        System.out.println("combineHash uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add((HashUtil.combineHashesOld(first[i], second[j])& 0x7fffffff) % 4591721);
            }
        }
        System.out.println("combineHashOld uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add((HashUtil.combineHashesHsieh(first[i], second[j])& 0x7fffffff) % 4591721);
            }
        }
        System.out.println("combineHashesHsieh uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add((HashUtil.combineHashesMurmur(first[i], second[j])& 0x7fffffff) % 4591721);
            }
        }
        System.out.println("combineHashesMurmur uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add((HashUtil.combineHashesBad(first[i], second[j])& 0x7fffffff) % 4591721);
            }
        }
        System.out.println("combineHashBad uniqueness "+unique.size());
        unique.clear();

    }

    public void xtestHashCombineUniqueness3()
    {
        final int MAX_COUNT = 2000;
        int[] first = new int[MAX_COUNT];
        int[] second = new int[MAX_COUNT];
        IntHashSet unique = new IntHashSet(MAX_COUNT);

        Random rand = new Random(167776191048573L);

        for(int i=0;i<MAX_COUNT;i++)
        {
//            first[i] = rand.nextInt();
            first[i] = i;
        }

        for(int i=0;i<MAX_COUNT;i++)
        {
//            second[i] = rand.nextInt();
            second[i] = i+MAX_COUNT;
        }

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add(HashUtil.combineHashes(first[i], second[j]) & ((1 << 16) - 1));
            }
        }
        System.out.println("combineHash uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add(HashUtil.combineHashesOld(first[i], second[j]) & ((1 << 16) - 1));
            }
        }
        System.out.println("combineHashOld uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add(HashUtil.combineHashesHsieh(first[i], second[j]) & ((1 << 16) - 1));
            }
        }
        System.out.println("combineHashesHsieh uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add(HashUtil.combineHashesMurmur(first[i], second[j]) & ((1 << 16) - 1));
            }
        }
        System.out.println("combineHashesMurmur uniqueness "+unique.size());
        unique.clear();

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<MAX_COUNT;j++)
            {
                unique.add(HashUtil.combineHashesBad(first[i], second[j]) & ((1 << 16) - 1));
            }
        }
        System.out.println("combineHashBad uniqueness "+unique.size());
        unique.clear();

    }

    public void xtestHashCombineUniqueness2()
    {
        final int MAX_COUNT = 10000;
        IntHashSet unique = new IntHashSet(MAX_COUNT);


        for(int j=0;j<MAX_COUNT;j++)
        {
            unique.add((HashUtil.combineHashes(j, 0) & ((1 << 16) - 1)));
        }
        System.out.println("combineHash uniqueness "+unique.size());
        unique.clear();

        for(int j=0;j<MAX_COUNT;j++)
        {
            unique.add((HashUtil.combineHashesOld(j, 0) & ((1 << 16) - 1)));
        }
        System.out.println("combineHashOld uniqueness "+unique.size());
        unique.clear();

        for(int j=0;j<MAX_COUNT;j++)
        {
            unique.add((HashUtil.combineHashesHsieh(j, 0) & ((1 << 16) - 1)));
        }
        System.out.println("combineHashesHsieh uniqueness "+unique.size());
        unique.clear();

        for(int j=0;j<MAX_COUNT;j++)
        {
            unique.add((HashUtil.combineHashesBad(j, 0) & ((1 << 16) - 1)));
        }
        System.out.println("combineHashBad uniqueness "+unique.size());
        unique.clear();

        for(int j=0;j<MAX_COUNT;j++)
        {
            unique.add((HashUtil.combineHashesMurmur(j, 1) & ((1 << 16) - 1)));
        }
        System.out.println("combineHashesMurmur uniqueness "+unique.size());
        unique.clear();

    }

    public byte[] serializeMessage() throws IOException
    {
        byte[] pileOfBytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2000);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(OrderFinder.description());
        oos.writeObject(OrderFinder.orderId());
        oos.flush();
        bos.flush();
        pileOfBytes = bos.toByteArray();
        bos.close();
        return pileOfBytes;
    }

    public void testAttributeSerializeSize() throws Exception
    {
        byte[] bytes = serializeMessage();
        System.out.println("Serialized size "+ bytes.length);
    }

    public void testConnectionManager() throws SQLException
    {
        ConnectionManagerForTests cm = ConnectionManagerForTests.getInstance();
        int n = 100000;
        int max = 10;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runGetAndCloseConnection(cm, n);
            long runTimes = System.nanoTime() - startTime;
            System.out.println("connection manager took: "+((double)runTimes)/n + " ns per get and close");
        }
    }

    private void runGetAndCloseConnection(ConnectionManagerForTests cm, int n)
            throws SQLException
    {
        for(int i=0;i<n;i++)
        {
            cm.getConnection().close();
        }
    }

    public void testCacheQuery()
    {
        Operation op = OrderFinder.description().eq("123");
        op = op.and(OrderFinder.items().productId().greaterThan(17));
        op = op.and(OrderFinder.items().productInfo().productDescription().eq("4jdh"));
        op = op.and(OrderFinder.orderId().eq(1234));
        op = op.and(OrderFinder.items().originalPrice().lessThan(1234));
        op = op.and(OrderFinder.itemForProduct(17).discountPrice().lessThan(12));
        op = op.and(OrderFinder.cheapItems(3.0).quantity().eq(123));
        op = op.and(OrderFinder.state().eq("wert"));
        op = op.and(OrderFinder.items().productInfo().productCode().eq("SDFV"));

        int MAX_COUNT = 1000000;
        createCachedQuery(op, MAX_COUNT);

        long now = System.currentTimeMillis();
        createCachedQuery(op, MAX_COUNT);
        System.out.println("cached query took "+((double)(System.currentTimeMillis() - now)*1000000)/MAX_COUNT+" ns per query");
    }

    private void createCachedQuery(Operation op, int MAX_COUNT)
    {
        for(int i=0;i<MAX_COUNT;i++)
        {
            CachedQuery q = new CachedQuery(op, null);
        }
    }

    public void testTerminateAuditOnlyFactor1()
    {
        doTestTerminateAuditOnly(1);
    }

    public void testTerminateAuditOnlyFactor10()
    {
        doTestTerminateAuditOnly(10);
    }

    public void testTerminateAuditOnlyFactor100()
    {
        doTestTerminateAuditOnly(100);
    }

    public void testTerminateAuditOnlyFactor1000()
    {
        doTestTerminateAuditOnly(1000);
    }

    public void doTestTerminateAuditOnly(int factor)
    {
        createAuditOnlyOrders(100 * factor, 200 * factor);
        long start = System.currentTimeMillis();
        terminateAuditOnlyOrders(factor);
        System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
    }

    private void terminateAuditOnlyOrders(final int factor)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderList list = new AuditedOrderList(AuditedOrderFinder.orderId().greaterThanEquals(100 * factor).and(AuditedOrderFinder.orderId().lessThan(200 * factor)));
                list.terminateAll();
                createAuditOnlyOrders(500 * factor, 100 * factor);
                list = new AuditedOrderList(AuditedOrderFinder.orderId().greaterThanEquals(200 * factor).and(AuditedOrderFinder.orderId().lessThan(300 * factor)));
                list.terminateAll();
                return null;
            }
        });
    }

    private void createAuditOnlyOrders(int start, int count)
    {
        AuditedOrderList list = new AuditedOrderList(count);
        for(int i=0;i<count;i++)
        {
            AuditedOrder order = new AuditedOrder();
            order.setOrderId(i+start);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setDescription("desc");
            order.setState("state");
            order.setUserId(i+5);
            list.add(order);
        }
        list.insertAll();
    }

    public void testTerminateBitemporalFactor1()
    {
        doTestTerminateBitemporal(1);
    }

    public void testTerminateBitemporalFactor10()
    {
        doTestTerminateBitemporal(10);
    }

    public void testTerminateBitemporalFactor100()
    {
        doTestTerminateBitemporal(100);
    }

    public void testTerminateBitemporalFactor1000()
    {
        doTestTerminateBitemporal(1000);
    }

    public void doTestTerminateBitemporal(int factor)
    {
        createBitemporalOrders(100 * factor, 200 * factor);
        long start = System.currentTimeMillis();
        terminateBitemporalOrders(factor);
        System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
    }

    private void terminateBitemporalOrders(final int factor)
    {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalOrderList list = new BitemporalOrderList(BitemporalOrderFinder.orderId().greaterThanEquals(100 * factor).and(BitemporalOrderFinder.orderId().lessThan(200 * factor)).and(BitemporalOrderFinder.businessDate().eq(now)));
                list.terminateAll();
                createBitemporalOrders(500 * factor, 100 * factor);
                list = new BitemporalOrderList(BitemporalOrderFinder.orderId().greaterThanEquals(200 * factor).and(BitemporalOrderFinder.orderId().lessThan(300 * factor)).and(BitemporalOrderFinder.businessDate().eq(now)));
                list.terminateAll();
                return null;
            }
        });
    }

    private void createBitemporalOrders(int start, int count)
    {
        Timestamp buzDate = new Timestamp(10000000);
        BitemporalOrderList list = new BitemporalOrderList(count);
        for(int i=0;i<count;i++)
        {
            BitemporalOrder order = new BitemporalOrder(buzDate);
            order.setOrderId(i+start);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setDescription("desc");
            order.setState("state");
            order.setUserId(i+5);
            list.add(order);
        }
        list.insertAll();
    }

    public void testExtractorBasedHashingStrategy()
    {
        int max = 1000000;
        Order[] orders = new Order[max];

        for(int i=0;i<max;i++)
        {
            Order order = new Order();
            order.setOrderId(i);
            order.setUserId(-i);
            order.setDescription("desc"+i/10);
            order.setTrackingId("track"+i/4);
            order.setState("state"+i);
            orders[i] = order;
        }

        for(int i=0;i<10;i++)
        {
            System.out.println("------------");
            computeHashCode(orders, ExtractorBasedHashStrategy.create(new Extractor[] { OrderFinder.orderId(), OrderFinder.state()}));
            computeHashCode(orders, new OrderIdAndStateHashingStrategy());
            computeHashCode(orders, ExtractorBasedHashStrategy.create(new Extractor[] { OrderFinder.orderId(), OrderFinder.userId(), OrderFinder.state()}));
            computeHashCode(orders, ExtractorBasedHashStrategy.create(new Extractor[] { OrderFinder.userId(), OrderFinder.description()}));
            computeHashCode(orders, ExtractorBasedHashStrategy.create(new Extractor[] { OrderFinder.trackingId(), OrderFinder.state()}));
            computeHashCode(orders, ExtractorBasedHashStrategy.create(new Extractor[] { OrderFinder.orderId()}));
            System.out.println("------------");
        }
    }

    private void computeHashCode(Order[] orders, ExtractorBasedHashStrategy extractorBasedHashStrategy)
    {
        long start = System.currentTimeMillis();
        int hash = 0;
        for(int i=5;i<orders.length-5;i++)
        {
            Order order = orders[i];
            hash += extractorBasedHashStrategy.computeHashCode(order);
            for(int j=-5;j<5;j++)
            {
                if (extractorBasedHashStrategy.equals(order, orders[i+j])) hash++;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("hash "+hash+ " took "+(end - start)/1000.0);
    }

    public void testCachedQueryVsFindByPk()
    {
        int sum = findUsingOperation();
        System.out.println("warmup sum: "+sum);
        for(int i=0;i<10;i++)
        {
            long start = System.currentTimeMillis();
            sum = findUsingOperation();
            long end = System.currentTimeMillis();
            System.out.println("Operations per msec: "+10000000.0/(end-start)+"    sum: "+sum);
        }

        sum = findUsingPk();
        System.out.println("warmup sum: "+sum);
        for(int i=0;i<10;i++)
        {
            long start = System.currentTimeMillis();
            sum = findUsingPk();
            long end = System.currentTimeMillis();
            System.out.println("findByPk per msec: "+10000000.0/(end-start)+"    sum: "+sum);
        }

    }

    private int findUsingOperation()
    {
        int sum = 0;
        for(int i=0;i<10000000;i++)
        {
            sum += OrderFinder.findOne(OrderFinder.orderId().eq(1)).getOrderId();
        }
        return sum;
    }

    private int findUsingPk()
    {
        int sum = 0;
        for(int i=0;i<10000000;i++)
        {
            sum += OrderFinder.findByPrimaryKey(1).getOrderId();
        }
        return sum;
    }


    public void testMutableBean()
    {
        runMutableBean();
        runMutableBean();
        runMutableBean();
        runMutableBean();
        runMutableBean();
        runMutableBean();
        runMutableBean();
    }

    private void runMutableBean()
    {
        long start = System.currentTimeMillis();
        int max = 100000000;
        for(int i=0;i< max;i++)
        {
            I3O3L3.POOL.getOrConstruct().release();
        }
        long time = System.currentTimeMillis() - start;
        System.out.println("took "+ time + " that's "+((double)time*1000000.0)/max+" ns per op");

    }

    public void testNewTimestampArray()
    {
        runNewTimestampArray();
        runNewTimestampArray();
        runNewTimestampArray();
        runNewTimestampArray();
        runNewTimestampArray();
        runNewTimestampArray();
        runNewTimestampArray();
    }

    private void runNewTimestampArray()
    {
        long start = System.currentTimeMillis();
        for(int i=0;i<100000000;i++)
        {
            Timestamp[] x = new Timestamp[2];
        }
        System.out.println("took "+(System.currentTimeMillis() - start));

    }
    public void testThreadLocal()
    {
        runThreadLocal();
        runThreadLocal();
        runThreadLocal();
        runThreadLocal();
        runThreadLocal();
        runThreadLocal();
        runThreadLocal();
    }

    private void runThreadLocal()
    {
        long start = System.currentTimeMillis();
        int max = 100000000;
        for(int i=0;i< max;i++)
        {
            getTempTimestamps();
        }
        long time = System.currentTimeMillis() - start;
        System.out.println("took "+ time + " that's "+((double)time*1000000.0)/max+" ns per op");

    }

    private final ThreadLocal tempTimestamps = new ThreadLocal();

    protected Timestamp[] getTempTimestamps()
    {
        Timestamp[] result = (Timestamp[]) this.tempTimestamps.get();
        if (result == null)
        {
            result = new Timestamp[2];
            this.tempTimestamps.set(result);
        }
        return result;
    }

    private static class OrderIdAndStateHashingStrategy extends ExtractorBasedHashStrategy
    {
        public int computeHashCode(Object object)
        {
            if (object instanceof OrderData)
            {
                OrderData o = (OrderData) object;
                return HashUtil.combineHashes(o.getOrderId(), o.getState().hashCode());
            }
            Order o = (Order) object;
            return HashUtil.combineHashes(o.getOrderId(), o.getState().hashCode());
        }

        public boolean equals(Object o1, Object o2)
        {
            Order ord1 = (Order) o1;
            Order ord2 = (Order) o2;
            return ord1.getOrderId() == ord2.getOrderId() && ord1.getState().equals(ord2.getState());
        }

        @Override
        public int computeHashCode(Object o, List extractors)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public int computeHashCode(Object valueHolder, Extractor[] extractors)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public int computeCombinedHashCode(Object o, int incomingHash)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public boolean equals(Object first, Object second, List secondExtractors)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public boolean equals(Object first, Object second, Extractor[] extractors)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public int computeUpdatedHashCode(Object o, AttributeUpdateWrapper updateWrapper)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public boolean equalsIncludingUpdate(Object original, Object newObject, AttributeUpdateWrapper updateWrapper)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public Extractor getFirstExtractor()
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public Extractor[] getExtractors()
        {
            throw new RuntimeException("not implemented");
        }
    }

    public void testFullSemiUniqueDatedIndex() throws Exception
    {

        FullSemiUniqueDatedIndex fullSemiUniqueDatedIndex = new FullSemiUniqueDatedIndex("idx", TinyBalanceFinder.getPrimaryKeyAttributes(), TinyBalanceFinder.getAsOfAttributes());

        SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp businessDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        int maxNoOfNonDuplicatePrimaryKey = 10000000;
        int maxNoOfMilestonedRecords = 4;
        int timeInMsToMove = 5000000;
        for (int noOfDistinctPK = 0; noOfDistinctPK < maxNoOfNonDuplicatePrimaryKey; noOfDistinctPK++)
        {
            Timestamp processingDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
            Timestamp processingDateTo = new Timestamp(processingDateFrom.getTime()+timeInMsToMove);
            for (int i = 0; i < maxNoOfMilestonedRecords; i++)
            {
                TinyBalance data = new TinyBalance(businessDateFrom, businessDateFrom);
                data.setBalanceId(noOfDistinctPK);
                data.setBusinessDateFrom(businessDateFrom);
                data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
                data.setProcessingDateFrom(new Timestamp(processingDateFrom.getTime() + i * timeInMsToMove));
                data.setProcessingDateTo(processingDateTo);
                Timestamp processingDateFromTmp =  processingDateFrom;
                processingDateFrom = processingDateTo;
                processingDateTo = new Timestamp(processingDateFromTmp.getTime()+timeInMsToMove);
                fullSemiUniqueDatedIndex.putSemiUnique(data);
            }
        }
        int noOfDuplicatePrimarykeys = 1000;
        int maxPrimaryKey = maxNoOfNonDuplicatePrimaryKey + noOfDuplicatePrimarykeys;
        for (int noOfDistinctPK = maxNoOfNonDuplicatePrimaryKey; noOfDistinctPK < maxPrimaryKey; noOfDistinctPK++)
        {
            Timestamp processingDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
            Timestamp processingDateTo = new Timestamp(timestampFormat.parse("2069-01-02 10:00:00").getTime());
            for (int i = 0; i < maxNoOfMilestonedRecords; i++)
            {
                TinyBalance data = new TinyBalance(businessDateFrom, businessDateFrom);
                data.setBalanceId(noOfDistinctPK);
                data.setBusinessDateFrom(businessDateFrom);
                data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
                processingDateFrom = new Timestamp(processingDateFrom.getTime() + i * timeInMsToMove);
                data.setProcessingDateFrom(processingDateFrom);
                data.setProcessingDateTo(processingDateTo);
                fullSemiUniqueDatedIndex.putSemiUnique(data);
            }
        }
        for (int j = 0; j < 5; j++)
        {
            long startTime = System.currentTimeMillis();
            List<Object> duplicates = fullSemiUniqueDatedIndex.collectMilestoningOverlaps();
            long timeTaken = System.currentTimeMillis() - startTime;

            System.out.println("Time taken to find duplicates from " + maxNoOfMilestonedRecords * maxNoOfNonDuplicatePrimaryKey + " objects is " + timeTaken + " ms");
            int totalNumberOfRecords = (maxNoOfNonDuplicatePrimaryKey * maxNoOfMilestonedRecords) + (noOfDuplicatePrimarykeys * maxNoOfMilestonedRecords);
            Assert.assertEquals(totalNumberOfRecords, fullSemiUniqueDatedIndex.size());
            int noOfRecordsInDuplicateList = (noOfDuplicatePrimarykeys * (maxNoOfMilestonedRecords - 1)) * 2;
            Assert.assertEquals(noOfRecordsInDuplicateList, duplicates.size());

        }

    }


/*
    public void testTransactionLocalPerformance()
    {
        int max = 1000;
        ThreadLocal[] threadLocals = new ThreadLocal[max];
        for(int i=0;i<max;i++)
        {
            threadLocals[i] = new ThreadLocal();
        }

        TransactionLocal[] transactionLocals = new TransactionLocal[max];
        TransactionLocalMap localMap = new TransactionLocalMap();
        for(int i=0;i<max;i++)
        {
            transactionLocals[i] = new TransactionLocal();
            localMap.put(transactionLocals[i], new Integer(i));
        }
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int MAX_COUNT = 1000;
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<max;j++)
            {
                MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
                assertEquals(new Integer(j), localMap.get(transactionLocals[j]));
            }
        }

        long now = System.currentTimeMillis();
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<max;j++)
            {
                MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
                localMap.get(transactionLocals[j]);
            }
        }
        System.out.println("Transaction local took "+(System.currentTimeMillis() - now)+" ms");

        for(int i=0;i<max;i++)
        {
            threadLocals[i].set(new Integer(i));
        }

        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<max;j++)
            {
                threadLocals[j].get();
            }
        }

        now = System.currentTimeMillis();
        for(int i=0;i<MAX_COUNT;i++)
        {
            for(int j=0;j<max;j++)
            {
                threadLocals[j].get();
            }
        }
        System.out.println("Thread local took "+(System.currentTimeMillis() - now)+" ms");
        tx.rollback();
    }
*/

    public Operation foo(OrderList adhocList)
    {
        Attribute[] pkAttributes = OrderFinder.getPrimaryKeyAttributes();
        if (pkAttributes.length == 1)
        {
            return pkAttributes[0].in(adhocList, pkAttributes[0]);
        }
        TupleAttribute pkTuple = pkAttributes[0].tupleWith(pkAttributes[1]);
        for(int i=2;i<pkAttributes.length;i++)
        {
            pkTuple = pkTuple.tupleWith(pkAttributes[i]);
        }
        return pkTuple.in(adhocList, pkAttributes);
    }
    public Operation getPkOperation(List adhocList, RelatedFinder finder)
    {
        Attribute[] pkAttributes = finder.getPrimaryKeyAttributes();
        if (pkAttributes.length == 1)
        {
            return pkAttributes[0].in(adhocList, pkAttributes[0]);
        }
        TupleAttribute pkTuple = pkAttributes[0].tupleWith(pkAttributes[1]);
        for(int i=2;i<pkAttributes.length;i++)
        {
            pkTuple = pkTuple.tupleWith(pkAttributes[i]);
        }
        return pkTuple.in(adhocList, pkAttributes);
    }

    public void testOffHeapAutoFree()
    {
        long sum = 0;
        long constructed = 0;
        while(true)
        {
            ArrayList list = new ArrayList();
            for(int i=0;i<1000;i++)
            {
                list.add(new OffHeapTest(100));
                constructed++;
            }
            sum += list.hashCode();
            if ((constructed & ((1 << 10) - 1)) == 0)
            {
                System.out.println("constructed: "+constructed+", freed: "+ OffHeapCleaner.getInstance().getFreedCount());
            }
            if (constructed < 0)
            {
                break;
            }
        }
        System.out.println("sum: "+sum);
    }

    public void testOffHeapAutoFreeManual()
    {
        long sum = 0;
        long constructed = 0;
        while(true)
        {
            ArrayList list = new ArrayList();
            for(int i=0;i<1000;i++)
            {
                list.add(new OffHeapTest(100));
                constructed++;
            }
            sum += list.hashCode();
            for(int i=0;i<1000;i++)
            {
                OffHeapTest t = (OffHeapTest) list.get(i);
                t.destroy();
            }

            if ((constructed & ((1 << 10) - 1)) == 0)
            {
                System.out.println("constructed: "+constructed+", freed: "+ OffHeapCleaner.getInstance().getFreedCount());
            }
            if (constructed < 0)
            {
                break;
            }
        }
        System.out.println("sum: "+sum);
    }

    public void testOffHeapAutoFreeAfterRealloc()
    {
        long sum = 0;
        long constructed = 0;
        while(true)
        {
            ArrayList list = new ArrayList();
            for(int i=0;i<1000;i++)
            {
                OffHeapTest e = new OffHeapTest(100);
                list.add(e);
                e.reallocate(200);
                constructed++;
            }
            sum += list.hashCode();
            if ((constructed & ((1 << 10) - 1)) == 0)
            {
                System.out.println("constructed: "+constructed+", freed: "+ OffHeapCleaner.getInstance().getFreedCount());
            }
            if (constructed < 0)
            {
                break;
            }
        }
        System.out.println("sum: "+sum);
    }

    private static class OffHeapTest extends OffHeapMemoryReference
    {
        public OffHeapTest(long initialSize)
        {
            super(initialSize);
            this.registerForGarbageCollection();
        }
    }
}
