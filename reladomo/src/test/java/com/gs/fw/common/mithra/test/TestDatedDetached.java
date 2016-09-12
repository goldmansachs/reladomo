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


import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItem;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemStatus;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderList;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderStatus;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
public class TestDatedDetached extends MithraTestAbstract
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TinyBalance.class,
            BitemporalOrder.class,
            BitemporalOrderItem.class,
            BitemporalOrderItemStatus.class,
            BitemporalOrderStatus.class
        };
    }

    public void testIsModifiedInMemory() throws Exception
    {
        TinyBalance bal = new TinyBalance(new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime()));
        bal.setBalanceId(1234456345);
        bal.setAcmapCode("A");
        assertTrue(bal.isModifiedSinceDetachment());
        assertTrue(bal.isModifiedSinceDetachment(TinyBalanceFinder.quantity()));
    }

    public void testOneObjectDetachedInsert() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());
        final TinyBalance balance = new TinyBalance(businessDate);
        balance.setBalanceId(1235);
        balance.setQuantity(1000.00);
        balance.setAcmapCode("B");
        TinyBalance inserted = balance.copyDetachedValuesToOriginalOrInsertIfNew();
        assertNotSame(inserted, balance);
        //todo: shouldn't need to reset
        balance.resetFromOriginalPersistentObject();
        assertTrue(balance.zIsDetached());

        Operation opWithoutDate = TinyBalanceFinder.acmapCode().eq("B");
        opWithoutDate = opWithoutDate.and(TinyBalanceFinder.balanceId().eq(1235));

        Operation op = opWithoutDate.and(TinyBalanceFinder.businessDate().eq(businessDate));

        int dbCount = this.getRetrievalCount();
        TinyBalance balance2 = TinyBalanceFinder.findOne(op);
        assertSame(inserted, balance2);
        assertNotSame(balance, balance2);
        assertEquals(dbCount, this.getRetrievalCount());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                balance.setQuantity(2000.0);
                balance.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
    }

    public void testOneObjectDetachedInsertUntil() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());
        Timestamp businessDateUntil = new Timestamp(timestampFormat.parse("2006-07-01 00:00:00").getTime());
        TinyBalance balance = new TinyBalance(businessDate);
        balance.setBalanceId(1235);
        balance.setQuantity(1000.00);
        balance.setAcmapCode("B");
        balance.copyDetachedValuesToOriginalOrInsertIfNewUntil(businessDateUntil);

        Operation opWithoutDate = TinyBalanceFinder.acmapCode().eq("B");
        opWithoutDate = opWithoutDate.and(TinyBalanceFinder.balanceId().eq(1235));

        Operation op = opWithoutDate.and(TinyBalanceFinder.businessDate().eq(businessDate));

        int dbCount = this.getRetrievalCount();
        TinyBalance balance2 = TinyBalanceFinder.findOne(op);
        assertSame(balance, balance2);
        assertEquals(dbCount, this.getRetrievalCount());

        TinyBalanceFinder.clearQueryCache();
        Operation op2 = opWithoutDate.and(TinyBalanceFinder.businessDate().eq( new Timestamp(timestampFormat.parse("2006-06-30 00:00:00").getTime())));
        TinyBalance balance3 = TinyBalanceFinder.findOne(op2);

        assertEquals(balance.getQuantity(), balance3.getQuantity(),0);

        Operation op3 = opWithoutDate.and(TinyBalanceFinder.businessDate().eq(businessDateUntil));
        TinyBalance balance4 = TinyBalanceFinder.findOne(op3);
        assertNull(balance4);
    }

    public void testOneObjectDetachedUpdateUntil() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2006-07-01 00:00:00").getTime());
        Operation opWithoutDate = TinyBalanceFinder.acmapCode().eq("B");
        opWithoutDate = opWithoutDate.and(TinyBalanceFinder.balanceId().eq(1234));
        Operation op = opWithoutDate.and(TinyBalanceFinder.businessDate().eq(businessDate));

        TinyBalance balance = TinyBalanceFinder.findOne(op);
        TinyBalance detachedBalance = balance.getDetachedCopy();
        assertTrue(detachedBalance != balance);
        detachedBalance.setQuantity(2000.00);

        detachedBalance.copyDetachedValuesToOriginalOrInsertIfNewUntil(until);
        assertEquals(1234, balance.getBalanceId());
        assertEquals(2000.00, balance.getQuantity(), 0);
        assertEquals(businessDate, balance.getBusinessDateFrom());
        assertEquals(until, balance.getBusinessDateTo());

        TinyBalanceFinder.clearQueryCache();
        Timestamp businessDate2 = new Timestamp(timestampFormat.parse("2006-07-01 00:00:00").getTime());
        Timestamp infinity = new Timestamp(timestampFormat.parse("9999-12-01 23:59:00.0").getTime());
        Operation op2 =  opWithoutDate.and(TinyBalanceFinder.businessDate().eq(businessDate2));
        TinyBalance balance2 = TinyBalanceFinder.findOne(op2);
        assertEquals(1234, balance2.getBalanceId());
        assertEquals(200.00, balance2.getQuantity(), 0);
        assertEquals(businessDate2, balance2.getBusinessDateFrom());
        assertEquals(infinity, balance2.getBusinessDateTo());

        Timestamp businessDate3 = new Timestamp(timestampFormat.parse("2005-12-31 00:00:00").getTime());
        Operation op3 =  opWithoutDate.and(TinyBalanceFinder.businessDate().eq(businessDate3));
        TinyBalance balance3 = TinyBalanceFinder.findOne(op3);
        assertEquals(1234, balance3.getBalanceId());
        assertEquals(200.00, balance3.getQuantity(), 0);
        assertEquals(new Timestamp(timestampFormat.parse("2005-12-15 18:30:00").getTime()), balance3.getBusinessDateFrom());
        assertEquals(businessDate, balance3.getBusinessDateTo());
    }

    public void testDetachedList() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2006-07-01 00:00:00").getTime());
        Timestamp infinity = new Timestamp(timestampFormat.parse("9999-12-01 23:59:00").getTime());
        Timestamp previousBusinessDate = new Timestamp(timestampFormat.parse("2005-12-31 00:00:00").getTime());
        Operation op = BitemporalOrderFinder.orderId().greaterThan(1);
        op = op.and(BitemporalOrderFinder.businessDate().eq(businessDate));

        BitemporalOrderList orderList = new BitemporalOrderList(op);
        assertEquals(3, orderList.size());
        BitemporalOrderList detachedList = orderList.getDetachedCopy();

        //Update Existing order
        int newUserId = 999;
        BitemporalOrder orderToUpdate = detachedList.getBitemporalOrderAt(0);
        int changedOrderId = orderToUpdate.getOrderId();
        int originalUserId = orderToUpdate.getUserId();
        orderToUpdate.setUserId(newUserId);

        //delete order
        BitemporalOrder orderToRemove = detachedList.getBitemporalOrderAt(1);
        int removedOrderId = orderToRemove.getOrderId();
        detachedList.remove(1);

        //Add new order
        BitemporalOrder newOrder = new BitemporalOrder(businessDate);
        newOrder.setOrderId(5);
        newOrder.setOrderDate(businessDate);
        newOrder.setState("In-Proress");
        newOrder.setTrackingId("123");
        newOrder.setUserId(321);
        detachedList.add(newOrder);

        detachedList.copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(until);

        orderList = null;
        BitemporalOrderFinder.clearQueryCache();
        //test new order

        assertNull(findOrder(5, previousBusinessDate));
        assertNotNull(findOrder(5, businessDate));
        assertNull(findOrder(5, infinity));
        //test removed order
        assertNotNull(findOrder(removedOrderId, previousBusinessDate));
        assertNull(findOrder(removedOrderId, businessDate));
        //test updated order
        assertEquals(originalUserId, findOrder(changedOrderId, previousBusinessDate).getUserId());
        assertEquals(newUserId,findOrder(changedOrderId, businessDate).getUserId() );
        assertEquals(originalUserId, findOrder(changedOrderId, infinity).getUserId());

    }

    private BitemporalOrder findOrder(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderFinder.
                findOne(BitemporalOrderFinder.orderId().eq(orderId).
                        and(BitemporalOrderFinder.businessDate().eq(businessDate)));
    }

    public void testIsDeletedOrMarkForDeletion() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());
        BitemporalOrder order1 = new BitemporalOrder(businessDate);
        order1.setOrderId(987);
        order1.setState("Created");
        order1.setUserId(123);
        order1.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertFalse(order1.isDeletedOrMarkForDeletion());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order1.insert();
        tx.commit();
        assertFalse(order1.isDeletedOrMarkForDeletion());

        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order1.terminate();
        tx.commit();
        assertTrue(order1.isDeletedOrMarkForDeletion());

        BitemporalOrder order2 = findOrder(1, businessDate);
        BitemporalOrder detachedBitemporalOrder = order2.getDetachedCopy();
        assertFalse(order2.isDeletedOrMarkForDeletion());

        detachedBitemporalOrder.terminate();
        assertFalse(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedBitemporalOrder.isDeletedOrMarkForDeletion());

        detachedBitemporalOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        assertTrue(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedBitemporalOrder.isDeletedOrMarkForDeletion());
    }

    public void testIsDeletedOrMarkForDeletionInTx() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        BitemporalOrder order1 = new BitemporalOrder(businessDate);
        order1.setOrderId(987);
        order1.setState("Created");
        order1.setUserId(123);
        order1.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertFalse(order1.isDeletedOrMarkForDeletion());
        order1.insert();
        assertFalse(order1.isDeletedOrMarkForDeletion());
        tx.commit();

        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order1.terminate();
        assertTrue(order1.isDeletedOrMarkForDeletion());
        tx.commit();
    }
}
