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

import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraManagerProvider;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;


public class TestCopyTransactionalObjectAttributes extends MithraTestAbstract
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    protected Class[] getRestrictedClassList()
    {
        return new Class[]{
                Order.class,
                TinyBalance.class
        };
    }


    public void testCopyNonPrimaryKeyAttributesFromInMemoryObjectInTx()
    throws Exception
    {
        Order newOrder = new Order();
        newOrder.setOrderDate(new Timestamp(System.currentTimeMillis()));
        newOrder.setState("NEW");
        newOrder.setDescription("New Order");
        newOrder.setTrackingId("Tracking Id");
        newOrder.setUserId(9876);
        newOrder.setOrderId(1);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();

        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        order.copyNonPrimaryKeyAttributesFrom(newOrder);
        tx.commit();
        assertTrue(compareOrderNonPrimaryKeyAttributes(order, newOrder));
    }

    public void testCopyNonPrimaryKeyAttributesFromInMemoryObjectInTx2()
    throws Exception
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order newOrder = new Order();
        newOrder.setOrderDate(new Timestamp(System.currentTimeMillis()));
        newOrder.setState("NEW");
        newOrder.setDescription("New Order");
        newOrder.setTrackingId("Tracking Id");
        newOrder.setUserId(9876);
        newOrder.setOrderId(1);
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        order.copyNonPrimaryKeyAttributesFrom(newOrder);

        tx.commit();
        assertTrue(compareOrderNonPrimaryKeyAttributes(order, newOrder));
    }

    public void testCopyNonPrimaryKeyAttributesFromNonDatedObject()
    throws Exception
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        Order newOrder = new Order();
        newOrder.copyNonPrimaryKeyAttributesFrom(order);
        assertTrue(compareOrderNonPrimaryKeyAttributes(order, newOrder));
    }

    public void testCopyNullNonPrimaryKeyAttributesFromNonDatedObject()
    throws Exception
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        Order newOrder = new Order();
        order.setUserIdNull();
        newOrder.copyNonPrimaryKeyAttributesFrom(order);
        assertTrue(newOrder.isUserIdNull());
        assertTrue(compareOrderNonPrimaryKeyAttributes(order, newOrder));
    }

    public void testCopyNonPrimaryKeyAttributesFromDatedObject()
    throws Exception
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2005-01-11 18:30:00.0").getTime());
        TinyBalance balance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().eq(20).and(TinyBalanceFinder.businessDate().eq(ts))));
        assertNotNull(balance);

        TinyBalance newBalance = new TinyBalance(ts);
        newBalance.copyNonPrimaryKeyAttributesFrom((TinyBalanceAbstract)balance);
        assertTrue(newBalance.getQuantity() == balance.getQuantity());
    }

    public void testCopyNullNonPrimaryKeyAttributesFromDatedObject()
    throws Exception
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2005-01-11 18:30:00.0").getTime());
        TinyBalance balance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().eq(20).and(TinyBalanceFinder.businessDate().eq(ts))));
        assertNotNull(balance);
        TinyBalance newBalance = new TinyBalance(ts);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        balance.setQuantityNull();
        tx.commit();
        
        newBalance.copyNonPrimaryKeyAttributesFrom((TinyBalanceAbstract)balance);
        assertTrue(balance.isQuantityNull());
        assertTrue(newBalance.isQuantityNull());
    }


    private boolean compareOrderNonPrimaryKeyAttributes(Order order, Order newOrder)
    {
        if (order == newOrder)
        {
            return true;
        }

        final OrderData newOrderData = (OrderData) newOrder.zGetCurrentData();
        final OrderData orderData = (OrderData) order.zGetCurrentData();

        if (orderData.zGetIsNullBits0() != newOrderData.zGetIsNullBits0())
		{
			return false;
		}
		if ((!orderData.isDescriptionNull()) ? (!orderData.getDescription().equals(newOrderData.getDescription()))
									   : (!newOrderData.isDescriptionNull()))
		{
			return false;
		}
		if ((!orderData.isOrderDateNull()) ? (!orderData.getOrderDate().equals(newOrderData.getOrderDate()))
									 : (!newOrderData.isOrderDateNull()))
		{
			return false;
		}
		if ((!orderData.isStateNull()) ? (!orderData.getState().equals(newOrderData.getState())) : (!newOrderData.isStateNull()))
		{
			return false;
		}
		if ((!orderData.isTrackingIdNull()) ? (!orderData.getTrackingId().equals(newOrderData.getTrackingId()))
									  : (!newOrderData.isTrackingIdNull()))
		{
			return false;
		}
		if ((!orderData.isUserIdNull())?(orderData.getUserId() != orderData.getUserId()):(!newOrderData.isUserIdNull()))
		{
			return false;
		}
		return true;
    }
}
