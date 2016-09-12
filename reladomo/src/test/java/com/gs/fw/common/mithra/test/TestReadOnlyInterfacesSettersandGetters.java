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
import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.lang.reflect.Method;
import java.sql.Timestamp;

public class TestReadOnlyInterfacesSettersandGetters extends MithraTestAbstract
{
    public void testHasSetterAndGetter() throws NoSuchMethodException
    {
        Method orderIdMethodSetter = Orderable.class.getMethod("setOrderId", int.class);
        assertNotNull(orderIdMethodSetter);

        Method orderIdMethodGetter = Orderable.class.getMethod("getOrderId", null);
        assertNotNull(orderIdMethodGetter);

        Method datedOrderIdMethodSetter = DatedOrderable.class.getMethod("setOrderDate", Timestamp.class);
        assertNotNull(datedOrderIdMethodSetter);

        Method datedOrderIdMethodGetter = DatedOrderable.class.getMethod("getOrderDate", null);
        assertNotNull(datedOrderIdMethodGetter);

        Method datedOrderBusinessDateFromMethodSetter = DatedOrderable.class.getMethod("setBusinessDateFrom", Timestamp.class);
        assertNotNull(datedOrderBusinessDateFromMethodSetter);

        Method datedOrderBusinessDateFromMethodGetter = DatedOrderable.class.getMethod("getBusinessDateFrom", null);
        assertNotNull(datedOrderBusinessDateFromMethodGetter);

        Method datedOrderBusinessDateToMethodSetter = DatedOrderable.class.getMethod("setBusinessDateTo", Timestamp.class);
        assertNotNull(datedOrderBusinessDateToMethodSetter);

        Method datedOrderBusinessDateToMethodGetter = DatedOrderable.class.getMethod("getBusinessDateTo", null);
        assertNotNull(datedOrderBusinessDateToMethodGetter);

        Method accountInterfaceDeskIdSetter = AccountInterface.class.getMethod("setDeskId", String.class);
        assertNotNull(accountInterfaceDeskIdSetter);

        Method accountInterfaceDeskIdGetter = AccountInterface.class.getMethod("getDeskId", null);
        assertNotNull(accountInterfaceDeskIdGetter);

        Method accountInterfaceAccountNumberSetter = AccountInterface.class.getMethod("setAccountNumber", String.class);
        assertNotNull(accountInterfaceAccountNumberSetter);

        Method accountInterfaceAccountNumberGetter = AccountInterface.class.getMethod("getAccountNumber", null);
        assertNotNull(accountInterfaceAccountNumberGetter);
    }

    public void testOrderableReadOnlyNoSetter()
    {
        Method orderIdMethod = null;
        try
        {
            orderIdMethod = OrderableReadOnly.class.getMethod("getOrderId", null);
        }
        catch (NoSuchMethodException e)
        {
            fail();
        }
        assertNotNull(orderIdMethod);

        try
        {
            OrderableReadOnly.class.getMethod("setOrderId", int.class);
            fail();
        }
        catch (NoSuchMethodException e)
        {
            //good
        }

        Method datedOrderableGetOrderDate = null;
        try
        {
            datedOrderableGetOrderDate = DatedOrderableReadOnly.class.getMethod("getOrderDate", null);
        }
        catch (NoSuchMethodException e)
        {
            fail();
        }
        assertNotNull(datedOrderableGetOrderDate);

        try
        {
            DatedOrderableReadOnly.class.getMethod("setOrderDate", Timestamp.class);
            fail();
        }
        catch (NoSuchMethodException e)
        {
            //good
        }

        Method datedOrderableGetBusinessDateFrom = null;
        try
        {
            datedOrderableGetBusinessDateFrom = DatedOrderableReadOnly.class.getMethod("getBusinessDateFrom", null);
        }
        catch (NoSuchMethodException e)
        {
            fail();
        }
        assertNotNull(datedOrderableGetBusinessDateFrom);

        try
        {
            DatedOrderableReadOnly.class.getMethod("setBusinessDateFrom", Timestamp.class);
            fail();
        }
        catch (NoSuchMethodException e)
        {
            //good
        }

        Method datedOrderableGetBusinessDateTo = null;
        try
        {
            datedOrderableGetBusinessDateTo = DatedOrderableReadOnly.class.getMethod("getBusinessDateTo", null);
        }
        catch (NoSuchMethodException e)
        {
            fail();
        }
        assertNotNull(datedOrderableGetBusinessDateTo);

        try
        {
            DatedOrderableReadOnly.class.getMethod("setBusinessDateTo", Timestamp.class);
            fail();
        }
        catch (NoSuchMethodException e)
        {
            //good
        }

        Method accountInterfaceGetDeskId = null;
        try
        {
            accountInterfaceGetDeskId = AccountInterfaceReadOnly.class.getMethod("getDeskId", null);
        }
        catch (NoSuchMethodException e)
        {
            fail();
        }
        assertNotNull(accountInterfaceGetDeskId);

        try
        {
            AccountInterfaceReadOnly.class.getMethod("setDeskId", String.class);
            fail();
        }
        catch (NoSuchMethodException e)
        {
            //good
        }

        Method accountInterfaceGetAccountNumber = null;
        try
        {
            accountInterfaceGetAccountNumber = AccountInterfaceReadOnly.class.getMethod("getAccountNumber", null);
        }
        catch (NoSuchMethodException e)
        {
            fail();
        }
        assertNotNull(accountInterfaceGetAccountNumber);

        try
        {
            AccountInterfaceReadOnly.class.getMethod("setAccountNumber", String.class);
            fail();
        }
        catch (NoSuchMethodException e)
        {
            //good
        }
    }

    public void testRelationshipGetters()
    {
        DatedOrderItemRelationInterface datedOrderItemRelationInterface = new BitemporalOrder(InfinityTimestamp.getParaInfinity());
        BitemporalOrderItemList expensiveItems = datedOrderItemRelationInterface.getExpensiveItems(2.0);
        assertEquals(0, expensiveItems.size());

        OrderItemRelationInterface orderItemRelationInterface = new AuditedOrder();
        OrderItemInterface itemForProduct = orderItemRelationInterface.getItemForProduct(3);
        assertNull(itemForProduct);
    }
}
