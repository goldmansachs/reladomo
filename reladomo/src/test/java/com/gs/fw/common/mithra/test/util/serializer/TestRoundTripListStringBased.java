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

package com.gs.fw.common.mithra.test.util.serializer;

import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.SerialView;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;
import com.gs.fw.common.mithra.util.serializer.SerializedList;
import org.junit.Test;

public abstract class TestRoundTripListStringBased extends MithraTestAbstract
{

    protected abstract SerializedList toSerializedString(String json) throws Exception;

    protected abstract String fromSerializedString(SerializedList serialized) throws Exception;

    @Test
    public void testOrder() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        String sb = fromSerializedString(new SerializedList<Order, OrderList>((OrderFinder.findMany(OrderFinder.all())), config));

        SerializedList<Order, OrderList> serialized = toSerializedString(sb);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
//        assertTrue(serialized.getWrapped().zIsDetached());

    }

    @Test
    public void testOrderWithDependentsAndLongMethods() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config = config.withDeepDependents();
        config = config.withAnnotatedMethods(SerialView.Longer.class);

        String sb = fromSerializedString(new SerializedList<Order, OrderList>((OrderFinder.findMany(OrderFinder.all())), config));

        SerializedList<Order, OrderList> serialized = toSerializedString(sb);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
    }

    @Test
    public void testOrderWithItems() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config = config.withDeepFetches(OrderFinder.items());
        String sb = fromSerializedString(new SerializedList<Order, OrderList>((OrderFinder.findMany(OrderFinder.all())), config));

        SerializedList<Order, OrderList> serialized = toSerializedString(sb);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
    }

    @Test
    public void testOrderWithItemsAndStatus() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config = config.withDeepFetches(OrderFinder.orderStatus(), OrderFinder.items());
        config = config.withAnnotatedMethods(SerialView.Shorter.class);

        String sb = fromSerializedString(new SerializedList<Order, OrderList>((OrderFinder.findMany(OrderFinder.all())), config));

        SerializedList<Order, OrderList> serialized = toSerializedString(sb);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
    }

    @Test
    public void testOrderTwoDeep() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config = config.withDeepFetches(OrderFinder.orderStatus(), OrderFinder.items().orderItemStatus());
        String sb = fromSerializedString(new SerializedList<Order, OrderList>((OrderFinder.findMany(OrderFinder.all())), config));

        SerializedList<Order, OrderList> serialized = toSerializedString(sb);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
    }

    @Test
    public void testOrderWithDependents() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config = config.withDeepDependents();
        String sb = fromSerializedString(new SerializedList<Order, OrderList>((OrderFinder.findMany(OrderFinder.all())), config));

        SerializedList<Order, OrderList> serialized = toSerializedString(sb);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
    }

    @Test
    public void testOrderWithDependentsNoMeta() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config = config.withDeepDependents();
        config = config.withoutMetaData();
        String sb = fromSerializedString(new SerializedList<Order, OrderList>((OrderFinder.findMany(OrderFinder.all())), config));

        SerializedList<Order, OrderList> serialized = toSerializedString(sb);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
    }

}
