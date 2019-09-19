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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.SerialView;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import org.junit.Test;

import java.sql.Timestamp;

public abstract class TestBitemporalRoundTripStringBased extends MithraTestAbstract
{

    protected abstract String toSerializedString(Serialized serialized) throws Exception;

    protected abstract Serialized fromSerializedString(String json) throws Exception;

    @Test
    public void testBitemporalOrder() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        String sb = toSerializedString(new Serialized(findOrder(1), config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        assertEquals(1, serialized.getWrapped().getOrderId());
        assertTrue(serialized.getWrapped().zIsDetached());
    }

    @Test
    public void testBitemporalOrderWithNullString() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        BitemporalOrder order = findOrder(1).getNonPersistentCopy();
        order.setDescription(null);
        String sb = toSerializedString(new Serialized(order, config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        BitemporalOrder wrapped = serialized.getWrapped();
        assertEquals(1, wrapped.getOrderId());
        assertNull(wrapped.getDescription());
    }

    private BitemporalOrder findOrder(int orderId)
    {
        Operation op = BitemporalOrderFinder.orderId().eq(orderId);
        op = op.and(BitemporalOrderFinder.businessDate().eq(getBusinessDate()));
        return BitemporalOrderFinder.findOne(op);
    }

    protected Timestamp getBusinessDate()
    {
        return newTimestamp("2017-05-18 10:00:00");
    }

    @Test
    public void testBitemporalOrderWithItemsAndStatus() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        config = config.withDeepFetches(BitemporalOrderFinder.orderStatus(), BitemporalOrderFinder.items());
        config = config.withAnnotatedMethods(SerialView.Shorter.class);

        String sb = toSerializedString(new Serialized((findOrder(1)), config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        assertEquals(1, serialized.getWrapped().getOrderId());
        assertTrue(serialized.getWrapped().zIsDetached());
        assertEquals(1, serialized.getWrapped().getItems().size());
    }

    @Test
    public void testBitemporalOrderTwoDeep() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        config = config.withDeepFetches(BitemporalOrderFinder.orderStatus(), BitemporalOrderFinder.items().orderItemStatus());
        String sb = toSerializedString(new Serialized((findOrder(1)), config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        assertEquals(1, serialized.getWrapped().getOrderId());
        assertTrue(serialized.getWrapped().zIsDetached());
        assertEquals(1, serialized.getWrapped().getItems().size());
    }

    @Test
    public void testBitemporalOrderWithItems() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        config = config.withDeepFetches(BitemporalOrderFinder.items());
        String sb = toSerializedString(new Serialized((findOrder(1)), config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        assertEquals(1, serialized.getWrapped().getOrderId());
        assertTrue(serialized.getWrapped().zIsDetached());
        assertEquals(1, serialized.getWrapped().getItems().size());
    }

    @Test
    public void testBitemporalOrderWithDependents() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        config = config.withDeepDependents();
        String sb = toSerializedString(new Serialized((findOrder(1)), config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        assertEquals(1, serialized.getWrapped().getOrderId());
        assertTrue(serialized.getWrapped().zIsDetached());
        assertEquals(1, serialized.getWrapped().getItems().size());
    }

    @Test
    public void testBitemporalOrderWithDependentsAndLongMethods() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        config = config.withDeepDependents();
        config = config.withAnnotatedMethods(SerialView.Longer.class);

        String sb = toSerializedString(new Serialized((findOrder(2)), config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        assertEquals(2, serialized.getWrapped().getOrderId());
        assertTrue(serialized.getWrapped().zIsDetached());
        assertEquals(3, serialized.getWrapped().getItems().size());
    }

    @Test
    public void testBitemporalOrderWithDependentsNoMeta() throws Exception
    {
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance());
        config = config.withDeepDependents();
        config = config.withoutMetaData();
        String sb = toSerializedString(new Serialized((findOrder(1)), config));

        Serialized<BitemporalOrder> serialized = fromSerializedString(sb);
        assertEquals(1, serialized.getWrapped().getOrderId());
        assertTrue(serialized.getWrapped().zIsDetached());
        assertEquals(1, serialized.getWrapped().getItems().size());
    }

}
