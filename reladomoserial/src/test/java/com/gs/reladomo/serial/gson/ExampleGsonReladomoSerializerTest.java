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

package com.gs.reladomo.serial.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.util.serializer.TestRoundTripStringBased;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import com.gs.reladomo.serial.jackson.ExampleJacksonReladomoSerializerTest;
import org.junit.Test;

public class ExampleGsonReladomoSerializerTest extends TestRoundTripStringBased
{
    @Override
    protected String toSerializedString(Serialized serialized) throws Exception
    {
        return register().toJson((Serialized<Order>) serialized);
    }

    @Override
    protected Serialized fromSerializedString(String json) throws Exception
    {
        java.lang.reflect.Type t = new TypeToken<Serialized<Order>>() {}.getType();
        return register().fromJson(json, t);
    }

    private Gson register()
    {
        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapterFactory(new GsonWrappedTypeAdaptorFactory());
        gsonBuilder.setPrettyPrinting();
        gsonBuilder.serializeNulls();
        return gsonBuilder.create();
    }

    @Test
    public void testOrderWithRemovedItem() throws Exception
    {
        String json = "{\n" +
                "  \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "  \"_rdoState\" : 20,\n" +
                "  \"orderId\" : 1,\n" +
                "  \"orderDate\": \"2004-01-12T05:00:00.000Z\",\n"+
                "  \"userId\" : 1,\n" +
                "  \"description\" : \"First order modified\",\n" +
                "  \"trackingId\" : \"123\",\n" +
                "  \"items\" : {\n" +
                "    \"_rdoMetaData\" : {\n" +
                "      \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.OrderItem\",\n" +
                "      \"_rdoListSize\" : 0\n" +
                "    },\n" +
                "    \"elements\" : []\n" +
                "  }\n" +
                "}";

        Serialized<Order> serialized = fromSerializedString(json);
        Order unwrappedOrder = serialized.getWrapped();
        assertEquals(1, unwrappedOrder.getOrderId());
        assertEquals("First order modified", unwrappedOrder.getDescription()); //modified attribute
        assertEquals("In-Progress", unwrappedOrder.getState()); // missing in json, should stay as it was
        assertTrue(unwrappedOrder.zIsDetached());
        assertEquals(0, unwrappedOrder.getItems().size());

        unwrappedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        Order order = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1));
        assertEquals(1, order.getOrderId());
        assertEquals("First order modified", order.getDescription()); //modified attribute
        assertEquals("In-Progress", order.getState()); // missing in json, should stay as it was
        assertEquals(0, order.getItems().size());
    }

}
