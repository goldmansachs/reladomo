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
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.util.serializer.TestRoundTripListStringBased;
import com.gs.fw.common.mithra.test.util.serializer.TestRoundTripStringBased;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import com.gs.fw.common.mithra.util.serializer.SerializedList;
import org.junit.Test;

public class ExampleGsonReladomoSerializerListTest extends TestRoundTripListStringBased
{
    @Override
    protected String fromSerializedString(SerializedList serialized) throws Exception
    {
        return register().toJson((SerializedList<Order, OrderList>) serialized);
    }

    @Override
    protected SerializedList toSerializedString(String json) throws Exception
    {
        java.lang.reflect.Type t = new TypeToken<SerializedList<Order, OrderList>>() {}.getType();
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
    public void testDeletedOrder() throws Exception
    {
        String json = "{\n" +
                "  \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "  \"_rdoListSize\": 7,\n" +
                "  \"elements\": [\n" +
                "    {\n" +
                "      \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "      \"_rdoState\" : "+ ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE+",\n" +
                "      \"orderId\": 1,\n" +
                "      \"orderDate\": \"2004-01-12T05:00:00.000Z\",\n" +
                "      \"userId\": 1,\n" +
                "      \"description\": \"First order\",\n" +
                "      \"state\": \"In-Progress\",\n" +
                "      \"trackingId\": \"123\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "      \"_rdoState\": 20,\n" +
                "      \"orderId\": 2,\n" +
                "      \"orderDate\": \"2004-02-12T05:00:00.000Z\",\n" +
                "      \"userId\": 1,\n" +
                "      \"description\": \"Second order\",\n" +
                "      \"state\": \"In-Progress\",\n" +
                "      \"trackingId\": \"124\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "      \"_rdoState\": 20,\n" +
                "      \"orderId\": 3,\n" +
                "      \"orderDate\": \"2004-03-12T05:00:00.000Z\",\n" +
                "      \"userId\": 1,\n" +
                "      \"description\": \"Third order\",\n" +
                "      \"state\": \"In-Progress\",\n" +
                "      \"trackingId\": \"125\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "      \"_rdoState\": 20,\n" +
                "      \"orderId\": 4,\n" +
                "      \"orderDate\": \"2004-04-12T04:00:00.000Z\",\n" +
                "      \"userId\": 2,\n" +
                "      \"description\": \"Fourth order, different user\",\n" +
                "      \"state\": \"In-Progress\",\n" +
                "      \"trackingId\": \"126\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "      \"_rdoState\": 20,\n" +
                "      \"orderId\": 55,\n" +
                "      \"orderDate\": \"2004-04-12T04:00:00.000Z\",\n" +
                "      \"userId\": 3,\n" +
                "      \"description\": \"Order number five\",\n" +
                "      \"state\": \"Gummed up\",\n" +
                "      \"trackingId\": \"127\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "      \"_rdoState\": 20,\n" +
                "      \"orderId\": 56,\n" +
                "      \"orderDate\": \"2004-04-12T04:00:00.000Z\",\n" +
                "      \"userId\": 3,\n" +
                "      \"description\": \"Sixth order, different user\",\n" +
                "      \"state\": \"Gummed up\",\n" +
                "      \"trackingId\": \"128\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"_rdoClassName\": \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "      \"_rdoState\": 20,\n" +
                "      \"orderId\": 57,\n" +
                "      \"orderDate\": \"2004-04-12T04:00:00.000Z\",\n" +
                "      \"userId\": 3,\n" +
                "      \"description\": \"Seventh order, different user\",\n" +
                "      \"state\": \"Gummed up\",\n" +
                "      \"trackingId\": \"129\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SerializedList<Order, OrderList> serialized = toSerializedString(json);
        OrderList list = serialized.getWrapped();
        assertEquals(7, list.size());
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).zIsDetached());
        }
        list.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        assertNull(OrderFinder.findByPrimaryKey(1));
    }
}
