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

package com.gs.reladomo.serial.jackson;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.util.serializer.TestBitemporalRoundTripStringBased;
import com.gs.fw.common.mithra.test.util.serializer.TestRoundTripStringBased;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class ExampleJacksonReladomoBitemporalSerializerTest extends TestBitemporalRoundTripStringBased
{
    @Override
    protected String toSerializedString(Serialized serialized) throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JacksonReladomoModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        return mapper.writeValueAsString(serialized);
    }

    protected Serialized fromSerializedString(String json) throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JacksonReladomoModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        JavaType customClassCollection = mapper.getTypeFactory().constructCollectionLikeType(Serialized.class, BitemporalOrder.class);

        return mapper.readValue(json, customClassCollection);
//        return mapper.readValue(json, new TypeReference<Serialized<Order>>() {});
//        return mapper.readValue(json, Serialized.class);
    }

    @Test
    public void testOrderWithRemovedItem() throws Exception
    {
        String json = "{\n" +
                "  \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrder\",\n" +
                "  \"_rdoState\" : 20,\n" +
                "  \"orderId\" : 1,\n" +
                "  \"orderDate\" : 1073883600000,\n" +
                "  \"userId\" : 1,\n" +
                "  \"description\" : \"First order modified\",\n" +
                "  \"trackingId\" : \"123\",\n" +
                "  \"businessDateFrom\" : 946702800000,\n" +
                "  \"businessDateTo\" : 253399726740000,\n" +
                "  \"processingDateFrom\" : 946702800000,\n" +
                "  \"processingDateTo\" : 253399726740000,\n" +
                "  \"businessDate\" : 1495116000000,\n" +
                "  \"processingDate\" : 253399726740000,\n" +
                "  \"items\" : {\n" +
                "    \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrderItem\",\n" +
                "    \"_rdoListSize\" : 0,\n" +
                "    \"elements\" : []\n" +
                "  }\n" +
                "}";

        Serialized<BitemporalOrder> serialized = fromSerializedString(json);
        BitemporalOrder unwrappedBitemporalOrder = serialized.getWrapped();
        assertEquals(1, unwrappedBitemporalOrder.getOrderId());
        assertEquals("First order modified", unwrappedBitemporalOrder.getDescription()); //modified attribute
        assertEquals("In-Progress", unwrappedBitemporalOrder.getState()); // missing in json, should stay as it was
        assertTrue(unwrappedBitemporalOrder.zIsDetached());
        assertEquals(0, unwrappedBitemporalOrder.getItems().size());

        unwrappedBitemporalOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        BitemporalOrder order = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(1).and(BitemporalOrderFinder.businessDate().eq(getBusinessDate())));
        assertEquals(1, order.getOrderId());
        assertEquals("First order modified", order.getDescription()); //modified attribute
        assertEquals("In-Progress", order.getState()); // missing in json, should stay as it was
        assertEquals(0, order.getItems().size());
    }

    @Test
    public void testInMemoryNoRelationships() throws Exception
    {
        String serialized = "{\n" +
                "  \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrderItem\",\n" +
//                "  \"_rdoState\" : 20,\n" +
                "  \"id\" : 70459,\n" +
                "  \"orderId\" : 1,\n" +
                "  \"productId\" : 1,\n" +
                "  \"quantity\" : 20.0,\n" +
                "  \"originalPrice\" : 10.5,\n" +
                "  \"discountPrice\" : 10.5,\n" +
                "  \"state\" : \"In-Progress\",\n" +
                "  \"businessDate\" : 1567363437186\n" +
                "}\n";
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JacksonReladomoModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        JavaType customClassCollection = mapper.getTypeFactory().constructCollectionLikeType(Serialized.class, BitemporalOrderItem.class);

        Serialized<BitemporalOrderItem> back = mapper.readValue(serialized, customClassCollection);
        BitemporalOrderItem wrapped = back.getWrapped();
        Assert.assertEquals(70459, wrapped.getId());
        Assert.assertEquals("In-Progress", wrapped.getState());
        Assert.assertEquals(BitemporalOrderItemFinder.processingDate().getInfinityDate(), wrapped.getProcessingDate());
    }

    @Test
    public void testOrderWithOneRemovedItem() throws Exception
    {
//        BitemporalOrder order = BitemporalOrderFinder.findByPrimaryKey(2, getBusinessDate(), BitemporalOrderFinder.processingDate().getInfinityDate());
//
//        System.out.println(toSerializedString(new Serialized(order,
//                SerializationConfig.shallowWithDefaultAttributes(BitemporalOrderFinder.getFinderInstance()).withDeepDependents())));

        String json = "{\n" +
                "  \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrder\",\n" +
                "  \"orderId\" : 2,\n" +
                "  \"orderDate\" : 1076562000000,\n" +
                "  \"userId\" : 1,\n" +
                "  \"description\" : \"Second order\",\n" +
                "  \"state\" : \"In-Progress\",\n" +
                "  \"trackingId\" : \"124\",\n" +
                "  \"businessDate\" : 1495116000000,\n" +
                "  \"processingDate\" : 253399726740000,\n" +
                "  \"items\" : {\n" +
                "    \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrderItem\",\n" +
                "    \"_rdoListSize\" : 2,\n" +
                "    \"elements\" : [ {\n" +
                "      \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrderItem\",\n" +
                "      \"id\" : 2,\n" +
                "      \"orderId\" : 2,\n" +
                "      \"productId\" : 1,\n" +
                "      \"quantity\" : 20.0,\n" +
                "      \"originalPrice\" : null,\n" +
                "      \"discountPrice\" : 10.5,\n" +
                "      \"state\" : \"In-Progress\",\n" +
                "      \"businessDate\" : 1495116000000,\n" +
                "      \"processingDate\" : 253399726740000,\n" +
                "      \"orderItemStatus\" : null\n" +
                "    }, {\n" +
                "      \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrderItem\",\n" +
                "      \"id\" : 3,\n" +
                "      \"orderId\" : 2,\n" +
                "      \"productId\" : 2,\n" +
                "      \"quantity\" : 20.0,\n" +
                "      \"originalPrice\" : 15.5,\n" +
                "      \"discountPrice\" : 10.0,\n" +
                "      \"state\" : \"In-Progress\",\n" +
                "      \"businessDate\" : 1495116000000,\n" +
                "      \"processingDate\" : 253399726740000,\n" +
                "      \"orderItemStatus\" : {\n" +
                "        \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.BitemporalOrderItemStatus\",\n" +
                "        \"itemId\" : 3,\n" +
                "        \"status\" : 20,\n" +
                "        \"lastUser\" : \"Trinity\",\n" +
                "        \"lastUpdateTime\" : 1073883600000,\n" +
                "        \"businessDate\" : 1495116000000,\n" +
                "        \"processingDate\" : 253399726740000\n" +
                "      }\n" +
                "    } ]\n" +
                "  },\n" +
                "  \"orderStatus\" : null\n" +
                "}\n";

        Serialized<BitemporalOrder> serialized = fromSerializedString(json);
        BitemporalOrder unwrappedBitemporalOrder = serialized.getWrapped();

        assertEquals(2, unwrappedBitemporalOrder.getOrderId());
        assertTrue(unwrappedBitemporalOrder.zIsDetached());
        assertEquals(2, unwrappedBitemporalOrder.getItems().size());

        unwrappedBitemporalOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        BitemporalOrder order = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(2).and(BitemporalOrderFinder.businessDate().eq(getBusinessDate())));
        assertEquals(2, order.getItems().size());
    }

}
