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

package com.gs.reladomo.serial.jaxrs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Account;
import com.gs.fw.common.mithra.test.domain.AccountFinder;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import com.gs.reladomo.serial.jackson.JacksonReladomoModule;
import com.gs.reladomo.serial.jaxrs.server.JacksonObjectMapperProvider;
import com.gs.reladomo.serial.jaxrs.server.EchoServer;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

public class JaxRsRoundTripSerializationTest extends MithraTestAbstract
{
    public static EchoServer echoServer;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        echoServer = new EchoServer();
        echoServer.start();
    }

    @AfterClass
    public static void tearDownClass() throws IOException
    {
        if (echoServer != null)
        {
            echoServer.stop();
        }
    }

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        setupClass();
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        tearDownClass();
    }

    @Test
    public void serializeOrder() throws Exception
    {
        String json = "{\n" +
                "  \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "  \"_rdoState\" : 20,\n" +
                "  \"orderId\" : 1,\n" +
                "  \"orderDate\" : 1073883600000,\n" +
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

        Order order = new Order();
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config.withDeepDependents();
        serialized = new Serialized(order, config);


        Response response = clientFor(echoServer.getBaseUrl())
                .path("echo/order")
                .request()
                .post(Entity.entity(serialized, MediaType.APPLICATION_JSON_TYPE));

        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void serializeAccount() throws Exception
    {
        Account a = new Account();
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(AccountFinder.getFinderInstance());
        config.withDeepDependents();
        Serialized<Account> serialized = new Serialized(a, config);


        Response response = clientFor(echoServer.getBaseUrl())
                .path("echo/account")
                .request()
                .post(Entity.entity(serialized, MediaType.APPLICATION_JSON_TYPE));

        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void testRoundTripOrder() throws Exception
    {
        Order order = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1));
        SerializationConfig config = SerializationConfig.shallowWithDefaultAttributes(OrderFinder.getFinderInstance());
        config.withDeepDependents();

        Serialized<Order> toEcho = new Serialized<Order>(order, config);

        Response response = clientFor(echoServer.getBaseUrl())
                .path("echo/order")
                .request()
                .post(Entity.entity(toEcho, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(200, response.getStatus());


        Serialized<Order> orderSerialized = response.readEntity(new GenericType<Serialized<Order>>()
        {
        });

        assertEquals(1, orderSerialized.getWrapped().getOrderId());
        assertTrue(orderSerialized.getWrapped().zIsDetached());

    }

    @Test
    public void testGetOrder() throws Exception
    {
        Response response = clientFor(echoServer.getBaseUrl())
                .path("echo/orderOne")
                .request().get();

        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void testWriteOrder() throws Exception
    {
        String json = "{\n" +
//                "  \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "  \"_rdoState\" : 20,\n" +
                "  \"orderId\" : 1,\n" +
                "  \"orderDate\" : 1073883600000,\n" +
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

        Response response = clientFor(echoServer.getBaseUrl())
                .path("echo/writeOrder")
                .request()
                .post(Entity.entity(json, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(204, response.getStatus());

        Order order = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1));
        assertEquals(1, order.getOrderId());
        assertEquals("First order modified", order.getDescription()); //modified attribute
        assertEquals("In-Progress", order.getState()); // missing in json, should stay as it was
        assertEquals(0, order.getItems().size());

    }

    @Test
    public void testAnything() throws Exception
    {
        String json = "{\n" +
                "  \"_rdoClassName\" : \"com.gs.fw.common.mithra.test.domain.Order\",\n" +
                "  \"_rdoState\" : 20,\n" +
                "  \"orderId\" : 1,\n" +
                "  \"orderDate\" : 1073883600000,\n" +
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

        Response response = clientFor(echoServer.getBaseUrl())
                .path("echo/writeAnything")
                .request()
                .post(Entity.entity(json, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(204, response.getStatus());

        Order order = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1));
        assertEquals(1, order.getOrderId());
        assertEquals("First order modified", order.getDescription()); //modified attribute
        assertEquals("In-Progress", order.getState()); // missing in json, should stay as it was
        assertEquals(0, order.getItems().size());

    }

    protected Serialized fromSerializedString(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JacksonReladomoModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        CollectionLikeType customClassCollection = mapper.getTypeFactory().constructCollectionLikeType(Serialized.class, Order.class);
        return (Serialized)mapper.readValue(json, customClassCollection);
    }

    private WebTarget clientFor(String url)
    {
        return ClientBuilder.newClient()
                .register(JacksonFeature.class)
                .register(JacksonObjectMapperProvider.class)
                .target(url);
    }
}