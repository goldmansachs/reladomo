package com.gs.reladomo.serial.jaxrs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

public class JaxRsRoundTripSerializationTest
{
    public static EchoServer echoServer;

    @BeforeClass
    public static void setup() throws IOException
    {
        echoServer = new EchoServer();
        echoServer.start();
    }

    @AfterClass
    public static void tearDown() throws IOException
    {
        if (echoServer != null)
        {
            echoServer.stop();
        }
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