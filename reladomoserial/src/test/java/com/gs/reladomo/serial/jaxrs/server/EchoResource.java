package com.gs.reladomo.serial.jaxrs.server;

import com.gs.fw.common.mithra.test.domain.Account;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.util.serializer.Serialized;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/echo")
public class EchoResource
{
    @Path("/order")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response echoOrder(Serialized<Order> order)
    {
        return Response.ok().entity(order).build();
    }

    @Path("/account")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response echoAccount(Serialized<Account> account)
    {
        return Response.ok().entity(account).build();
    }
}