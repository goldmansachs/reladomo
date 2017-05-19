package com.gs.reladomo.serial.jaxrs.server;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EchoServer
{
    private HttpServer server;
    private String baseUrl;

    public EchoServer() throws IOException
    {
        this.baseUrl = "http://localhost:" + findFreePort();
        this.server = makeHttpServer();
    }

    public void start() throws IOException
    {
        server.start();
    }

    public void stop()
    {
        server.shutdownNow();
    }

    public String getBaseUrl()
    {
        return baseUrl;
    }

    private HttpServer makeHttpServer() throws IOException
    {
        ResourceConfig rc = initResources();
        enableDebugLogs();
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(baseUrl), rc);
    }

    private ResourceConfig initResources()
    {
        ResourceConfig rc = new ResourceConfig();
        rc.register(EchoResource.class);
        rc.register(JacksonFeature.class);
        rc.register(JacksonObjectMapperProvider.class);
        return rc;

        /*
        ObjectMapper ob = new ObjectMapper();
        JacksonJsonProvider jc = new JacksonJsonProvider();
        jc.setMapper(ob);
        rc.register(jc);
        */
    }

    private void enableDebugLogs()
    {
        Logger l = Logger.getLogger("org.glassfish.grizzly.http.server.HttpHandler");
        l.setLevel(Level.FINE);
        l.setUseParentHandlers(false);
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);
        l.addHandler(ch);
    }

    private static int findFreePort() throws IOException
    {
        return 49440;
//        ServerSocket serverSocket = new ServerSocket(0);
//        return serverSocket.getLocalPort();
    }
}