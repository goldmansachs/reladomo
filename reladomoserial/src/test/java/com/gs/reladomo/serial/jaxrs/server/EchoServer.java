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

package com.gs.reladomo.serial.jaxrs.server;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Random;
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

    private static int getPid()
    {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        if (pid != null)
        {
            int atIndex = pid.indexOf("@");
            if (atIndex > 0)
            {
                pid = pid.substring(0, atIndex);
            }
            try
            {
                return Integer.parseInt(pid);
            }
            catch (NumberFormatException e)
            {
                //ignore
            }
        }
        return new Random().nextInt(10000);
    }

    private static int findFreePort() throws IOException
    {
        return 10240+(getPid() % 32000);
    }
}