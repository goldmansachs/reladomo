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

package com.gs.fw.common.mithra.test.tinyproxy;

import java.net.MalformedURLException;

import com.gs.fw.common.mithra.test.util.tinyproxy.FastServletProxyFactory;
import com.gs.fw.common.mithra.test.util.tinyproxy.ThankYouWriter;
import com.gs.fw.common.mithra.test.util.tinyproxy.PspServlet;
import junit.framework.TestCase;
import org.junit.Assert;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public abstract class PspTestCase
        extends TestCase
{
    protected Server server;
    protected PspServlet servlet;

    private int port;
    private String pspUrl;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        this.setupServerWithHandler(null);
    }

    public int getPort()
    {
        return this.port;
    }

    public String getPspUrl()
    {
        return this.pspUrl;
    }

    protected void setupServerWithHandler(
            Handler handler) throws Exception
    {
        this.port = (int) (Math.random() * 10000.0 + 10000.0);
        this.pspUrl = "http://localhost:" + this.port + "/PspServlet";
        this.server = new Server(this.port);
        Context context = new Context(server, "/", Context.SESSIONS);
        if (handler != null)
        {
            context.addHandler(handler);
        }
        ServletHolder holder = context.addServlet(PspServlet.class, "/PspServlet");
        holder.setInitParameter("serviceInterface.Echo", "com.gs.fw.common.mithra.test.tinyproxy.Echo");
        holder.setInitParameter("serviceClass.Echo", "com.gs.fw.common.mithra.test.tinyproxy.EchoImpl");
        holder.setInitOrder(10);

        this.server.start();
        this.servlet = (PspServlet) holder.getServlet();
    }

    @Override
    protected void tearDown() throws Exception
    {
        FastServletProxyFactory.clearServerChunkSupportAndIds();
        ThankYouWriter.getInstance().stopThankYouThread();
        if (this.server != null)
        {
            this.server.stop();
        }
        super.tearDown();
    }

    protected Echo buildEchoProxy() throws MalformedURLException
    {
        FastServletProxyFactory fspf = new FastServletProxyFactory();

        Echo echo = fspf.create(Echo.class, this.pspUrl);

        Assert.assertNotSame(echo.getClass(), EchoImpl.class);

        return echo;
    }
}
