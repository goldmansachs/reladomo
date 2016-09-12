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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;

import com.gs.fw.common.mithra.test.util.tinyproxy.FastServletProxyFactory;
import com.gs.fw.common.mithra.test.util.tinyproxy.PspRuntimeException;
import org.junit.Assert;
import org.mortbay.component.LifeCycle;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Response;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.servlet.Context;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ErrorConditionsTest extends PspTestCase
{
    @Override
    protected void setUp() throws Exception
    {
        // nb: don't perform default server startup (override prevents this)
    }

    public void testPingFailure()
    {
        Assert.assertFalse(new FastServletProxyFactory().isServiceAvailable(this.getPspUrl()));
    }

    public void testPingFailureOnce() throws Exception
    {
        setErrorCausingHandler(1);

        FastServletProxyFactory factory = new FastServletProxyFactory();
        Assert.assertFalse(factory.isServiceAvailable(this.getPspUrl()));
        Assert.assertTrue(factory.isServiceAvailable(this.getPspUrl()));
    }

    public void setErrorCausingHandler(int causeErrorOnEventNumber) throws Exception
    {
        HttpErrorCausingHandler handler = new HttpErrorCausingHandler(causeErrorOnEventNumber);
        HandlerList list = new HandlerList();
        list.addHandler(handler);
        this.setupServerWithHandler(list);
    }

    public void testNoChunkingSupport() throws Exception
    {
        setErrorCausingHandler(1);

        Echo echo = this.buildEchoProxy();

        Assert.assertEquals("hello", echo.echo("hello"));
        Assert.assertFalse(FastServletProxyFactory.serverSupportsChunking(new URL(this.getPspUrl())));
        StringBuilder largeBuffer = new StringBuilder(5000);
        for (int i = 0; i < 5000; i++)
        {
            largeBuffer.append(i);
        }
        Assert.assertTrue(largeBuffer.length() > 5000);
        String largeString = largeBuffer.toString();
        Assert.assertEquals(largeString, echo.echo(largeString));
    }

    public void testRetry() throws Exception
    {
        setErrorCausingHandler(2);

        Echo echo = this.buildEchoProxy();
        Assert.assertEquals("hello", echo.echo("hello"));
    }

    // the following test takes too long (2 minutes)
    public void xtestNoServer() throws Exception
    {
        ServerSocket ss = new ServerSocket(this.getPort());
        new DummyServer(ss);
        try
        {
            Echo echo = this.buildEchoProxy();
            echo.echo("nothing");
            Assert.fail("should not get here");
        }
        catch (PspRuntimeException e)
        {
            // ok
        }
    }

    public static class HttpErrorCausingHandler implements Handler
    {
        private boolean started;
        private Server server;
        private int causeErrorOnEventNumber = -1;
        private int currentEventNumber;

        public HttpErrorCausingHandler(int causeErrorOnEventNumber)
        {
            this.causeErrorOnEventNumber = causeErrorOnEventNumber;
        }

        @Override
        public void handle(String s, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, int i) throws IOException, ServletException
        {
            this.currentEventNumber++;
            if (this.currentEventNumber == this.causeErrorOnEventNumber)
            {
                throw new IOException("exception for testing");
            }
        }

        @Override
        public void setServer(Server server)
        {
            this.server = server;
        }

        @Override
        public Server getServer()
        {
            return this.server;
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public boolean isRunning()
        {
            return this.started;
        }

        @Override
        public boolean isStarting()
        {
            return false;
        }

        @Override
        public boolean isStopping()
        {
            return false;
        }

        @Override
        public boolean isStopped()
        {
            return !this.isStarted();
        }

        @Override
        public boolean isFailed()
        {
            return false;
        }

        @Override
        public void addLifeCycleListener(Listener listener)
        {
        }

        @Override
        public void removeLifeCycleListener(Listener listener)
        {
        }

        public void start() throws Exception
        {
            this.started = true;
        }

        public void stop() throws InterruptedException
        {
            this.started = false;
        }

        public boolean isStarted()
        {
            return this.started;
        }
    }

    public static class DummyServer extends Thread
    {
        private final ServerSocket serverSocket;

        public DummyServer(ServerSocket serverSocket)
        {
            this.serverSocket = serverSocket;
            this.start();
        }

        @Override
        public void run()
        {
            try
            {
                Socket s = this.serverSocket.accept();
                s.close();
                this.serverSocket.close();
            }
            catch (IOException e)
            {
                //nothing to do
            }
        }
    }
}
