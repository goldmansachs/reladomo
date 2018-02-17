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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test.util.tinyproxy;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class PspServlet implements Servlet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PspServlet.class.getName());

    private static final AtomicInteger CLIENT_ID = new AtomicInteger((int) (Math.random() * 20000.0) * 100000);
    private static long vmId;

    protected ServletConfig config;

    private final UnifiedMap serviceMap = new UnifiedMap();

    private int pings;
    private int methodInvocations;
    private int thankYous;
    private int resendRequests;
    private long startTime;
    private final UnifiedSet<String> registeredUrls = new UnifiedSet<String>();
    private boolean binaryLoggingEnabled;

    /**
     * Returns the named initialization parameter.
     *
     * @param name name of parameter
     * @return the initialization parameter
     */
    public String getInitParameter(String name)
    {
        return this.config.getInitParameter(name);
    }

    /**
     * Returns the servlet context.
     */
    public ServletConfig getServletConfig()
    {
        return this.config;
    }

    /**
     * Cleanup the service instance.
     */
    public void destroy()
    {
        vmId = 0L;
    }

    public String getServletInfo()
    {
        return "Proxy Method Invokator Servlet";
    }

    /**
     * Initialize the servlet, including the service object.
     */
    public void init(ServletConfig config) throws ServletException
    {
        this.startTime = System.currentTimeMillis();
        this.config = config;

        Enumeration parameterNameEnum = this.config.getInitParameterNames();
        boolean foundConfig = false;
        while (parameterNameEnum.hasMoreElements())
        {
            String paramName = (String) parameterNameEnum.nextElement();
            if (paramName.startsWith("serviceInterface"))
            {
                String interfaceName = this.getInitParameter(paramName);
                String definitionName = paramName.substring("serviceInterface".length(), paramName.length());
                String classParamName = "serviceClass" + definitionName;
                String className = this.getInitParameter(classParamName);
                Class interfaceClass = this.loadClass(interfaceName); // just checking to see it exists
                Class serviceClass = this.loadClass(className);
                boolean serviceImplementsInterface = interfaceClass.isAssignableFrom(serviceClass);
                if (!serviceImplementsInterface)
                {
                    LOGGER.warn("The class {} does not implement {}. This may be a serious error in your configuration. This class will not be available locally.", serviceClass.getName(), interfaceName);
                }
                Object service;
                try
                {
                    service = serviceClass.newInstance();
                }
                catch (Exception e)
                {
                    LOGGER.error("Caught exception while instantiating service class: {}", serviceClass, e);
                    throw new ServletException(e);
                }
                MethodResolver methodResolver = new MethodResolver(serviceClass);
                this.serviceMap.put(interfaceName, new ServiceDefinition(service, methodResolver));
                foundConfig = true;
            }
        }
        if (vmId == 0L)
        {
            vmId = System.currentTimeMillis() >> 8 << 32;
        }
        if (!foundConfig)
        {
            throw new ServletException(
                    "PspServlet must be configured using serviceInterface.x and serviceClass.x parameter names (x can be anything)");
        }
    }

    protected Class loadClass(String className) throws ServletException
    {
        Class serviceClass;
        try
        {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();

            if (loader == null)
            {
                serviceClass = Class.forName(className);
            }
            else
            {
                serviceClass = Class.forName(className, false, loader);
            }
        }
        catch (Exception e)
        {
            throw new ServletException(e);
        }
        return serviceClass;
    }

    /**
     * Execute a request.  The path-info of the request selects the bean.
     * Once the bean's selected, it will be applied.
     */
    public void service(ServletRequest request, ServletResponse response) throws IOException, ServletException
    {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;

        if ("POST".equals(req.getMethod()))
        {
            this.processPost(request, response);
        }
        else if ("GET".equals(req.getMethod()))
        {
            this.printStatistics(res);
        }
        else
        {
            String errorMessage = "PSP Servlet Requires POST";
            res.sendError(405, errorMessage);
        }
    }

    public int getThankYous()
    {
        return this.thankYous;
    }

    protected void processPost(ServletRequest request, ServletResponse response) throws IOException, ServletException
    {
        InputStream is = request.getInputStream();

        byte requestType = (byte) is.read();
        if (requestType == StreamBasedInvocator.PING_REQUEST)
        {
            this.pings++;
            response.setContentLength(0);
            return;
        }
        if (requestType == StreamBasedInvocator.INIT_REQUEST)
        {
            this.serviceInitRequest(response, is);
            return;
        }
        HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        String lenString = httpServletRequest.getHeader("Content-length");
        if (lenString != null)
        {
            int len = Integer.parseInt(lenString);
        }
        try
        {
            ObjectInputStream in = new ObjectInputStream(is);
            switch (requestType)
            {
                case StreamBasedInvocator.INVOKE_REQUEST:
                    this.serviceInvokeRequest(request, response, in);
                    break;
                case StreamBasedInvocator.RESEND_REQUEST:
                    this.serviceResendRequest(response, in);
                    break;
                case StreamBasedInvocator.THANK_YOU_REQUEST:
                    this.serviceThankYou(in);
                    break;
            }
        }
        catch (IOException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            LOGGER.error("unexpected exception", e);
            throw new ServletException(e);
        }
    }

    private void serviceInitRequest(ServletResponse response, InputStream is) throws IOException
    {
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int n;
        while ((n = is.read(buffer)) >= 0)
        {
            outBuffer.write(buffer, 0, n);
        }
        String url = outBuffer.toString("ISO8859_1");
        synchronized (this.registeredUrls)
        {
            if (!this.registeredUrls.contains(url))
            {
                this.registeredUrls.add(url);
            }
        }
        int id = CLIENT_ID.incrementAndGet();
        long vmAndClientId = vmId | (long) id;
        String clientCount = String.valueOf(vmAndClientId);
        response.setContentLength(clientCount.length());
        response.getWriter().write(clientCount);
    }

    private void serviceThankYou(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        this.thankYous++;
        //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
        int thankYouNotes = in.readInt();
        for (int i = 0; i < thankYouNotes; i++)
        {
            //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            com.gs.fw.common.mithra.test.util.tinyproxy.ContextCache.getInstance().removeContext((RequestId) in.readObject());
        }
    }

    private void serviceResendRequest(
            ServletResponse response,
            ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        this.resendRequests++;
        //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
        RequestId resendRequestId = (RequestId) in.readObject();
        Context resendContext = com.gs.fw.common.mithra.test.util.tinyproxy.ContextCache.getInstance().getContext(resendRequestId);
        if (resendContext == null || resendContext.isCreatedState() || resendContext.isReadingParameters())
        {
            response.getOutputStream().write(StreamBasedInvocator.REQUEST_NEVER_ARRVIED_STATUS);
        }
        else
        {
            resendContext.waitForInvocationToFinish();
            resendContext.writeResponse(response.getOutputStream());
        }
    }

    private void serviceInvokeRequest(
            ServletRequest request,
            ServletResponse response,
            ObjectInputStream in) throws Exception
    {
        this.methodInvocations++;
        //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
        RequestId requestId = (RequestId) in.readObject();
        Context invokeContext = com.gs.fw.common.mithra.test.util.tinyproxy.ContextCache.getInstance().getOrCreateContext(requestId);
        String serviceInterface = (String) in.readObject();
        ServiceDefinition serviceDefinition = (ServiceDefinition) this.serviceMap.get(serviceInterface);
        if (serviceDefinition == null)
        {
            invokeContext.setReturnValue(new PspRuntimeException("PspServlet is not servicing "
                    + serviceInterface), true);
        }
        else
        {
            new StreamBasedInvocator().invoke(in,
                    invokeContext,
                    serviceDefinition.getService(),
                    serviceDefinition.getMethodResolver(),
                    request.getRemoteAddr(),
                    requestId
            );
        }
        invokeContext.writeResponse(response.getOutputStream());
    }

    protected void printStatistics(HttpServletResponse res) throws IOException
    {
        res.setContentType("text/html");
        res.getWriter().print("<html><body>");
        res.getWriter().print("<h1>PSP Servlet</h1><br>Configured for <br>");
        Iterator it = this.serviceMap.keySet().iterator();
        while (it.hasNext())
        {
            res.getWriter().print(it.next() + "<br>");
        }
        res.getWriter().print("<br>Total Method Invocations: " + this.methodInvocations + "<br>");
        res.getWriter().print("<br>Total Resend Requests: " + this.resendRequests + "<br>");
        res.getWriter().print("<br>Total Coalesced Thank You Requests: " + this.thankYous + "<br>");
        res.getWriter().print("<br>Total Pings: " + this.pings + "<br>");
        long seconds = (System.currentTimeMillis() - this.startTime) / 1000L;
        res.getWriter().print("<br>Uptime: " + seconds + " sec (about " + seconds / 3600L + " hours " + seconds / 60L % 60L + " minutes)<br>");
        res.getWriter().print("</body></html>");
    }

    protected static class ServiceDefinition
    {
        private final Object service;
        private final MethodResolver methodResolver;

        protected ServiceDefinition(
                Object service,
                MethodResolver methodResolver)
        {
            this.service = service;
            this.methodResolver = methodResolver;
        }

        public Object getService()
        {
            return this.service;
        }

        public MethodResolver getMethodResolver()
        {
            return this.methodResolver;
        }

    }
}
