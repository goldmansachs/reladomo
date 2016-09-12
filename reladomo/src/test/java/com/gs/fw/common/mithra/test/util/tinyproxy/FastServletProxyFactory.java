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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Map;

import com.gs.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastServletProxyFactory
{
    public static final String MAX_CONNECTIONS_PER_HOST = "fastServletProxyFactory.maxConnectionsPerHost";
    public static final String MAX_TOTAL_CONNECTION = "fastServletProxyFactory.maxTotalConnections";
    private static final int PING_TIMEOUT = 5000;
    private static final Map CHUNK_SUPPORTED = UnifiedMap.newMap();
    private static final Logger LOGGER = LoggerFactory.getLogger(FastServletProxyFactory.class);

    /**
     * Creates a new proxy with the specified URL.  The returned object
     * is a proxy with the interface specified by api.
     * <p/>
     * <pre>
     * String url = "http://localhost:7001/objectmanager/PspServlet");
     * RemoteObjectManager rom = (RemoteObjectManager) factory.create(RemoteObjectManager.class, url);
     * </pre>
     *
     * @param api the interface the proxy class needs to implement
     * @param url the URL where the client object is located.
     * @return a proxy to the object with the specified interface.
     */
    public <T> T create(Class<T> api, String url) throws MalformedURLException
    {
        URL someUrl = new URL(url);
        boolean supportsChunking = serverSupportsChunking(someUrl);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("chunking support at "+url+" :"+supportsChunking+" received client id: "+getProxyId(someUrl));
        }
        InvocationHandler handler = new FastServletProxyInvocationHandler(someUrl, api, this);
        return (T) Proxy.newProxyInstance(api.getClassLoader(),
                new Class[]{api},
                handler);
    }

    public int fastFailPing(URL url) throws IOException
     {
         HttpURLConnection urlc = null;
         try
         {
             urlc = (HttpURLConnection) url.openConnection();
             urlc.setRequestMethod("POST");
             urlc.setDoInput(true);
             urlc.setDoOutput(true);
             urlc.setRequestProperty("Content-Length", "" + 1);
             OutputStream outputStream = urlc.getOutputStream();
             outputStream.write(StreamBasedInvocator.PING_REQUEST);
             outputStream.close();
             return urlc.getResponseCode();
         }
         catch (IOException e)
         {
             return OutputStreamWriter.flushErrorStream(urlc);
         }
     }

    public boolean isServiceAvailable(String url)
    {
        boolean result = false;
        try
        {
            result = this.fastFailPing(new URL(url)) == 200;
        }
        catch (IOException e)
        {
            LOGGER.debug("ping failed with ", e);
        }
        return result;
    }

    public static long getProxyId(URL url)
    {
        serverSupportsChunking(url); // make sure we've talked to server at least once
        String key = url.getHost() + ":" + url.getPort();
        ServerId result = (ServerId) CHUNK_SUPPORTED.get(key);
        if (result == null)
        {
            return generateRandomProxyId();
        }
        return result.getProxyId();
    }

    public static boolean serverSupportsChunking(URL url)
    {
        String key = url.getHost() + ":" + url.getPort();
        if (CHUNK_SUPPORTED.get(key) == null)
        {
            HttpURLConnection urlc = null;
            try
            {
                urlc = (HttpURLConnection) url.openConnection();
                urlc.setRequestMethod("POST");
                urlc.setDoInput(true);
                urlc.setDoOutput(true);
                byte[] servletUrl = url.toString().getBytes("ISO8859_1");
                urlc.setChunkedStreamingMode(1 + servletUrl.length);
                OutputStream outputStream = urlc.getOutputStream();
                outputStream.write(StreamBasedInvocator.INIT_REQUEST);
                outputStream.write(servletUrl);
                outputStream.close();
                int code = urlc.getResponseCode();
                switch (code)
                {
                    case 200:
                        String responseBody = OutputStreamWriter.getResponseBodyAsString(urlc);
                        CHUNK_SUPPORTED.put(key, new ServerId(true, Long.parseLong(responseBody)));
                        break;
                    case 400:
                    case 500:
                        CHUNK_SUPPORTED.put(key, new ServerId(false, generateRandomProxyId()));
                        break;
                    case 404:
                        LOGGER.error("Could not find {} (HTTP/404). Looks like the servlet is not properly configured!", url);
                        break;
                    case 401:
                    case 403:
                        throw new PspRuntimeException("Authorization required for " + url + " (HTTP/" + code + "). Please provide valid credentials to servlet factory!");
                    default:
                        LOGGER.error("unhandled response code {} while determining chunk support", code);
                        break;
                }
            }
            catch (IOException e)
            {
                if (!isServerDownOrBusy(url, e))
                {
                    LOGGER.error("Could not determine chunk support for {} ", url, e);
                    CHUNK_SUPPORTED.put(key, new ServerId(false, generateRandomProxyId())); // we really shouldn't do this, but oh well, weblogic 5 is a piece of crap
                }
                OutputStreamWriter.flushErrorStream(urlc);
            }
        }
        ServerId result = (ServerId) CHUNK_SUPPORTED.get(key);
        return result != null && result.isChunkSupported();
    }

    public static void clearServerChunkSupportAndIds()
    {
        CHUNK_SUPPORTED.clear();
    }

    private static boolean isServerDownOrBusy(URL url, Throwable e)
    {
        if (e instanceof ConnectException || e instanceof SocketTimeoutException)
        {
            LOGGER.error("Looks like the service at {} is down or not responding. Could not determine chunk support.", url, e);
            return true;
        }
        if (e.getCause() == null)
        {
            return false;
        }
        return isServerDownOrBusy(url, e.getCause());
    }

    private static long generateRandomProxyId()
    {
        return (long) (Math.random() * 2000000000000L);
    }

    public static class ServerId
    {
        private final boolean chunkSupported;
        private final long proxyId;

        public ServerId(boolean chunkSupported, long proxyId)
        {
            this.chunkSupported = chunkSupported;
            this.proxyId = proxyId;
        }

        public boolean isChunkSupported()
        {
            return this.chunkSupported;
        }

        public long getProxyId()
        {
            return this.proxyId;
        }
    }
}

