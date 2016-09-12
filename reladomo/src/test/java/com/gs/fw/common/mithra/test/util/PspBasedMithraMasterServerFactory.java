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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.test.util.tinyproxy.FastServletProxyFactory;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.cache.offheap.MasterCacheService;

import java.net.MalformedURLException;
import java.util.Properties;



public class PspBasedMithraMasterServerFactory
{

    private static int port;

    protected static MasterCacheService createFactory(Properties properties, String url)
    {
        FastServletProxyFactory factory = new FastServletProxyFactory();
        try
        {
            return factory.create(MasterCacheService.class, url);
        }
        catch (MalformedURLException e)
        {
            throw new MithraException("malformed url: "+url, e);
        }
    }

    public static int getPort()
    {
        return port;
    }

    public static void setPort(int port)
    {
        PspBasedMithraMasterServerFactory.port = port;
    }

    public static MasterCacheService getInstance(Properties properties)
    {
        String url = "http://localhost:"+port+"/PspServlet";
        return createFactory(properties, url);
    }
}
