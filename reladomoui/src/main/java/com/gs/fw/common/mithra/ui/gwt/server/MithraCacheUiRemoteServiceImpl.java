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

package com.gs.fw.common.mithra.ui.gwt.server;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.gs.fw.common.base.psp.PspServiceRegistry;
import com.gs.fw.common.base.psp.client.FastServletProxyFactory;
import com.gs.fw.common.mithra.ui.gwt.client.*;
import com.gs.fw.common.mithra.ui.gwt.client.CachedClassData;
import com.gs.fw.common.mithra.ui.gwt.client.JvmMemory;

import java.net.MalformedURLException;
import java.util.*;



public class MithraCacheUiRemoteServiceImpl extends RemoteServiceServlet implements MithraCacheUiRemoteService
{      
    private final FastServletProxyFactory proxyFactory = new FastServletProxyFactory();
    private final MithraCacheRemoteService localhostRemoteService = new MithraCacheRemoteServiceImpl();
    
    public MithraCacheUiRemoteServiceImpl()
    {

    }

    private MithraCacheRemoteService getRemoteService(String mithraManagerLocation)
    {
        if(mithraManagerLocation == null || mithraManagerLocation.equals(""))
        {            
            return this.localhostRemoteService;
        }

        MithraCacheRemoteService cacheRemoteService = PspServiceRegistry.getInstance().getLocalService(mithraManagerLocation, MithraCacheRemoteService.class);

        if(cacheRemoteService == null)
        {
            try
            {
                cacheRemoteService = proxyFactory.create(MithraCacheRemoteService.class, mithraManagerLocation);
            }
            catch( MalformedURLException e )
            {
                throw new RuntimeException(e);
            }

            PspServiceRegistry.getInstance().addServiceForUrl(mithraManagerLocation, MithraCacheRemoteService.class, cacheRemoteService);
        }

        return cacheRemoteService;
    }
    
    public JvmMemory getJvmMemory(String mithraManagerLocation)
    {
        return this.getRemoteService(mithraManagerLocation).getJvmMemory();
    }

    public JvmMemory forceGc(String mithraManagerLocation)
    {
        return this.getRemoteService(mithraManagerLocation).forceGc();
    }

    public List<CachedClassData> clearAllQueryCaches(String mithraManagerLocation)
    {
        return this.getRemoteService(mithraManagerLocation).clearAllQueryCaches();
    }

    public CachedClassData clearCache(String mithraManagerLocation, String className)
    {
        return this.getRemoteService(mithraManagerLocation).clearCache(className);
    }

    public List<CachedClassData> getCachedClasses(String mithraManagerLocation)
    {
        return this.getRemoteService(mithraManagerLocation).getCachedClasses();
    }

    public CachedClassData reloadCache(String mithraManagerLocation, String className)
    {
        return this.getRemoteService(mithraManagerLocation).reloadCache(className);
    }

    public CachedClassData turnSqlOff(String mithraManagerLocation, String className)
    {
        return this.getRemoteService(mithraManagerLocation).turnSqlOff(className);        
    }

    public CachedClassData turnSqlOn(String mithraManagerLocation, String className)
    {
        return this.getRemoteService(mithraManagerLocation).turnSqlOn(className);        
    }

    public CachedClassData turnSqlMaxOn(String mithraManagerLocation, String className)
    {
        return this.getRemoteService(mithraManagerLocation).turnSqlMaxOn(className);        
    }
}
