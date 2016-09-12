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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.cache.offheap.MasterCacheUplink;
import com.gs.fw.common.mithra.util.MithraConfigurationManager;

import java.util.List;

/*
 *******************************************************************************
 * File:        : $Source : $
 *
 *
 *
 *******************************************************************************
 */
public class MithraRuntimeConfig
{
    private List<MithraConfigurationManager.Config> configs;
    private List<MithraDatabaseObject> databaseObjects;
    private List<MithraObjectPortal> objectPortals;
    private Object connectionManager;
    private MasterCacheUplink masterCacheUplink;
    private int loaderThreads;

    public MasterCacheUplink getMasterCacheUplink()
    {
        return masterCacheUplink;
    }

    public void setMasterCacheUplink(MasterCacheUplink masterCacheUplink)
    {
        this.masterCacheUplink = masterCacheUplink;
    }

    public MithraRuntimeConfig(int loaderThreads)
    {
        this.loaderThreads = loaderThreads;
    }

    public int getLoaderThreads()
    {
        return loaderThreads;
    }

    public List<MithraDatabaseObject> getDatabaseObjects()
    {
        return databaseObjects;
    }

    public List<MithraObjectPortal> getObjectPortals()
    {
        return objectPortals;
    }

    public void setObjectPortals(List<MithraObjectPortal> objectPortals)
    {
        this.objectPortals = objectPortals;
    }

    public Object getConnectionManager()
    {
        return connectionManager;
    }

    public void setDatabaseObjects(List<MithraDatabaseObject> databaseObjects)
    {
        this.databaseObjects = databaseObjects;
    }

    public void setConnectionManager(Object connectionManager)
    {
        this.connectionManager = connectionManager;
    }

    public List<MithraConfigurationManager.Config> getConfigs()
    {
        return configs;
    }

    public void setConfigs(List<MithraConfigurationManager.Config> configs)
    {
        this.configs = configs;
    }
}
