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

package com.gs.fw.common.mithra.cache.offheap;


import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.remote.RemoteMithraObjectConfig;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.gs.fw.common.mithra.util.StringPool;

import java.util.Arrays;
import java.util.Set;

public class MasterCacheServiceImpl implements MasterCacheService
{
    @Override
    public MasterRetrieveStringResult retrieveStrings(int startIndex)
    {
        return StringPool.getInstance().retrieveOffHeapStrings(startIndex);
    }

    @Override
    public RemoteMithraObjectConfig[] getObjectConfigurations()
    {
        Set<RemoteMithraObjectConfig> set = MithraManagerProvider.getMithraManager().getCacheReplicableConfigSet();
        RemoteMithraObjectConfig[] result = new RemoteMithraObjectConfig[set.size()];
        set.toArray(result);
        Arrays.sort(result);
        return result;
    }

    @Override
    public MasterSyncResult syncWithMasterCache(String businessClassName, long maxReplicatedPageVersion)
    {
        try
        {
            Class finderClass = Class.forName(businessClassName+"Finder");
            return new MithraRuntimeCacheController(finderClass).getMithraObjectPortal().getCache().sendSyncResult(maxReplicatedPageVersion);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not find class "+businessClassName, e);
        }
    }

    @Override
    public MasterRetrieveInitialSyncSizeResult retrieveInitialSyncSize()
    {
        return new MasterRetrieveInitialSyncSizeResult();
    }
}
