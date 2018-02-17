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

package com.gs.fw.common.mithra.cache.offheap;


import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

public class MasterRetrieveInitialSyncSizeResult implements Externalizable
{
    private transient Map<String, Long> nameToSizeMap;

    public MasterRetrieveInitialSyncSizeResult()
    {
        // for externalizable
    }

    public Map<String, Long> getNameToSizeMap()
    {
        return nameToSizeMap;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        nameToSizeMap = UnifiedMap.newMap(200);
        String businessClassName = (String) in.readObject();
        while(businessClassName != null)
        {
            long size = in.readLong();
            nameToSizeMap.put(businessClassName, size);
            businessClassName = (String) in.readObject();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        Set<MithraRuntimeCacheController> runtimeCacheControllerSet = MithraManagerProvider.getMithraManager().getConfigManager().getRuntimeCacheControllerSet();
        for(MithraRuntimeCacheController mrcc: runtimeCacheControllerSet)
        {
            String businessClassName = mrcc.getMithraObjectPortal().getBusinessClassName();
            long offHeapUsedDataSize = mrcc.getOffHeapUsedDataSize();
            if (offHeapUsedDataSize > 0)
            {
                out.writeObject(businessClassName);
                out.writeLong(offHeapUsedDataSize);
            }
        }
        out.writeObject(null);
    }
}
