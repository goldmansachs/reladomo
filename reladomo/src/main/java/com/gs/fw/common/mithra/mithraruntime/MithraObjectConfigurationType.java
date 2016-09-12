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

package com.gs.fw.common.mithra.mithraruntime;
import java.util.*;
public class MithraObjectConfigurationType
 extends MithraObjectConfigurationTypeAbstract

{
    public int getFinalRelationshipCacheSize(MithraRuntimeType runtime, int defaultSize)
    {
        if (this.isRelationshipCacheSizeSet())
        {
            return this.getRelationshipCacheSize();
        }
        if (runtime.isDefaultRelationshipCacheSizeSet())
        {
            return runtime.getDefaultRelationshipCacheSize();
        }
        return defaultSize;
    }

    public long getFinalCacheTimeToLive(MithraRuntimeType runtime)
    {
        if (this.isCacheTimeToLiveSet())
        {
            return this.getCacheTimeToLive();
        }
        if (runtime.isDefaultCacheTimeToLiveSet())
        {
            return runtime.getDefaultCacheTimeToLive();
        }
        return 0;
    }

    public long getFinalRelationshipCacheTimeToLive(MithraRuntimeType runtime)
    {
        if (this.isRelationshipCacheTimeToLiveSet())
        {
            return this.getRelationshipCacheTimeToLive();
        }
        if (runtime.isDefaultRelationshipCacheTimeToLiveSet())
        {
            return runtime.getDefaultRelationshipCacheTimeToLive();
        }
        return 0;
    }

    public int getFinalMinQueriesToKeep(MithraRuntimeType runtime, int defaultToKeep)
    {
        if (this.isMinQueriesToKeepSet())
        {
            return this.getMinQueriesToKeep();
        }
        if (runtime.isDefaultMinQueriesToKeepSet())
        {
            return runtime.getDefaultMinQueriesToKeep();
        }
        return defaultToKeep;
    }
}
