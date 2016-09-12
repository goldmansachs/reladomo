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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.util.PersisterId;

import java.io.Serializable;



public class RemoteMithraObjectConfig implements Serializable, Comparable
{

    private int relationshipCacheSize;
    private int minQueriesToKeep;
    private String className;
    private int serialVersion;
    private boolean useMultiUpdate;
    private String pureNotificationId;
    private int hierarchyDepth;
    private long cacheTimeToLive;
    private long relationshipCacheTimeToLive;
    private String factoryParameter;
    private PersisterId persisterId;

    public RemoteMithraObjectConfig(int relationshipCacheSize, int minQueriesToKeep, String className,
            int serialVersion, boolean useMultiUpdate, int hierarchyDepth, String pureNotificationId,
            long cacheTimeToLive, long relationshipCacheTimeToLive, String factoryParameter, PersisterId persisterId)
    {
        this.relationshipCacheSize = relationshipCacheSize;
        this.minQueriesToKeep = minQueriesToKeep;
        this.className = className;
        this.serialVersion = serialVersion;
        this.useMultiUpdate = useMultiUpdate;
        this.hierarchyDepth = hierarchyDepth;
        this.pureNotificationId = pureNotificationId;
        this.cacheTimeToLive = cacheTimeToLive;
        this.relationshipCacheTimeToLive = relationshipCacheTimeToLive;
        this.factoryParameter = factoryParameter;
        this.persisterId = persisterId;
    }

    public boolean isPure()
    {
        return pureNotificationId != null;
    }

    public String getPureNotificationId()
    {
        return pureNotificationId;
    }

    public int getRelationshipCacheSize()
    {
        return relationshipCacheSize;
    }

    public int getMinQueriesToKeep()
    {
        return minQueriesToKeep;
    }

    public String getClassName()
    {
        return className;
    }

    public int getSerialVersion()
    {
        return serialVersion;
    }

    public boolean useMultiUpdate()
    {
        return this.useMultiUpdate;
    }

    public int getHierarchyDepth()
    {
        return hierarchyDepth;
    }

    public long getCacheTimeToLive()
    {
        return cacheTimeToLive;
    }

    public long getRelationshipCacheTimeToLive()
    {
        return relationshipCacheTimeToLive;
    }

    public PersisterId getPersisterId()
    {
        return persisterId;
    }

    public int compareTo(Object o)
    {
        RemoteMithraObjectConfig other = (RemoteMithraObjectConfig) o;
        int result = this.getHierarchyDepth() - other.getHierarchyDepth();
        if (result == 0)
        {
            result = this.getClassName().compareTo(other.getClassName());
        }
        return result;
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof RemoteMithraObjectConfig))
        {
            return false;
        }

        final RemoteMithraObjectConfig remoteMithraObjectConfig = (RemoteMithraObjectConfig) o;
        return className.equals(remoteMithraObjectConfig.className);
    }

    public int hashCode()
    {
        return className.hashCode();
    }

    public String getFactoryParameter()
    {
        return factoryParameter;
    }
}
