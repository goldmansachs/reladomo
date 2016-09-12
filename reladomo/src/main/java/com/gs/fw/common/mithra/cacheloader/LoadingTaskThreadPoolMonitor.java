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

package com.gs.fw.common.mithra.cacheloader;

import java.util.List;

import com.gs.collections.impl.list.mutable.FastList;

public class LoadingTaskThreadPoolMonitor
{
    private final String poolName;
    private long waitedForSyslog;
    private long busyIOThreads;
    private long ioTaskQueue;
    private long cpuTaskQueue;
    private long dependentKeyIndices;
    private long indexSize;
    private long maxSize;
    private long producedKeys;


    public LoadingTaskThreadPoolMonitor(String poolName)
    {
        this.poolName = poolName;
    }

    public void setWaitedForSyslog(long waitedForSyslog)
    {
        this.waitedForSyslog = waitedForSyslog;
    }

    public void setBusyIOThreads(long busyIOThreads)
    {
        this.busyIOThreads = busyIOThreads;
    }

    public void setIoTaskQueue(long ioTaskQueue)
    {
        this.ioTaskQueue = ioTaskQueue;
    }

    public void setCpuTaskQueue(long cpuTaskQueue)
    {
        this.cpuTaskQueue = cpuTaskQueue;
    }

    public void setDependentKeyIndices(long dependentKeyIndices)
    {
        this.dependentKeyIndices = dependentKeyIndices;
    }

    public void setIndexSize(long indexSize)
    {
        this.indexSize = indexSize;
    }

    public void setMaxSize(long maxSize)
    {
        this.maxSize = maxSize;
    }

    public void setProducedKeys(long producedKeys)
    {
        this.producedKeys = producedKeys;
    }

    public String getPoolName()
    {
        return this.poolName;
    }

    public long getWaitedForSyslog()
    {
        return this.waitedForSyslog;
    }

    public long getBusyIOThreads()
    {
        return this.busyIOThreads;
    }

    public long getIoTaskQueue()
    {
        return this.ioTaskQueue;
    }

    public long getCpuTaskQueue()
    {
        return this.cpuTaskQueue;
    }

    public long getDependentKeyIndices()
    {
        return this.dependentKeyIndices;
    }

    public long getIndexSize()
    {
        return this.indexSize;
    }

    public long getMaxSize()
    {
        return this.maxSize;
    }

    public long getProducedKeys()
    {
        return this.producedKeys;
    }
}
