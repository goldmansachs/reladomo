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

public class CacheLoaderMonitor
{
    private List<LoadingTaskMonitor> loadingTaskStates;
    private List<DependentKeyIndexMonitor> dependentKeyIndexMonitors;
    private CacheLoaderContext cacheLoaderContext;
    private Throwable exception;
    private List<LoadingTaskThreadPoolMonitor> threadPoolMonitors;
    private Exception lastLoadingException = null;

    public void setCacheLoaderContext(CacheLoaderContext cacheLoaderContext)
    {
        this.cacheLoaderContext = cacheLoaderContext;
    }

    public synchronized void update()
    {
        this.reset();
        if (this.cacheLoaderContext != null)
        {
            this.cacheLoaderContext.updateCacheLoaderMonitor(this);
        }
    }

    public void shutdown()
    {
        this.cacheLoaderContext.getEngine().shutdown();
    }

    private void reset()
    {
        this.loadingTaskStates = FastList.newList();
        this.dependentKeyIndexMonitors = FastList.newList();
        this.threadPoolMonitors = FastList.newList();
    }

    public void addLoadingTaskMonitor(LoadingTaskMonitor taskMonitor)
    {
        this.loadingTaskStates.add(taskMonitor);
    }

    public void addDependentKeyIndexMonitors(List<DependentKeyIndexMonitor> sublist)
    {
        this.dependentKeyIndexMonitors.addAll(sublist);
    }

    public List<LoadingTaskThreadPoolMonitor> getThreadPoolMonitors()
    {
        return this.threadPoolMonitors;
    }

    public void setThreadPoolMonitors(List<LoadingTaskThreadPoolMonitor> threadPoolMonitors)
    {
        this.threadPoolMonitors = threadPoolMonitors;
    }

    public List<LoadingTaskMonitor> getLoadingTaskStates()
    {
        return loadingTaskStates;
    }

    public List<DependentKeyIndexMonitor> getDependentKeyIndexMonitors()
    {
        return dependentKeyIndexMonitors;
    }

    public Throwable getException()
    {
        return exception;
    }

    public Exception getLastLoadingException()
    {
        return this.lastLoadingException;
    }

    public void setLastLoadingException(Exception lastLoadingException)
    {
        this.lastLoadingException = lastLoadingException;
    }

    public void setException(Throwable exception)
    {
        this.exception = exception;
    }

    public synchronized String asDetailedString()
    {
        StringBuilder builder = new StringBuilder("TASKS:\n");
        for (LoadingTaskMonitor each : this.loadingTaskStates)
        {
            builder.append(each.toString()).append("\n");
        }

        builder.append("DEPENDENT KEY INDEXES:\n");
        for (DependentKeyIndexMonitor each : dependentKeyIndexMonitors)
        {
            builder.append(each.toString()).append("\n");
        }
        return builder.toString();
    }
}
