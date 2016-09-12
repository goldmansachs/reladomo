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


import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.factory.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public final class LoadingTaskRunner implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(LoadingTaskRunner.class);
    private final LoadingTask loadingTask;
    private MutableList<LoadingTaskRunner> prerequisites = Lists.fixedSize.of();
    private MutableList<String> prerequisiteClassNames = Lists.fixedSize.of();

    private final String threadPoolName;
    private final CacheLoaderEngine cacheLoaderEngine;
    private final LoadingTaskMonitor loadingTaskMonitor;

    private volatile State state;

    public LoadingTaskRunner(CacheLoaderEngine cacheLoaderEngine, String threadPoolName, LoadingTask loadingTask, State state)
    {
        this.loadingTask = loadingTask;
        this.threadPoolName = threadPoolName;
        this.cacheLoaderEngine = cacheLoaderEngine;

        this.loadingTaskMonitor = new LoadingTaskMonitor(this.loadingTask, this.threadPoolName);

        this.cacheLoaderEngine.changeTaskCount(1);
        this.state = state;
    }

    public void addPrerequisiteClassNames(Collection<String> additionalNames)
    {
        this.prerequisiteClassNames = this.prerequisiteClassNames.withAll(additionalNames);
    }

    public void addPrerequisite(LoadingTaskRunner prerequisite)
    {
        this.prerequisites = this.prerequisites.with(prerequisite);
    }

    public void attachPrerequisites(CacheLoaderContext context)
    {
        for (LoadingTaskRunner each : context.getDataSetLoaders())
        {
            LoadingTask prerequisitesTask = each.getLoadingTask();
            Object prerequisitesTaskSourceAttribute = prerequisitesTask.getSourceAttribute();
            boolean sourceAttributeMatch =
                    !CacheLoaderConfig.isSourceAttribute(this.getLoadingTask().getSourceAttribute())
                            || !CacheLoaderConfig.isSourceAttribute(prerequisitesTaskSourceAttribute)
                            || prerequisitesTaskSourceAttribute.equals(this.getLoadingTask().getSourceAttribute());
            if (this.prerequisiteClassNames.contains(prerequisitesTask.getClassName()) && sourceAttributeMatch)
            {
                this.addPrerequisite(each);
            }
        }
    }

    public void run()
    {
        this.state = State.PROCESSING;
        int loadedSize = -1;
        this.loadingTaskMonitor.startMonitoring(this.state);
        try
        {
            loadedSize = this.loadingTask.load();
            this.state = State.COMPLETED;
        }
        catch (Throwable e)
        {
            this.state = State.FAILED;
            throw new RuntimeException("" + this.loadingTask + " failed.", e);
        }
        finally
        {
            this.cacheLoaderEngine.changeTaskCount(-1);
            this.cacheLoaderEngine.signalTaskCompleted();

            this.loadingTaskMonitor.finishMonitoring(this.loadingTask, loadedSize, this.state);
            if (this.state == State.FAILED)
            {
                logger.error("SQL of failed task: " + this.loadingTaskMonitor.getSql());
            }
            if (logger.isDebugEnabled())
            {
                logger.debug("Completed " + this.loadingTaskMonitor);
            }
        }
    }

    public boolean isReadyToLoad()
    {
        for (LoadingTaskRunner each : prerequisites)
        {
            if (!each.isDone())
            {
                return false;
            }
        }

        return true;
    }

    public boolean isDone()
    {
        return this.getState() == State.COMPLETED;
    }

    public State getState()
    {
        return this.state;
    }

    public String getThreadPoolName()
    {
        return threadPoolName;
    }

    public LoadingTaskMonitor getLoadingTaskMonitor()
    {
        return loadingTaskMonitor;
    }

    protected synchronized void advance(LoadingTaskThreadPoolHolder executor)
    {
        if (state == State.WAITING_FOR_PREREQUISITES && this.isReadyToLoad())
        {
            this.state = State.QUEUED;
            executor.getThreadPool().addToIOQueue(this);
        }
    }

    public LoadingTask getLoadingTask()
    {
        return loadingTask;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(this.state)
                .append(this.loadingTask);
        return builder.toString();
    }

    public enum State
    {
        WAITING_FOR_PREREQUISITES,
        QUEUED,
        PROCESSING,
        COMPLETED,
        FAILED
    }
}
