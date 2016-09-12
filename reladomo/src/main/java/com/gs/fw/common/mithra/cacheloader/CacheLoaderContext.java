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
/*
 *******************************************************************************
  $Source$

  $Date: 2016-08-04 14:37:38 -0400 (Thu, 04 Aug 2016) $
  $Author: rezaem $
 *******************************************************************************
 */

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheLoaderContext
{
    private final List<Timestamp> businessDates;
    private CacheLoaderEngine engine;
    private List<LoadingTaskRunner> dataSetLoaders = FastList.newList();
    private Map<DependentLoadingTaskSpawnerKey, DependentLoadingTaskSpawner> dependentLoadingTaskSpawnerMap = UnifiedMap.newMap();

    private RefreshInterval refreshInterval;
    private Timestamp initialLoadEndTime;
    private QualifiedLoadContext qualifiedLoadContext = new NonQualifiedLoadContext();
    private CacheLoaderManagerImpl cacheLoaderManager;

    static private Logger logger = LoggerFactory.getLogger(CacheLoaderContext.class);

    public CacheLoaderContext(CacheLoaderManagerImpl cacheLoaderManager, List<Timestamp> businessDates)
    {
        this.cacheLoaderManager = cacheLoaderManager;
        this.businessDates = businessDates;
        this.engine = this.buildLoaderEngine();
    }

    private CacheLoaderEngine buildLoaderEngine()
    {
        CacheLoaderEngine engine = new CacheLoaderEngine();
        this.cacheLoaderManager.configure(engine);
        return engine;
    }

    protected void execute(CacheLoaderMonitor monitor)
    {
        monitor.setCacheLoaderContext(this);
        monitor.update();

        try
        {
            for (TopLevelLoaderFactory factory : cacheLoaderManager.zGetTopLevelLoaderFactories())
            {
                if (this.qualifiedLoadContext.qualifies(factory.getClassToLoad(), false))
                {
                    for (Object sourceAttribute : factory.getSourceAttributes())
                    {
                        factory.createLoadTasksPerBusinessDate(this, sourceAttribute, null);
                    }
                }
            }
            for (LoadingTaskRunner each : this.getDataSetLoaders())
            {
                each.attachPrerequisites(this);
            }

            this.createLoadingTasksAndDependentLoaders(cacheLoaderManager.zGetDependentSetLoaderFactories());

            for (LoadingTaskRunner loadingTaskRunner : this.dataSetLoaders)
            {
                this.engine.addTaskToLoadAndSetupThreadPool(loadingTaskRunner);
            }

            monitor.update();

            this.engine.waitUntilAllTasksCompleted();

            monitor.update();
        }
        catch (RuntimeException e)
        {
            monitor.setException(e);
            throw e;

        }
        finally
        {
            monitor.setCacheLoaderContext(null);
        }
        if (logger.isDebugEnabled())
        {
            logger.debug(monitor.asDetailedString());
        }
    }

    private void createLoadingTasksAndDependentLoaders(List<DependentLoaderFactory> dependentLoaderFactories)
    {
        for (DependentLoaderFactory factory : dependentLoaderFactories)
        {
            factory.createLoadingTasksAndDependentLoaders(this);
        }

        for (DependentLoaderFactory factory : cacheLoaderManager.zGetDependentSetLoaderFactories())
        {
            if (this.qualifiedLoadContext.qualifies(factory.getClassToLoad(), true))
            {
                factory.attachDependents(this);
            }
        }
    }

    public void setRefreshInterval(RefreshInterval refreshInterval)
    {
        this.refreshInterval = refreshInterval;
    }

    public void setQualifiedLoadContext(QualifiedLoadContext qualifiedLoadContext)
    {
        this.qualifiedLoadContext = qualifiedLoadContext;
    }

    public List<Timestamp> getBusinessDates()
    {
        return this.businessDates;
    }

    public CacheLoaderEngine getEngine()
    {
        return this.engine;
    }

    public List<LoadingTaskRunner> getDataSetLoaders()
    {
        return this.dataSetLoaders;
    }

    public RefreshInterval getRefreshInterval()
    {
        return this.refreshInterval;
    }

    public QualifiedLoadContext getQualifiedLoadContext()
    {
        return this.qualifiedLoadContext;
    }

    public Map<DependentLoadingTaskSpawnerKey, DependentLoadingTaskSpawner> getDependentLoadingTaskSpawnerMap()
    {
        return this.dependentLoadingTaskSpawnerMap;
    }

    public Timestamp getInitialLoadEndTime()
    {
        return initialLoadEndTime;
    }

    public void setInitialLoadEndTime(Timestamp initialLoadEndTime)
    {
        this.initialLoadEndTime = initialLoadEndTime;
    }

    public String toString()
    {
        StringBuilder buffer = new StringBuilder(this.getClass().getName());
        buffer.append("business date: ").append(this.businessDates);

        if (this.refreshInterval != null)
        {
            buffer.append(" refresh interval: ").append(this.refreshInterval);
        }

        return buffer.toString();
    }

    public void updateCacheLoaderMonitor(CacheLoaderMonitor monitor)
    {
        monitor.setThreadPoolMonitors(this.engine.getThreadPoolMonitors());
        if (this.engine.getTaskMonitors() == null)
        {
            for (LoadingTaskRunner each : this.dataSetLoaders)
            {
                monitor.addLoadingTaskMonitor(each.getLoadingTaskMonitor());
            }
        }
        else
        {
            for (LoadingTaskMonitor each : this.engine.getTaskMonitors())
            {
                monitor.addLoadingTaskMonitor(each);
            }
        }
        for (DependentLoadingTaskSpawner each : UnifiedSet.newSet(this.dependentLoadingTaskSpawnerMap.values()))
        {
            monitor.addDependentKeyIndexMonitors(each.getDependentKeyIndexMonitors());
        }
    }
}
