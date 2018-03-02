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

package com.gs.fw.common.mithra.cacheloader;


import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.TrueFilter;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public final class CacheLoaderManagerImpl implements CacheLoaderManager
{
    private List<TopLevelLoaderFactory> topLevelLoaderFactories = FastList.newList();
    private List<DependentLoaderFactory> dependentSetLoaderFactories = FastList.newList();
    private ConfigValues configValues = new ConfigValues();

    public void runInitialLoad(List<Timestamp> businessDates, Timestamp initialLoadEndTime, CacheLoaderMonitor monitor)
    {
        CacheLoaderContext context = new CacheLoaderContext(this, businessDates);
        context.setInitialLoadEndTime(initialLoadEndTime);
        context.execute(monitor);
        this.stampLatestRefreshTime(initialLoadEndTime);
    }

    public void runRefresh(List<Timestamp> businessDates, RefreshInterval refreshInterval, CacheLoaderMonitor monitor)
    {
        CacheLoaderContext context = new CacheLoaderContext(this, businessDates);
        context.setRefreshInterval(refreshInterval);
        context.execute(monitor);
        this.stampLatestRefreshTime(refreshInterval.getEnd());
    }

    public void runQualifiedLoad(List<Timestamp> businessDates,
                                 Map<String, AdditionalOperationBuilder> classesToLoadWithAdditionalOperations,
                                 boolean loadDependent,
                                 CacheLoaderMonitor monitor)
    {
        CacheLoaderContext context = new CacheLoaderContext(this, businessDates);
        context.setQualifiedLoadContext(new QualifiedByOperationLoadContext(classesToLoadWithAdditionalOperations, loadDependent));
        context.execute(monitor);
    }

    public void loadConfiguration(InputStream configFile)
    {
        CacheLoaderConfig config = new CacheLoaderConfig();
        config.parseConfiguration(configFile);
        this.configValues = config.readConfigValues();
        this.topLevelLoaderFactories = config.readTopLevelLoaderFactories();
        this.dependentSetLoaderFactories = config.readDependentLoaderFactories();

        List<LoaderFactory> ownerLoaderFactories = FastList.<LoaderFactory>newList(this.topLevelLoaderFactories);
        ownerLoaderFactories.addAll(this.dependentSetLoaderFactories);

        for (DependentLoaderFactory each : this.dependentSetLoaderFactories)
        {
            each.findOwnerLoaderFactory(ownerLoaderFactories);
        }

        this.resolveSourceAttributesForDependentLoaders();
    }

    private void stampLatestRefreshTime(Timestamp latestRefreshTime)
    {
        long time = latestRefreshTime.getTime();
        for (LoaderFactory each : this.topLevelLoaderFactories)
        {
            each.getClassController().getFinderInstance().getMithraObjectPortal().setLatestRefreshTime(time);
        }
        for (LoaderFactory each : this.dependentSetLoaderFactories)
        {
            each.getClassController().getFinderInstance().getMithraObjectPortal().setLatestRefreshTime(time);
        }
    }

    private void resolveSourceAttributesForDependentLoaders()
    {
        boolean changed = true;
        while (changed)
        {
            changed = false;
            for (DependentLoaderFactory each : this.dependentSetLoaderFactories)
            {
                if (each.pullOwnerSourceAttributes())
                {
                    changed = true;
                }
            }
        }

        for (DependentLoaderFactory each : this.dependentSetLoaderFactories)
        {
            if (each.getSourceAttributes() == null)
            {
                throw new RuntimeException("did not resolve source attributes on " + each);
            }
        }
    }

    public Map<String, BooleanFilter> createCacheFilterOfDatesToKeep(List<Timestamp> businessDates)
    {
        UnifiedMap<String, BooleanFilter> filterMap = UnifiedMap.newMap();
        if (businessDates == null || businessDates.isEmpty())
        {
            BooleanFilter trueFilter = TrueFilter.instance();
            for (TopLevelLoaderFactory factory : this.zGetTopLevelLoaderFactories())
            {
                filterMap.put(factory.getClassToLoad(), trueFilter);
            }
            for (DependentLoaderFactory factory : this.zGetDependentSetLoaderFactories())
            {
                filterMap.put(factory.getClassToLoad(), trueFilter);
            }
        }
        else
        {
            for (Timestamp businessDate : businessDates)
            {
                for (TopLevelLoaderFactory factory : this.zGetTopLevelLoaderFactories())
                {
                    this.addBusinessDateFilterToClassMap(businessDate, factory, filterMap);
                }

                for (DependentLoaderFactory factory : this.zGetDependentSetLoaderFactories())
                {
                    this.addBusinessDateFilterToClassMap(businessDate, factory, filterMap);
                }
            }
        }

        return filterMap;
    }

    @Override
    public void loadDependentObjectsFor(List ownerObjects, List<Timestamp> businessDates, Timestamp loadEndTime, CacheLoaderMonitor monitor)
    {
        if (ownerObjects != null && !ownerObjects.isEmpty())
        {
            CacheLoaderContext context = new CacheLoaderContext(this, businessDates);
            context.setQualifiedLoadContext(new QualifiedByOwnerObjectListLoadContext(ownerObjects));
            context.execute(monitor);
        }
    }

    private void addBusinessDateFilterToClassMap(Timestamp businessDate, LoaderFactory factory, Map<String, BooleanFilter> map)
    {
        BooleanFilter filter = factory.createCacheFilterOfDatesToDrop(businessDate);

        String className = factory.getClassToLoad();
        Filter otherFilter = map.get(className);
        filter = otherFilter == null ? filter : filter.and(otherFilter);
        map.put(className, filter);
    }

    public List<TopLevelLoaderFactory> zGetTopLevelLoaderFactories()
    {
        return this.topLevelLoaderFactories;
    }


    public List<DependentLoaderFactory> zGetDependentSetLoaderFactories()
    {
        return this.dependentSetLoaderFactories;
    }

    public void configure(CacheLoaderEngine engine)
    {
        engine.setConfigValues(this.configValues);
    }
}
