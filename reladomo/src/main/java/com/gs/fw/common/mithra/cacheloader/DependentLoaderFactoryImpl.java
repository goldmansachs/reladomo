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


import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.EqualityMapper;
import com.gs.fw.common.mithra.finder.FilteredMapper;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.MultiEqualityMapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.gs.fw.common.mithra.util.OperationBasedFilter;
import com.gs.fw.common.mithra.util.Pair;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DependentLoaderFactoryImpl implements DependentLoaderFactory
{
    private String relationship;
    private String ownerClassName;

    private Extractor[] relationshipExtractors;
    private Attribute[] relationshipAttributes;

    private Map<Attribute, Attribute> relationshipAttributeMap = UnifiedMap.newMap();
    private Operation filteredMapperOperation;
    private Operation ownerObjectFilter;

    private AbstractLoaderFactory helperFactory = new FullyMilestonedTopLevelLoaderFactory();
    private List<LoaderFactory> ownerLoaderFactories = FastList.newList();
    private List sourceAttributes = FastList.newList();

    public void createLoadingTasksAndDependentLoaders(CacheLoaderContext context)
    {
        if (context.getRefreshInterval() != null)
        {
            this.createDependentLoadTasksForRefresh(context);

            MithraRuntimeCacheController ownerClassController = CacheLoaderConfig.createRuntimeCacheController(this.ownerClassName);
            Cache ownerCache = ownerClassController.getFinderInstance().getMithraObjectPortal().getCache();
            BooleanFilter filter = CacheIndexBasedFilter.create(ownerCache, relationshipAttributeMap, this.ownerObjectFilter, this.relationship);
            if (this.filteredMapperOperation != null)
            {
                filter = new OperationBasedFilter(this.filteredMapperOperation).and(filter);
            }
            for (Object sourceAttribute : this.getSourceAttributes())
            {
                this.helperFactory.createLoadTasksPerBusinessDate(context, sourceAttribute, filter);
            }
        }
        else
        {
            this.createDependentLoadTasksForInitialLoad(context);
        }
    }

    private void createDependentLoadTasksForInitialLoad(CacheLoaderContext context)
    {
        List<DateCluster> dateClusters = this.helperFactory.createDateClusters(context, context.getBusinessDates());

        for (Object sourceAttribute : this.getSourceAttributes())
        {
            for (DateCluster dateCluster : dateClusters)
            {
                this.initDependentTasks(context, sourceAttribute, dateCluster);
            }
        }
    }

    private void createDependentLoadTasksForRefresh(CacheLoaderContext context)
    {
        for (Object sourceAttribute : this.getSourceAttributes())
        {
            for (Timestamp businessDate : context.getBusinessDates())
            {
                this.initDependentTasks(context, sourceAttribute, new DateCluster(businessDate));
            }
        }
    }

    private void initDependentTasks(CacheLoaderContext context, Object sourceAttribute, DateCluster dateCluster)
    {
        Operation operation = this.buildLoadOperation(sourceAttribute, dateCluster, context);

        List<DependentLoadingTaskSpawnerKey> singleDatedTaskKeys = this.createSingleDatedKeys(sourceAttribute, dateCluster);

        DependentLoadingTaskSpawner taskSpawner = new DependentLoadingTaskSpawner(
                this.relationshipAttributes,
                this.helperFactory.getDatabaseIdentifier(sourceAttribute),
                sourceAttribute,
                operation,
                this.getClassController(),
                context,
                this.helperFactory.getAdditionalOperationBuilders(this.getClassToLoad(), context), dateCluster);

        for (DependentLoadingTaskSpawnerKey taskKey : singleDatedTaskKeys)
        {
            if (context.getDependentLoadingTaskSpawnerMap().get(taskKey) == null)
            {
                context.getDependentLoadingTaskSpawnerMap().put(taskKey, taskSpawner);
            }
        }
    }

    public MithraRuntimeCacheController getClassController()
    {
        return this.helperFactory.getClassController();
    }

    private List<DependentLoadingTaskSpawnerKey> createSingleDatedKeys(Object sourceAttribute, DateCluster dateCluster)
    {
        List<DependentLoadingTaskSpawnerKey> dependentKeys = FastList.newList();
        for (Timestamp businessDate : dateCluster.getBusinessDates())
        {
            dependentKeys.add(new DependentLoadingTaskSpawnerKey(
                    sourceAttribute,
                    this.helperFactory.shiftBusinessDate(businessDate),
                    this.helperFactory,
                    this.relationshipAttributes,
                    this.filteredMapperOperation));
        }
        return dependentKeys;
    }

    private Operation buildLoadOperation(Object sourceAttribute, DateCluster dateCluster, CacheLoaderContext cacheLoaderContext)
    {
        Operation operation = this.helperFactory.buildLoadOperation(dateCluster, cacheLoaderContext);
        return this.combineLoadOperationWithFilteredMapper(sourceAttribute, operation);
    }

    private Operation combineLoadOperationWithFilteredMapper(Object sourceAttribute, Operation operation)
    {
        operation = this.helperFactory.addSourceAttributeOperation(operation, sourceAttribute);
        if (this.filteredMapperOperation != null)
        {
            operation = operation == null ? this.filteredMapperOperation : operation.and(this.filteredMapperOperation);
        }
        return operation;
    }

    public void findOwnerLoaderFactory(List<LoaderFactory> ownerLoaderFactories)
    {
        for (LoaderFactory each : ownerLoaderFactories)
        {
            if (each.getClassToLoad().equals(this.ownerClassName))
            {
                this.ownerLoaderFactories.add(each);
            }
        }

        if (this.ownerLoaderFactories.isEmpty())
        {
            throw new RuntimeException(this.toString() + " cannot find corresponding owner factory (driver class: " + this.ownerClassName + ')');
        }
    }

    public void attachDependents(CacheLoaderContext context)
    {
        this.initializeOwnerObjectSetForQualifiedLoader(context);

        for (LoadingTaskRunner each : context.getDataSetLoaders())
        {
            LoadingTask loadingTask = each.getLoadingTask();
            if (loadingTask.getDateCluster() == null)
            {   // date cluster is defined in the post load filter. should propagate the dependency across all dates
                for (Timestamp businessDate : context.getBusinessDates())
                {
                    DateCluster shiftedDateCluster = this.helperFactory.shiftDateCluster(new DateCluster(businessDate));
                    this.attachDependentToLoadingTask(context, shiftedDateCluster, loadingTask);
                }
            }
            else
            {
                DateCluster shiftedDateCluster = this.helperFactory.shiftDateCluster(loadingTask.getDateCluster());
                this.attachDependentToLoadingTask(context, shiftedDateCluster, loadingTask);
            }
        }

        // attach subdependents to dependents
        for (DependentLoadingTaskSpawner each : context.getDependentLoadingTaskSpawnerMap().values())
        {
            DateCluster shiftedDateCluster = this.helperFactory.shiftDateCluster(each.getDateCluster());
            this.attachDependentToLoadingTask(context, shiftedDateCluster, each);
        }
    }

    public String getOwnerClassName()
    {
        return this.ownerClassName;
    }

    private void attachDependentToLoadingTask(CacheLoaderContext context, DateCluster dateCluster, LoadingTask parentTask)
    {
        if (!context.getQualifiedLoadContext().qualifies(this.getClassToLoad(), true))
        {
            return;
        }
        Object parentSourceAttribute = parentTask.getSourceAttribute();
        if (parentTask.getClassName().equals(this.ownerClassName))
        {
            if (CacheLoaderConfig.isSourceAttribute(parentSourceAttribute))
            {
                if (this.getSourceAttributes().contains(parentSourceAttribute)) // NYK->NYK
                {
                    this.attachDependentTask(context, dateCluster, parentTask, parentSourceAttribute);
                }
                else if (!CacheLoaderConfig.isSourceAttribute(this.getSourceAttributes().get(0))) // NYK->global
                {
                    this.attachDependentTask(context, dateCluster, parentTask, this.getSourceAttributes().get(0));
                }
            }
            else
            {
                for (Object dependentAttribute : this.getSourceAttributes())   // global->global or global->NYK|TKO
                {
                    this.attachDependentTask(context, dateCluster, parentTask, dependentAttribute);
                }
            }
        }
    }

    private void attachDependentTask(CacheLoaderContext context, DateCluster dateCluster, LoadingTask parentTask, Object sourceAttribute)
    {
        Set<DependentLoadingTaskSpawner> dependentTaskSpawners = this.findDependentTaskSpawners(context, dateCluster, sourceAttribute);
        for (DependentLoadingTaskSpawner spawner : dependentTaskSpawners)
        {
            DependentKeyIndex dependentKeyIndex = spawner.createDependentKeyIndex(context.getEngine(), this.relationshipExtractors, this.ownerObjectFilter);
            parentTask.addDependentThread(dependentKeyIndex);
        }
    }

    private Set<DependentLoadingTaskSpawner> findDependentTaskSpawners(CacheLoaderContext context, DateCluster dateCluster, Object sourceAttribute)
    {
        Set<DependentLoadingTaskSpawner> dependentTaskSpawners = UnifiedSet.newSet();
        for (Timestamp businessDate : dateCluster.getBusinessDates())
        {
            DependentLoadingTaskSpawnerKey keyFromParentTask = new DependentLoadingTaskSpawnerKey(sourceAttribute, businessDate, this.helperFactory, this.relationshipAttributes, this.filteredMapperOperation);
            dependentTaskSpawners.add(context.getDependentLoadingTaskSpawnerMap().get(keyFromParentTask));
        }
        return dependentTaskSpawners;
    }

    private void initializeOwnerObjectSetForQualifiedLoader(CacheLoaderContext context)
    {
        if (!context.getQualifiedLoadContext().qualifiesDependentsFor(this.ownerClassName, getClassToLoad()))
        {
            return;
        }
        MithraRuntimeCacheController ownerClassLoader = CacheLoaderConfig.createRuntimeCacheController(this.ownerClassName);
        List<Timestamp> shiftedBusinessDates = FastList.newList();
        for (Timestamp each : context.getBusinessDates())
        {
            DateCluster shiftedDateRange = this.helperFactory.shiftDateCluster(new DateCluster(each));
            for (Object sourceAttribute : this.getSourceAttributes())
            {
                for (LoaderFactory ownerLoaderFactory : this.ownerLoaderFactories)
                {
                    Set<DependentLoadingTaskSpawner> taskSpawners = findDependentTaskSpawners(context, shiftedDateRange, sourceAttribute);

                    Operation operation = ownerLoaderFactory.createFindAllOperation(shiftedBusinessDates, sourceAttribute);
                    List cachedKeyHolders = context.getQualifiedLoadContext().getKeyHoldersForQualifiedDependents(operation, ownerClassLoader.getFinderInstance());

                    for (DependentLoadingTaskSpawner taskSpawner : taskSpawners)
                    {
                        DependentKeyIndex dependentKeyIndex = taskSpawner.createDependentKeyIndex(context.getEngine(), this.relationshipExtractors, this.ownerObjectFilter);
                        context.getEngine().changeStripedCount(cachedKeyHolders.size());
                        dependentKeyIndex.putStripeOnQueue(cachedKeyHolders);
                        dependentKeyIndex.createTaskRunner(LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);
                    }
                }
            }
        }
    }

    public void setRelationship(String relationship)
    {
        this.relationship = relationship;

        int i = relationship.lastIndexOf('.');
        this.ownerClassName = relationship.substring(0, i);
        String relationshipName = relationship.substring(i + 1);

        MithraRuntimeCacheController ownerClassController = CacheLoaderConfig.createRuntimeCacheController(this.ownerClassName);
        AbstractRelatedFinder mapperFinder = (AbstractRelatedFinder) ownerClassController.getFinderInstanceFromFinderClass().getRelationshipFinderByName(relationshipName);
        Mapper mapper = mapperFinder.zGetMapper();

        this.setClassToLoadFromMapper(mapperFinder);

        if (mapper instanceof FilteredMapper)
        {
            FilteredMapper filteredMapper = (FilteredMapper) mapper;
            mapper = filteredMapper.getUnderlyingMapper();
            this.filteredMapperOperation = filteredMapper.getRightFilters();
            this.validateFilter(this.filteredMapperOperation);
            this.ownerObjectFilter = filteredMapper.getLeftFilters();
            this.validateFilter(this.ownerObjectFilter);
        }

        this.addDanglingAsOfAttributes(ownerClassController.getFinderInstanceFromFinderClass());
        List<Pair<Extractor, Attribute>> mappingPairs = FastList.newList();
        if (mapper instanceof MultiEqualityMapper)
        {
            MultiEqualityMapper multiEqualityMapper = (MultiEqualityMapper) mapper;
            InternalList eqalityMappers = multiEqualityMapper.getEqualityMappers();

            for (i = 0; i < eqalityMappers.size(); i++)
            {
                this.addRelationshipAttribute(mappingPairs, (Mapper) eqalityMappers.get(i));
            }
        }
        else if (mapper instanceof EqualityMapper)
        {
            this.addRelationshipAttribute(mappingPairs, mapper);
        }
        else
        {
            throw new RuntimeException("relationship for " + mapper.getClass().getName() + " not implemented");
        }

        this.relationshipAttributes = new Attribute[mappingPairs.size()];
        this.relationshipExtractors = new Extractor[mappingPairs.size()];
        for (i = 0; i < mappingPairs.size(); i++)
        {
            Pair<Extractor, Attribute> each = mappingPairs.get(i);
            this.relationshipAttributes[i] = each.getTwo();
            this.relationshipExtractors[i] = each.getOne();
        }
    }

    private void validateFilter(Operation op)
    {
        if (op == null)
        {
            return;
        }
        final UnifiedSet<Attribute> attributes = UnifiedSet.newSet();
        op.zAddAllLeftAttributes(attributes);
        for (Attribute each : attributes)
        {
            if (each.isAsOfAttribute())
            {
                throw new RuntimeException("CacheLoader cannot handle AsOfOperation filter (" + op + ") on the dependent relationship " + this.relationship +
                        " all Milestoning logic has to be implemented in the cache loader factory classes (i.e. PmeYtdBalanceTopLevelLoaderFactory). " +
                        "Consider creating cacheLoader-specific relationship without AsOfAttribute filtering and with specialized factory in the cacheLoader.xml.");
            }
            if (each.isSourceAttribute())
            {
                throw new RuntimeException("CacheLoader cannot handle Source attribute filter (" + op + ") on the dependent relationship " + this.relationship +
                        " the source attributes have to be explicitly set in the cacheLoader.xml sourceAttributes properties. " +
                        "Consider creating cacheLoader-specific relationship without source attribute filtering and with source attribute defined in the cacheLoader.xml.");
            }
        }
    }

    private void addDanglingAsOfAttributes(RelatedFinder ownerFinder)
    {
        final AsOfAttribute[] asOfAttributes = ownerFinder.getAsOfAttributes();
        if (asOfAttributes == null)
        {
            return;
        }

        for (AsOfAttribute each : asOfAttributes)
        {
            this.relationshipAttributeMap.put(each, null);
        }
    }

    private void setClassToLoadFromMapper(AbstractRelatedFinder relatedFinder)
    {
        this.helperFactory.setClassToLoad(relatedFinder.getMithraObjectPortal().getClassMetaData().getBusinessOrInterfaceClassName());
    }

    private void addRelationshipAttribute(List<Pair<Extractor, Attribute>> list, Mapper mapper)
    {
        this.relationshipAttributeMap.put(mapper.getAnyLeftAttribute(), mapper.getAnyRightAttribute());

        if (!mapper.getAnyRightAttribute().isAsOfAttribute() && !mapper.getAnyRightAttribute().isSourceAttribute())
        {
            list.add(new Pair(mapper.getAnyLeftAttribute(), mapper.getAnyRightAttribute()));
        }
    }

    public String getClassToLoad()
    {
        return this.helperFactory.getClassToLoad();
    }

    public BooleanFilter createCacheFilterOfDatesToDrop(Timestamp loadedDate)
    {
        return this.helperFactory.createCacheFilterOfDatesToDrop(loadedDate);
    }

    public void setParams(List<ConfigParameter> params)
    {
        this.helperFactory.setParams(params);
    }

    public void setHelperFactory(AbstractLoaderFactory helperFactory)
    {
        this.helperFactory = helperFactory;
    }

    public boolean pullOwnerSourceAttributes()
    {
        if (this.helperFactory.getSourceAttributes() != null) // hardcoded in the cacheLoader.xml
        {
            boolean firstTime = this.sourceAttributes.isEmpty();
            this.sourceAttributes = this.helperFactory.getSourceAttributes();
            return firstTime;
        }

        if (!this.helperFactory.hasSourceAttributeOnDomainObject())
        {
            boolean firstTime = this.sourceAttributes.isEmpty();
            this.sourceAttributes = CacheLoaderConfig.getNoSourceAttributeList();
            return firstTime;
        }

        Set set = UnifiedSet.newSet(this.sourceAttributes);
        for (LoaderFactory each : this.ownerLoaderFactories)
        {
            set.addAll(each.getSourceAttributes());
        }

        if (set.size() == this.sourceAttributes.size())
        {
            return false;
        }

        this.sourceAttributes = FastList.newList(set);
        return true;
    }

    public List getSourceAttributes()
    {
        return this.sourceAttributes;
    }

    public Operation createFindAllOperation(List<Timestamp> businessDates, Object sourceAttribute)
    {
        return this.helperFactory.createFindAllOperation(businessDates, sourceAttribute);
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getClass().getSimpleName());
        if (this.ownerObjectFilter != null)
        {
            builder.append('/').append(this.ownerObjectFilter);
        }
        builder.append(" for ").append(this.relationship);
        return builder.toString();
    }

}
