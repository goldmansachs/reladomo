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
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.gs.fw.common.mithra.util.Pair;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class AbstractLoaderFactory implements TopLevelLoaderFactory
{
    private String classToLoad;
    private MithraRuntimeCacheController runtimeCacheController;
    private List<ConfigParameter> params;
    private List sourceAttributes;
    private Set<String> prerequisiteClassNames = UnifiedSet.newSet();

    public void setClassToLoad(String classToLoad)
    {
        this.classToLoad = classToLoad;
        this.runtimeCacheController = CacheLoaderConfig.createRuntimeCacheController(classToLoad);
    }

    public String getClassToLoad()
    {
        return this.classToLoad;
    }

    public void setSourceAttributes(List sourceAttributes)
    {
        this.sourceAttributes = sourceAttributes;
    }

    public List getSourceAttributes()
    {
        return this.sourceAttributes;
    }

    public void addPrerequisiteClassNames(Collection<String> prerequisiteClassNames)
    {
        this.prerequisiteClassNames.addAll(prerequisiteClassNames);
    }

    public MithraRuntimeCacheController getClassController()
    {
        return this.runtimeCacheController;
    }

    public Attribute getAttributeByName(String name)
    {
        return this.runtimeCacheController.getFinderInstance().getAttributeByName(name);
    }

    /**
     * combine business date (from this.buildLoadOperation), source attribute (from this.addSourceAttributeOperation)
     * and processing time (from this.buildRefreshOperations)
     */
    public void createLoadTasksPerBusinessDate(CacheLoaderContext context, Object sourceAttribute, BooleanFilter postLoadFilter)
    {
        if (context.getRefreshInterval() != null)
        {
            this.createLoadTasksForRefresh(context, sourceAttribute, postLoadFilter);
        }
        else
        {
            this.createLoadTasksForInitialLoad(context, sourceAttribute, postLoadFilter);
        }
    }

    private void createLoadTasksForInitialLoad(CacheLoaderContext context, Object sourceAttribute, BooleanFilter postLoadFilter)
    {
        List<DateCluster> dateClusters = this.createDateClusters(context, context.getBusinessDates());
        if (dateClusters.isEmpty())
        {
            this.createLoadTaskWithBusinessDatePostLoadFilterBuilder(context, sourceAttribute, postLoadFilter, null, FastList.<Pair<Timestamp, BooleanFilter>>newList());
        }
        else
        {
            for (DateCluster dateCluster : dateClusters)
            {
                List<Pair<Timestamp, BooleanFilter>> businessDateFilters = dateCluster.size() > 1 ?
                    this.createBusinessDateFilters(dateCluster.getBusinessDates()) : FastList.<Pair<Timestamp, BooleanFilter>>newList();

                this.createLoadTaskWithBusinessDatePostLoadFilterBuilder(context, sourceAttribute, postLoadFilter, dateCluster, businessDateFilters);
            }
        }
    }

    private void createLoadTaskWithBusinessDatePostLoadFilterBuilder(CacheLoaderContext context,
                                                                     Object sourceAttribute,
                                                                     BooleanFilter postLoadFilter,
                                                                     DateCluster dateCluster,
                                                                     List<Pair<Timestamp, BooleanFilter>> businessDateFilters)
    {
        PostLoadFilterBuilder postLoadFilterBuilder = new PostLoadFilterBuilder(postLoadFilter, null, null, businessDateFilters);
        Operation loadOperation = this.buildLoadOperation(dateCluster, context);
        loadOperation = this.addSourceAttributeOperation(loadOperation, sourceAttribute);

        this.createLoadTaskWithPostLoadFilterBuilder(context, dateCluster, sourceAttribute, postLoadFilterBuilder,
                                                     FastList.newListWith(new TaskOperationDefinition(loadOperation, true)));
    }

    protected List<DateCluster> createDateClusters(CacheLoaderContext context, List<Timestamp> businessDates)
    {
        if (this.createSingleDateClusters(context))
        {
            return DateClusterCreator.createSingleDateClusters(businessDates);
        }
        else
        {
            return DateClusterCreator.createMultiDateClusters(businessDates);
        }
    }

    public boolean areAllAdditionalOperationBuildersDateInvariant(String classToLoad, CacheLoaderContext context)
    {
        List<AdditionalOperationBuilder> additionalOperationBuilders = this.getAdditionalOperationBuilders(classToLoad, context);
        for (AdditionalOperationBuilder builder : additionalOperationBuilders)
        {
            if (!builder.isBusinessDateInvariant())
            {
                return false;
            }
        }
        return true;
    }

    private void createLoadTasksForRefresh(CacheLoaderContext context, Object sourceAttribute, BooleanFilter postLoadFilter)
    {
        if (context.getBusinessDates().size() == 1 || this.createSingleDateClusters(context))
        {
            List<DateCluster> dateClusters = DateClusterCreator.createSingleDateClusters(context.getBusinessDates());
            for (DateCluster dateCluster : dateClusters)
            {   // pass business date in SQL.
                Operation loadOperation = this.addSourceAttributeOperation(this.buildLoadOperation(dateCluster, context), sourceAttribute);
                List<TaskOperationDefinition> refreshOperationsPerTask = this.buildRefreshTaskDefinitions(context, loadOperation);

                PostLoadFilterBuilder postLoadFilterBuilder = new PostLoadFilterBuilder(postLoadFilter, null, null, FastList.<Pair<Timestamp, BooleanFilter>>newList());
                createLoadTaskWithPostLoadFilterBuilder(context, dateCluster, sourceAttribute, postLoadFilterBuilder, refreshOperationsPerTask);
            }
        }
        else
        {   // filter business date in post load
            Operation loadOperation = this.addSourceAttributeOperation(this.buildLoadOperation(new DateCluster(context.getBusinessDates()), context), sourceAttribute);
            List<TaskOperationDefinition> refreshOperationsPerTask = this.buildRefreshTaskDefinitions(context, loadOperation);

            PostLoadFilterBuilder postLoadFilterBuilder = new PostLoadFilterBuilder(
                    postLoadFilter, this.getAdditionalOperationBuilders(this.classToLoad, context),
                    this.getClassController().getFinderInstance(), this.createBusinessDateFilters(context.getBusinessDates()));
            createLoadTaskWithPostLoadFilterBuilder(context, null, sourceAttribute, postLoadFilterBuilder, refreshOperationsPerTask);
        }
    }

    protected List<Pair<Timestamp, BooleanFilter>> createBusinessDateFilters(List<Timestamp> businessDates)
    {
        List<Pair<Timestamp, BooleanFilter>> businessDateFilters = FastList.newList();
        for (Timestamp each : businessDates)
        {
            Timestamp businessDate = shiftBusinessDate(each);
            businessDateFilters.add(Pair.of(businessDate, this.createCacheFilterOfDatesToDrop(each).negate()));
        }
        return businessDateFilters;
    }

    /**
     * @param context
     * @param dateCluster              null for refresh
     * @param sourceAttribute
     * @param postLoadFilterBuilder
     * @param taskOperationDefinitions
     */
    private void createLoadTaskWithPostLoadFilterBuilder(CacheLoaderContext context, DateCluster dateCluster,
                                                         Object sourceAttribute, PostLoadFilterBuilder postLoadFilterBuilder,
                                                         List<TaskOperationDefinition> taskOperationDefinitions)
    {
        Operation sourceAttributeOperation = this.createFindAllOperation(context.getBusinessDates(), sourceAttribute);
        RelatedFinder relatedFinder = this.getClassController().getFinderInstance();

        DateCluster shiftedDateCluster = dateCluster == null ? null : this.shiftDateCluster(dateCluster);
        List<AdditionalOperationBuilder> additionalOperationBuilders = dateCluster == null ? FastList.<AdditionalOperationBuilder>newList() : this.getAdditionalOperationBuilders(this.classToLoad, context);

        for (TaskOperationDefinition taskOperationDefinition : taskOperationDefinitions)
        {
            Operation loadOperationWithSourceAttribute = sourceAttributeOperation.and(taskOperationDefinition.getOperation());

            // filter business date in SQL statement
            LoadOperationBuilder loadOperationBuilder = new LoadOperationBuilder(loadOperationWithSourceAttribute, additionalOperationBuilders, shiftedDateCluster, relatedFinder);
            LoadingTaskImpl loadingTask = new LoadingTaskImpl(this.getClassController(), loadOperationBuilder, postLoadFilterBuilder, dateCluster);
            loadingTask.needDependentLoaders(taskOperationDefinition.needDependentLoaders());
            loadingTask.setSourceAttribute(sourceAttribute);

            String threadPool = this.getDatabaseIdentifier(sourceAttribute);
            LoadingTaskRunner loadingTaskRunner = new LoadingTaskRunner(context.getEngine(), threadPool, loadingTask, LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);

            loadingTaskRunner.addPrerequisiteClassNames(this.prerequisiteClassNames);
            for (AdditionalOperationBuilder each : additionalOperationBuilders)
            {
                if (each instanceof AdditionalOperationBuilderWithPrerequisites)
                {
                    loadingTaskRunner.addPrerequisiteClassNames(
                            ((AdditionalOperationBuilderWithPrerequisites) each).getPrerequisitesClassNames());
                }
            }

            context.getDataSetLoaders().add(loadingTaskRunner);
        }
    }

    protected List<AdditionalOperationBuilder> getAdditionalOperationBuilders(String classToLoad, CacheLoaderContext context)
    {
        List<AdditionalOperationBuilder> builders = FastList.newList();

        AdditionalOperationBuilder limitedOperationBuilder =
                context.getQualifiedLoadContext().getAdditionalOperationBuilder(classToLoad);
        if (limitedOperationBuilder != null)
        {
            builders.add(limitedOperationBuilder);
        }

        List<String> operationBuilderClassNames = this.getParamValuesNamed("operationBuilder");

        if (operationBuilderClassNames == null || operationBuilderClassNames.size() == 0)
        {
            return builders;
        }

        for (String name : operationBuilderClassNames)
        {
            builders.add((AdditionalOperationBuilder) CacheLoaderConfig.newInstance(name));
        }

        return builders;
    }

    public Operation buildLoadOperation(DateCluster dateCluster, CacheLoaderContext cacheLoaderContext)
    {
        return this.getClassController().getFinderInstance().all();
    }

    public abstract List<TaskOperationDefinition> buildRefreshTaskDefinitions(CacheLoaderContext context, Operation loadOperation);

    /**
     * override this method in subclass if the factory needs to load each business date separately.
     */
    protected boolean loadBusinessDatesSeparately(CacheLoaderContext context)
    {
        return false;
    }

    private boolean createSingleDateClusters(CacheLoaderContext context)
    {
        return this.loadBusinessDatesSeparately(context) || !this.areAllAdditionalOperationBuildersDateInvariant(this.classToLoad, context);
    }

    protected DateCluster shiftDateCluster(DateCluster dateCluster)
    {
        List<Timestamp> shiftedBusinessDates = FastList.newList(dateCluster.size());
        for (Timestamp businessDate : dateCluster.getBusinessDates())
        {
            shiftedBusinessDates.add(this.shiftBusinessDate(businessDate));
        }
        return new DateCluster(shiftedBusinessDates);
    }

    /**
     * allows to transform business date into PME or other date to load.
     */
    protected Timestamp shiftBusinessDate(Timestamp businessDate)
    {
        return businessDate;
    }

    public Operation addSourceAttributeOperation(Operation operation, Object sourceAttributeValue)
    {
        Attribute sourceAttribute = this.runtimeCacheController.getFinderInstance().getSourceAttribute();
        if (sourceAttribute != null && CacheLoaderConfig.isSourceAttribute(sourceAttributeValue))
        {
            operation = operation.and(sourceAttribute.nonPrimitiveEq(sourceAttributeValue));
        }
        return operation;
    }

    public boolean hasSourceAttributeOnDomainObject()
    {
        return this.runtimeCacheController.getFinderInstance().getSourceAttribute() != null;
    }

    protected String getDatabaseIdentifier(Object sourceAttribute)
    {
        Object connectionManager = this.runtimeCacheController.getMithraObjectPortal().getDatabaseObject().getConnectionManager();
        if (connectionManager instanceof SourcelessConnectionManager)
        {
            return ((SourcelessConnectionManager) connectionManager).getDatabaseIdentifier();
        }
        else if (connectionManager instanceof ObjectSourceConnectionManager)
        {
            if (!CacheLoaderConfig.isSourceAttribute(sourceAttribute))
            {
                throw new RuntimeException("Factory must define sourceAttributes (" + this + ").");
            }
            return ((ObjectSourceConnectionManager) connectionManager).getDatabaseIdentifier(sourceAttribute);
        }
        else
        {
            throw new RuntimeException("Cannot handle ConnectionManager class: " + connectionManager.getClass() + " factory: " + this);
        }
    }

    public void setParams(List<ConfigParameter> params)
    {
        this.params = params;
    }

    public List<String> getParamValuesNamed(String name)
    {
        List list = FastList.newList();

        if (this.params != null)
        {
            for (ConfigParameter each : this.params)
            {
                if (name.equals(each.getKey()))
                {
                    list.add(each.getValue());
                }
            }
        }
        return list;
    }

    /**
     * the equals is used in DependentLoadingTaskSpawnerKey. The key has the region (so regionAttribute is
     * excluded. The runtimeMithraConfig is excluded because it is based on the class name.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractLoaderFactory that = (AbstractLoaderFactory) o;

        if (classToLoad != null ? !classToLoad.equals(that.classToLoad) : that.classToLoad != null) return false;
        if (params != null ? !params.equals(that.params) : that.params != null) return false;
        if (sourceAttributes != null ? !sourceAttributes.equals(that.sourceAttributes) : that.sourceAttributes != null)
            return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = classToLoad != null ? classToLoad.hashCode() : 0;
        result = 31 * result + (params != null ? params.hashCode() : 0);
        result = 31 * result + (sourceAttributes != null ? sourceAttributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "(" + this.classToLoad + ") with " + this.params;
    }

    protected AsOfAttribute getBusinessDateAttribute()
    {
        return (AsOfAttribute) this.getAttributeByName("businessDate");
    }

    protected AsOfAttribute getProcessingDateAttribute()
    {
        return (AsOfAttribute) this.getAttributeByName("processingDate");
    }
}
