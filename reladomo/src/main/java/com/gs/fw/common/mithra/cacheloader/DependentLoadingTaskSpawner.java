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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.Arrays;
import java.util.List;


public class DependentLoadingTaskSpawner implements LoadingTask
{
    public static final int TASK_SIZE = 25000;

    private final String threadPoolName;
    private final Object sourceAttribute;
    private final Operation datedOperation;
    private final MithraRuntimeCacheController cacheController;
    private final CacheLoaderContext cacheLoaderContext;
    private final List<AdditionalOperationBuilder> additionalOperationBuilders;
    private final List subDependentThreads = FastList.newList();
    private final Attribute[] attributes;
    private final TupleAttribute tupleAttribute;
    private String classToLoad;

    private final List<DependentKeyIndex> dependentKeyIndexes = FastList.newList();
    private final DateCluster dateCluster;

    /**
     * must be initialized only by the builder
     */
    public DependentLoadingTaskSpawner(Attribute[] attributes, String threadPoolName, Object sourceAttribute,
                                       Operation datedOperation,
                                       MithraRuntimeCacheController cacheController, CacheLoaderContext cacheLoaderContext,
                                       List<AdditionalOperationBuilder> additionalOperationBuilders, DateCluster dateCluster)
    {
        this.threadPoolName = threadPoolName;
        this.sourceAttribute = sourceAttribute;
        this.datedOperation = datedOperation;
        this.cacheController = cacheController;
        this.cacheLoaderContext = cacheLoaderContext;
        this.attributes = attributes;
        this.dateCluster = dateCluster;
        this.tupleAttribute = buildMultipleAttributes(attributes);
        this.additionalOperationBuilders = additionalOperationBuilders;
        this.classToLoad = this.cacheController.getClassName();

        this.cacheLoaderContext.getEngine().getOrCreateThreadPool(this.threadPoolName);
    }

    @Override
    public String getOperationAsString()
    {
        return this.datedOperation.toString();
    }

    @Override
    public void addDependentThread(DependentKeyIndex subDependentThread)
    {
        this.subDependentThreads.add(subDependentThread);
    }

    @Override
    public DateCluster getDateCluster()
    {
        return this.dateCluster;
    }

    protected LoadingTaskRunner createTaskRunner(List keyHolders, Extractor[] keyExtractors, LoadingTaskRunner.State state)
    {
        final Operation keyInOperation = this.tupleAttribute == null
                ? this.attributes[0].in(keyHolders, keyExtractors[0])
                : this.tupleAttribute.in(keyHolders, keyExtractors);

        Operation operation = this.datedOperation == null ? keyInOperation : this.datedOperation.and(keyInOperation);

        LoadOperationBuilder loadOperationBuilder = new LoadOperationBuilder(operation, this.additionalOperationBuilders, this.dateCluster, this.cacheController.getFinderInstance());
        PostLoadFilterBuilder postLoadFilterBuilder = new PostLoadFilterBuilder(null, null, null, null);

        LoadingTaskImpl loadingTask = new LoadingTaskImpl(this.cacheController, loadOperationBuilder, postLoadFilterBuilder, this.dateCluster);
        loadingTask.setSyslogChecker(this.cacheLoaderContext.getEngine().getTempdbSyslogCheckerForConnectionPool(this.threadPoolName));
        loadingTask.setSourceAttribute(this.sourceAttribute);
        loadingTask.setDependentKeyIndices(this.subDependentThreads);

        LoadingTaskRunner taskRunner = new LoadingTaskRunner(this.cacheLoaderContext.getEngine(), this.threadPoolName, loadingTask, state);
        this.cacheLoaderContext.getEngine().addTaskToLoad(taskRunner);
        return taskRunner;
    }

    public String getThreadPoolName()
    {
        return threadPoolName;
    }

    protected static TupleAttribute buildMultipleAttributes(Attribute[] attributes)
    {
        if (attributes.length == 1)
        {
            return null;
        }
        TupleAttribute ta = attributes[0].tupleWith(attributes[1]);
        for (int i = 2; i < attributes.length; i++)
        {
            ta = ta.tupleWith(attributes[i]);
        }
        return ta;
    }

    protected List addOwnersToLoadIfAbsent(FullUniqueIndex index, List stripe, Extractor[] ownerKeyExtractor)
    {
        // stripe is shared with other KeyIndexes - see the DependentCursor
        FastList toAdd = FastList.newList(stripe.size());
        for (int i=0; i<stripe.size(); i++)
        {
            Object owner = stripe.get(i);
            if (!hasOwnerQueuedOrLoaded(ownerKeyExtractor, owner))
            {
                index.put(owner);
                toAdd.add(owner);
            }
        }
        return toAdd;
    }

    private boolean hasOwnerQueuedOrLoaded(Extractor[] ownerKeyExtractor, Object owner)
    {
        final int size = this.dependentKeyIndexes.size();
        for (int i=0;i< size;i++)
        {
            DependentKeyIndex each = this.dependentKeyIndexes.get(i);
            if (each.hasOwnerQueuedOrLoaded(owner, ownerKeyExtractor))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * executed from a single thread during the cacheloader startup.
     */
    public DependentKeyIndex createDependentKeyIndex(CacheLoaderEngine cacheLoaderEngine, Extractor[] keyExtractors, Operation ownerObjectFilter)
    {
        DependentKeyIndex dependentKeyIndex = null;

        for (DependentKeyIndex each : this.dependentKeyIndexes)
        {
            if (Arrays.equals(each.getKeyExtractors(), keyExtractors))
            {
                dependentKeyIndex = each;
                break;
            }
        }

        if (dependentKeyIndex == null)
        {
            dependentKeyIndex = (keyExtractors.length > 1)
                    ? new DependentTupleKeyIndex(cacheLoaderEngine, this, keyExtractors)
                    : new DependentSingleKeyIndex(cacheLoaderEngine, this, keyExtractors);

            dependentKeyIndex.setOwnerObjectFilter(ownerObjectFilter);

            final LoadingTaskThreadPoolHolder threadPoolHolder = cacheLoaderEngine.getOrCreateThreadPool(this.getThreadPoolName());
            dependentKeyIndex.setLoadingTaskThreadPoolHolder(threadPoolHolder);
            threadPoolHolder.addDependentKeyIndex(dependentKeyIndex);
            this.dependentKeyIndexes.add(dependentKeyIndex);
        }
        else
        {
            dependentKeyIndex.orOwnerObjectFilter(ownerObjectFilter);
        }
        return dependentKeyIndex;
    }

    @Override
    public Object getSourceAttribute()
    {
        return sourceAttribute;
    }

    @Override
    public int load()
    {
        throw new RuntimeException("This is not a real loading task - just a task spawner");
    }

    @Override
    public String getClassName()
    {
        return this.classToLoad;
    }

    public List<DependentKeyIndexMonitor> getDependentKeyIndexMonitors()
    {
        List list = FastList.newList(this.dependentKeyIndexes.size());
        for (DependentKeyIndex each : this.dependentKeyIndexes)
        {
            list.add(new DependentKeyIndexMonitor(this.getClassName(), this.threadPoolName,
                    FastList.newListWith(each.getKeyExtractors()).toString(),
                    each.getLoadedOrQueuedOwnerCount()));
        }

        return list;
    }

    @Override
    public String toString()
    {
        String sourceName = CacheLoaderConfig.isSourceAttribute(this.getSourceAttribute()) ? "@" + this.getSourceAttribute() : "";

        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName());
        builder.append(" ").append(this.classToLoad.substring(this.classToLoad.lastIndexOf('.') + 1));
        builder.append("/").append(this.datedOperation);
        builder.append(" on ").append(sourceName);
        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        DependentLoadingTaskSpawner that = (DependentLoadingTaskSpawner) o;

        if (additionalOperationBuilders != null ? !additionalOperationBuilders.equals(that.additionalOperationBuilders) : that.additionalOperationBuilders != null)
        {
            return false;
        }
        if (!Arrays.equals(attributes, that.attributes))
        {
            return false;
        }
        if (cacheController != null ? !cacheController.equals(that.cacheController) : that.cacheController != null)
        {
            return false;
        }
        if (cacheLoaderContext != null ? !cacheLoaderContext.equals(that.cacheLoaderContext) : that.cacheLoaderContext != null)
        {
            return false;
        }
        if (classToLoad != null ? !classToLoad.equals(that.classToLoad) : that.classToLoad != null)
        {
            return false;
        }
        if (dateCluster != null ? !dateCluster.equals(that.dateCluster) : that.dateCluster != null)
        {
            return false;
        }
        if (datedOperation != null ? !datedOperation.equals(that.datedOperation) : that.datedOperation != null)
        {
            return false;
        }
        if (dependentKeyIndexes != null ? !dependentKeyIndexes.equals(that.dependentKeyIndexes) : that.dependentKeyIndexes != null)
        {
            return false;
        }
        if (sourceAttribute != null ? !sourceAttribute.equals(that.sourceAttribute) : that.sourceAttribute != null)
        {
            return false;
        }
        if (subDependentThreads != null ? !subDependentThreads.equals(that.subDependentThreads) : that.subDependentThreads != null)
        {
            return false;
        }
        if (threadPoolName != null ? !threadPoolName.equals(that.threadPoolName) : that.threadPoolName != null)
        {
            return false;
        }
        if (tupleAttribute != null ? !tupleAttribute.equals(that.tupleAttribute) : that.tupleAttribute != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = threadPoolName != null ? threadPoolName.hashCode() : 0;
        result = 31 * result + (sourceAttribute != null ? sourceAttribute.hashCode() : 0);
        result = 31 * result + (datedOperation != null ? datedOperation.hashCode() : 0);
        result = 31 * result + (cacheController != null ? cacheController.hashCode() : 0);
        result = 31 * result + (cacheLoaderContext != null ? cacheLoaderContext.hashCode() : 0);
        result = 31 * result + (additionalOperationBuilders != null ? additionalOperationBuilders.hashCode() : 0);
        result = 31 * result + (subDependentThreads != null ? subDependentThreads.hashCode() : 0);
        result = 31 * result + (attributes != null ? Arrays.hashCode(attributes) : 0);
        result = 31 * result + (tupleAttribute != null ? tupleAttribute.hashCode() : 0);
        result = 31 * result + (classToLoad != null ? classToLoad.hashCode() : 0);
        result = 31 * result + (dependentKeyIndexes != null ? dependentKeyIndexes.hashCode() : 0);
        result = 31 * result + (dateCluster != null ? dateCluster.hashCode() : 0);
        return result;
    }
}
