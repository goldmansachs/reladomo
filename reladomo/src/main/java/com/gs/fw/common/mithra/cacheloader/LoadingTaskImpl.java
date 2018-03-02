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


import com.gs.fw.common.mithra.MithraDatabaseObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.database.SyslogChecker;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.FalseFilter;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;

public class LoadingTaskImpl implements LoadingTask
{
    public static final int QUEUE_STRIPE_SIZE = 2000;
    private LoadOperationBuilder loadOperationBuilder;
    private PostLoadFilterBuilder postLoadFilterBuilder;

    private Object sourceAttribute;
    private MithraRuntimeCacheController mithraController;
    private List<DependentKeyIndex> dependentKeyIndices = FastList.newList();
    private boolean needDependentLoaders = true;
    private final DateCluster dateCluster;
    private SyslogChecker syslogChecker;

    private int newOrChangedCount = 0;
    private List stripe = FastList.newList(QUEUE_STRIPE_SIZE);

    public LoadingTaskImpl(MithraRuntimeCacheController mithraController, LoadOperationBuilder loadOperationBuilder, PostLoadFilterBuilder postLoadFilterBuilder, DateCluster dateCluster)
    {
        this.loadOperationBuilder = loadOperationBuilder;
        this.postLoadFilterBuilder = postLoadFilterBuilder;
        this.mithraController = mithraController;
        this.dateCluster = dateCluster;
    }

    public void setSyslogChecker(SyslogChecker syslogChecker)
    {
        this.syslogChecker = syslogChecker;
    }

    public void setDependentKeyIndices(List<DependentKeyIndex> dependentThreads)
    {
        this.dependentKeyIndices = dependentThreads;
    }

    public void addDependentThread(DependentKeyIndex dependentThread)
    {
        this.dependentKeyIndices.add(dependentThread);
    }

    public void needDependentLoaders(boolean  needDependentLoaders)
    {
        this.needDependentLoaders = needDependentLoaders;
    }

    public String getClassName()
    {
        return this.mithraController.getClassName();
    }

    public int load()
    {
        Operation loadOperation = this.loadOperationBuilder.build(this.sourceAttribute);
        BooleanFilter postLoadFilter = this.postLoadFilterBuilder.build();
        this.loadOperationBuilder = null;
        this.postLoadFilterBuilder = null;

        if (loadOperation instanceof None || FalseFilter.instance().equals(postLoadFilter))
        {
            return 0;
        }

        this.checkAndWaitForSyslog();

        Cursor cursor = buildCursor(loadOperation, postLoadFilter);
        try
        {
            while (cursor.hasNext())
            {
                this.putOnDependentQueues((MithraObject) cursor.next());
            }
        }
        finally
        {
            cursor.close();
            this.finishDependentQueues();
        }

        this.dereference();

        return newOrChangedCount;
    }

    private Cursor buildCursor(Operation loadOperation, BooleanFilter postLoadFilter)
    {
        return this.mithraController.getMithraObjectPortal().findCursorFromServer(
                loadOperation, postLoadFilter,
                null, // no ordering
                0,    // retrieve as many objects as necessary
                true, // read from the DB
                1,    // use single thread
                false);
    }

    public void putOnDependentQueues(MithraObject keyHolder)
    {
        this.newOrChangedCount++;

        if (this.dependentKeyIndices.isEmpty() || !this.needDependentLoaders)
        {
            return;
        }

        this.stripe.add(keyHolder.zGetCurrentData());

        this.dependentKeyIndices.get(0).getCacheLoaderEngine().changeStripedCount(this.dependentKeyIndices.size());

        if (this.stripe.size() == QUEUE_STRIPE_SIZE)
        {
            for (int i = 0; i < this.dependentKeyIndices.size(); i++)
            {
                this.dependentKeyIndices.get(i).putStripeOnQueue(this.stripe);
            }
            this.stripe = FastList.newList(QUEUE_STRIPE_SIZE);
        }
    }

    public void finishDependentQueues()
    {
        if (!this.stripe.isEmpty())
        {
            for (int i = 0; i < this.dependentKeyIndices.size(); i++)
            {
                this.dependentKeyIndices.get(i).putStripeOnQueue(this.stripe);
            }
        }
    }

    private void checkAndWaitForSyslog()
    {
        if (this.syslogChecker == null)
        {
            return;
        }
        Object theSourceAttribute = CacheLoaderConfig.isSourceAttribute(this.sourceAttribute) ? this.sourceAttribute : null;

        final MithraDatabaseObject databaseObject = mithraController.getMithraObjectPortal().getDatabaseObject();
        Object connectionManager = databaseObject.getConnectionManager();

        String tempdbSchema;

        if (connectionManager instanceof SourcelessConnectionManager)
        {
            tempdbSchema = ((SourcelessConnectionManager) connectionManager).getDatabaseType().getTempDbSchemaName();
        }
        else
        {
            tempdbSchema = ((ObjectSourceConnectionManager) connectionManager).getDatabaseType(sourceAttribute).getTempDbSchemaName();
        }

        if (tempdbSchema != null)
        {
            if (tempdbSchema.endsWith("."))
            {
                tempdbSchema = tempdbSchema.substring(0, tempdbSchema.length() - 1);
            }
            this.syslogChecker.checkAndWaitForSyslogSynchronized(theSourceAttribute, tempdbSchema, databaseObject);
        }
    }

    private void dereference()
    {
        this.mithraController.clearQueryCache();
        this.dependentKeyIndices = null;
        this.stripe = null;
    }

    public Object getSourceAttribute()
    {
        return this.sourceAttribute;
    }

    public String getOperationAsString()
    {
        return this.loadOperationBuilder.getOperationAsString();
    }


    @Override
    public DateCluster getDateCluster()
    {
        return this.dateCluster;
    }

    public void setSourceAttribute(Object sourceAttribute)
    {
        this.sourceAttribute = sourceAttribute;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Task ").append(" ")
                .append(this.mithraController.getFinderClass().getSimpleName());
        if (CacheLoaderConfig.isSourceAttribute(this.getSourceAttribute()))
        {
            builder.append("@").append(this.getSourceAttribute());
        }
        return builder.toString();
    }
}
