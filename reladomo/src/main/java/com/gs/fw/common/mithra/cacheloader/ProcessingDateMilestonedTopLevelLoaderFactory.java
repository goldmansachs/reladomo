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


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.FalseFilter;

import java.sql.Timestamp;
import java.util.List;

public class ProcessingDateMilestonedTopLevelLoaderFactory extends AbstractLoaderFactory implements TopLevelLoaderFactory
{
    @Override
    public Operation buildLoadOperation(DateCluster dateCluster, CacheLoaderContext cacheLoaderContext)
    {
        Operation processingDateOp = this.getProcessingDateAttribute().equalsEdgePoint();
        Operation op = processingDateOp.and(super.buildLoadOperation(dateCluster, cacheLoaderContext));

        if (cacheLoaderContext.getInitialLoadEndTime() != null)
        {
            op = op.and(op.and(this.getProcessingDateAttribute().getFromAttribute().lessThanEquals(cacheLoaderContext.getInitialLoadEndTime())));
        }

        return op;
    }

    @Override
    public List<TaskOperationDefinition> buildRefreshTaskDefinitions(CacheLoaderContext context, Operation loadOperation)
    {
        Timestamp start = context.getRefreshInterval().getStart();
        Timestamp end = context.getRefreshInterval().getEnd();

        final Operation inGreaterThenStart = this.getProcessingDateAttribute().getFromAttribute().greaterThanEquals(start);
        final Operation inLessThenEnd = this.getProcessingDateAttribute().getFromAttribute().lessThanEquals(end);
        final Operation loadChangedInZ = inGreaterThenStart.and(inLessThenEnd);

        final Operation outGreaterThenStart = this.getProcessingDateAttribute().getToAttribute().greaterThanEquals(start);
        final Operation outLessThenEnd = this.getProcessingDateAttribute().getToAttribute().lessThanEquals(end);
        final Operation loadChangedOutZ = outGreaterThenStart.and(outLessThenEnd);

        return FastList.newListWith(
                new TaskOperationDefinition(loadChangedInZ, true),
                new TaskOperationDefinition(loadChangedOutZ, false)
        );
    }

    @Override
    public Operation createFindAllOperation(List<Timestamp> businessDates, Object sourceAttribute)
    {
        Operation processingDateOp = this.getProcessingDateAttribute().equalsEdgePoint();
        return this.addSourceAttributeOperation(processingDateOp, sourceAttribute);
    }

    @Override
    public BooleanFilter createCacheFilterOfDatesToDrop(Timestamp loadedDate)
    {
        return FalseFilter.instance();
    }
}
