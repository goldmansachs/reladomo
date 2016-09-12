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
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.KeepOnlySpecifiedDatesFilter;

import java.sql.Timestamp;
import java.util.List;


public class BusinessDateMilestonedTopLevelLoaderFactory extends AbstractLoaderFactory implements TopLevelLoaderFactory
{
    @Override
    public Operation buildLoadOperation(DateCluster dateCluster, CacheLoaderContext cacheLoaderContext)
    {
        DateCluster shiftedDateCluster = this.shiftDateCluster(dateCluster);

        Operation milestoneOp = getBusinessDateOp(shiftedDateCluster, this.getClassController().getFinderInstance());

        return milestoneOp.and(super.buildLoadOperation(dateCluster, cacheLoaderContext));
    }

    @Override
    public List<TaskOperationDefinition> buildRefreshTaskDefinitions(CacheLoaderContext context, Operation loadOperation)
    {
        return FastList.newList();
    }

    @Override
    public Operation createFindAllOperation(List<Timestamp> businessDates, Object sourceAttribute)
    {
        Operation businessDateOp = this.getBusinessDateAttribute().equalsEdgePoint();
        return this.addSourceAttributeOperation(businessDateOp, sourceAttribute);
    }

    @Override
    public BooleanFilter createCacheFilterOfDatesToDrop(Timestamp loadedDate)
    {
        return new KeepOnlySpecifiedDatesFilter(this.getBusinessDateAttribute(), FastList.newListWith(loadedDate));
    }

    protected static Operation getBusinessDateOp(DateCluster dateCluster, RelatedFinder finder)
    {
        AsOfAttribute businessDate = (AsOfAttribute) finder.getAttributeByName("businessDate");
        if (dateCluster.hasManyDates())
        {
            Operation fromZOp = businessDate.getFromAttribute().lessThanEquals(dateCluster.getEndDate());
            Operation thruZOp = businessDate.getToAttribute().greaterThan(dateCluster.getStartDate());
            Operation businessDateOp = businessDate.equalsEdgePoint();
            return businessDateOp.and(fromZOp).and(thruZOp);
        }
        else
        {
            return businessDate.eq(dateCluster.getBusinessDate());
        }
    }
}
