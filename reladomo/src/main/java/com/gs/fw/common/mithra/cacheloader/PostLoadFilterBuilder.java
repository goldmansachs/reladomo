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

import java.sql.Timestamp;
import java.util.List;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.FalseFilter;
import com.gs.fw.common.mithra.util.OperationBasedFilter;
import com.gs.fw.common.mithra.util.Pair;


public class PostLoadFilterBuilder
{
    private final BooleanFilter postLoadFilter;
    private final List<AdditionalOperationBuilder> additionalOperationBuilders;
    private final RelatedFinder relatedFinder;
    private final List<Pair<Timestamp, BooleanFilter>> businessDateFilters;

    public PostLoadFilterBuilder(BooleanFilter postLoadFilter, List<AdditionalOperationBuilder> additionalOperationBuilders, RelatedFinder relatedFinder, List<Pair<Timestamp, BooleanFilter>> businessDateFilters)
    {
        this.postLoadFilter = postLoadFilter;
        this.additionalOperationBuilders = additionalOperationBuilders;
        this.relatedFinder = relatedFinder;
        this.businessDateFilters = businessDateFilters;
    }

    public BooleanFilter build()
    {
        if (this.businessDateFilters == null)
        {
            Operation postLoadOperation = this.buildPostLoadOperation(null);
            if (postLoadOperation == null)
            {
                return this.postLoadFilter;
            }
            return postLoadOperation.zIsNone() ? FalseFilter.instance() : new OperationBasedFilter(postLoadOperation).and(this.postLoadFilter);
        }

        List<BooleanFilter> filters = FastList.newList();
        for (Pair<Timestamp, BooleanFilter> each : this.businessDateFilters)
        {
            Timestamp businessDate = each.getOne();
            BooleanFilter filter = each.getTwo();

            Operation postLoadOperation = this.buildPostLoadOperation(businessDate);
            if (postLoadOperation != null)
            {
                if (postLoadOperation.zIsNone())
                {
                    return FalseFilter.instance();
                }
                filter = filter.and(new OperationBasedFilter(postLoadOperation));
            }

            filters.add(filter);
        }

        if (filters.size() == 0)
        {
            return this.postLoadFilter;
        }

        BooleanFilter allDatesFilter = filters.get(0);
        for (int i=1; i<filters.size(); i++)
        {
            allDatesFilter = allDatesFilter.or(filters.get(i));
        }

        return  this.postLoadFilter == null ? allDatesFilter : postLoadFilter.and(allDatesFilter);
    }

    private Operation buildPostLoadOperation(Timestamp businessDate)
    {
        Operation op = null;
        if (this.additionalOperationBuilders != null)
        {
            for (AdditionalOperationBuilder builder : this.additionalOperationBuilders)
            {
                final Operation additionalOperation = builder.buildOperation(businessDate, this.relatedFinder);
                op = op == null ? additionalOperation : op.and(additionalOperation);
            }
        }
        return op;
    }
}
