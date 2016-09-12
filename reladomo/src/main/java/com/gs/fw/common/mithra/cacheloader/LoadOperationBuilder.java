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

import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.util.List;


public class LoadOperationBuilder
{
    private final Operation operation;
    private final List<AdditionalOperationBuilder> additionalOperationBuilders;
    private final RelatedFinder relatedFinder;
    private final DateCluster dateCluster;

    public LoadOperationBuilder(Operation operation, List<AdditionalOperationBuilder> additionalOperationBuilders, DateCluster dateCluster, RelatedFinder relatedFinder)
    {
        this.operation = operation;
        this.additionalOperationBuilders = additionalOperationBuilders;
        this.dateCluster = dateCluster;
        this.relatedFinder = relatedFinder;
    }

    public Operation build(Object sourceAttribute)
    {
        Operation op = this.operation;
        if (this.additionalOperationBuilders != null)
        {
            for (AdditionalOperationBuilder builder : this.additionalOperationBuilders)
            {
                final Operation additionalOp = builder instanceof AdditionalOperationBuilderWithStringSourceAttribute
                        ? ((AdditionalOperationBuilderWithStringSourceAttribute)builder).buildOperation(this.dateCluster.getBusinessDate(), this.relatedFinder, sourceAttribute)
                        : builder.buildOperation(this.dateCluster.getBusinessDate(), this.relatedFinder);
                op = additionalOp instanceof None ? additionalOp : op.and(additionalOp);
            }
        }

        return op;
    }

    String getOperationAsString()
    {
        return this.operation == null ? "" : this.operation.toString();
    }
}
