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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CompactUpdateCountOperation;

import java.util.List;



public class AnalyzedOperation
{

    private Operation originalOperation;
    private OrderBy orderBy;

    private AsOfEqualityChecker asOfEqualityChecker;

    public AnalyzedOperation(Operation originalOperation)
    {
        this.originalOperation = originalOperation;
    }

    public AnalyzedOperation(Operation originalOperation, OrderBy orderBy)
    {
        this.originalOperation = originalOperation;
        this.orderBy = orderBy;
    }

    public Operation getOriginalOperation()
    {
        return originalOperation;
    }

    public boolean isAnalyzedOperationDifferent()
    {
        return requiresAsOfEqualityCheck() && this.getAsOfEqualityChecker().getOperation() != this.originalOperation;
    }

    private boolean requiresAsOfEqualityCheck()
    {
        if (this.originalOperation instanceof CompactUpdateCountOperation)
        {
            if (!((CompactUpdateCountOperation)this.originalOperation).requiresAsOfEqualityCheck())
            {
                return false;
            }
        }
        return true;
    }

    private void analyze()
    {
        if (this.asOfEqualityChecker == null)
        {
            this.asOfEqualityChecker = new AsOfEqualityChecker(this.originalOperation, this.orderBy);
        }
    }

    public AsOfEqualityChecker getAsOfEqualityChecker()
    {
        analyze();
        return asOfEqualityChecker;
    }

    public Operation getAnalyzedOperation()
    {
        if (this.asOfEqualityChecker != null || requiresAsOfEqualityCheck())
        {
            return this.getAsOfEqualityChecker().getOperation();
        }
        return this.originalOperation;
    }

    public ObjectWithMapperStack[] getAllAsOfAttributes()
    {
        return this.getAsOfEqualityChecker().getAllAsOfAttributesWithMapperStack();
    }

    public boolean hasAsOfAttributes()
    {
        return this.getAsOfEqualityChecker().hasAsOfAttributes();
    }

    public ObjectWithMapperStack getAsOfOperation(ObjectWithMapperStack attribute)
    {
        return this.getAsOfEqualityChecker().getAsOfOperation(attribute);
    }

    public ObjectWithMapperStack getAsOfOperationForTopLevel(AsOfAttribute attribute)
    {
        ObjectWithMapperStack objectWithMapperStack = new ObjectWithMapperStack(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL, attribute);
        ObjectWithMapperStack asOfOperation = this.getAsOfEqualityChecker().getAsOfOperation(objectWithMapperStack);
        if (asOfOperation == null)
        {
            throw new MithraBusinessException("Could not find operation for "+attribute.toString()+
                    ". Dated objects must include operations for their as of attributes");
        }
        return asOfOperation;
    }
}
