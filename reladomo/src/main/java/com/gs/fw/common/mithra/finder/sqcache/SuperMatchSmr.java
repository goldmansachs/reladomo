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

package com.gs.fw.common.mithra.finder.sqcache;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.QueryCache;

import java.util.List;

public class SuperMatchSmr extends ShapeMatchResult
{
    private Operation existingOperation;
    private Operation newOperation;

    private Operation lookupOperation;
    private Operation filterOperation;

    public SuperMatchSmr(Operation existingOperation, Operation newOperation)
    {
        this.existingOperation = existingOperation;
        this.newOperation = newOperation;
        this.lookupOperation = existingOperation;
        this.filterOperation = newOperation;
    }

    public SuperMatchSmr(Operation existingOperation, Operation newOperation, Operation lookupOperation, Operation filterOperation)
    {
        this.existingOperation = existingOperation;
        this.newOperation = newOperation;
        this.lookupOperation = lookupOperation;
        this.filterOperation = filterOperation;
    }

    @Override
    public boolean isSuperMatch()
    {
        return true;
    }

    @Override
    public List resolve(QueryCache queryCache)
    {
        CachedQuery byEquality = queryCache.findByEquality(this.lookupOperation);
        if (byEquality != null)
        {
            return this.filterOperation.applyOperation(byEquality.getResult());
        }
        return null;
    }

    public Operation getLookUpOperation()
    {
        return lookupOperation;
    }
}
