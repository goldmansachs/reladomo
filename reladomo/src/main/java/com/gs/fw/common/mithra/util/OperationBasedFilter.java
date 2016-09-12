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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.finder.Operation;


public class OperationBasedFilter extends AbstractBooleanFilter
{
    private final Operation operation;

    public OperationBasedFilter(Operation op)
    {
        this.operation = op;
    }

    public boolean matches(Object o)
    {
        Boolean matches = operation.matches(o);
        return matches != null && matches; // note it treats null as false
    }

    public String toString()
    {
        return this.getClass().getSimpleName() + " " + this.operation;
    }

    public BooleanFilter and(Filter that)
    {
        if (that instanceof OperationBasedFilter)
        {
            return new OperationBasedFilter(this.operation.and(((OperationBasedFilter) that).operation));
        }
        else
        {
            return super.and(that);
        }
    }

    public BooleanFilter or(Filter that)
    {
        if (that instanceof OperationBasedFilter)
        {
            return new OperationBasedFilter(this.operation.or(((OperationBasedFilter) that).operation));
        }
        else
        {
            return super.or(that);
        }
    }
}
