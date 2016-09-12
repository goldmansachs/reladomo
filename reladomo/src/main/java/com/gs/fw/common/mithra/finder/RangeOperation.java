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

import com.gs.fw.common.mithra.attribute.Attribute;

import java.util.List;



public abstract class RangeOperation extends AbstractAtomicOperation
{

    protected RangeOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected RangeOperation()
    {
        // for externalizable
    }

    protected boolean isIndexed()
    {
        // todo: rezaem: implement ordered index and use in range operations
        return false;
    }

    public boolean usesUniqueIndex()
    {
        return false;
    }

    public boolean usesImmutableUniqueIndex()
    {
        return false;
    }

    public boolean usesNonUniqueIndex()
    {
        // todo: rezaem: implement ordered index and use in range operations
        return false;
    }

    @Override
    public int zEstimateReturnSize()
    {
        return this.getResultObjectPortal().getCache().estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        return this.getResultObjectPortal().getCache().estimateQuerySize();
    }

    protected List getByIndex()
    {
        // todo: rezaem: implement ordered index and use in range operations
        throw new RuntimeException("not implemented");
    }

    public List applyOperationToPartialCache()
    {
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return null;
    }

    public Attribute getSingleColumnAttribute()
    {
        return this.getAttribute();
    }
}
