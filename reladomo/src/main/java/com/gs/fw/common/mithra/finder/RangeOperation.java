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
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;

import java.util.List;



public abstract class RangeOperation extends AbstractAtomicOperation
{
    protected static final int GREATER_DIR = 1;
    protected static final int LESS_DIR = -1;

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

    public abstract int getDirection();

    @Override
    public ShapeMatchResult zShapeMatch(Operation existingOperation)
    {
        if (existingOperation instanceof AtomicOperation)
        {
            if (((AtomicOperation)existingOperation).getAttribute().equals(this.getAttribute()))
            {
                if (existingOperation instanceof AtomicEqualityOperation)
                {
                    return NoMatchSmr.INSTANCE;
                }
                else if (existingOperation instanceof AtomicNotEqualityOperation)
                {
                    AtomicNotEqualityOperation notEqualityOperation = (AtomicNotEqualityOperation) existingOperation;
                    if (!this.matchesWithoutDeleteCheck(notEqualityOperation, notEqualityOperation.getStaticExtractor()))
                    {
                        return new SuperMatchSmr(existingOperation, this);
                    }
                    return NoMatchSmr.INSTANCE;
                }
                else if (existingOperation instanceof InOperation)
                {
                    return NoMatchSmr.INSTANCE;
                }
                else if (existingOperation instanceof NotInOperation)
                {
                    // too hard to loop here
                    return NoMatchSmr.INSTANCE;
                }
                else if (existingOperation instanceof RangeOperation)
                {
                    RangeOperation rangeOperation = (RangeOperation) existingOperation;
                    if (rangeOperation.getDirection() == this.getDirection() && rangeOperation.matchesWithoutDeleteCheck(this, getStaticExtractor()))
                    {
                        return new SuperMatchSmr(existingOperation, this);
                    }
                }
                else if (existingOperation instanceof IsNotNullOperation)
                {
                    return new SuperMatchSmr(existingOperation, this);
                }
                else
                {
                    // StringLikeOperation
                    // StringNotLikeOperation
                    // AtomicSelf*Operation
                    return NoMatchSmr.INSTANCE;
                }
            }
        }
        // there are complex cases we're ignoring here.
        return NoMatchSmr.INSTANCE;
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()) && op.getDirection() == this.getDirection())
        {
            if (this.matchesWithoutDeleteCheck(op, op.getStaticExtractor()))
            {
                return op;
            }
            return this;
        }
        return null;
    }

    @Override
    public Operation zCombinedAnd(Operation op)
    {
        return op.zCombinedAndWithRange(this);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return op.zCombinedAndWithRange(this);
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            if (!op.zIsNullOperation() && this.matchesWithoutDeleteCheck(op, op.getStaticExtractor()))
            {
                return op;
            }
            return new None(this.getAttribute());
        }
        return null;
    }

    public abstract Extractor getStaticExtractor();

    @Override
    public int zShapeHash()
    {
        return this.getAttribute().hashCode() ^ this.getClass().hashCode();
    }
}
