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
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;

import java.util.List;



public abstract class AtomicNotEqualityOperation extends AbstractAtomicOperation
{

    protected AtomicNotEqualityOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected AtomicNotEqualityOperation()
    {
        // for externalizable
    }

    public int getIndexRef()
    {
        return -1; // can't use index
    }

    protected List getByIndex()
    {
        throw new RuntimeException("should never be called");
    }

    public Operation zCombinedAnd(Operation op)
    {
        //todo: rezaem: add zCombinedAndWithAtomicNotEquality
        if (op instanceof AtomicNotEqualityOperation)
        {
            if (this.equals(op)) return this;
            return null;
        }
        if (op instanceof AtomicEqualityOperation)
        {
            return this.zCombinedAndWithAtomicEquality((AtomicEqualityOperation) op);
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            if (this.getParameterAsObject().equals(op.getParameterAsObject()))
            {
                return new None(this.getAttribute());
            }
            else return op;
        }
        return null;
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return op.zCombinedAndWithAtomic(this);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return null;
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()) && !op.matchesWithoutDeleteCheck(this, this.getStaticExtractor()))
        {
            return op;
        }
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        // todo: rezaem: implement combine
        return null;
    }

    public List applyOperationToPartialCache()
    {
        return null;
    }

    public abstract Object getParameterAsObject();

    public void generateSql(SqlQuery query)
    {
        query.appendWhereClause( this.getAttribute().getFullyQualifiedLeftHandExpression(query)+" <> ?");
        query.addSqlParameterSetter(this);
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("!=");
        toStringContext.append(this.getParameterAsObject().toString());
    }

    @Override
    public ShapeMatchResult zShapeMatch(Operation existingOperation)
    {
        // not-equality is almost the entire set (it's just missing a single point)
        // that makes it almost impossible for not-equality to be a subset of anything
        if (existingOperation.equals(this))
        {
            return ExactMatchSmr.INSTANCE;
        }
        return NoMatchSmr.INSTANCE;
    }

    protected abstract Extractor getStaticExtractor();

    @Override
    public int zShapeHash()
    {
        return this.getAttribute().hashCode() ^ 0x78f57132;
    }
}
