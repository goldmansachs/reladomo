
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
import com.gs.fw.common.mithra.finder.asofop.AsOfEdgePointOperation;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;

import java.util.List;

public abstract class AtomicEqualityOperation extends AbstractAtomicOperation implements SourceOperation, OperationWithParameterExtractor, EqualityOperation
{

    protected AtomicEqualityOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected AtomicEqualityOperation()
    {
        // for externalizable
    }

    public boolean zIsNullOperation()
    {
        return false;
    }

    @Override
    public int zEstimateReturnSize()
    {
        if (this.usesUniqueIndex())
        {
            return 1;
        }
        return super.zEstimateReturnSize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        if (this.usesUniqueIndex())
        {
            return 1;
        }
        return super.zEstimateMaxReturnSize();
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return this;
    }

    public int getEqualityOpCount()
    {
        return 1;
    }

    public void addEqAttributes(List attributeList)
    {
        attributeList.add(this.getAttribute());
    }

    public Extractor getParameterExtractorFor(Attribute attribute)
    {
        return this.getParameterExtractor();
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        Operation combined = ((Operation) op).zCombinedAndWithAtomicEquality(this);
        if (combined == null) combined = new AndOperation(this, op);
        return combined;
    }

    public Operation zCombinedAnd(Operation op)
    {
        return op.zCombinedAndWithAtomicEquality(this);
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return new MultiEqualityOperation(this, op);
        }
        return null;
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return op.zCombinedAndWithAtomicEquality(this);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return op.zCombinedAndWithAtomicEquality(this);
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
    {
        return op.zCombinedAndWithAtomicEquality(this);
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return op.zCombinedAndWithAtomicEquality(this);
    }

    public Object getSourceAttributeValue(SqlQuery query, int sourceNumber, boolean isSelectedObject)
    {
        return this.getParameterAsObject();
    }

    public boolean isSameSourceOperation(SourceOperation other)
    {
        if (other instanceof AtomicEqualityOperation)
        {
            AtomicEqualityOperation aeo = (AtomicEqualityOperation) other;
            return this.getParameterAsObject().equals(aeo.getParameterAsObject());
        }
        return false;
    }

    public int getSourceAttributeValueCount()
    {
        return 1;
    }

    public List applyOperationToPartialCache()
    {
        if (this.usesUniqueIndex())
        {
            List result = this.getByIndex();
            if (result.size() > 0) return result;
        }
        return null;
    }

    public abstract int getParameterHashCode();

    public abstract Object getParameterAsObject();
    
    public abstract boolean parameterValueEquals(Object other, Extractor extractor);

    public void generateSql(SqlQuery query)
    {
        if (!this.getAttribute().isSourceAttribute())
        {
            query.appendWhereClause(this.getAttribute().getFullyQualifiedLeftHandExpression(query));
            query.appendWhereClause("= ?");
            query.addSqlParameterSetter(this);
        }
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        if (this.getAttribute().isSourceAttribute())
        {
            extractor.setSourceOperation(this);
        }
        if (registerEquality) extractor.registerRelatedAttributeEquality(this.getAttribute());
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("=");
        toStringContext.append(this.getParameterAsObject().toString());
    }

    @Override
    public ShapeMatchResult zShapeMatch(Operation existingOperation)
    {
        if (existingOperation instanceof AsOfEdgePointOperation)
        {
            if (existingOperation.equals(this))
            {
                return ExactMatchSmr.INSTANCE;
            }
            return NoMatchSmr.INSTANCE;
        }
        if (existingOperation instanceof AbstractAtomicOperation)
        {
            AbstractAtomicOperation existingAtomic = (AbstractAtomicOperation) existingOperation;
            if (existingAtomic.getAttribute().equals(this.getAttribute()))
            {
                if (existingOperation.getClass() == this.getClass())
                {
                    // this case only matters when this io is not the top level op being evaluated,
                    // but it's part of a bigger op, like and/or.
                    return ExactMatchSmr.INSTANCE;
                }
                else if (this instanceof IsNullOperation)
                {
                    return NoMatchSmr.INSTANCE;// not-in and not-eq imply not-null
                }
                else if (this instanceof AsOfEdgePointOperation)
                {
                    return NoMatchSmr.INSTANCE;
                }
                else if (existingAtomic.matchesWithoutDeleteCheck(this, this.getStaticExtractor()))
                {
                    return new SuperMatchSmr(existingOperation, this);
                }
            }
        }
        else if (existingOperation instanceof AndOperation)
        {
            return ((AndOperation) existingOperation).reverseShapeMatch(this);
        }
        else if (existingOperation instanceof OrOperation)
        {
            return ((OrOperation)existingOperation).oneAtATimeReverseShapeMatch(this);
        }
        // there are complex cases we're ignoring here.
        return NoMatchSmr.INSTANCE;
    }

    @Override
    public int zShapeHash()
    {
        return this.getAttribute().hashCode();
    }

    protected abstract Extractor getStaticExtractor();
}
