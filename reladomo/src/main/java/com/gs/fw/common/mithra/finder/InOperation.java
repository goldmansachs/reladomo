
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.PersisterId;

public abstract class InOperation extends AtomicSetBasedOperation implements OperationWithParameterExtractor
{

    public InOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected InOperation()
    {
        // for Externalizable
    }

    @Override
    protected boolean isNot()
    {
        return false;
    }

    /**
     * @deprecated this method does not do anything anymore. if you need a between clause, add one with greaterThan and lessThan
     * @param useBetweenClause not meaningful
     */
    public void setUseBetweenClause(boolean useBetweenClause)
    {
    }

    /* in operations use an immutable index multiple times, which can return partial results */
    public boolean usesImmutableUniqueIndex()
    {
        return false;
    }

    @Override
    public int zEstimateReturnSize()
    {
        if (this.isIndexed())
        {
            return this.getCache().getAverageReturnSize(this.getIndexRef(), this.getSetSize());
        }
        else return this.getCache().estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        if (this.isIndexed())
        {
            return this.getCache().getMaxReturnSize(this.getIndexRef(), this.getSetSize());
        }
        else return this.getCache().estimateQuerySize();
    }

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        return op.zCombinedAndWithIn(this);
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

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        Operation combined = ((Operation) op).zCombinedAndWithIn(this);
        if (combined == null) combined = new AndOperation(this, op);
        return combined;
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return op.zCombinedAndWithIn(this);
    }

    public List applyOperationToPartialCache()
    {
        if (this.usesUniqueIndex())
        {
            List result = this.getByIndex();
            if (result.size() == this.getSetSize()) return result;
        }
        return null;
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return new MultiEqualityOperation(this, op);
        }
        return null;
    }

    public int getClauseCount(SqlQuery query)
    {
        if (this.getAttribute().isSourceAttribute()) return 0;
        return this.getSetSize();
    }

    public void writeExternalForSublcass(ObjectOutput out) throws IOException
    {
        out.writeObject(this.getAttribute());
        out.writeBoolean(false); //legacy
    }

    public void readExternalForSublcass(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setAttribute((Attribute) in.readObject());
        in.readBoolean();
    }

    public boolean maySplit()
    {
        return true;
    }

    public void generateTupleTempContextJoinSql(SqlQuery sqlQuery, TupleTempContext tempContext, Object source, PersisterId persisterId, int position, boolean inOrClause)
    {
        String fullyQualifiedColumnName = this.getAttribute().getFullyQualifiedLeftHandExpression(sqlQuery);
        String tempTableName = tempContext.getFullyQualifiedTableName(source, persisterId);
        if (inOrClause)
        {
            sqlQuery.addTupleTempContextJoin(tempTableName, false,
                                fullyQualifiedColumnName + " = "+ tempTableName +".c0",
                                fullyQualifiedColumnName + " in ( )", tempTableName +".c0"+" is not null", position);
        }
        else
        {
            sqlQuery.addTupleTempContextJoin(tempTableName, true,
                    fullyQualifiedColumnName + " = "+ tempTableName +".c0",
                    fullyQualifiedColumnName + " in ( )", "", position);
        }
    }

    @Override
    public ShapeMatchResult zShapeMatch(Operation existingOperation)
    {
        if (this.getSetSize() < MAX_SHAPE_MATCH_SIZE)
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
                        if (this.setContains(existingOperation, ((AtomicNotEqualityOperation) existingOperation).getStaticExtractor()))
                        {
                            return NoMatchSmr.INSTANCE;
                        }
                        return new SuperMatchSmr(existingOperation, this);
                    }
                    else if (existingOperation instanceof AtomicSetBasedOperation)
                    {
                        if (existingOperation instanceof InOperation && ((InOperation) existingOperation).getSetSize() >= this.getSetSize())
                        {
                            return this.shapeMatchSet((InOperation)existingOperation);
                        }
                    }
                    else if (existingOperation instanceof RangeOperation)
                    {
                        // too hard to loop here. ignoring for now.
                        return NoMatchSmr.INSTANCE;
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
            //ignoring more complex cases for now.
        }
        return NoMatchSmr.INSTANCE;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        return !extractor.isAttributeNull(o) && this.setContains(o, extractor);
    }

    protected abstract ShapeMatchResult shapeMatchSet(InOperation existingOperation);
}
