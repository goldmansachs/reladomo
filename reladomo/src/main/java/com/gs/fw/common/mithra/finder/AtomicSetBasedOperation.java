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

import java.math.*;
import java.sql.*;
import java.util.Date;
import java.util.*;

import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.databasetype.*;
import com.gs.fw.common.mithra.tempobject.*;
import com.gs.fw.common.mithra.util.*;
import com.gs.fw.common.mithra.util.Time;


public abstract class AtomicSetBasedOperation extends AbstractAtomicOperation implements SetBasedAtomicOperation
{
    private static final long serialVersionUID = 3420203156804055576L;

    public static final int IN_CLAUSE_BULK_INSERT_THRESHOLD = 1000;
    private static final int POSSIBLE_SPLIT_THRESHOLD = 10;

    protected AtomicSetBasedOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected AtomicSetBasedOperation()
    {
    }

    protected abstract boolean isNot();

    public void generateSql(SqlQuery query)
    {
        if (this.getAttribute().isSourceAttribute()) return;
        String fullyQualifiedColumnName = this.getAttribute().getFullyQualifiedLeftHandExpression(query);
        query.appendWhereClause(fullyQualifiedColumnName);
        if (isNot()) query.appendWhereClause("not");
        query.appendWhereClause("in (");
        if (query.mayNeedToSplit() && this.getSetSize() > POSSIBLE_SPLIT_THRESHOLD)
        {
            query.appendWhereClause(")");
            query.setSetBasedClausePosition(this);
        }
        else
        {
            StringBuilder buffer = new StringBuilder(this.getSetSize() * 2);
            for (int i = 0; i < this.getSetSize() - 1; i++)
            {
                buffer.append("?,");
            }
            buffer.append("?)");
            query.appendWhereClause(buffer);
        }
        query.addSqlParameterSetter(this);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        int start = 0;
        int valuesSet = 0;
        int numberToSet = this.getSetSize();
        populateCopiedArray();
        if (query.hasChunkedUnions(this))
        {
            int union = query.getCurrentUnionNumber();
            int numberOfChunksPerIn = query.getNumberOfChunksPerIn();
            int numberOfQuestions = this.getSetSize() / numberOfChunksPerIn;
            if ((this.getSetSize() % numberOfChunksPerIn) > 0) numberOfQuestions++;
            start = numberOfQuestions * union;
            numberToSet = numberOfQuestions;
            if (start + numberToSet > this.getSetSize()) numberToSet = this.getSetSize() - start;
        }
        if (!query.isSubSelectInstead(this))
        {
            valuesSet += this.setSqlParameters(pstmt, startIndex+valuesSet, query.getTimeZone(), start, numberToSet, query.getDatabaseType());
        }
        return valuesSet;
    }

    public String getSubSelectStringForTupleTempContext(TupleTempContext tempContext, Object source, PersisterId persisterId)
    {
        return "select c0 from "+tempContext.getFullyQualifiedTableName(source, persisterId);
    }

    protected abstract int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException;

    public abstract int getSetSize();

    protected abstract void populateCopiedArray();

    public boolean getSetValueAsBoolean(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public byte getSetValueAsByte(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public byte[] getSetValueAsByteArray(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public BigDecimal getSetValueAsBigDecimal(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public char getSetValueAsChar(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public Date getSetValueAsDate(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public Time getSetValueAsTime(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public double getSetValueAsDouble(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public float getSetValueAsFloat(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public int getSetValueAsInt(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public long getSetValueAsLong(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public short getSetValueAsShort(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public String getSetValueAsString(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public Timestamp getSetValueAsTimestamp(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public TupleTempContext createTempContextAndInsert(SqlQuery query)
    {
        this.populateCopiedArray();
        Attribute[] prototypeAttributes;
        Attribute sourceAttr = query.getAnalyzedOperation().getOriginalOperation().getResultObjectPortal().getFinder().getSourceAttribute();
        Object source = null;
        int[] maxLengths;
        if (sourceAttr != null)
        {
            prototypeAttributes = new Attribute[2];
            maxLengths = new int[2];
            prototypeAttributes[1] = sourceAttr;
            source = query.getSourceAttributeValueForCurrentSource();
        }
        else
        {
            prototypeAttributes = new Attribute[1];
            maxLengths = new int[1];
        }
        prototypeAttributes[0] = this.getAttribute();
        maxLengths[0] = this.getMaxLength();
        TupleTempContext tempContext = new TupleTempContext(prototypeAttributes, sourceAttr, maxLengths, true);
        try
        {
            tempContext.insert(this, query.getAnalyzedOperation().getOriginalOperation().getResultObjectPortal(),
                    IN_CLAUSE_BULK_INSERT_THRESHOLD, source, query.isParallel());
        }
        catch (RuntimeException e)
        {
            tempContext.destroy();
            throw e;
        }
        return tempContext;
    }

    protected int getMaxLength()
    {
        return 0;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        if (isNot()) toStringContext.append("not");
        toStringContext.append("in");
        this.appendSetToString(toStringContext);
    }

    protected abstract void appendSetToString(ToStringContext toStringContext);
}
