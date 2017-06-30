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

package com.gs.fw.common.mithra.finder.asofop;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.paramop.OpWithObjectParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithTimestampParamExtractor;
import com.gs.fw.common.mithra.finder.timestamp.TimestampEqOperation;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.TimestampPool;
import com.gs.reladomo.metadata.PrivateReladomoClassMetaData;


public class AsOfEqOperation extends AtomicEqualityOperation implements SqlParameterSetter, AsOfOperation, Externalizable, OpWithObjectParam
{

    private Timestamp parameter;

    public AsOfEqOperation(AsOfAttribute attribute, Timestamp parameter)
    {
        super(attribute);
        parameter = TimestampPool.getInstance().getOrAddToCache(parameter, false);
        this.parameter = parameter;
    }

    public AsOfEqOperation()
    {
        // for externalizable
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithTimestampParamExtractor.INSTANCE;
    }

    public List applyOperationToFullCache()
    {
        Cache cache = this.getCache();
        IndexReference indexRef = cache.getBestIndexReference(ListFactory.create(this.getAttribute()));
        return indexRef.isValid() ? cache.get(indexRef.indexReference, this.parameter) : null;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        TimestampExtractor asOfAttribute = (TimestampExtractor)extractor;
        return asOfAttribute.timestampValueOf(o).equals(parameter);
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return new MultiEqualityOperation(this, asOfEqOperations);
    }

    protected List getByIndex()
    {
        throw new RuntimeException("should never get here");
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        ((AsOfAttribute)this.getAttribute()).getToAttribute().setSqlParameter(startIndex, pstmt, parameter, query.getTimeZone(), query.getDatabaseType());
        return 1;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        AsOfAttribute attribute = (AsOfAttribute) in.readObject();
        this.setAttribute(attribute);
        this.parameter = attribute.getToAttribute().readFromStream(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(this.getAttribute());
        AsOfAttribute asOfAttribute = (AsOfAttribute) this.getAttribute();
        asOfAttribute.getToAttribute().writeToStream(out, (Timestamp)this.getParameterAsObject());
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (obj instanceof AsOfEqOperation)
        {
            AsOfEqOperation other = (AsOfEqOperation) obj;
            return this.parameter.equals(other.parameter) && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    public Object getParameterAsObject()
    {
        return this.parameter;
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        Object secondValue = extractor.valueOf(other);
        if (this.parameter == secondValue) return true; // takes care of both null

        return (this.parameter != null) && this.parameter.equals(secondValue);
    }

    public Timestamp getParameter()
    {
        return parameter;
    }

    public int populateAsOfDateFromResultSet(MithraDataObject inflatedData, ResultSet rs,
            int resultSetPosition, Timestamp[] asOfDates, int asOfDatePosition,
            ObjectWithMapperStack asOfOperationStack, TimeZone databaseTimeZone, DatabaseType dt) throws SQLException
    {
        asOfDates[asOfDatePosition] = this.getParameter();
        return 0;
    }

    public Timestamp inflateAsOfDate(MithraDataObject inflatedData)
    {
        return this.getParameter();
    }

    public boolean addsToAsOfOperationWhereClause(ObjectWithMapperStack asOfAttributeWithMapperStack, ObjectWithMapperStack asOfOperationStack)
    {
        return true;
    }

    public boolean requiresResultSetToPopulate(ObjectWithMapperStack asOfOperationStack)
    {
        return false;
    }

    public AtomicOperation createAsOfOperationCopy(TemporalAttribute rightAttribute, Operation op)
    {
        if (rightAttribute.isAsOfAttribute())
        {
            return new AsOfEqOperation((AsOfAttribute) rightAttribute, this.parameter);
        }
        return new TimestampEqOperation((TimestampAttribute) rightAttribute, this.parameter);
    }

    @Override
    public int zGetAsOfOperationPriority()
    {
        return 100;
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    public List applyOperationToPartialCache()
    {
        return null;
    }

    @Override
    public int getParameterHashCode()
    {
        return this.parameter.hashCode();
    }

    public void generateSql(SqlQuery query)
    {
		// nothing to do
    }

    public int getClauseCount(SqlQuery query)
    {
        return 2;
    }

    public void generateSql(SqlQuery query, ObjectWithMapperStack attributeWithStack, ObjectWithMapperStack asOfOperationWithStack)
    {
        query.restoreMapperStack(attributeWithStack);
        boolean insertedAnd = query.beginAnd();
        TemporalAttribute temporalAttriute = (TemporalAttribute) attributeWithStack.getObject();
        if (temporalAttriute.isAsOfAttribute())
        {
            AsOfAttribute attribute = (AsOfAttribute) temporalAttriute;
            Timestamp infinity = attribute.getInfinityDate();
            if (this.parameter.equals(infinity))
            {
                query.addSqlParameterSetter(this);
                query.appendWhereClause(attribute.getFullyQualifiedToColumnName(query) + " = ?");
            }
            else
            {
                query.addSqlParameterSetter(this);
                query.addSqlParameterSetter(this); // since we have two clauses
                if (attribute.isToIsInclusive())
                {
                    query.appendWhereClause(attribute.getFullyQualifiedFromColumnName(query) + " < ? and "+
                            attribute.getFullyQualifiedToColumnName(query) + " >= ?");
                }
                else
                {
                    query.appendWhereClause(attribute.getFullyQualifiedFromColumnName(query) + " <= ? and "+
                            attribute.getFullyQualifiedToColumnName(query) + " > ?");
                }
            }
        }
//        else
//        {
//            TimestampAttribute attribute = (TimestampAttribute) temporalAttriute;
//            query.addSqlParameterSetter(this);
//            query.appendWhereClause(attribute.getFullyQualifiedLeftHandExpression(query)+ " = ?");
//        }
        query.endAnd(insertedAnd);
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        checker.registerAsOfAttributes(this.getAttribute().getAsOfAttributes());
        checker.setAsOfEqOperation(this);
    }

    public Operation susbtituteOtherAttribute(Attribute other)
    {
        if (other instanceof TimestampAttribute)
        {
            return new TimestampEqOperation((TimestampAttribute)other, (Timestamp) this.getParameterAsObject());
        }
        if (other instanceof AsOfAttribute)
        {
            return new AsOfEqOperation((AsOfAttribute)other, (Timestamp) this.getParameterAsObject());
        }
        return null;
    }

    public boolean zHasAsOfOperation()
    {
        AsOfAttribute[] asOfAttributes = ((PrivateReladomoClassMetaData)this.getResultObjectPortal().getClassMetaData()).getCachedAsOfAttributes();
        return asOfAttributes != null && asOfAttributes[0].equals(this.getAttribute());
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return asOfAttribute.equals(this.getAttribute()) ? this : null;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("=");
        toStringContext.append("\""+this.getParameterAsObject().toString()+"\"");
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements AsOfExtractor
    {
        public Timestamp timestampValueOf(Object o)
        {
            return getParameter();
        }

        public Object valueOf(Object o)
        {
            return this.timestampValueOf(o);
        }

        @Override
        public long timestampValueOfAsLong(Object o)
        {
            return getParameter().getTime();
        }

        public int valueHashCode(Object o)
        {
            return this.timestampValueOf(o).hashCode();
        }

        public boolean valueEquals(Object first, Object second)
        {
            if (first == second)
            {
                return true;
            }
            boolean firstNull = this.isAttributeNull(first);
            boolean secondNull = this.isAttributeNull(second);
            if (firstNull)
            {
                return secondNull;
            }
            return this.valueOf(first).equals(this.valueOf(second));
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            boolean firstNull = this.isAttributeNull(first);
            boolean secondNull = secondExtractor.isAttributeNull(second);
            if (firstNull != secondNull)
            {
                return false;
            }
            if (!firstNull)
            {
                return this.valueOf(first).equals(secondExtractor.valueOf(second));
            }
            return true;
        }

        public Timestamp getDataSpecificValue(MithraDataObject data)
        {
            return getParameter();
        }

        public boolean dataMatches(Object data, Timestamp asOfDate, AsOfAttribute asOfAttribute)
        {
            return asOfAttribute.dataMatches(data, getParameter());
        }

        public boolean matchesMoreThanOne()
        {
            return false;
        }
    }

}
