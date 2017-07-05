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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TemporalAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.reladomo.metadata.PrivateReladomoClassMetaData;


public class AsOfEdgePointOperation extends AtomicEqualityOperation implements AsOfOperation
{

    private Attribute edgeAttribute;

    public AsOfEdgePointOperation(AsOfAttribute asOfAttribute, Attribute edgeAttribute)
    {
        super(asOfAttribute);
        this.edgeAttribute = edgeAttribute;
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        throw new RuntimeException("should not get here");
    }

    public Attribute getEdgeAttribute()
    {
        return edgeAttribute;
    }

    public Operation susbtituteOtherAttribute(Attribute other)
    {
        return null; // no substitution allowed!
    }

    public AsOfAttribute getAsOfAttribute()
    {
        return (AsOfAttribute) this.getAttribute();
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
        return false;
    }

    @Override
    public int zEstimateReturnSize()
    {
        return this.getCache().size();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        return this.getCache().size();
    }

    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), this, new Extractor[] {new ParameterExtractor()}, true);
    }

    public List applyOperationToFullCache()
    {
        Cache cache = this.getCache();
        IndexReference indexRef = cache.getBestIndexReference(ListFactory.create(this.getAttribute()));
        return indexRef.isValid() ? cache.get(indexRef.indexReference, this, new Extractor[] { new ParameterExtractor() }, true) : null;
    }

    public List applyOperationToPartialCache()
    {
        return null;
    }

    public int getClauseCount(SqlQuery query)
    {
        return 2; // worst case scenario
    }

   public void generateSql(SqlQuery query, ObjectWithMapperStack attributeWithStack, ObjectWithMapperStack asOfOperationWithStack)
    {
        if (!((TemporalAttribute)attributeWithStack.getObject()).isAsOfAttribute())
        {
            return;
        }
//        todo: add extra columns to select and remove distinct
        if (attributeWithStack.compareTo(asOfOperationWithStack) < 0)
        {
            generateReverseJoin(query, attributeWithStack, asOfOperationWithStack);
            return;
        }

        String fullyQualifiedColumnName = this.getFullyQualifiedColumnName(query, asOfOperationWithStack);
        query.restoreMapperStack(attributeWithStack);
        boolean insertedAnd = query.beginAnd();
        AsOfAttribute attribute = (AsOfAttribute) attributeWithStack.getObject();
        String toColumn = attribute.getFullyQualifiedToColumnName(query);
        String fromColumn = attribute.getFullyQualifiedFromColumnName(query);
        if (attribute.isToIsInclusive())
        {
            query.generateAsOfJoinSql(asOfOperationWithStack.getMapperStack(), fullyQualifiedColumnName, fromColumn, ">");
            query.generateAsOfJoinSql(asOfOperationWithStack.getMapperStack(), fullyQualifiedColumnName, toColumn, "<=");
        }
        else
        {
            if (this.getAsOfAttribute().isToIsInclusive()) // opposite signs in a relationship... blech
            {
                query.generateMixedJoinSql(asOfOperationWithStack.getMapperStack(), fullyQualifiedColumnName, fromColumn,
                        toColumn, new AsOfEdgePointInfinityParameterSetter(attribute, this.getAsOfAttribute()));
            }
            else
            {
                query.generateAsOfJoinSql(asOfOperationWithStack.getMapperStack(), fullyQualifiedColumnName, fromColumn, ">=");
                query.generateAsOfJoinSql(asOfOperationWithStack.getMapperStack(), fullyQualifiedColumnName, toColumn, "<");
            }
        }
        query.endAnd(insertedAnd);
    }

    private void generateReverseJoin(SqlQuery query, ObjectWithMapperStack attributeWithStack, ObjectWithMapperStack asOfOperationWithStack)
    {
        query.restoreMapperStack(attributeWithStack);
        AsOfAttribute attribute = (AsOfAttribute) attributeWithStack.getObject();
        String fromColumn = attribute.getFullyQualifiedFromColumnName(query);
        String toColumnName = attribute.getFullyQualifiedToColumnName(query);
        query.clearMapperStack();
        query.restoreMapperStack(asOfOperationWithStack);
        String fullyQualifiedColumnName = this.getEdgeAttribute().getFullyQualifiedLeftHandExpression(query);

        boolean insertedAnd = query.beginAnd();
        if (attribute.isToIsInclusive())
        {
            query.generateAsOfJoinSql(attributeWithStack.getMapperStack(), fromColumn, fullyQualifiedColumnName, "<");
            query.generateAsOfJoinSql(attributeWithStack.getMapperStack(), toColumnName, fullyQualifiedColumnName, ">=");
        }
        else
        {
            if (this.getAsOfAttribute().isToIsInclusive()) // opposite signs in a relationship... blech
            {
                query.generateReverseMixedJoinSql(attributeWithStack.getMapperStack(), fullyQualifiedColumnName, fromColumn,
                        toColumnName, new AsOfEdgePointInfinityParameterSetter(attribute, this.getAsOfAttribute()));
            }
            else
            {
                query.generateAsOfJoinSql(attributeWithStack.getMapperStack(), fromColumn, fullyQualifiedColumnName, "<=");
                query.generateAsOfJoinSql(attributeWithStack.getMapperStack(), toColumnName, fullyQualifiedColumnName, ">");
            }
        }
        query.endAnd(insertedAnd);
        if (attributeWithStack.getMapperStack().isEmpty())
        {
            query.addExtraJoinColumn(fullyQualifiedColumnName);
        }
    }

    public String getFullyQualifiedColumnName(SqlQuery query, ObjectWithMapperStack asOfOperationWithStack)
    {
        query.restoreMapperStack(asOfOperationWithStack);
        String result = this.getEdgeAttribute().getFullyQualifiedLeftHandExpression(query);
        query.clearMapperStack();
        return result;
    }

    public void generateSql(SqlQuery query)
    {
        // nothing to do
    }

    public boolean addsToAsOfOperationWhereClause(ObjectWithMapperStack asOfAttributeWithMapperStack, ObjectWithMapperStack asOfOperationStack)
    {
        return !asOfOperationStack.getMapperStack().equals(asOfAttributeWithMapperStack.getMapperStack());
    }

    public boolean requiresResultSetToPopulate(ObjectWithMapperStack asOfOperationStack)
    {
        return !asOfOperationStack.getMapperStack().isEmpty();
    }

    public AtomicOperation createAsOfOperationCopy(TemporalAttribute rightAttribute, Operation op)
    {
        return null;
    }

    @Override
    public int zGetAsOfOperationPriority()
    {
        return 20;
    }

    public Timestamp inflateAsOfDate(MithraDataObject inflatedData)
    {
        return (Timestamp) this.getEdgeAttribute().valueOf(inflatedData);
    }

    public int populateAsOfDateFromResultSet(MithraDataObject inflatedData, ResultSet rs,
            int resultSetPosition, Timestamp[] asOfDates, int asOfDatePosition,
            ObjectWithMapperStack asOfOperationStack, TimeZone databaseTimeZone, DatabaseType dt) throws SQLException
    {
        if (asOfOperationStack.getMapperStack().isEmpty())
        {
            asOfDates[asOfDatePosition] = (Timestamp) this.getEdgeAttribute().valueOf(inflatedData);
            return 0;
        }
        else
        {
            TimestampAttribute edgeAttribute = (TimestampAttribute) this.getEdgeAttribute();
            Timestamp timestamp = edgeAttribute.zReadTimestampFromResultSet(resultSetPosition, rs, databaseTimeZone, dt);
            asOfDates[asOfDatePosition] = timestamp;
            return 1;
        }
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        return 0;
    }

    @Override
    public boolean zIsShapeCachable()
    {
        return false;
    }

    @Override
    public Boolean matches(Object o)
    {
        return true;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        return false;
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (obj instanceof AsOfEdgePointOperation)
        {
            return this.getEdgeAttribute().equals(((AsOfEdgePointOperation)obj).getEdgeAttribute());
        }
        return false;
    }

    public int hashCode()
    {
        return this.getEdgeAttribute().hashCode() ^ 0x76210381;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        checker.registerAsOfAttributes(this.getAttribute().getAsOfAttributes());
        checker.setAsOfEdgePointOperation(this);
    }

    @Override
    public int getParameterHashCode()
    {
        return 0x76210381; // this is a dummy value. this method won't be called.
    }

    public Object getParameterAsObject()
    {
        return null;
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        throw new RuntimeException("not implemented");
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        // do nothing
    }

    public boolean zHasAsOfOperation()
    {
        AsOfAttribute[] asOfAttributes = ((PrivateReladomoClassMetaData)this.getResultObjectPortal().getClassMetaData()).getCachedAsOfAttributes();
        return asOfAttributes != null && asOfAttributes[0].equals(this.getAsOfAttribute());
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return asOfAttribute.equals(this.getAttribute()) ? this : null;
    }

    @Override
    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("equalsEdgePoint");
    }

    private static final class AsOfEdgePointInfinityParameterSetter implements SqlParameterSetter
    {
        private AsOfAttribute first;
        private AsOfAttribute second;

        public AsOfEdgePointInfinityParameterSetter(AsOfAttribute first, AsOfAttribute second)
        {
            this.first = first;
            this.second = second;
        }

        public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
        {
            pstmt.setTimestamp(startIndex, this.first.getInfinityDate());
            startIndex++;
            pstmt.setTimestamp(startIndex, this.second.getInfinityDate());
            return 2;
        }
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements AsOfExtractor
    {
        public Timestamp timestampValueOf(Object o)
        {
            return null;
        }

        @Override
        public long timestampValueOfAsLong(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public Object valueOf(Object o)
        {
            throw new RuntimeException("not expected to get here");
        }

        public int valueHashCode(Object o)
        {
            throw new RuntimeException("not expected to get here");
        }

        public boolean valueEquals(Object first, Object second)
        {
            throw new RuntimeException("not expected to get here");
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            throw new RuntimeException("not expected to get here");
        }

        public Timestamp getDataSpecificValue(MithraDataObject data)
        {
            return (Timestamp) getEdgeAttribute().valueOf(data);
        }

        public boolean dataMatches(Object data, Timestamp asOfDate, AsOfAttribute asOfAttribute)
        {
            return true;
        }

        public boolean matchesMoreThanOne()
        {
            return true;
        }
    }
}
