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

package com.gs.fw.common.mithra.finder.timestamp;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TemporalAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.asofop.AsOfOperation;
import com.gs.fw.common.mithra.finder.asofop.AsOfTimestampEqualityMapper;
import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;



public class TimestampAsOfEqualityMapper extends EqualityMapper implements AsOfOperation
{

    public TimestampAsOfEqualityMapper(TimestampAttribute left, AsOfAttribute right)
    {
        super(left, right);
        this.setReverseMapper(new AsOfTimestampEqualityMapper((AsOfAttribute) getRight(), (TimestampAttribute) getLeft(), this));
    }

    public TimestampAsOfEqualityMapper(TimestampAttribute left, AsOfAttribute right, EqualityMapper reverseMapper)
    {
        super(left, right);
        this.setReverseMapper(reverseMapper);
    }

    public TimestampAsOfEqualityMapper(TimestampAttribute left, AsOfAttribute right, boolean anonymous)
    {
        this(left, right);
        this.setReverseMapper(new AsOfTimestampEqualityMapper((AsOfAttribute) getRight(), (TimestampAttribute) getLeft(), this));
        this.setAnonymous(anonymous);
    }

    @Override
    public List map(List joinedList)
    {
        //todo: rezaem: implement in memory finders for dated objects
        throw new RuntimeException("in memory looks ups not yet supported for dated objects");
    }

    @Override
    protected List basicMapOne(Attribute right, Object joined, Operation extraLeftOperation)
    {
        Operation operation = ((TimestampAttribute)this.getLeft()).eq(((AsOfAttribute)right).timestampValueOf(joined));
        if (extraLeftOperation != null) operation = operation.and(extraLeftOperation);
        return operation.getResultObjectPortal().zFindInMemoryWithoutAnalysis(operation, true);
    }

    public Operation getOperationFromResult(Object result, Map<Attribute, Object> tempOperationPool)
    {
        AsOfAttribute right = (AsOfAttribute) getRight();
        return right.eq(right.timestampValueOf(result));
    }

    public Operation getOperationFromOriginal(Object original, Map<Attribute, Object> tempOperationPool)
    {
        return ((AsOfAttribute)getRight()).eq(((TimestampAttribute)getLeft()).timestampValueOf(original));
    }

    @Override
    public Operation getPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return ((AsOfAttribute)getRight()).eq(ImmutableTimestamp.ZERO);
    }

    @Override
    public List mapReturnNullIfIncompleteIndexHit(List joinedList)
    {
        //todo: rezaem: implement in memory finders for dated objects
        return null;
    }

    @Override
    protected List basicMap(Attribute right, List joinedList)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected List basicMapReturnNullIfIncompleteIndexHit(Attribute right, List joinedList)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected List basicMapReturnNullIfIncompleteIndexHit(Attribute right, List joinedList, Operation extraOperationOnResult)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void generateSql(SqlQuery query)
    {
        boolean needToGenerate = !(query.isMappedAlready(this));
        query.pushMapper(this);
        if (needToGenerate)
        {
            query.addAsOfAttributeSql();
        }
    }

    public void generateSql(SqlQuery query, ObjectWithMapperStack attributeWithStack, ObjectWithMapperStack asOfOperationWithStack)
    {
        if (!((TemporalAttribute)attributeWithStack.getObject()).isAsOfAttribute())
        {
            return;
        }
        if (attributeWithStack.compareTo(asOfOperationWithStack) < 0)
        {
            generateReverseJoin(query, attributeWithStack, asOfOperationWithStack);
            return;
        }
        String fullyQualifiedColumnName = this.getFullyQualifiedColumnName(query, asOfOperationWithStack);
        MapperStackImpl leftHandStack = (MapperStackImpl) asOfOperationWithStack.getMapperStack().clone();
        leftHandStack.popMapper();
        query.restoreMapperStack(attributeWithStack);
        AsOfAttribute attribute = (AsOfAttribute) attributeWithStack.getObject();
        if (attribute.isToIsInclusive())
        {
            query.generateAsOfJoinSql(leftHandStack, fullyQualifiedColumnName, attribute.getFullyQualifiedFromColumnName(query), ">");
            query.generateAsOfJoinSql(leftHandStack, fullyQualifiedColumnName, attribute.getFullyQualifiedToColumnName(query), "<=");
        }
        else
        {
            query.generateMixedJoinSql(leftHandStack, fullyQualifiedColumnName, attribute.getFullyQualifiedFromColumnName(query),
                    attribute.getFullyQualifiedToColumnName(query), new InfinitySqlParameterSetter(attribute.getInfinityDate()));
        }
    }

    private void generateReverseJoin(SqlQuery query, ObjectWithMapperStack attributeWithStack, ObjectWithMapperStack asOfOperationWithStack)
    {
        query.restoreMapperStack(attributeWithStack);
        AsOfAttribute attribute = (AsOfAttribute) attributeWithStack.getObject();
        String fromColumn = attribute.getFullyQualifiedFromColumnName(query);
        String toColumn = attribute.getFullyQualifiedToColumnName(query);
        query.clearMapperStack();
        query.restoreMapperStack(asOfOperationWithStack);
        query.temporarilyPopMapper();
        String fullyQualifiedColumnName = this.getLeft().getFullyQualifiedLeftHandExpression(query);

        if (attribute.isToIsInclusive())
        {
            query.generateAsOfJoinSql(attributeWithStack.getMapperStack(), fromColumn, fullyQualifiedColumnName, "<");
            query.generateAsOfJoinSql(attributeWithStack.getMapperStack(), toColumn, fullyQualifiedColumnName, ">=");
        }
        else
        {
            query.generateReverseMixedJoinSql(attributeWithStack.getMapperStack(), fullyQualifiedColumnName, fromColumn, toColumn,
                    new InfinitySqlParameterSetter(attribute.getInfinityDate()));
        }
        if (attributeWithStack.getMapperStack().isEmpty())
        {
            query.addExtraJoinColumn(fullyQualifiedColumnName);
        }
    }

    public int populateAsOfDateFromResultSet(MithraDataObject inflatedData, ResultSet rs,
            int resultSetPosition, Timestamp[] asOfDates, int asOfDatePosition,
            ObjectWithMapperStack asOfOperationStack, TimeZone databaseTimeZone, DatabaseType dt) throws SQLException
    {
        TimestampAttribute otherAttribute = (TimestampAttribute) this.getLeft();
        Timestamp timestamp = otherAttribute.zReadTimestampFromResultSet(resultSetPosition, rs, databaseTimeZone, dt);
        AsOfAttribute asOfAttribute = (AsOfAttribute) this.getRight();
        timestamp = asOfAttribute.getToAttribute().zFixPrecisionAndInfinityIfNecessary(timestamp, databaseTimeZone);
        asOfDates[asOfDatePosition] = timestamp;
        return 1;
    }

    public Timestamp inflateAsOfDate(MithraDataObject inflatedData)
    {
        throw new RuntimeException("should not get here");
    }

    public String getFullyQualifiedColumnName(SqlQuery query, ObjectWithMapperStack asOfOperationWithStack)
    {
        query.restoreMapperStack(asOfOperationWithStack);
        query.temporarilyPopMapper();
        String result = this.getLeft().getFullyQualifiedLeftHandExpression(query);
        query.clearMapperStack();
        return result;
    }

    public boolean addsToAsOfOperationWhereClause(ObjectWithMapperStack asOfAttributeWithMapperStack, ObjectWithMapperStack asOfOperationStack)
    {
        return true;
    }

    public boolean requiresResultSetToPopulate(ObjectWithMapperStack asOfOperationStack)
    {
        return true;
    }

    public AtomicOperation createAsOfOperationCopy(TemporalAttribute rightAttribute, Operation op)
    {
        return null;
    }

    @Override
    public int zGetAsOfOperationPriority()
    {
        return 50;
    }

    @Override
    protected void registerLeftAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        super.registerLeftAsOfAttributesAndOperations(checker);
        checker.registerTimestampTemporalAttribute(checker.constructWithMapperStack(this.getLeft()));
    }

    @Override
    protected void registerRightAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        super.registerRightAsOfAttributesAndOperations(checker);
        checker.setEqualityAsOfOperation((TemporalAttribute) this.getLeft(), (TemporalAttribute) this.getRight());
        checker.setAsOfOperation(checker.constructWithMapperStack(this.getRight()), this);
    }

    @Override
    protected Mapper substituteNewLeft(Attribute newLeft)
    {
        return new TimestampAsOfEqualityMapper((TimestampAttribute)newLeft, (AsOfAttribute) this.getRight());
    }

    public boolean isRightHandPartialCacheResolvable()
    {
        return false;
    }

    private static class InfinitySqlParameterSetter implements SqlParameterSetter
    {
        private Timestamp infinity;

        public InfinitySqlParameterSetter(Timestamp infinityDate)
        {
            this.infinity = infinityDate;
        }

        @Override
        public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
        {
            pstmt.setTimestamp(startIndex, this.infinity);
            startIndex++;
            pstmt.setTimestamp(startIndex, this.infinity);
            return 2;
        }
    }
}
