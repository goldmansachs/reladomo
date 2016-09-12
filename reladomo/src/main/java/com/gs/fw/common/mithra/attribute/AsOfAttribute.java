
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

package com.gs.fw.common.mithra.attribute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.AggregateAttribute;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.aggregate.attribute.IntegerAggregateAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.extractor.asm.AsOfAttributeExtractorWriter;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.EqualityMapper;
import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.asofop.AsOfEdgePointOperation;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqualityMapper;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfTimestampEqualityMapper;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import java.util.Set;

public abstract class AsOfAttribute<T> extends Attribute<T, Timestamp> implements com.gs.fw.finder.attribute.AsOfAttribute<T>, AsOfExtractor<T>, TemporalAttribute
{

    private static final long serialVersionUID = -7111780760817354743L;
    private static final AsOfAttributeExtractorWriter extractorWriter = new AsOfAttributeExtractorWriter();

    private transient TimestampAttribute fromAttribute;
    private transient TimestampAttribute toAttribute;

    private transient Timestamp infinityDate;
    private transient long infinityTime;
    private transient boolean futureExpiringRowsExist;
    private transient boolean toIsInclusive;
    private transient Timestamp defaultDate;
    private transient String attributeName;
    private transient boolean isProcessingDate;

    protected AsOfAttribute(String attributeName, String busClassNameWithDots, String busClassName, boolean isNullable,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties, boolean transactional, boolean isOptimistic,
            TimestampAttribute fromAttribute, TimestampAttribute toAttribute, Timestamp infinityDate,
            boolean futureExpiringRowsExist, boolean toIsInclusive, Timestamp defaultDate, boolean isProcessingDate)
    {
        this.fromAttribute = fromAttribute;
        this.toAttribute = toAttribute;
        this.infinityDate = infinityDate;
        this.infinityTime = infinityDate.getTime();
        this.futureExpiringRowsExist = futureExpiringRowsExist;
        this.toIsInclusive = toIsInclusive;
        this.defaultDate = defaultDate;
        this.attributeName = attributeName;
        this.isProcessingDate = isProcessingDate;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullable, relatedFinder, properties, transactional);
    }

    public boolean isInfinityNull()
    {
        return false;
    }

    protected AsOfAttribute() {}

    public boolean isProcessingDate()
    {
        return isProcessingDate;
    }

    protected byte getPrecision()
    {
        return this.fromAttribute.getPrecision();
    }

    @Override
    public long timestampValueOfAsLong(T o)
    {
        return this.timestampValueOf(o).getTime();
    }

    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeObject(this.valueOf(o));
    }

    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setValue(o, (Timestamp) in.readObject());
    }

    public TimestampAttribute getFromAttribute()
    {
        return fromAttribute;
    }

    public TimestampAttribute getToAttribute()
    {
        return toAttribute;
    }

    public Timestamp getInfinityDate()
    {
        return infinityDate;
    }

    public boolean isFutureExpiringRowsExist()
    {
        return futureExpiringRowsExist;
    }

    public boolean isToIsInclusive()
    {
        return toIsInclusive;
    }

    public Timestamp getDefaultDate()
    {
        return defaultDate;
    }

    public String getFullyQualifiedFromColumnName(SqlQuery query)
    {
        return this.fromAttribute.getFullyQualifiedLeftHandExpression(query);
    }

    public String getFullyQualifiedToColumnName(SqlQuery query)
    {
        return this.toAttribute.getFullyQualifiedLeftHandExpression(query);
    }

    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq((Timestamp)other);
    }

    public Operation eq(Timestamp asOf)
    {
        if (asOf == null)
        {
            return new None(this);
        }
        return new AsOfEqOperation(this, asOf);
    }

    public Operation eq(Date other)
    {
        if (other == null)
        {
            return new None(this);
        }
        return this.eq(new ImmutableTimestamp(other.getTime()));
    }

    public Operation equalsEdgePoint()
    {
        TimestampAttribute edge = this.getToAttribute();
        if (!this.isToIsInclusive())
        {
            edge = this.getFromAttribute();
        }
        return new AsOfEdgePointOperation(this, edge);
    }

    public Operation equalsInfinity()
    {
        return this.eq(this.getInfinityDate());
    }

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    public int valueHashCode(T o)
    {
        return this.timestampValueOf(o).hashCode();
    }

    public boolean valueEquals(T first, T second)
    {
        return this.timestampValueOf(first).equals(this.timestampValueOf(second));
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Timestamp> secondExtractor)
    {
        return this.timestampValueOf(first).equals(secondExtractor.valueOf(second));
    }

    public OrderBy ascendingOrderBy()
    {
        throw new RuntimeException("cannot order by as of attribute!");
    }

    public OrderBy descendingOrderBy()
    {
        throw new RuntimeException("cannot order by as of attribute!");
    }

    public String getAttributeName()
    {
        return attributeName;
    }

    public Class valueType()
    {
        return Timestamp.class;
    }

    public EqualityMapper constructEqualityMapper(Attribute right)
    {
        if (right instanceof AsOfAttribute)
        {
            return new AsOfEqualityMapper(this, (AsOfAttribute) right);
        }
        else
        {
            return new AsOfTimestampEqualityMapper(this, (TimestampAttribute) right);
        }
    }

    public EqualityMapper constructEqualityMapper(TimestampAttribute right)
    {
        return new AsOfTimestampEqualityMapper(this, right);
    }

    public Operation eq(AsOfAttribute other)
    {
        return new MappedOperation(new AsOfEqualityMapper(this, other), new All(other));
    }

    public Operation eq(TimestampAttribute other)
    {
        return new MappedOperation(new AsOfTimestampEqualityMapper(this, other), new All(other));
    }

    public void setValue(T o, Timestamp newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setTimestampValue(T o, Timestamp newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(T o, Timestamp newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public Timestamp valueOf(T anObject)
    {
        return this.timestampValueOf(anObject);
    }

    public boolean dataMatches(Object data, Timestamp asOfDate)
    {
        long asOfDateLong = asOfDate.getTime();
        if (!(asOfDateLong == infinityTime || toIsInclusive))
        {
            asOfDateLong++;
        }
        return this.fromAttribute.timestampValueOfAsLong(data) < asOfDateLong && this.toAttribute.timestampValueOfAsLong(data) >= asOfDateLong;
    }

    public boolean dataMatches(Object data, Timestamp asOfDate, AsOfAttribute asOfAttribute)
    {
        return asOfAttribute.dataMatches(data, asOfDate);
    }

    public boolean hasRangeOverlap(MithraDataObject data, long start, long end)
    {
        long dataStart = this.getFromAttribute().timestampValueOfAsLong(data);
        long dataEnd = this.getToAttribute().timestampValueOfAsLong(data);

        return !(end <= dataStart || start >= dataEnd);
    }

    public boolean asOfDateMatchesRange(Timestamp asOfDate, Timestamp from, Timestamp to)
    {
        if (asOfDate.equals(this.getInfinityDate()))
        {
            return to.equals(this.getInfinityDate());
        }
        else
        {
            if (to.after(asOfDate) || (this.isToIsInclusive() && to.equals(asOfDate)))
            {
                return from.before(asOfDate) || (!this.isToIsInclusive() && from.equals(asOfDate));
            }
        }
        return false;
    }

    public AsOfAttribute getCompatibleAsOfAttribute(AsOfAttribute[] others)
    {
        for (int i = 0; i < others.length; i++)
        {
            if (others[i].attributeName.equals(this.attributeName))
            {
                return others[i];
            }
        }
        return null;
    }

    public boolean isAsOfAttribute()
    {
        return true;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        TimestampExtractor timestampExtractor = (TimestampExtractor) extractor;
        Timestamp value = null;
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Timestamp other = timestampExtractor.timestampValueOf(objects.get(i));
            if (value == null)
            {
                value = other;
            }
            else
            {
                if (other != null && !other.equals(value))
                {
                    throw new RuntimeException("only one as of attribute value is supported");
                }
            }
        }
        if (value == null)
        {
            throw new RuntimeException("must have at least one non-null as of attribute value");
        }
        return this.eq(value);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final TimestampExtractor timestampExtractor = (TimestampExtractor) extractor;
        Timestamp value = null;
        for (Object object : objects)
        {
            final Timestamp other = timestampExtractor.timestampValueOf(object);
            if (value == null)
            {
                value = other;
            }
            else
            {
                if (other != null && !other.equals(value))
                {
                    throw new RuntimeException("only one as of attribute value is supported");
                }
            }
        }
        if (value == null)
        {
            throw new RuntimeException("must have at least one non-null as of attribute value");
        }
        return this.eq(value);
    }

    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        TimestampExtractor timestampExtractor = (TimestampExtractor) extractor;
        Timestamp value = null;
        for(int i=0;i<objects.size();i++)
        {
            Timestamp other = timestampExtractor.timestampValueOf(objects.get(i));
            if (value == null)
            {
                value = other;
            }
            else
            {
                if (other != null && !other.equals(value))
                {
                    return new None(this);
                }
            }
        }
        if (value == null)
        {
            throw new RuntimeException("must have at least one non-null as of attribute value");
        }
        return this.eq(value);
    }

    public int appendWhereClauseForValue(Timestamp value, StringBuffer whereClause)
    {
        Timestamp infinity = this.getInfinityDate();
        if (value.equals(infinity))
        {
            whereClause.append(this.getToAttribute().getColumnName());
            whereClause.append(" = ?");
            return 1;
        }
        else
        {
            if (this.isToIsInclusive())
            {
                whereClause.append(this.getFromAttribute().getColumnName());
                whereClause.append(" < ? and ");
                whereClause.append(this.getToAttribute().getColumnName());
                whereClause.append(" >= ?");
            }
            else
            {
                whereClause.append(this.getFromAttribute().getColumnName());
                whereClause.append(" <= ? and ");
                whereClause.append(this.getToAttribute().getColumnName());
                whereClause.append(" > ?");
            }
            return 2;
        }
    }

    public void appendInfinityWhereClause(StringBuffer whereClause)
    {
        whereClause.append(this.getToAttribute().getColumnName());
        whereClause.append(" = ?");
    }

    public int appendWhereClauseForRange(Timestamp start, Timestamp end, StringBuffer whereClause)
    {
        int numParams = 1;
        if (!end.equals(this.getInfinityDate()))
        {
            whereClause.append(this.getFromAttribute().getColumnName());
            // note: this would be <= if we wanted to stich
            whereClause.append(" < ? and ");
            numParams = 2;
        }
            whereClause.append(this.getToAttribute().getColumnName());
            // note: this would be >= if we wanted to stich
            whereClause.append(" > ?");
        return numParams;
    }

    public Timestamp getDataSpecificValue(MithraDataObject data)
    {
        throw new RuntimeException("should never get here");
    }

    public boolean matchesMoreThanOne()
    {
        return false;
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        throw new RuntimeException("should never get here");
    }

    public AggregateAttribute min()
    {
        throw new RuntimeException("not implemented");
    }

    public AggregateAttribute max()
    {
        throw new RuntimeException("not implemented");
    }

    public IntegerAggregateAttribute count()
    {
        throw new RuntimeException("not implemented");
    }

    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        throw new RuntimeException("not implemented");
    }

    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a AsOfAttribute");
    }

    public void parseStringAndSet(String value, Object data, int lineNumber, Format format) throws ParseException
    {
        throw new RuntimeException("method parseStringAndSet() can not be called on a AsOfAttribute");
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        throw new RuntimeException("should never get here");
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        throw new RuntimeException("should never get here");
    }

    public void setUntil(Object o, Timestamp newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("cannot set an as of attribute until");
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public static boolean isMilestoningOverlap(Object obj1, Object obj2, AsOfAttribute[] asOfAttributes)
    {
        boolean overlap = true;
        for (AsOfAttribute asOfAttribute : asOfAttributes)
        {
            overlap = overlap && isOverlap(obj1, obj2, asOfAttribute.getFromAttribute(), asOfAttribute.getToAttribute());
        }

        return overlap;
    }

    public static boolean isMilestoningValid(Object obj, AsOfAttribute[] asOfAttributes)
    {
        for (AsOfAttribute asOfAttribute : asOfAttributes)
        {
            Timestamp from = asOfAttribute.getFromAttribute().valueOf(obj);
            Timestamp to = asOfAttribute.getToAttribute().valueOf(obj);
            if (from.equals(to) || from.after(to))
            {
                return false;
            }
        }
        return true;
    }

    private static boolean isOverlap(Object obj1, Object obj2, TimestampAttribute from, TimestampAttribute to)
    {
        return from.valueOf(obj1).before(to.valueOf(obj2)) &&
                from.valueOf(obj2).before(to.valueOf(obj1));
    }

    public static AsOfAttribute generate(String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic,TimestampAttribute fromAttribute, TimestampAttribute toAttribute, Timestamp infinityDate,
            boolean futureExpiringRowsExist, boolean toIsInclusive, Timestamp defaultDate, boolean isProcessingDate, boolean isInfinityNull)
    {
        AsOfAttribute e = null;
        try
        {
            e = (AsOfAttribute) extractorWriter.createClass(attributeName,
                    busClassName, isInfinityNull).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.fromAttribute = fromAttribute;
        e.toAttribute = toAttribute;
        e.infinityDate = infinityDate;
        e.infinityTime = infinityDate.getTime();
        e.futureExpiringRowsExist = futureExpiringRowsExist;
        e.toIsInclusive = toIsInclusive;
        e.defaultDate = defaultDate;
        e.attributeName = attributeName;
        e.isProcessingDate = isProcessingDate;
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        return e;
    }

    @Override
    public void zAddDepenedentAttributesToSet(Set set)
    {
        super.zAddDepenedentAttributesToSet(set);
        set.add(fromAttribute);
        set.add(toAttribute);
    }

    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        Timestamp timestamp = this.timestampValueOf(result);
        return createOrGetOperation(tempOperationPool, this, timestamp);
    }

    private Operation createOrGetOperation(Map<Attribute, Object> tempOperationPool, AsOfAttribute right, Timestamp timestamp)
    {
        Map<Timestamp, Operation> values = (Map<Timestamp, Operation>) tempOperationPool.get(right);
        Operation existing = null;
        if (values == null)
        {
            values = new UnifiedMap();
            tempOperationPool.put(right, values);
        }
        else
        {
            existing = values.get(timestamp);
        }
        if (existing == null)
        {
            existing = right.eq(timestamp);
            if (values.size() < 100) values.put(timestamp, existing);
        }
        return existing;
    }

    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        return createOrGetOperation(tempOperationPool, this, ((AsOfAttribute)left).timestampValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(new ImmutableTimestamp(0));
    }

    @Override
    public Timestamp timestampValueOf(T o)
    {
        throw new RuntimeException("not implemented");
    }
}
