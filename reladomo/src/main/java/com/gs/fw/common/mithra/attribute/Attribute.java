
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

import com.gs.collections.api.block.function.Function;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.aggregate.attribute.IntegerAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.CountCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OffHeapableExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.Nullable;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public abstract class Attribute<Owner, V> implements com.gs.fw.finder.Attribute<Owner>, UpdateCountHolder, Extractor<Owner, V>, OffHeapableExtractor<Owner, V>, Serializable, Cloneable
{
    private transient UpdateCountHolder updateCountHolder = new UpdateCountHolderImpl();
    private transient RelatedFinder relatedFinder;
    private transient String attributeName;
    private transient final AttributeMetaData metaData = new AttributeMetaData(this);
    private transient String owningRelationshipName;
    private transient String owningReverseRelationshipOwnerPackage;
    private transient String owningReverseRelationshipOwner;
    private transient String owningReverseRelationshipName;

    private transient Rep serializationReplacement;
    private transient String busClassNameWithDots;
    private transient String busClassName;
    private transient Map<String, Object> properties;
    private transient Attribute<Owner, V> shadowAttribute = this;
    private transient int hashcode = 1;
    private transient boolean isNullable;
    protected transient boolean isIdentity;
    protected transient int offHeapFieldOffset;
    protected transient int offHeapNullBitsOffset;
    protected transient int offHeapNullBitsPosition;

    protected static final byte NULL_VALUE = 100;
    protected static final byte NOT_NULL_VALUE = 50;

    private static final long serialVersionUID = 8920718558296615883L;

    protected void setAll(String attributeName, String busClassNameWithDots, String busClassName, boolean isNullable,
            RelatedFinder relatedFinder, Map<String, Object> properties, boolean transactional)
    {
        this.attributeName = attributeName;
        this.busClassNameWithDots = busClassNameWithDots;
        this.busClassName = busClassName;
        this.isNullable = isNullable;
        this.relatedFinder = relatedFinder;
        this.properties = properties;
        this.hashcode = this.busClassName.hashCode() ^ this.attributeName.hashCode();
        if (!transactional)
        {
            this.updateCountHolder = new NonTransactionalUpdateCountHolder();
        }
    }

    protected void setOffHeapOffsets(int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition)
    {
        this.offHeapFieldOffset = offHeapFieldOffset;
        this.offHeapNullBitsOffset = offHeapNullBitsOffset;
        this.offHeapNullBitsPosition = offHeapNullBitsPosition;
    }

    public AttributeMetaData getMetaData()
    {
        return metaData;
    }

    protected boolean isIdentity()
    {
        return isIdentity;
    }

    protected void setShadowAttribute(Attribute<Owner, V> shadowAttribute)
    {
        this.shadowAttribute = shadowAttribute;
    }

    public Attribute zGetShadowAttribute()
    {
        return this.shadowAttribute;
    }

    public boolean hasShadowAttriute()
    {
        return this.shadowAttribute != this;
    }

    public Object getProperty(String key)
    {
        if (properties == null) return null;
        return properties.get(key);
    }

    public String getAttributeName()
    {
        return attributeName;
    }

    public Operation isNull()
    {
        return new IsNullOperation(this);
    }

    public Operation isNotNull()
    {
        return new IsNotNullOperation(this);
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.relatedFinder.getMithraObjectPortal();
    }

    public abstract Class valueType();

    public void serializeValue(Owner o, ObjectOutput out) throws IOException
    {
        if (this.isAttributeNull(o))
        {
            out.writeByte(NULL_VALUE);
        }
        else
        {
            out.writeByte(NOT_NULL_VALUE);
            this.serializedNonNullValue(o, out);
        }
    }

    public void deserializeValue(Owner o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        byte result = in.readByte();
        switch(result)
        {
            case NULL_VALUE:
                this.setValueNull(o);
                break;
            case NOT_NULL_VALUE:
                this.deserializedNonNullValue(o, in);
                break;
            default:
                throw new IOException("unexpected byte in stream "+result);
        }
    }

    protected abstract void serializedNonNullValue(Owner o, ObjectOutput out) throws IOException;

    protected abstract void deserializedNonNullValue(Owner o, ObjectInput in) throws IOException, ClassNotFoundException;

    public abstract Operation nonPrimitiveEq(Object other);

    @Override
    public void setUpdateCountDetachedMode(boolean isDetachedMode)
    {
        this.updateCountHolder.setUpdateCountDetachedMode(isDetachedMode);
    }

    public int getUpdateCount()
    {
        return updateCountHolder.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return updateCountHolder.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
        updateCountHolder.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        updateCountHolder.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        updateCountHolder.rollbackUpdateCount();
    }

    public SourceAttributeType getSourceAttributeType()
    {
        return relatedFinder.getSourceAttributeType();
    }

    public boolean equals(Object other)
    {
        if (other == this)
        {
            return true;
        }
        if (other instanceof Attribute)
        {
            Attribute a = (Attribute) other;
            return this.busClassName == a.busClassName && this.attributeName == a.attributeName;  // guaranteed to have been interned.
        }
        return false;
    }

    public int hashCode()
    {
        return this.hashcode;
    }

    public EqualityMapper constructEqualityMapper(Attribute right)
    {
        return new EqualityMapper(this, right);
    }

    public Attribute getSourceAttribute()
    {
        return relatedFinder.getSourceAttribute();
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return relatedFinder.getAsOfAttributes();
    }

    public boolean isSourceAttribute()
    {
        return false;
    }

    // todo: rezaem: create an in operation that takes an extractor
    // this is tricky, because it requires a new set that not only takes a hashcode strategy
    // but has a new method: contains(object, hashStrategy)
/*
    public Operation in(List dataHolders, Extractor extractor)
    {
        return new InOperationWithExtractor(this, dataHolders, extractor);
    }
*/
    protected Operation joinEqWithSourceAndAsOfCheck(Attribute other)
    {
        Mapper mapper = null;
        if (this.getSourceAttributeType() != null && !(other instanceof MappedAttribute) && this.getSourceAttributeType().equals(other.getSourceAttributeType()))
        {
            MultiEqualityMapper mem = new MultiEqualityMapper(this, other);
            mem.addAutoGeneratedAttributeMap(this.getSourceAttribute(), other.getSourceAttribute());
            mapper = mem;
        }
        mapper = constructEqualityMapperWithAsOfCheck(other, mapper);
        if (mapper == null)
        {
            mapper = this.constructEqualityMapper(other);
        }
        mapper.setAnonymous(true);
        Attribute target = other;
        if (other instanceof MappedAttribute)
        {
            target = ((MappedAttribute)other).getWrappedAttribute();
        }
        return new MappedOperation(mapper, new All(target));
    }

    public Mapper constructEqualityMapperWithAsOfCheck(Attribute other)
    {
        return constructEqualityMapperWithAsOfCheck(other, null);
    }

    private Mapper constructEqualityMapperWithAsOfCheck(Attribute other, Mapper mapper)
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        if (!(other instanceof  MappedAttribute) && asOfAttributes != null && other.getAsOfAttributes() != null)
        {
            MultiEqualityMapper mem = (MultiEqualityMapper) mapper;
            for(int i=0;i<asOfAttributes.length;i++)
            {
                AsOfAttribute compatible = asOfAttributes[i].getCompatibleAsOfAttribute(other.getAsOfAttributes());
                if (compatible != null)
                {
                    if (mem == null)
                    {
                        mem = new MultiEqualityMapper(this, other);
                    }
                    mem.addAutoGeneratedAttributeMap(asOfAttributes[i], compatible);
                }
            }
            mapper = mem;
        }
        return mapper;
    }

    public boolean isAsOfAttribute()
    {
        return false;
    }

    public abstract Operation in(List objects, Extractor extractor);
    public abstract Operation in(Iterable objects, Extractor extractor);

    public abstract Operation zInWithMax(int maxInClause, List objects, Extractor extractor);

    public void parseNumberAndSet(double value, Owner data, int lineNumber) throws ParseException
    {
        throw new ParseException("did not expect a number "+value+" for attribute "+this.toString()+" on line " + lineNumber, lineNumber);
    }

    public abstract void parseStringAndSet(String value, Owner data, int lineNumber, Format format)
            throws ParseException;

    public void parseWordAndSet(String word, Owner data, int lineNumber) throws ParseException
    {
        if (word.equals("null"))
        {
            this.setValueNull(data);
        }
        else
        {
            throw new ParseException("Could not parse "+word+" on line "+lineNumber, lineNumber);
        }
    }

    public abstract int zCountUniqueInstances(MithraDataObject[] dataObjects);

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
         throw new RuntimeException("not implemented");
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), value);
    }

    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("not implemented");

    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return this.getOwnerPortal();
    }

    public Operation zGetMapperOperation()
    {
        throw new RuntimeException("not implemented");
    }

    public abstract String getFullyQualifiedLeftHandExpression(SqlQuery query);

    public abstract String getColumnName();

    public void generateMapperSql(AggregateSqlQuery query)
    {
        // do nothing. mapped attributes will override
    }

    public boolean zFindDeepRelationshipInMemory(Operation op)
    {
        return true;
    }

    public void forEach(ObjectProcedure proc, Object newValue, Object context)
    {
        proc.execute(newValue, context);
    }

    protected NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder, NumericAttribute wrappedAttribute)
    {
        NumericAttribute attr = wrappedAttribute;
        if (mapperRemainder != null)
        {
            attr = createMappedAttributeWithMapperRemainder(mapperRemainder);
        }
        return attr;
    }

    protected NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder)
    {
        return null;
    }

    protected NumericAttribute createOtherMappedAttributeWithMapperRemainder(MappedAttribute mappedAttribute, Mapper otherMapperRemainder)
    {
        return (NumericAttribute) createMappedAttributeWithMapperRemainder(mappedAttribute, otherMapperRemainder);
    }

    protected Attribute createMappedAttributeWithMapperRemainder(MappedAttribute mappedAttribute, Mapper otherMapperRemainder)
    {
        Attribute otherAttr = mappedAttribute.getWrappedAttribute();
        if (otherMapperRemainder != null)
        {
            DeepRelationshipAttribute parentSelector = ((DeepRelationshipAttribute) mappedAttribute.getParentSelector());
            Function parentSelectorRemainder = otherMapperRemainder.getParentSelectorRemainder(parentSelector);
            otherAttr = (Attribute) mappedAttribute.cloneForNewMapper(otherMapperRemainder, parentSelectorRemainder);
        }
        return otherAttr;
    }

    public abstract OrderBy ascendingOrderBy();

    public abstract OrderBy descendingOrderBy();

    public IntegerAggregateAttribute count()
    {
        return new IntegerAggregateAttribute(new CountCalculator(this));
    }

    public MithraAggregateAttribute min()
    {
        throw new UnsupportedOperationException("min is not supported in this attribute");
    }

    public MithraAggregateAttribute max()
    {
        throw new UnsupportedOperationException("max is not supported in this attribute");
    }

    protected Rep getSerializationReplacement()
    {
        if (this.serializationReplacement == null)
        {
            this.serializationReplacement = new Rep(this.getBusClassNameWithDots(), this.attributeName);
        }
        return this.serializationReplacement;
    }

    public abstract String zGetSqlForDatabaseType(DatabaseType databaseType);

    public abstract AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data);

    protected String getBusClassName()
    {
        return busClassName;
    }

    public String zGetTopOwnerClassName()
    {
        return this.busClassName;
    }

    protected String getBusClassNameWithDots()
    {
        return busClassNameWithDots;
    }

    protected boolean isNullable()
    {
        return isNullable;
    }

    public void setValueNullUntil(Owner o, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void copyValueFrom(Owner dest, Owner src)
    {
        this.setValue(dest, this.valueOf(src));
    }

    public String toString()
    {
        if (this.busClassName != null)
        {
            return this.busClassName+"."+this.attributeName;
        }
        return super.toString();
    }

    protected void appendNullable(StringBuilder sb, DatabaseType dt)
    {
        if (this.isNullable())
        {
            sb.append(dt.getNullableColumnConstraintString());
        }
        else
        {
            sb.append(" not null");
        }
    }

    protected Operation filterEqWithCheck(Attribute other)
    {
        if (other instanceof MappedAttribute)
        {
            // condition 3 without common mapper
            Mapper mapper = this.constructEqualityMapper(other);
            mapper.setAnonymous(true);
            return new MappedOperation(mapper, new All(((MappedAttribute)other).getWrappedAttribute()));
        }
        return new AtomicSelfEqualityOperation(this, other); // condition 1, no mapper
    }

    /*
        First reduce the left and right mappers to the common ancestor, then there are 4 possibilities here
        1) leftReduced and rightReduced are not mapped. This is a plain filter operation with a common mapper on top
        2) leftReduced is mapped, rightReduced is not mapped. equivalent to (3), just flipped.
        3) leftReduced is not mapped, rightReduced is mapped. If the rightReduced mapper is one level, it's a join constraint. otherwise, it's a triangle join constraint
        4) leftReduced and rightReduced are both mapped. This is a triangle join constraint.
     */
    protected Operation filterEqForMappedAttribute(Attribute other)
    {
        MappedAttribute left = (MappedAttribute) this;
        if (other instanceof MappedAttribute)
        {
            MappedAttribute right = (MappedAttribute) other;
            if (left.getMapper().equals(right.getMapper()))
            {
                // condition (1) with common mapper
                return new MappedOperation(left.getMapper(), left.getWrappedAttribute().filterEqWithCheck(right.getWrappedAttribute()));
            }
            Mapper commonMapper = left.getMapper().getCommonMapper(right.getMapper());
            if (left.getMapper().equals(commonMapper))
            {
                // left is shorter than right. Condition (3) with common mapper
                return filterEqWithPartialCommon(left, right, commonMapper);
            }
            if (right.getMapper().equals(commonMapper))
            {
                // right is shorter than left. Condition (2) with common mapper
                return filterEqWithPartialCommon(right, left, commonMapper);
            }
            else
            {
                // condition 4
                Mapper mapper = this.constructEqualityMapper(other);
                mapper.setAnonymous(true);
                return new MappedOperation(mapper, new All(((MappedAttribute)other).getWrappedAttribute()));
            }
        }
        else
        {
            return other.filterEqWithCheck(this); // Condition (2) without common mapper
        }
    }

    private Operation filterEqWithPartialCommon(MappedAttribute left, MappedAttribute right, Mapper commonMapper)
    {
        Attribute reducedRight = createMappedAttributeWithMapperRemainder(right, right.getMapper().getMapperRemainder(commonMapper));
        Operation op = left.getWrappedAttribute().filterEqWithCheck(reducedRight);
        return new MappedOperation(left.getMapper(), op);
    }

    public void zAddDependentPortalsToSet(Set set)
    {
        set.add(this.getOwnerPortal());
    }

    public void zAddDepenedentAttributesToSet(Set set)
    {
        set.add(this);
    }

    public abstract Operation zGetOperationFromResult(Owner result, Map<Attribute, Object> tempOperationPool);
    public abstract Operation zGetOperationFromOriginal(Object original, Attribute left, Map<Attribute, Object> tempOperationPool);
    public abstract Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool);

    public Operation zCreateMappedOperation()
    {
        return NoOperation.instance();
    }

    protected void computeMappedAttributeName(Attribute wrappedAttribute, Function parentSelector)
    {
        if (parentSelector instanceof AbstractRelatedFinder)
        {
            InternalList list = new InternalList();
            list.add(parentSelector);
            AbstractRelatedFinder finder = (AbstractRelatedFinder) parentSelector;
            while(finder.getParentDeepRelationshipAttribute() != null)
            {
                finder = (AbstractRelatedFinder) finder.getParentDeepRelationshipAttribute();
                list.add(finder);
            }
            StringBuilder builder = new StringBuilder((list.size()+1)*10);
            for(int i=list.size() - 1;i >=0 ; i--)
            {
                builder.append(((AbstractRelatedFinder)list.get(i)).getRelationshipName()).append('.');
            }
            builder.append(wrappedAttribute.getAttributeName());
            this.attributeName = builder.toString();
        }
    }

    public TupleAttribute tupleWith(Attribute attr)
    {
        return new TupleAttributeImpl(this, attr);
    }

    public TupleAttribute tupleWith(Attribute... attrs)
    {
        return new TupleAttributeImpl(this, attrs);
    }

    public TupleAttribute tupleWith(TupleAttribute attr)
    {
        return new TupleAttributeImpl(this, (TupleAttributeImpl) attr);
    }

    public void zAppendToString(ToStringContext toStringContext)
    {
        toStringContext.append(toStringContext.getCurrentAttributePrefix()+"."+this.getAttributeName());
    }

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        throw new RuntimeException("not implemented");
    }

    public RelatedFinder getOwningRelationship()
    {
        if (this.owningRelationshipName == null)
        {
            return null;
        }
        return this.relatedFinder.getRelationshipFinderByName(this.owningRelationshipName);
    }

    public void zSetOwningRelationship(String owningRelationshipName)
    {
        this.owningRelationshipName = owningRelationshipName;
    }

    public String getOwningReverseRelationshipOwnerPackage()
    {
        return this.owningReverseRelationshipOwnerPackage;
    }

    public String getOwningReverseRelationshipOwner()
    {
        return this.owningReverseRelationshipOwner;
    }

    public String getOwningReverseRelationshipName()
    {
        return this.owningReverseRelationshipName;
    }

    public void zSetOwningReverseRelationship(
            String owningReverseRelationshipOwnerPackage,
            String owningReverseRelationshipOwner,
            String owningReverseRelationshipName)
    {
        this.owningReverseRelationshipOwnerPackage = owningReverseRelationshipOwnerPackage;
        this.owningReverseRelationshipOwner = owningReverseRelationshipOwner;
        this.owningReverseRelationshipName = owningReverseRelationshipName;
    }

    public void zWriteSerial(ReladomoSerializationContext context, SerialWriter writer, Owner reladomoObject) throws Exception
    {
        if (this.isAttributeNull(reladomoObject))
        {
            writer.writeNull(context, this.getAttributeName(), this.valueType());
        }
        else
        {
            zWriteNonNullSerial(context, writer, reladomoObject);
        }
    }

    protected abstract void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, Owner reladomoObject) throws IOException;
}
