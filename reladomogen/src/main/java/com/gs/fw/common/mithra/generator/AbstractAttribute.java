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

package com.gs.fw.common.mithra.generator;

import com.gs.fw.common.mithra.generator.metamodel.*;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.type.*;
import com.gs.fw.common.mithra.generator.util.StringBuilderBuilder;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.*;


public abstract class AbstractAttribute implements CommonAttribute, Comparable
{
    private MithraObjectTypeWrapper owner;
    public static final String QUOTE = "\"";
    private AttributePureType wrapped;
    private List<DependentRelationship> dependentRelationshipsToSet;

    protected JavaType type;
    private boolean asOfAttributeTo;
    private boolean asOfAttributeFrom;
    private boolean setAsString = false;
    private boolean isInherited = false;
    private MithraObjectTypeWrapper originalOwner;
    protected int offHeapFieldOffset = -1;
    protected int offHeapNullBitsOffset = -1;
    protected int offHeapNullBitsPosition = -1;

    protected int onHeapNullableIndex = -1;
    protected int onHeapMutablePkNullableIndex = -1;

    public AbstractAttribute(AttributePureType wrapped, MithraObjectTypeWrapper owner) throws JavaTypeException
    {
        this(owner, wrapped);
        this.type = JavaType.create(this.getAttributeType().getJavaType());
    }

    protected AbstractAttribute(MithraObjectTypeWrapper owner)
    {
        this.owner = owner;
    }

    protected AbstractAttribute(MithraObjectTypeWrapper owner, AttributePureType wrapped)
    {
        this.owner = owner;
        this.wrapped = wrapped;
    }

    public int getOnHeapMutablePkNullableIndex()
    {
        return onHeapMutablePkNullableIndex;
    }

    public void setOnHeapMutablePkNullableIndex(int onHeapMutablePkNullableIndex)
    {
        if (this.onHeapMutablePkNullableIndex != -1) throw new RuntimeException("should not get here");
        this.onHeapMutablePkNullableIndex = onHeapMutablePkNullableIndex;
    }

    public boolean isComparableUsingOffHeapBytes()
    {
        return (this.isPrimitive() && !this.isNullable()) || this.isBooleanAttribute() || this.isStringAttribute() || this.isTimestampAttribute() || this.isDateAttribute();
    }

    public String getOffHeapComparisonMethod()
    {
        int size = this.getType().getOffHeapSize();
        switch(size)
        {
            case 1:
                return "zGetByte";
            case 2:
                return "zGetShort";
            case 4:
                return "zGetInteger";
            case 8:
                return "zGetLong";
        }
        throw new RuntimeException("should not get here");
    }

    public int getOnHeapNullableIndex()
    {
        return onHeapNullableIndex;
    }

    public void setOnHeapNullableIndex(int onHeapNullableIndex)
    {
        if (this.onHeapNullableIndex != -1) throw new RuntimeException("should not get here");
        this.onHeapNullableIndex = onHeapNullableIndex;
    }

    public int getOffHeapFieldOffset()
    {
        return offHeapFieldOffset;
    }

    public void setOffHeapFieldOffset(int offHeapFieldOffset)
    {
        this.offHeapFieldOffset = offHeapFieldOffset;
    }

    public int getOffHeapNullBitsOffset()
    {
        return offHeapNullBitsOffset;
    }

    public boolean hasOffHeapNullBitsOffset()
    {
        return this.offHeapNullBitsOffset >= 0;
    }

    public void setOffHeapNullBitsOffset(int offHeapNullBitsOffset)
    {
        this.offHeapNullBitsOffset = offHeapNullBitsOffset;
    }

    public int getOffHeapNullBitsPosition()
    {
        return offHeapNullBitsPosition;
    }

    public void setOffHeapNullBitsPosition(int offHeapNullBitsPosition)
    {
        this.offHeapNullBitsPosition = offHeapNullBitsPosition;
    }

    public String getOffHeapGetterExpression()
    {
        String result = "zGet"+this.getType().getJavaTypeStringPrimary()+"(_storage, ";
        result += this.getOffHeapFieldOffset();
        result += ")";
        return result;
    }

    public String getOffHeapSetterExpression()
    {
        String result = "zSet"+this.getType().getJavaTypeStringPrimary()+"(_storage, ";
        result += " value, ";
        result += this.getOffHeapFieldOffset();
        if (this.isPrimitive() && !this.isBooleanAttribute())
        {
            result += ", "+this.getOffHeapNullBitsOffset();
            result += ", "+this.getOffHeapNullBitsPosition();
        }
        result += ")";
        return result;
    }

    public String getNullGetterExpression()
    {
        return this.getOwner().getNullGetterExpressionForIndex(this.onHeapNullableIndex);
    }

    public String getNullGetterExpressionForMutable()
    {
        return this.getOwner().getNullGetterExpressionForMutableIndex(this.onHeapMutablePkNullableIndex);
    }

    public String getNullSetterExpression()
    {
        return this.getOwner().getNullSetterExpressionForIndex(this.onHeapNullableIndex);
    }

    public String getNullSetterExpressionForMutable()
    {
        return this.getOwner().getNullSetterExpressionForMutableForIndex(this.onHeapMutablePkNullableIndex);
    }

    public String getNotNullSetterExpression()
    {
        return this.getOwner().getNotNullSetterExpressionForIndex(this.onHeapNullableIndex);
    }

    public String getNotNullSetterExpressionForMutablePk()
    {
        return this.getOwner().getNotNullSetterExpressionForMutablePk(this.onHeapMutablePkNullableIndex);
    }

    public void setJavaType(String type) throws JavaTypeException
    {
        this.type = JavaType.create(type);
    }

    public boolean isInherited()
    {
        return this.isInherited;
    }

    public void setInherited(boolean inherited)
    {
        this.isInherited = inherited;
    }

    @Override
    public boolean isFinalGetter()
    {
        return this.wrapped.isFinalGetterSet() ? this.wrapped.isFinalGetter() : this.owner.isDefaultFinalGetters();
    }

    public boolean hasUniqueAlias()
    {
        return this.getUniqueAlias() != null;
    }

    public String getUniqueAlias()
    {
        if (this.originalOwner != null) return this.originalOwner.getUniqueAlias();
        return this.owner.getUniqueAlias();
    }

    public boolean isUsedForOptimisticLocking()
    {
        return this.getAttributeType() != null && this.getAttributeType().isUseForOptimisticLocking();
    }

    public boolean isSetAsString()
    {
        AttributePureType type = this.getAttributeType();
        return this.setAsString || (type != null && type.isSetAsString());
    }

    public void setSetAsString(boolean setAsString)
    {
        this.setAsString = setAsString;
    }

    public String getDefaultIfNull()
    {
        return this.getAttributeType().getDefaultIfNull();
    }

    public String getName()
    {
        return this.getAttributeType().getName();
    }

    protected void setName(String name)
    {
        this.getAttributeType().setName(name);
    }

    public boolean hasModifyTimePrecisionOnSet()
    {
        return this.getAttributeType().isModifyTimePrecisionOnSetSet();
    }

    public TimePrecisionType getModifyTimePrecisionOnSet()
    {
        return this.getAttributeType().getModifyTimePrecisionOnSet();
    }

    public boolean hasMaxLength()
    {
        return this.getAttributeType().isMaxLengthSet();
    }

    public int getMaxLength()
    {
        return this.getAttributeType().getMaxLength();
    }

    public int getScale()
    {
        return this.getAttributeType().getScale();
    }

    public int getPrecision()
    {
        return this.getAttributeType().getPrecision();
    }

    public String getConvertTimeZoneString(String databaseTimeZone)
    {
        String retStr = "MithraTimestamp.DefaultTimeZone";
        if (isUTCTimezone())
        {
            retStr = "MithraTimestamp.UtcTimeZone";
        }
        else if (isDatabaseTimezone())
        {
            retStr = databaseTimeZone;
        }
        return retStr;
    }

    public String getConvertTimeZoneString()
    {
        return getConvertTimeZoneString("databaseTimeZone");
    }

    public String getSqlSetParameters(String attributeGetter)
    {
        String result = "";
        if (this.isTimestampAttribute())
        {
            result = "conversionTimeZone = " + this.getConvertTimeZoneString() + ";\n";
            if (this.isAsOfAttributeTo())
            {
                result += "if (data." + attributeGetter + ".getTime() == " + this.getOwner().getFinderClassName() + "." + this.getAsOfAttributeNameForAsOfAttributeTo() + "().getInfinityDate().getTime())\n";
                result += "{ \n conversionTimeZone = MithraTimestamp.DefaultTimeZone; \n }\n";
            }
            result += "dt.setTimestamp(stm, pos, data." + attributeGetter + ", false, conversionTimeZone);\n";
            result += "pos++;\n";
        }
        else if(this.isTimeAttribute())
        {
            result += "stm." + this.getSqlParameterSetter() + "(pos++, " + this.convertSqlParameter("data." + attributeGetter) + ".convertToSql());\n";
        }
        else
        {
            result += "stm." + this.getSqlParameterSetter() + "(pos++, " + this.convertSqlParameter("data." + attributeGetter) + ");\n";
        }
        return result;
    }

    public int getMaxLengthForComparison()
    {
        if (this.hasMaxLength())
        {
            return this.getAttributeType().getMaxLength();
        }
        return 255;
    }

    public boolean isSetPoolable()
    {
        return this.getAttributeType().isPoolableSet();
    }

    public boolean isPoolable()
    {
        return getType().canBePooled() && this.getAttributeType().isPoolable();
    }

    public boolean isNullable()
    {
        return this.getAttributeType().isNullable() && (this.getAttributeType().isNullableSet() || !this.getAttributeType().isPrimaryKey());
    }

    public boolean isReadonly()
    {
       return this.getAttributeType().isReadonly();
    }

    public boolean trimString()
    {
        return this.isStringAttribute() && this.getAttributeType().isTrim();
    }

    public boolean mustTrim()
    {
        return this.trimString();
    }

    public String getQuotedColumnName()
    {
        String columnName = this.getPlainColumnName();
        if (columnNameRequiresQuotes(columnName))
        {
            return "\"\\\"" + columnName + "\\\"\"";
        }
        return "\"" + columnName + "\"";
    }

    public boolean mustWarnDuringCreation()
    {
        return this.getType() instanceof StringJavaType && !this.hasMaxLength();
    }

    public JavaType getType()
    {
        return this.type;
    }

    public String getColumnName()
    {
        String columnName = getPlainColumnName();
        if (columnNameRequiresQuotes(columnName))
        {
            columnName = "\""+columnName+"\"";
        }
        return columnName;
    }

    public String getPlainColumnName()
    {
        return this.getAttributeType().getColumnName();
    }

    private boolean columnNameRequiresQuotes(String columnName)
    {
        return (columnName != null && !columnName.startsWith("\\") && (columnName.contains(" ") || isSqlKeyword(columnName)));
    }

    private boolean isSqlKeyword(String columnName)
    {
        return SqlKeywords.isKeyword(columnName.toUpperCase());
    }

    public void setColumnName(String columnName)
    {
        this.getAttributeType().setColumnName(columnName);
    }

    public boolean isPrimaryKey()
    {
        return this.getAttributeType().isPrimaryKey();
    }

    public boolean isIdentity()
    {
        return this.getAttributeType().isIdentity();
    }

    public boolean isMutablePrimaryKey()
    {
        return this.getAttributeType() != null && this.getAttributeType().isMutablePrimaryKey();
    }

    public boolean isInPlaceUpdate()
    {
        return this.getAttributeType() != null && this.getAttributeType().isInPlaceUpdate();
    }

    public boolean isShadowAttribute()
    {
        return this.isMutablePrimaryKey() || (this.isUsedForOptimisticLocking() && this.isTimestampAttribute());
    }

    public String getResultSetGetter(String paramOne, String paramTwo)
    {
        return "_rs.get" + this.getResultSetType() + "(" + paramOne + "," + paramTwo + ")";
    }

    public boolean isPrimaryKeyUsingSimulatedSequence()
    {
        return this.getAttributeType().isSimulatedSequenceSet();
    }

    public SimulatedSequenceType getSimulatedSequence()
    {
        return this.getAttributeType().getSimulatedSequence();
    }

    public boolean hasSimulatedSequenceSourceAttribute()
    {
        return this.getSimulatedSequence().isHasSourceAttribute() || (!this.getSimulatedSequence().isHasSourceAttributeSet() && this.getOwner().hasSourceAttribute());
    }

    public boolean isSetPrimaryKeyGeneratorStrategy()
    {
        return this.getAttributeType() != null && this.getAttributeType().isPrimaryKeyGeneratorStrategySet();
    }

    public String getPrimaryKeyGeneratorStrategy()
    {
        return this.getAttributeType().getPrimaryKeyGeneratorStrategy().value();
    }

    public String getResultSetGetter(String params)
    {
        String result = "_rs.get" + this.getResultSetType() + "(" + params + ")";
        if (this.getType().isBigDecimal())
        {
            result = "com.gs.fw.common.mithra.util.BigDecimalUtil.validateBigDecimalValue("+result+", "+this.getPrecision()+", "+this.getScale()+")";
        }
        else if(this.getType().isTime())
        {
            result = "_dt.getTime(_rs, _pos++)";
        }
        return result;
    }

    public String getResultSetGetterForString(String params)
    {
        String postProcess = "";
        if (this.trimString())
        {
            postProcess = "trimString";
        }
        return postProcess+"(_rs.get" + this.getResultSetType() + "(" + params + "))";
    }

    public String getSqlParameterSetter()
    {
        if (this.isSetAsString())
        {
            return "setString";
        }
        return "set"+this.getResultSetType();
    }

    public String convertSqlParameter(String param)
    {
        if (this.isSetAsString())
        {
            if (this.getType().isTimestamp())
            {
                return "convertTimestampToString(" + param + ", dt)";
            }
            if(this.getType().isTime())
            {
                return "convertTimeToString(" + param + ", dt)";
            }
            else
            {
                return "convertDateOnlyToString(" + param + ", dt)";
            }
        }
        return this.getType().convertSqlParameter(param);
    }

    public List<String> validateAndUseMissingValuesFromSuperClass(CommonAttribute attribute)
    {
        Attribute superClassAttribute = (Attribute) attribute;
        List<String> errors = this.checkAttributeMismatch(superClassAttribute);
        if(errors.isEmpty())
        {
            this.validateIsSetPoolable(superClassAttribute);
            this.validateIsSetTrim(superClassAttribute);
            this.validateIsSetTruncate(superClassAttribute);
            this.validateIsSetNullable(superClassAttribute);
            this.validateIsSetTimezoneConversion(superClassAttribute);
            this.validateColumnName(superClassAttribute);
            this.validateDefaultIfNull(superClassAttribute);
        }
        return errors;
    }

    protected void validateIsSetPoolable(Attribute superClassAttribute)
    {
        if (!this.getAttributeType().isPoolableSet() && superClassAttribute.getAttributeType().isPoolableSet())
        {
            this.getAttributeType().setPoolable(superClassAttribute.isPoolable());
        }
    }

    protected void validateIsSetTrim(Attribute superClassAttribute)
    {
        if (!this.getAttributeType().isTrimSet() && superClassAttribute.getAttributeType().isTrimSet())
        {
            this.getAttributeType().setTrim(superClassAttribute.getAttributeType().isTrim());
        }
    }

    protected void validateIsSetTruncate(Attribute superClassAttribute)
    {
        if (!this.getAttributeType().isTruncateSet() && superClassAttribute.getAttributeType().isTruncateSet())
        {
            this.getAttributeType().setTruncate(superClassAttribute.getAttributeType().isTruncate());
        }
    }

    protected void validateIsSetNullable(Attribute superClassAttribute)
    {
        if (!this.getAttributeType().isNullableSet() && superClassAttribute.getAttributeType().isNullableSet())
        {
            this.getAttributeType().setNullable(superClassAttribute.getAttributeType().isNullable());
        }
    }

    protected void validateIsSetTimezoneConversion(Attribute superClassAttribute)
    {
        if (!this.getAttributeType().isTimezoneConversionSet() && superClassAttribute.getAttributeType().isTimezoneConversionSet())
        {
            this.getAttributeType().setTimezoneConversion(superClassAttribute.getAttributeType().getTimezoneConversion());
        }
    }

    protected void validateColumnName(Attribute superClassAttribute)
    {
        if (getPlainColumnName() == null)
        {
            this.getAttributeType().setColumnName(superClassAttribute.getColumnName());
        }
    }

    protected void validateDefaultIfNull(Attribute superClassAttribute)
    {
        if (this.getAttributeType().getDefaultIfNull() == null)
        {
            this.getAttributeType().setDefaultIfNull(superClassAttribute.getDefaultIfNull());
        }
    }

    protected List<String> checkAttributeMismatch(Attribute superClassAttribute)
    {
        List<String> errors = new ArrayList<String>();
        if(!superClassAttribute.getType().equals(this.getType()))
        {
            errors.add("java type for attribute '" + this.getName() + "' does not match java type for same attribute in superclass '" + superClassAttribute.getName());
        }
        if(!(superClassAttribute.isPrimaryKey() == this.isPrimaryKey()))
        {
            if(superClassAttribute.isPrimaryKey())
			{
				errors.add("attribute '" + this.getName() + "' is a primaryKey in superclass");
			}
			else
			{
				errors.add("attribute '" + this.getName() + "' is not a primaryKey in superclass");
			}
        }
        return errors;
    }

    public void setIsAsOfAttributeTo(boolean asOfAttributeTo)
    {
        this.asOfAttributeTo = asOfAttributeTo;
    }

    public void setIsAsOfAttributeFrom(boolean asOfAttributeFrom)
    {
        this.asOfAttributeFrom = asOfAttributeFrom;
    }

    public boolean isAsOfAttributeTo()
    {
        return this.asOfAttributeTo;
    }

    public boolean isAsOfAttributeInfinityNull()
    {
        return this.isAsOfAttributeTo() && ((AsOfAttribute)this.getOwner().getAttributeByName(this.getAsOfAttributeNameForAsOfAttributeTo())).isInfinityNull();
    }

    public boolean isAsOfAttributeFrom()
    {
        return this.asOfAttributeFrom;
    }

    public boolean needsUntilImplementation()
    {
        return this.getOwner().hasBusinessDateAsOfAttribute() && this.getOwner().isTransactional() && !(this.isAsOfAttributeFrom() || this.isAsOfAttributeTo());
    }

    public Attribute cloneForNewOwner(MithraObjectTypeWrapper newOwner)
    {
        AbstractAttribute result = new Attribute(this.getAttributeType(), newOwner);
        result.asOfAttributeTo = this.asOfAttributeTo;
        result.asOfAttributeFrom = this.asOfAttributeFrom;
        result.setAsString = this.setAsString;
        if (this.originalOwner != null)
        {
            result.originalOwner = this.originalOwner;
        }
        else
        {
            result.originalOwner = this.owner;
        }
        return (Attribute) result;
    }

    public boolean truncate()
    {
        return this.getAttributeType().isTruncate();
    }
//
//    public boolean modifyTimePrecisionOnSet()
//    {
//        return this.getAttributeType().isModifyTimePrecisionOnSet();
//    }

    public MithraObjectTypeWrapper getOriginalOwner()
    {
        return this.originalOwner;
    }


    public boolean mustSetRelatedObjectAttribute()
    {
        if (this.dependentRelationshipsToSet != null)
        {
            DependentRelationship[] relationshipsToSet = this.getDependentRelationships();
            for(int r=0;r<relationshipsToSet.length;r++)
            {
                RelationshipAttribute relationshipAttribute = relationshipsToSet[ r ].getRelationshipAttribute();
                if (relationshipAttribute.isRelatedDependent())
                {
                    return true;
                }
            }
        }
        return false;
    }

    public void addDependentRelationship(RelationshipAttribute relationshipAttribute, AbstractAttribute attributeToSet)
    {
        if (this.dependentRelationshipsToSet == null)
        {
            this.dependentRelationshipsToSet = new ArrayList<DependentRelationship>();
        }
        this.dependentRelationshipsToSet.add(new DependentRelationship(relationshipAttribute, attributeToSet));
    }

    public DependentRelationship[] getDependentRelationships()
    {
        DependentRelationship[] result = new DependentRelationship[this.dependentRelationshipsToSet.size()];
        this.dependentRelationshipsToSet.toArray(result);
        return result;
    }

    public boolean hasParameters()
    {
        return false;
    }

    public String getGetter()
    {
        return this.getType().getGetterPrefix()+StringUtility.firstLetterToUpper(this.getName());
    }

    public String getPrivateGetter()
    {
        return this.getGetter();
    }

    public String getGetterOrMutableGetter()
    {
        String result = this.getGetter();
        if (this.isMutablePrimaryKey())
        {
            result = "_old" + result;
        }
        return result;
    }

    public String getNullGetter()
    {
        return "is" + StringUtility.firstLetterToUpper(this.getName()) + "Null()";
    }

    public String getNullSetter()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getName()) + "Null()";
    }

    public String getNullGetterUseMutableIfApplicable()
    {
        String result = this.getNullGetter();
        if (this.isMutablePrimaryKey())
        {
            result = "_old"+result;
        }
        return result;
    }

    public String getSetter()
    {
        return "set"+StringUtility.firstLetterToUpper(this.getName());
    }

    public String getPrivateSetter()
    {
        return getSetter();
    }

    public AttributePureType getAttributeType()
    {
        return this.wrapped;
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isJavaLiteral() || this.getType().isComparableTo(node);
        }

    public boolean isComparableTo(AbstractAttribute attribute)
    {
        return this.getType().isComparableTo(attribute.getType());
    }

    public String getTypeAsString()
    {
        return this.getType().getJavaTypeString();
    }

    public String getSetterTypeAsString()
    {
        if (this.isDateAttribute())
        {
            return "java.util.Date";
        }
        return this.getTypeAsString();
    }

    public String getStorageType()
    {
        return this.getType().getJavaTypeString();
    }

    public String getResultSetType()
    {
        return StringUtility.firstLetterToUpper(this.getType().getResultSetName());
    }

    public String getFinderAttributeSuperClassType()
    {
        return getType().getFinderAttributeSuperClassType();
    }

    public String getFinderAttributeType()
    {
        return getType().getFinderAttributeType();
    }

    public String getExtractionMethodName()
    {
        return getType().getExtractionMethodName();
    }

    public String getValueSetterMethodName()
    {
        return getType().getValueSetterMethodName();
    }

    public boolean isPrimitive()
    {
        return getType().isPrimitive();
    }

    public boolean isArray()
    {
        return getType().isArray();
    }

    public String getIsNullMethodName()
    {
        return "is"+StringUtility.firstLetterToUpper(this.getName())+"Null";
    }

    public boolean isSourceAttribute()
    {
        return false;
    }

    public boolean isAsOfAttribute()
    {
        return false;
    }

    public boolean needsSourceAttribute()
    {
        return this.getOwner().hasSourceAttribute() && !this.isSourceAttribute();
    }

    public boolean needsAsOfAttributes()
    {
        return this.getOwner().hasAsOfAttributes();
    }

    public boolean hasSourceOrAsOfAttribute()
    {
        return this.needsSourceAttribute() || this.getOwner().hasAsOfAttributes();
    }

    public String getEqualityMethodName()
    {
        boolean needsSourceAttribute = this.needsSourceAttribute();
        boolean needsAsOfAttribute = this.getOwner().hasAsOfAttributes();
        if (needsSourceAttribute && needsAsOfAttribute)
        {
            return "eqWithSourceAndAsOfCheck";
        }
        else if (needsSourceAttribute)
        {
            return "eqWithSourceCheck";
        }
        else if (needsAsOfAttribute)
        {
            return "eqWithAsOfCheck";
        }
        else
        {
            throw new IllegalArgumentException("method called but there is no source or asOfAttribute");
        }
    }

    public boolean isNullablePrimitive()
    {
        return this.isNullable() && this.isPrimitive();
    }

    public MithraObjectTypeWrapper getOwner()
    {
        return this.owner;
    }

    public List getProperty()
    {
        return this.getAttributeType().getProperties();
    }

    public boolean hasProperties()
    {
        return !(this.getAttributeType() == null || this.getAttributeType().getProperties().isEmpty());
    }

    public Map<String, String> getProperties()
    {
        Map<String, String> properties = new HashMap<String, String>();
        if (this.hasProperties())
        {
            List<PropertyType> propertyTypes = this.getAttributeType().getProperties();
            for (PropertyType property : propertyTypes)
            {
                properties.put(property.getKey(), (property.getValue() == null) ? "Boolean.TRUE" : property.getValue());
            }
        }
        return properties;
    }

    public int compareTo(Object o)
    {
        if (o instanceof AbstractAttribute)
        {
            AbstractAttribute other = (AbstractAttribute) o;
            return this.getName().compareTo(other.getName());
        }
        return 0;
    }

    public String getSqlTypeAsString()
    {
        return getType().getSqlTypeAsString();
    }

    public String getSqlTypeAsStringForNull()
    {
        return getType().getSqlTypeAsStringForNull();
    }

    public boolean isDoubleAttribute()
    {
        return getType() instanceof DoubleJavaType;
    }

    public boolean isCharAttribute()
    {
        return getType() instanceof CharJavaType;
    }

    public boolean hasLength()
    {
        return getType() instanceof StringJavaType || getType() instanceof ByteArrayJavaType;
    }

    public boolean hasTrim()
    {
        return getType() instanceof StringJavaType;
    }

    public boolean isStringAttribute()
    {
        return getType() instanceof StringJavaType;
    }

    public boolean isTimestampAttribute()
    {
        return getType() instanceof TimestampJavaType;
    }

    public boolean isTimeAttribute()
    {
        return getType() instanceof TimeJavaType;
    }

    public boolean isDateAttribute()
    {
        return getType() instanceof DateJavaType;
    }

    public boolean isBigDecimalAttribute()
    {
        return getType() instanceof BigDecimalJavaType;
    }

    public boolean isBooleanAttribute()
    {
        return getType() instanceof BooleanJavaType;
    }

    public String getIncrementer()
    {
        return "increment"+StringUtility.firstLetterToUpper(this.getName());
    }

    public String getOffHeapDeserializationStatement()
    {
        String readStatement;
        if (this.getType() instanceof ByteArrayJavaType)
        {
            throw new RuntimeException("not implemented");
        }
        if (this.isTimestampAttribute())
        {
            String methodStart = "";
            methodStart += "MithraTimestamp.read";
            if (this.isTimezoneNone())
            {
                methodStart += "TimezoneInsensitiveTimestamp";
            }
            else
            {
                methodStart += "Timestamp";
            }
            if (this.isAsOfAttributeTo())
            {
                methodStart += "WithInfinity(in, "+
                        this.getOwner().getFinderClassName()+"."+this.getAsOfAttributeNameForAsOfAttributeTo()+"().getInfinityDate())";
            }
            else
            {
                methodStart += "(in)";
            }
            readStatement = methodStart;
        }
        if (this.isDateAttribute())
        {
            readStatement = this.getType().getIoCast()+"MithraTimestamp.readTimezoneInsensitiveDate(in)";
        }
        readStatement = this.getType().getIoCast()+ "in.read"+ this.getType().getIoType()+"()";
        String setStatement = "zSet"+this.getType().getJavaTypeStringPrimary()+"(_storage, "+readStatement+", "+this.getOffHeapFieldOffset();
        if (this.isPrimitive() && !this.isBooleanAttribute())
        {
            setStatement += ", "+this.getOffHeapNullBitsOffset()+", "+
                this.getOffHeapNullBitsPosition();
            if (this.isNullable())
            {
                setStatement += ", "+getNullGetterExpression();
            }
            else
            {
                setStatement += ", false";
            }
        }
        setStatement += ")";
        return setStatement;
    }

    public String getDeserializationStatement()
    {
        String name = this.getName();
        return getDeserializationStatementForName(name);
    }

    public String getDeserializationStatementForName(String name)
    {
        if (this.getType() instanceof ByteArrayJavaType)
        {
            String lengthVar = "_"+ name +"Length";
            String result = "int "+lengthVar+" = in.readInt(); ";
            result += "if ("+lengthVar+" == -1) "+ name +" = null; ";
            result += "else { ";
            result += name +" = new byte["+lengthVar+"];";
            result += "in.readFully("+ name +");";
            result += "}";
            return result;
        }
        if (this.isTimestampAttribute())
        {
            String methodStart = "";
            if (this.isPoolable())
            {
                methodStart = this.getType().getJavaTypeString()+"Pool.getInstance().getOrAddToCache(";
            }
            methodStart += "MithraTimestamp.read";
            if (this.isTimezoneNone())
            {
                methodStart += "TimezoneInsensitiveTimestamp";
            }
            else
            {
                methodStart += "Timestamp";
            }
            if (this.isAsOfAttributeTo())
            {
                methodStart += "WithInfinity(in, "+
                        this.getOwner().getFinderClassName()+"."+this.getAsOfAttributeNameForAsOfAttributeTo()+"().getInfinityDate())";
            }
            else
            {
                methodStart += "(in)";
            }
            if (this.isPoolable())
            {
                methodStart += ", "+this.getOwner().getFinderClassName()+".isFullCache(), "+this.getOwner().getFinderClassName()+".isOffHeap())";
            }
            return "this."+ name + " = "+methodStart;
        }
        if (this.isDateAttribute())
        {
            return "this."+ name +" = "+this.getType().getIoCast()+"MithraTimestamp.readTimezoneInsensitiveDate(in)";
        }
        if (this.isPoolable())
        {
            return "this."+ name +" = "+this.getType().getJavaTypeString()+"Pool.getInstance().getOrAddToCache("+this.getType().getIoCast()+ "in.read"+ this.getType().getIoType()+"(), "+this.getOwner().getFinderClassName()+".isFullCache())";
        }
        return "this."+ name +" = "+this.getType().getIoCast()+ "in.read"+ this.getType().getIoType()+"()";
    }

    public String getSerializationStatement()
    {
        return getSerializationStatementForName(this.getName());
    }

    public String getOffHeapSerializationStatement()
    {
        String getter = this.getGetter()+"()";
        if (this.isTimestampAttribute())
        {
            getter = getTimestampLongGetter();
        }
        return getSerializationStatementForName(getter);
    }

    public String getSerializationStatementForName(String name)
    {
        if (this.getType() instanceof ByteArrayJavaType)
        {
            String result = "if ("+ name +" == null) out.writeInt(-1); ";
            result += "else { ";
            result += "out.writeInt("+ name +".length); ";
            result += "out.write("+ name +"); ";
            result += "}";
            return result;
        }
        if (this.isTimestampAttribute())
        {
            String methodStart = "MithraTimestamp.write";
            if (this.isTimezoneNone())
            {
                methodStart += "TimezoneInsensitiveTimestamp";
            }
            else
            {
                methodStart += "Timestamp";
            }
            if (this.isAsOfAttributeTo())
            {
                return methodStart+"WithInfinity(out, "+"this."+ name +", "+
                        this.getOwner().getFinderClassName()+"."+this.getAsOfAttributeNameForAsOfAttributeTo()+"().getInfinityDate())";
            }
            else
            {
                return methodStart+"(out, "+"this."+ name +")";
            }
        }
        if (this.isDateAttribute())
        {
            return "MithraTimestamp.writeTimezoneInsensitiveDate(out, "+"this."+ name +")";
        }
        return "out.write"+this.getType().getIoType()+"(this."+ name +")";
    }

    public String getTimestampLongGetter()
    {
        return "zGet"+StringUtility.firstLetterToUpper(this.getName())+"AsLong()";
    }

    public String getTimeLongGetter()
    {
        return "zGet"+StringUtility.firstLetterToUpper(this.getName())+"AsLong()";
    }

    public String getOffHeapTimeLongGetter()
    {
        return "zGetOffHeap"+StringUtility.firstLetterToUpper(this.getName())+"AsLong()";
    }

    public String getStringOffHeapIntGetter()
    {
        return "zGet"+StringUtility.firstLetterToUpper(this.getName())+"AsInt()";
    }

    public String getPrintableForm()
    {
        if (this.isAsOfAttributeFrom() || this.isAsOfAttributeTo())
        {
            return QUOTE+this.getName()+": "+QUOTE+"+"+"(is"+StringUtility.firstLetterToUpper(getName())+"Null() ? \"null\" : "+"PrintablePreparedStatement.timestampFormat.print("+getTimestampLongGetter()+"))";
        }
        return QUOTE+this.getName()+": "+QUOTE+"+"+this.getType().getPrintableForm(this.getGetter()+"()", this.getNullGetter(), this.isNullable());
    }

    public String getVisibility()
    {
        return "public";
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof Attribute)
        {
            Attribute other = (Attribute) obj;
            if (this.getName().equals(other.getName()) && this.getOwner().getClassName().equals(other.getOwner().getClassName()))
            {
                return true;
            }
        }
        return false;
    }

    public int hashCode()
    {
        return this.getName().hashCode();
    }

    public String getAsOfAttributeNameForAsOfAttributeTo()
    {
        return this.getName().substring(0, this.getName().length() - 2); // chop off "To"
    }

    public String getAsOfAttributeNameForAsOfAttributeFrom()
    {
        return this.getName().substring(0, this.getName().length() - 4); // chop off "From"
    }

    public TimezoneConversionType getDefaultTimezoneConversion()
    {
        return TimezoneConversionType.NONE;
    }

    public boolean isDatabaseTimezone()
    {
        return this.getTimezoneConversion().isConvertToDatabaseTimezone();
    }

    public TimezoneConversionType getTimezoneConversion()
    {
        TimezoneConversionType timezoneConversion = this.getAttributeType().getTimezoneConversion();
        if (timezoneConversion == null)
        {
            timezoneConversion = this.getDefaultTimezoneConversion();
        }
        return timezoneConversion;
    }

    public boolean isUTCTimezone()
    {
        return this.getTimezoneConversion().isConvertToUtc();
    }

    public boolean isTimezoneNone()
    {
        return this.getTimezoneConversion().isNone();
    }

    public boolean isTimezoneConversionNeeded()
    {
        return ( (this.getType() instanceof TimestampJavaType) && (!isTimezoneNone()));
    }

    public String getPrimitiveCastType(AbstractAttribute other)
    {
        if (other.getType().equals(this.getType()))
        {
            return "";
        }
        return "("+this.getType().getJavaTypeString()+")";
    }

    public boolean isImmutable()
    {
        return (this.isPrimaryKey() && !this.isMutablePrimaryKey()) || this.isReadonly();
    }

    public String getDirtyVersion()
    {
        if (this.isTimestampAttribute())
        {
            return "new Timestamp(0)";
        }
        return "-1";
    }

    public String getIncrementedVersion()
    {
        if (this.isTimestampAttribute())
        {
            return "new Timestamp(MithraManagerProvider.getMithraManager().getCurrentProcessingTime())";
        }
        return "data."+this.getGetter()+"() + 1";
    }

    public void validate(List<String> errors)
    {
        if (!this.isSetPrimaryKeyGeneratorStrategy() && this.isPrimaryKeyUsingSimulatedSequence())
        {
            errors.add(this.owner.getClassName()+" attribute "+this.getName()+" has a simulated sequence defined, but primaryKeyGeneratorStrategy=\"SimulatedSequence\" is not specified for the attribute");
        }
        if (!this.getType().canBePooled() && this.isSetPoolable())
        {
            errors.add(this.owner.getClassName()+" cannot pool attribute "+this.getName()+" only String and Timestamp attributes can be pooled");
        }
        if (this.getAttributeType().isTrimSet() && !(getType() instanceof StringJavaType))
        {
            errors.add(this.owner.getClassName()+" cannot trim attribute "+this.getName()+" only String attributes can be trimmed");
        }
        if (this.getAttributeType().isSetAsString())
        {
            if (!this.isTimestampAttribute() && !this.isDateAttribute() && !this.isAsOfAttribute())
            {
                errors.add(this.owner.getClassName()+" setAsString for attribute "+this.getName()+" can only be used with Date or Timestamp attributes");
            }
        }

        if(this.isBigDecimalAttribute())
        {
            if(!(this.getAttributeType().isScaleSet() && this.getAttributeType().isPrecisionSet()))
            {
                errors.add("BigDecimal attribute '"+this.getName()+"' in "+this.owner.getClassName()+" must specify precision and scale.");
            }

            int scale = this.getAttributeType().getScale();
            int precision = this.getAttributeType().getPrecision();

            if(scale < 0)
            {
                errors.add("Invalid scale value "+scale+". BigDecimal attribute '"+this.getName()+"' in "+this.owner.getClassName()+" must specify a non-negative scale value.");
            }
            if(precision < 1)
            {
                errors.add("Invalid precision value "+precision+". BigDecimal attribute '"+this.getName()+"' in "+this.owner.getClassName()+" must specify a precision > 1.");
            }

            if(scale > precision)
            {
                errors.add("Invalid scale value "+scale+". BigDecimal attribute '"+this.getName()+"' in "+this.owner.getClassName()+" must specify a scale < precision.");
            }
        }
    }

    private String quoteString(String s)
    {
        if (s == null) return "null";
        else return '"'+s+'"';
    }
    /*
            String columnName, String uniqueAlias, String attributeName,
            String dataClassName, String busClassName, boolean isNullable,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic

            String and ByteArray have a length

            Timestamp has: byte conversionType, boolean setAsString, boolean isAsOfAttributeTo, Timestamp infinity
            BigDecimal has: int precision, int scale
     */
    public String getGeneratorParameters()
    {
        String result = getCommonParameters("this", true);
        if (this.getType().isIntOrLong())
        {
            result += ",";
            result += this.isPrimaryKeyUsingSimulatedSequence();
        }
        result += ", "+(this.isMutablePrimaryKey() || this.isUsedForOptimisticLocking());
        return result;
    }

    public TimestampPrecisionType getTimestampPrecision()
    {
        return wrapped.getTimestampPrecision();
    }

    protected String getCommonParameters(String finder, boolean forGeneration)
    {
        String result = getQuotedColumnName();
        result += ",";
        if (this.getUniqueAlias() == null)
        {
            result += "\"\"";
        }
        else
        {
            result += quoteString(this.getUniqueAlias());
        }
        result += ",";
        result += getCommonConstructorParameters(finder);
        if (forGeneration)
        {
            result += ",";
            result += this.offHeapFieldOffset;
            result += ",";
            result += this.offHeapNullBitsOffset;
            result += ",";
            result += this.offHeapNullBitsPosition;
        }

        if (this.hasLength())
        {
            result += ",";
            if (this.hasMaxLength())
            {
                result += this.getMaxLength();
            }
            else
            {
                result += "Integer.MAX_VALUE";
            }
        }

        if(this.hasTrim())
        {
            result += ",";
            result += this.mustTrim();
        }

        if (this.isTimestampAttribute())
        {
            result += ",";
            if (this.isTimezoneConversionNeeded())
            {
                if (this.isUTCTimezone())
                {
                    result += "TimestampAttribute.CONVERT_TO_UTC";
                }
                else if (this.isDatabaseTimezone())
                {
                    result += "TimestampAttribute.CONVERT_TO_DATABASE";
                }
            }
            else
            {
                result += "TimestampAttribute.CONVERT_NONE";
            }
            result += ",";
            result += this.isSetAsString();
            result += ",";
            result += this.isAsOfAttributeTo();
            result += ",";
            if (this.isAsOfAttributeTo())
            {
                result += this.getOwner().getFinderClassName()+"."+this.getAsOfAttributeNameForAsOfAttributeTo()+"Infinity";
            }
            else if (this.isAsOfAttributeFrom())
            {
                result += this.getOwner().getFinderClassName()+"."+this.getAsOfAttributeNameForAsOfAttributeFrom()+"Infinity";
            }
            else
            {
                result += "null";
            }
            result += ", "+getTimestampPrecision().asByte();
        }
        if (this.isDateAttribute())
        {
            result += ",";
            result += this.isSetAsString();
        }

        if(this.isBigDecimalAttribute())
        {
            result += ","+this.getPrecision()+","+this.getScale();
        }
        return result;
    }

    public String getConstructorParameters()
    {
        return this.getCommonParameters("finder", false);
    }

    protected String getCommonConstructorParameters()
    {
        return getCommonConstructorParameters("this");
    }

    protected String getCommonConstructorParameters(String finder)
    {
        String result = quoteString(this.getName());
        result += ",";
        result += "BUSINESS_CLASS_NAME_WITH_DOTS";
        result += ",";
        result += "IMPL_CLASS_NAME_WITH_SLASHES";
        result += ",";
        result += this.isNullable();
        result += ",";
        result += this.getOwner().hasBusinessDateAsOfAttribute();
        result += ",";
        result += finder;
        result += ",";
        if (this.hasProperties())
        {
            result += this.getName()+"Properties";
        }
        else
        {
            result += "null";
        }
        result += ",";
        result += this.getOwner().isTransactional();
        result += ",";
        result += this.getOwner().getOptimisticLockAttribute() == this;
        if (this.mayBeIdentity())
        {
            result += ","+this.isIdentity();
        }
        return result;
    }

    protected boolean mayBeIdentity()
    {
        return this.getType().mayBeIdentity();
    }

    public boolean needsGeneratedAttribute()
    {
        return !this.getVisibility().equals("public");
    }

    public String getExtractorSuperClass()
    {
        return "Just"+this.getType().getJavaTypeStringPrimary()+"Extractor";
    }

    public String parseLiteralAndCast(String value)
    {
        return this.getType().parseLiteralAndCast(value);
    }

    public String getTableQualifiedMappedColumnName()
    {
        return this.getOwner().getDefaultTable()+"."+this.getColumnName();
    }

    public boolean isMapped()
    {
        return true;
    }

    public String getBeanGetter(int intCount, int longCount, int objectCount)
    {
        return this.getType().getBeanGetter(intCount, longCount, objectCount);
    }

    public String getBeanSetter(BeanState beanState)
    {
        return this.getType().getBeanSetter(beanState);
    }

    public boolean isBeanIntType()
    {
        return this.getType().isBeanIntType();
    }

    public boolean isBeanLongType()
    {
        return this.getType().isBeanLongType();
    }

    public boolean isBeanObjectType()
    {
        return this.getType().isBeanObjectType();
    }

    public String getColumnNameWithEscapedQuote()
    {
        String columnName = getPlainColumnName();
        if (columnNameRequiresQuotes(columnName))
        {
            columnName = "\\\""+columnName+"\\\"";
        }
        return columnName;
    }
}
