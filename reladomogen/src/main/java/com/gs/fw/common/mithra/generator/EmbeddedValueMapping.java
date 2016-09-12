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
import com.gs.fw.common.mithra.generator.type.StringJavaType;
import com.gs.fw.common.mithra.generator.util.StringUtility;

public class EmbeddedValueMapping extends Attribute
{

    private EmbeddedValueMappingType wrapped;
    private EmbeddedValue parent;
    private String mappingName;
    private String shortName;

    public EmbeddedValueMapping(EmbeddedValueMappingType wrapped, MithraObjectTypeWrapper owner, EmbeddedValue parent)
    {
        this(wrapped, owner, parent, wrapped.getAttribute());
    }

    private EmbeddedValueMapping(EmbeddedValueMappingType wrapped, MithraObjectTypeWrapper owner, EmbeddedValue parent, String name)
    {
        super(owner);
        this.wrapped = wrapped;
        this.parent = parent;
        this.mappingName = name;
        this.shortName = name;
        if (this.wrapped.getUnderlyingAttribute() != null)
        {
            this.setName(this.wrapped.getUnderlyingAttribute());
        }
        else
        {
            this.setName(parent.getNestedName() + StringUtility.firstLetterToUpper(this.getShortName()));
        }
    }

    private EmbeddedValueMappingType getMappingType()
    {
        return this.wrapped;
    }

    public EmbeddedValue getParent()
    {
        return this.parent;
    }

    public String getMappingName()
    {
        return this.mappingName;
    }

    @Override
    public boolean isFinalGetter()
    {
        return this.wrapped.isFinalGetterSet() ? this.wrapped.isFinalGetter() : this.getOwner().isDefaultFinalGetters();
    }

    public String getName()
    {
        return this.getMappingType().getAttribute();
    }

    protected void setName(String name)
    {
        this.getMappingType().setAttribute(name);
    }

    public String getColumnName()
    {
        return this.getMappingType().getColumnName();
    }

    public void setColumnName(String columnName)
    {
        this.getMappingType().setColumnName(columnName);
    }

    public boolean hasMaxLength()
    {
        return this.getMappingType().isMaxLengthSet();
    }

    public int getMaxLength()
    {
        return this.getMappingType().getMaxLength();
    }

    public boolean mustTrim()
    {
        return this.isStringAttribute() && this.getMappingType().isTrim();
    }

    public boolean hasModifyTimePrecisionOnSet()
    {
        return this.getMappingType().isModifyTimePrecisionOnSetSet();
    }

    public TimePrecisionType getModifyTimePrecisionOnSet()
    {
        return this.getMappingType().getModifyTimePrecisionOnSet();
    }

    public int getMaxLengthForComparison()
    {
        if (this.hasMaxLength())
        {
            return this.getMappingType().getMaxLength();
        }
        return 255;
    }

    public boolean isPrimaryKey()
    {
        return this.getMappingType().isPrimaryKey();
    }

    public boolean isIdentity()
    {
        return this.getMappingType().isIdentitySet();
    }

    public boolean isMutablePrimaryKey()
    {
        return this.getMappingType().isMutablePrimaryKey();
    }

    public boolean isPrimaryKeyUsingSimulatedSequence()
    {
        return this.getMappingType().isSimulatedSequenceSet();
    }

    public SimulatedSequenceType getSimulatedSequence()
    {
        return this.getMappingType().getSimulatedSequence();
    }

    public boolean isSetPrimaryKeyGeneratorStrategy()
    {
        return this.getMappingType() != null && this.getMappingType().isPrimaryKeyGeneratorStrategySet();
    }

    public String getPrimaryKeyGeneratorStrategy()
    {
        return this.getMappingType().getPrimaryKeyGeneratorStrategy().value();
    }

    public boolean isNullable()
    {
        return this.getMappingType().isNullable() && (this.getMappingType().isNullableSet() || !this.getMappingType().isPrimaryKey());
    }

    public boolean isPoolable()
    {
        if (this.getMappingType().isPoolableSet() && !(getType() instanceof StringJavaType))
        {
            throw new MithraGeneratorException("In " + this.getOwner().getClassName() + ".xml: Cannot specify 'poolable' on attribute '" + this.getName() + "' of type '" + super.getTypeAsString() + "' because it not of type 'String'");
        }

        return getType() instanceof StringJavaType && this.getMappingType().isPoolable();
    }

    public String getDefaultIfNull()
    {
        return this.getMappingType().getDefaultIfNull();
    }

    public boolean trimString()
    {
        if (this.getMappingType().isTrimSet() && !(getType() instanceof StringJavaType))
        {
            throw new MithraGeneratorException("In " + this.getOwner().getClassName() + ".xml: Cannot specify 'trim' on attribute '" + this.getName() + "' of type '" + super.getTypeAsString() + "' because it not of type 'String'");
        }

        return getType() instanceof StringJavaType && this.getMappingType().isTrim();
    }

    public boolean isReadonly()
    {
       return this.getMappingType().isReadonly();
    }

    public boolean isUsedForOptimisticLocking()
    {
        return this.getMappingType().isUseForOptimisticLocking();
    }

    public boolean truncate()
    {
        return this.getMappingType().isTruncate();
    }

    public TimezoneConversionType getTimezoneConversion()
    {
        TimezoneConversionType timezoneConversion = this.getMappingType().getTimezoneConversion();
        if (timezoneConversion == null)
        {
            timezoneConversion = this.getDefaultTimezoneConversion();
        }
        return timezoneConversion;
    }

    @Override
    public TimestampPrecisionType getTimestampPrecision()
    {
        return this.getMappingType().getTimestampPrecision();
    }

    public boolean isRoot()
    {
        return this.parent.isRoot();
    }

    public boolean isNested()
    {
        return this.parent.isNested();
    }

    public int getHierarchyDepth()
    {
        return this.parent.getHierarchyDepth();
    }

    protected void validateIsSetPoolable(Attribute superClassAttribute)
    {
        boolean isSetPoolable;
        boolean isPoolable;
        if (superClassAttribute instanceof EmbeddedValueMapping)
        {
            isSetPoolable = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isPoolableSet();
            isPoolable = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isPoolable();
        }
        else
        {
            isSetPoolable = superClassAttribute.getAttributeType().isPoolableSet();
            isPoolable = superClassAttribute.getAttributeType().isPoolable();
        }
        if (!this.getMappingType().isPoolableSet() && isSetPoolable)
        {
            this.getMappingType().setPoolable(isPoolable);
        }
    }

    protected void validateIsSetTrim(Attribute superClassAttribute)
    {
        boolean isSetTrim;
        boolean isTrim;
        if (superClassAttribute instanceof EmbeddedValueMapping)
        {
            isSetTrim = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isTrimSet();
            isTrim = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isTrim();
        }
        else
        {
            isSetTrim = superClassAttribute.getAttributeType().isTrimSet();
            isTrim = superClassAttribute.getAttributeType().isTrim();
        }
        if (!this.getMappingType().isTrimSet() && isSetTrim)
        {
            this.getMappingType().setTrim(isTrim);
        }
    }

    protected void validateIsSetTruncate(Attribute superClassAttribute)
    {
        boolean isSetTruncate;
        boolean isTruncate;
        if (superClassAttribute instanceof EmbeddedValueMapping)
        {
            isSetTruncate = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isTruncateSet();
            isTruncate = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isTruncate();
        }
        else
        {
            isSetTruncate = superClassAttribute.getAttributeType().isTruncateSet();
            isTruncate = superClassAttribute.getAttributeType().isTruncate();
        }
        if (!this.getMappingType().isTruncateSet() && isSetTruncate)
        {
            this.getMappingType().setTruncate(isTruncate);
        }
    }

    protected void validateIsSetNullable(Attribute superClassAttribute)
    {
        boolean isSetNullable;
        boolean isNullable;
        if (superClassAttribute instanceof EmbeddedValueMapping)
        {
            isSetNullable = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isNullableSet();
            isNullable = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isNullable();
        }
        else
        {
            isSetNullable = superClassAttribute.getAttributeType().isNullableSet();
            isNullable = superClassAttribute.getAttributeType().isNullable();
        }
        if (!this.getMappingType().isNullableSet() && isSetNullable)
        {
            this.getMappingType().setNullable(isNullable);
        }
    }

    protected void validateIsSetTimezoneConversion(Attribute superClassAttribute)
    {
        boolean isSetTimezoneConversion;
        TimezoneConversionType timezoneConversion;
        if (superClassAttribute instanceof EmbeddedValueMapping)
        {
            isSetTimezoneConversion = ((EmbeddedValueMapping) superClassAttribute).getMappingType().isTimezoneConversionSet();
            timezoneConversion = ((EmbeddedValueMapping) superClassAttribute).getMappingType().getTimezoneConversion();
        }
        else
        {
            isSetTimezoneConversion = superClassAttribute.getAttributeType().isTimezoneConversionSet();
            timezoneConversion = superClassAttribute.getAttributeType().getTimezoneConversion();
        }
        if (!this.getMappingType().isTimezoneConversionSet() && isSetTimezoneConversion)
        {
            this.getMappingType().setTimezoneConversion(timezoneConversion);
        }
    }

    protected void validateColumnName(Attribute superClassAttribute)
    {
        if (this.getMappingType().getColumnName() == null)
        {
            this.getMappingType().setColumnName(superClassAttribute.getColumnName());
        }
    }

    protected void validateDefaultIfNull(Attribute superClassAttribute)
    {
        if (this.getMappingType().getDefaultIfNull() == null)
        {
            this.getMappingType().setDefaultIfNull(superClassAttribute.getDefaultIfNull());
        }
    }

    public EmbeddedValueMapping cloneForNewOwner(MithraObjectTypeWrapper newOwner)
    {
        AbstractAttribute result = new EmbeddedValueMapping(this.getMappingType(), newOwner, this.getParent(), this.getShortName());
        result.setJavaType(this.getTypeAsString());
        return (EmbeddedValueMapping) result;
    }

    public String getShortName()
    {
        return shortName;
    }

    public String getShortNameNullGetter()
    {
        return "is" + StringUtility.firstLetterToUpper(this.getShortName()) + "Null()";
    }

    public String getShortNameGetter()
    {
        return "get" + StringUtility.firstLetterToUpper(this.getShortName());
    }

    public String getShortNameSetter()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getShortName());
    }

    public String getChainedNullGetter()
    {
        return this.parent.getChainedGetter() + "()." + this.getShortNameNullGetter();
    }

    public String getChainedGetter()
    {
        return this.parent.getChainedGetter() + "()." + this.getShortNameGetter();
    }

    public String getChainedGetterAfterDepth(int hierarchyDepth)
    {
        return this.parent.getChainedGetterAfterDepth(hierarchyDepth) + "()." + this.getShortNameGetter();
    }

    public String getChainedNullGetterAfterDepth(int hierarchyDepth)
    {
        return this.parent.getChainedGetterAfterDepth(hierarchyDepth) + "()." + this.getShortNameNullGetter();
    }

    public String getChainedSetter()
    {
        return this.parent.getChainedGetter() + "()." + this.getShortNameSetter();
    }

    public String getSetterUntil()
    {
        return super.getSetter() + "Until";
    }

    public String getVisibility()
    {
        return "protected";
    }

    public int getScale()
    {
        return wrapped.getScale();
    }

    public int getPrecision()
    {
        return wrapped.getPrecision();
    }

    @Override
    public boolean isMapped()
    {
        return false;
    }
}
