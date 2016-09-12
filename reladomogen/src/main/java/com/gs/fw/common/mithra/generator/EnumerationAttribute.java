
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
import com.gs.fw.common.mithra.generator.type.JavaType;

import java.util.ArrayList;
import java.util.List;

public class EnumerationAttribute extends Attribute
{

    private final EnumerationAttributeType wrapped;
    private final List<EnumerationAttributeMapping> attributeMappings = new ArrayList<EnumerationAttributeMapping>();

    public EnumerationAttribute(MithraObjectTypeWrapper owner, EnumerationAttributeType wrapped)
    {
        super(owner);
        this.wrapped = wrapped;
        super.type = JavaType.createEnumeration(this.getEnumerationType(), this.getUnderlyingType());
        this.setJavaType(this.getUnderlyingType());
        this.extractMemberMappings();
    }

    private EnumerationAttributeType getEnumerationAttributeType()
    {
        return this.wrapped;
    }

    @Override
    public boolean isFinalGetter()
    {
        return this.getOwner().isDefaultFinalGetters();
    }

    public String getEnumerationType()
    {
        return this.getEnumerationAttributeType().getType();
    }

    public String getUnderlyingType()
    {
        return this.getEnumerationAttributeType().getPersistenceType();
    }

    public void setJavaType(String type)
    {
        // do not allow this to change once set in the constructor
    }

    public boolean isPrimitive()
    {
        return false;
    }

    public String getName()
    {
        return this.getEnumerationAttributeType().getName();
    }

    protected void setName(String name)
    {
        this.getEnumerationAttributeType().setName(name);
    }

    public String getColumnName()
    {
        return this.getEnumerationAttributeType().getColumnName();
    }

    public void setColumnName(String columnName)
    {
        this.getEnumerationAttributeType().setColumnName(columnName);
    }

    public boolean isNullable()
    {
        return this.getEnumerationAttributeType().isNullable();
    }

    public String getDefaultIfNull()
    {
        return this.getEnumerationAttributeType().getDefaultIfNull();
    }

    public boolean isReadonly()
    {
        return this.getEnumerationAttributeType().isReadonly();
    }

    public boolean hasModifyTimePrecisionOnSet()
    {
        return this.getEnumerationAttributeType().isModifyTimePrecisionOnSetSet();
    }

    public TimePrecisionType getModifyTimePrecisionOnSet()
    {
        return this.getEnumerationAttributeType().getModifyTimePrecisionOnSet();
    }

    public boolean hasMaxLength()
    {
        return this.getEnumerationAttributeType().isMaxLengthSet();
    }

    public int getMaxLength()
    {
        return this.getEnumerationAttributeType().getMaxLength();
    }

    public boolean mustTrim()
    {
        return false;
    }

    public int getMaxLengthForComparison()
    {
        if (this.hasMaxLength())
        {
            return this.getEnumerationAttributeType().getMaxLength();
        }
        return 255;
    }

    public boolean truncate()
    {
        return false;
    }

    public boolean modifyTimePrecisionOnSet()
    {
        return false;
    }

    public boolean trimString()
    {
        return false;
    }

    public boolean isPoolable()
    {
        return false;
    }

    public boolean isUsedForOptimisticLocking()
    {
        return false;
    }

    public boolean isPrimaryKey()
    {
        return false;
    }

    public boolean isMutablePrimaryKey()
    {
        return false;
    }

    public boolean isPrimaryKeyUsingSimulatedSequence()
    {
        return false;
    }

    public SimulatedSequenceType getSimulatedSequence()
    {
        throw new UnsupportedOperationException("An enumeration attribute may not serve as a primary key");
    }

    public boolean isSetPrimaryKeyGeneratorStrategy()
    {
        return false;
    }

    public String getPrimaryKeyGeneratorStrategy()
    {
        throw new UnsupportedOperationException("An enumeration attribute may not serve as a primary key");
    }

    public TimezoneConversionType getTimezoneConversion()
    {
        throw new UnsupportedOperationException("An enumeration attribute may not specify a TimezoneConversionType");
    }

    public EnumerationAttributeMapping[] getMembers()
    {
        EnumerationAttributeMapping[] attributeMappings = new EnumerationAttributeMapping[this.attributeMappings.size()];
        return this.attributeMappings.toArray(attributeMappings);
    }

    private void extractMemberMappings()
    {
        for (int i = 0; i < this.wrapped.getMappings().size(); i++)
        {
            EnumerationMappingType member = (EnumerationMappingType) this.wrapped.getMappings().get(i);
            this.attributeMappings.add(new EnumerationAttributeMapping(this, member));
        }
    }

    @Override
    public boolean isMapped()
    {
        return false;
    }
}
