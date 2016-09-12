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

import com.gs.fw.common.mithra.generator.metamodel.PropertyType;
import com.gs.fw.common.mithra.generator.metamodel.SourceAttributeType;
import com.gs.fw.common.mithra.generator.metamodel.TimePrecisionType;
import com.gs.fw.common.mithra.generator.type.JavaType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SourceAttribute extends Attribute
{
    private JavaType type;
    private SourceAttributeType wrapped;

    public SourceAttribute(SourceAttributeType sourceAttributeType, MithraObjectTypeWrapper owner)
    {
        super(owner);
        this.wrapped = sourceAttributeType;
        this.type = JavaType.create(wrapped.getJavaType());
    }

    @Override
    public boolean isFinalGetter()
    {
        return this.wrapped.isFinalGetterSet() ? this.wrapped.isFinalGetter() : this.getOwner().isDefaultFinalGetters();
    }

    public boolean isPrimaryKeyUsingSimulatedSequence()
    {
        return false;
    }

    public boolean isIntSourceAttribute()
    {
        return  "Integer.TYPE".equals(this.type.getJavaTypeClass());
    }

    public boolean isStringSourceAttribute()
    {
        return  "String.class".equals(this.type.getJavaTypeClass());
    }

    public boolean isPoolable()
    {
        return this.isStringAttribute();
    }

    public String getName()
    {
        return wrapped.getName();
    }

    public boolean isNullable()
    {
        return false;
    }

    public boolean isReadonly()
    {
        return false;
    }

    public String getQuotedColumnName()
    {
        return "null";
    }

    public boolean isMutablePrimaryKey()
    {
        return false;
    }

    public JavaType getType()
    {
        return this.type;
    }

    public String getAttributeClass()
    {
        return this.getType().getJavaTypeClass();
    }

    public boolean isSourceAttribute()
    {
        return true;
    }

    public boolean isPrimaryKey()
    {
        return false;
    }

    public boolean isIdentity()
    {
        return false;
    }

    public boolean mustTrim()
    {
        return false;
    }

    public boolean hasMaxLength()
    {
        return false;
    }

    public int getMaxLength()
    {
        return 0;
    }

    public boolean hasModifyTimePrecisionOnSet()
    {
        return false;
    }

    public TimePrecisionType getModifyTimePrecisionOnSet()
    {
        return null;
    }

    public List getProperty()
    {
        return this.wrapped.getProperties();
    }

    public boolean hasProperties()
    {
        return this.wrapped.getProperties() != null && !this.wrapped.getProperties().isEmpty();
    }

    public Map<String, String> getProperties()
    {
        Map<String, String> properties = new HashMap<String, String>();
        if (this.hasProperties())
        {
            List<PropertyType> propertyTypes = this.wrapped.getProperties();
            for (PropertyType property : propertyTypes)
            {
                properties.put(property.getKey(), (property.getValue() == null) ? "Boolean.TRUE" : property.getValue());
            }
        }
        return properties;
    }

    public List validateAndUseMissingValuesFromSuperClass(CommonAttribute superClassAttribute) {
        List errors = new ArrayList();
        SourceAttribute superClassSourceAttribute = (SourceAttribute)superClassAttribute;
        if(!superClassSourceAttribute.getFinderAttributeType().equals(this.getFinderAttributeType()))
        {
            errors.add("java type for attribute '" + this.getName() + "' does not match java type for same attribute in superclass '" + superClassAttribute.getName());
        }
        return errors;
    }

    public Attribute cloneForNewOwner(MithraObjectTypeWrapper newOwner)
    {
        return new SourceAttribute(this.wrapped, newOwner);
    }

    @Override
    public boolean isMapped()
    {
        return false;
    }
}
