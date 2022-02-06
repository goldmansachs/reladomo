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
import com.gs.fw.common.mithra.generator.type.TimestampJavaType;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AsOfAttribute extends Attribute
{

    private AsOfAttributePureType wrapped;
    private boolean isProcessingDate;
    private JavaType type = new TimestampJavaType();

    public AsOfAttribute(AsOfAttributePureType wrapped, MithraObjectTypeWrapper owner)
    {
        super(owner);
        this.wrapped = wrapped;
        this.isProcessingDate = this.wrapped.isIsProcessingDate();
    }

    public TimestampPrecisionType getTimestampPrecision()
    {
        return wrapped.getTimestampPrecision();
    }

    @Override
    public boolean isFinalGetter()
    {
        return this.wrapped.isFinalGetterSet() ? this.wrapped.isFinalGetter() : this.getOwner().isDefaultFinalGetters();
    }

    public boolean isProcessingDateSet()
    {
        return this.wrapped.isIsProcessingDateSet();
    }

    public boolean isInfinityNull()
    {
        return wrapped.isInfinityIsNull();
    }

    public boolean isSetAsString()
    {
        return wrapped.isSetAsString();
    }

    public boolean isReadonly()
    {
        return false;
    }

    public String getName()
    {
        return wrapped.getName();
    }

    public boolean isAsOfAttribute()
    {
        return true;
    }

    public boolean isTimezoneConversionNeeded()
    {
        return !this.isTimezoneNone();
    }

    public TimezoneConversionType getTimezoneConversion()
    {
        TimezoneConversionType conversionType = this.wrapped.getTimezoneConversion();
        if (conversionType == null)
        {
            conversionType = this.getDefaultTimezoneConversion();
        }
        return conversionType;
    }

    public boolean isProcessingDate()
    {
        return isProcessingDate;
    }

    public void setProcessingDate(boolean processingDate)
    {
        isProcessingDate = processingDate;
    }

    public boolean isNullable()
    {
        return false;
    }

    @Override
    public String getPlainColumnName()
    {
        return null;
    }

    public String getQuotedColumnName()
    {
        throw new RuntimeException("not supported!");
    }

    public JavaType getType()
    {
        return type;
    }

    public String getFromColumnName()
    {
        return this.wrapped.getFromColumnName();
    }

    public String getToColumnName()
    {
        return wrapped.getToColumnName();
    }

    public String getInfinityDate()
    {
        return wrapped.getInfinityDate();
    }

    public boolean isToIsInclusive()
    {
        return wrapped.isToIsInclusive();
    }

    public boolean futureExpiringRowsExist()
    {
        return wrapped.isFutureExpiringRowsExist();
    }

    public boolean isPrimaryKey()
    {
        return false;
    }

    public boolean isIdentity()
    {
        return false;
    }

    public String getFinderAttributeType()
    {
        return "AsOfAttribute";
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

    public String getInfinityExpression()
    {
        if (this.isInfinityNull())
        {
            return "com.gs.fw.common.mithra.util.NullDataTimestamp.getInstance()";
        }
        else
        {
            String original = this.wrapped.getInfinityDate();
            return createTimestampExpression(original);
        }
    }

    private String createTimestampExpression(String original)
    {
        if (original == null)
        {
            return "null";
        }
        original = original.trim();
        if (original.startsWith("[") && original.endsWith("]"))
        {
            return original.substring(1, original.length() - 1);
        }
        else
        {
            throw new RuntimeException("timestamp parsing not yet implemented");
        }
    }

    public String getDefaultDateExpression()
    {
        String original = this.wrapped.getDefaultIfNotSpecified();
        return createTimestampExpression(original);
    }

    public boolean hasModifyTimePrecisionOnSet()
    {
        return false;
    }

    public TimePrecisionType getModifyTimePrecisionOnSet()
    {
        return null;
    }

    public boolean hasMaxLength()
    {
        return false;
    }

    public int getMaxLength()
    {
        return 0;
    }

    public boolean mustTrim()
    {
        return false;
    }

    public AsOfAttribute getCompatibleAsOfAttribute(MithraObjectTypeWrapper fromObject)
    {
        return fromObject.getCompatibleAsOfAttribute(this);
    }

    public List<String> validateAndUseMissingValuesFromSuperClass(CommonAttribute superClassAttribute)
    {
        AsOfAttribute superClassAsOfAttribute = (AsOfAttribute)superClassAttribute;
        List<String> errors = new ArrayList<String>();
        if(!superClassAsOfAttribute.getType().equals(this.getType()))
        {
            errors.add("java type for attribute '" + this.getName() + "' does not match java type for same attribute in superclass '" + superClassAttribute.getName());
        }
        if(errors.size() == 0)
        {
            if(wrapped.getInfinityDate() == null)
            {
                wrapped.setInfinityDate(superClassAsOfAttribute.getInfinityDate());
            }
            if(wrapped.getFromColumnName() == null)
            {
                wrapped.setFromColumnName(superClassAsOfAttribute.getFromColumnName());
            }
            if(wrapped.getToColumnName() == null)
            {
                wrapped.setToColumnName(superClassAsOfAttribute.getToColumnName());
            }
            if(!wrapped.isToIsInclusiveSet())
            {
                wrapped.setToIsInclusive(superClassAsOfAttribute.isToIsInclusive());
            }
            if(wrapped.getDefaultIfNotSpecified() == null)
            {
                wrapped.setDefaultIfNotSpecified(superClassAsOfAttribute.wrapped.getDefaultIfNotSpecified());
            }
            if (!this.wrapped.isPoolableSet() && superClassAsOfAttribute.wrapped.isPoolableSet())
            {
                this.wrapped.setPoolable(superClassAsOfAttribute.isPoolable());
            }
        }
        return errors;
    }

    public Attribute cloneForNewOwner(MithraObjectTypeWrapper newOwner)
    {
        return new AsOfAttribute(this.wrapped, newOwner);
    }

    public String getSerializationStatement(String variable)
    {
        if (this.isTimezoneConversionNeeded())
        {
            return "MithraTimestamp.writeTimestampWithInfinity(out, "+variable+", "+this.getInfinityExpression()+")";
        }
        return "MithraTimestamp.writeTimezoneInsensitiveTimestampWithInfinity(out, "+variable+", "+this.getInfinityExpression()+")";
    }

    public String getDeserializationStatement()
    {
        String result;
        if (this.isTimezoneConversionNeeded())
        {
            result = "MithraTimestamp.readTimestampWithInfinity(in, "+this.getInfinityExpression()+")";
        }
        else
        {
            result = "MithraTimestamp.readTimezoneInsensitiveTimestampWithInfinity(in, "+this.getInfinityExpression()+")";
        }
        if (this.isPoolable())
        {
            result = "TimestampPool.getInstance().getOrAddToCache("+result+", "+this.getOwner().getFinderClassName()+".isFullCache())";
        }
        return result;
    }

    public boolean isPoolable()
    {
        return this.wrapped.isPoolable();
    }

    /*
            String attributeName, String dataClassName, String busClassName, boolean isNullable,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties, boolean transactional, boolean isOptimistic,

            specific:
            TimestampAttribute fromAttribute, TimestampAttribute toAttribute, Timestamp infinityDate,
            boolean futureExpiringRowsExist, boolean toIsInclusive, Timestamp defaultDate, boolean isProcessingDate
    */
    public String getGeneratorParameters()
    {
        String result = this.getCommonConstructorParameters();
        result += ",";
        result += "this."+ this.getName() +"From()";
        result += ",";
        result += "this."+ this.getName() +"To()";
        result += ",";
        result += this.getOwner().getFinderClassName()+"."+this.getName()+"Infinity";
        result += ",";
        result += this.futureExpiringRowsExist();
        result += ",";
        result += this.isToIsInclusive();
        result += ",";
        result += this.getOwner().getFinderClassName()+"."+this.getName()+"Default";
        result += ",";
        result += this.isProcessingDate();
        result += ",";
        result += this.isInfinityNull();
        return result;
    }

    @Override
    public boolean isMapped()
    {
        return false;
    }

    public String getTimestampLongFromGetter()
    {
        return "zGet"+ StringUtility.firstLetterToUpper(this.getName())+"FromAsLong()";
    }

    public String getTimestampLongToGetter()
    {
        return "zGet"+ StringUtility.firstLetterToUpper(this.getName())+"ToAsLong()";
    }

    public Attribute getAsOfAttributeTo()
    {
        return this.getOwner().getAttributeByName(this.getName()+"To");
    }
}
