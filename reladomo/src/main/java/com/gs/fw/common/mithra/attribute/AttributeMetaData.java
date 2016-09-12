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


public class AttributeMetaData
{
    private Attribute attribute;

    public AttributeMetaData(Attribute attribute)
    {
        this.attribute = attribute;
    }

    public boolean isIdentity()
    {
        return attribute.isIdentity();
    }

    public boolean isNullable()
    {
        return attribute.isNullable();
    }

    public String getColumnName()
    {
        return attribute.getColumnName();
    }

    public String getAttributeName()
    {
        return attribute.getAttributeName();
    }

    public Object getProperty(String key)
    {
        return attribute.getProperty(key);
    }

    public boolean isAsOfAttribute()
    {
        return attribute.isAsOfAttribute();
    }

    public boolean isSourceAttribute()
    {
        return attribute.isSourceAttribute();
    }

    public Attribute getSourceAttribute()
    {
        return attribute.getSourceAttribute();
    }

    public SourceAttributeType getSourceAttributeType()
    {
        return attribute.getSourceAttributeType();
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return attribute.getAsOfAttributes();
    }

    public boolean mustTrimString()
    {
        if (this.attribute instanceof StringAttribute)
        {
            return ((StringAttribute) this.attribute).mustTrim();
        }
        return false;
    }
    /**
     * @return the maxLength defined in the xml for String attributes or Integer.MAX_VALUE if one is not defined. For non-string attributes, returns 0.
     */
    public int getStringMaxLength()
    {
        if (this.attribute instanceof StringAttribute)
        {
            return ((StringAttribute)this.attribute).getMaxLength();
        }
        return 0;
    }
}
