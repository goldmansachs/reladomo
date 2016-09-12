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

import java.util.HashMap;



public class SourceAttributeType
{

    private String sourceAttributeName;
    private Class sourceAttributeClass;
    private Class sourceAttributeType;


    private transient static HashMap pool = new HashMap();

    public static SourceAttributeType create(String sourceAttributeName, Class sourceAttributeClass)
    {
        SourceAttributeType temp = new SourceAttributeType(sourceAttributeName, sourceAttributeClass);
        if (pool.containsKey(temp))
        {
            return (SourceAttributeType) pool.get(temp);
        }
        else
        {
            pool.put(temp, temp);
            return temp;
        }
    }

    protected SourceAttributeType(String sourceAttributeName, Class sourceAttributeClass)
    {
        this.sourceAttributeName = sourceAttributeName;
        this.sourceAttributeClass = sourceAttributeClass;
        if (sourceAttributeClass.getName().equals("com.gs.fw.common.mithra.attribute.StringAttribute"))
        {
            this.sourceAttributeType = String.class;
        }
        else
        {
            this.sourceAttributeType = Integer.TYPE;
        }
    }

    public Class getSourceAttributeUnderlyingClass()
    {
        return this.sourceAttributeType;
    }

    public boolean isStringSourceAttribute()
    {
        return this.sourceAttributeType == String.class;
    }

    public boolean isIntSourceAttribute()
    {
        return this.sourceAttributeType == Integer.TYPE;
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof SourceAttributeType))
        {
            return false;
        }

        final SourceAttributeType sourceAttributeType = (SourceAttributeType) o;

        if (!sourceAttributeClass.equals(sourceAttributeType.sourceAttributeClass))
        {
            return false;
        }
        if (!sourceAttributeName.equals(sourceAttributeType.sourceAttributeName))
        {
            return false;
        }

        return true;
    }

    public int hashCode()
    {
        int result;
        result = sourceAttributeName.hashCode();
        result = 29 * result + sourceAttributeClass.hashCode();
        return result;
    }
}
