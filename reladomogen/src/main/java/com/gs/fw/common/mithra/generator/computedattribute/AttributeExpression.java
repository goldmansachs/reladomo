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

package com.gs.fw.common.mithra.generator.computedattribute;


import com.gs.fw.common.mithra.generator.AbstractAttribute;
import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.computedattribute.type.Type;

import java.util.List;
import java.util.Set;

public class AttributeExpression extends Expression
{
    private String attributeName;
    private AbstractAttribute singleColumnAttribute;

    public AttributeExpression(String attributeName)
    {
        this.attributeName = attributeName;
    }

    public String getAttributeName()
    {
        return attributeName;
    }

    @Override
    public void addAttributeList(Set<String> result)
    {
        result.add(this.attributeName);
    }

    @Override
    public void resolveAttributes(MithraObjectTypeWrapper wrapper, List<String> errors)
    {
        Attribute attributeByName = wrapper.getAttributeByName(this.attributeName);
        if (attributeByName != null)
        {
            singleColumnAttribute = attributeByName;
        }
        else
        {
            //todo: check other computed attributes
            errors.add("Could not find attribute "+this.attributeName+" in object "+wrapper.getClassName());
        }
    }

    @Override
    public Type getType()
    {
        if (this.singleColumnAttribute != null)
        {
            return this.singleColumnAttribute.getType().asComputedAttributeType();
        }
        return null;
    }
}
