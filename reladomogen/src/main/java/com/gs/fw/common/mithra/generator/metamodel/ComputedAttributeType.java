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

package com.gs.fw.common.mithra.generator.metamodel;

import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.computedattribute.ComputedAttributeParser;
import com.gs.fw.common.mithra.generator.computedattribute.Expression;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComputedAttributeType extends ComputedAttributeTypeAbstract
{
    private Expression expression;

    public void parseComputedAttribute(List<String> errors)
    {
        this.expression = new ComputedAttributeParser().parse(this.value(), " object "+((MithraObjectTypeAbstract)this.parent()).getClassName()+
                " and attribute "+this.getName(), errors);

    }

    public void resolveAttributes(MithraObjectTypeWrapper wrapper, List<String> errors)
    {
        if (this.expression != null) this.expression.resolveAttributes(wrapper, errors);
    }

    public Set<String> getAttributeList()
    {
        Set<String> result = new HashSet<String>();
        if (this.expression != null) this.expression.addAttributeList(result);
        return result;
    }
}
