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


import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.computedattribute.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CaseExpression extends Expression
{
    private Expression sourceExpression;
    private List<CaseFragment> fragments = new ArrayList<CaseFragment>();
    private boolean finished;

    public CaseExpression(Expression expression)
    {
        this.sourceExpression = expression;
    }

    public void markFinished()
    {
        this.finished = true;
        //todo: resolve number types more specifically
    }

    public void startNewKey()
    {
        fragments.add(new CaseFragment(this.sourceExpression.getType()));
    }

    public void endKey()
    {
        lastFragment().endKey();
    }

    public CaseFragment lastFragment()
    {
        return fragments.get(fragments.size() - 1);
    }

    public void addNullKey()
    {
        lastFragment().addNullKey();
    }

    public void addStringConstantKey(String constant)
    {
        lastFragment().addStringKey(constant);
    }

    public void addNumberKey(double nval)
    {
        lastFragment().addNumberKey(nval);
    }

    public void addBooleanKey(boolean value)
    {
        lastFragment().addBooleanKey(value);
    }

    public void addDefaultKey()
    {
        lastFragment().addDefaultKey();
    }

    public void setValue(Expression param)
    {
        lastFragment().setValue(param);
    }

    @Override
    public void addAttributeList(Set<String> result)
    {
        this.sourceExpression.addAttributeList(result);
        for(int i=0;i< fragments.size();i++)
        {
            fragments.get(i).addAttributeList(result);
        }
    }

    @Override
    public void resolveAttributes(MithraObjectTypeWrapper wrapper, List<String> errors)
    {
        this.sourceExpression.resolveAttributes(wrapper, errors);
        for(int i=0;i< fragments.size();i++)
        {
            fragments.get(i).resolveAttributes(wrapper, errors);
        }
    }

    @Override
    public Type getType()
    {
        Type valueType = fragments.get(0).getValueType();
        for(int i=1;i<fragments.size();i++)
        {
            valueType = valueType.computeMostCompatibleType(fragments.get(1).getValueType());
        }
        return valueType;
    }
}
