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
import com.gs.fw.common.mithra.generator.computedattribute.type.BooleanType;
import com.gs.fw.common.mithra.generator.computedattribute.type.NumberType;
import com.gs.fw.common.mithra.generator.computedattribute.type.StringType;
import com.gs.fw.common.mithra.generator.computedattribute.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CaseFragment
{
    private boolean keyState = true;
    private Type keyType;
    private Type valueType;
    private List keys = new ArrayList();
    private boolean isDefault;
    private Expression value;

    public CaseFragment(Type keyType)
    {
        this.keyType = keyType;
    }

    public void endKey()
    {
        keyState = false;
    }

    public void addNullKey()
    {
        checkKeyState();
        keys.add(null);
    }

    private void checkKeyState()
    {
        if (isDefault)
        {
            throw new ParseException("the 'default' case must appear on its own");
        }
        if (!keyState)
        {
            throw new ParseException("was expecting a value and got a selector key instead!");
        }
    }

    public void addStringKey(String constant)
    {
        checkKeyState();
        checkKeyType(StringType.class);
        keys.add(constant);
    }

    private void checkKeyType(Class typeClass)
    {
        if (!typeClass.equals(keyType.getClass()))
        {
            throw new ParseException("was expecting "+keyType.toString()+" type, but got "+typeClass.getName().toString());
        }
    }


    public void addNumberKey(double nval)
    {
        checkKeyState();
        if (!(keyType instanceof NumberType))
        {
            throw new ParseException("was expecting a "+keyType.toString()+" not a number "+nval);
        }
        keys.add(((NumberType)keyType).convertFromDouble(nval));
    }

    public void addBooleanKey(boolean key)
    {
        checkKeyState();
        checkKeyType(BooleanType.class);
        if (!keys.isEmpty())
        {
            throw new ParseException("it's meaningless to combine multiple boolean case selectors");
        }
        keys.add(key);
    }

    public void addDefaultKey()
    {
        this.isDefault = true;
    }

    public void setValue(Expression param)
    {
        if (keyState)
        {
            throw new ParseException("was expecting a value not a key");
        }
        if (valueType == null)
        {
            this.valueType = param.getType();
        }
        else
        {
            if (!valueType.isCompatibleWith(param.getType()))
            {
                throw new ParseException("can't mix value types "+valueType.toString()+" with "+param.getType());
            }
        }

        if (this.value != null)
        {
            throw new ParseException("expecting only one value");
        }
        this.value = param;
    }

    public void addAttributeList(Set<String> result)
    {
        this.value.addAttributeList(result);
    }

    public void resolveAttributes(MithraObjectTypeWrapper wrapper, List<String> errors)
    {
        this.value.resolveAttributes(wrapper, errors);
    }

    public Type getValueType()
    {
        return this.value.getType();
    }
}
