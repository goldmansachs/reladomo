
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

package com.gs.fw.common.mithra.generator.type;

import com.gs.fw.common.mithra.generator.BeanState;
import com.gs.fw.common.mithra.generator.util.StringUtility;

public abstract class PrimitiveWrapperJavaType extends JavaType
{

    public boolean isPrimitive()
    {
        return true;
    }

    public boolean isNumber()
    {
        return true;
    }

    public boolean isComparableTo(JavaType other)
    {
        return other.isNumber();
    }

    public String getPrimitiveComparisonString(String p1, String p2)
    {
        return p1+" != "+p2;
    }

    @Override
    public String getBeanGetter(int intCount, int longCount, int objectCount)
    {
        return "getI"+intCount+"As"+ StringUtility.firstLetterToUpper(this.getJavaTypeStringPrimary());
    }

    @Override
    public String getBeanSetter(BeanState beanState)
    {
        return "setI"+beanState.getIntCount()+"AsInteger";
    }

    @Override
    public boolean isBeanIntType()
    {
        return true;
    }

    @Override
    public boolean isBeanObjectType()
    {
        return false;
    }
}
