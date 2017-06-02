

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
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

/**
 * <b><code>BooleanJavaType</code></b>
 */
public class BooleanJavaType extends PrimitiveWrapperJavaType
{

    public String getTypeName()
    {
        return "BOOLEAN";
    }

    public String getJavaTypeString()
    {
        return "boolean";
    }

    public String getJavaTypeStringPrimary()
    {
        return "Boolean";
    }

    public String convertToObject(String name)
    {
        return "Boolean.valueOf(" + name + ")";
    }

    public String convertToPrimitive(String name)
    {
        return name + ".booleanValue()";
    }

    public boolean isNumber()
    {
        return false;
    }

    public int getSqlType()
    {
        return java.sql.Types.BIT;
    }

    public String getSqlTypeAsString()
    {
        return "java.sql.Types.BIT";
    }

    public String getSqlDataType()
    {
        return "bit";
    }

    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        if (nullable)
        {
            return databaseType.getSqlDataTypeForNullableBoolean();
        }
        return databaseType.getSqlDataTypeForBoolean();
    }

    public String getDefaultInitialValue()
    {
        return "false";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isBoolean();
    }

    public boolean isComparableTo(JavaType other)
    {
        return other instanceof BooleanJavaType;
    }

    public String getValueIfNull(String valueIfNull)
    {
        Boolean result = Boolean.valueOf(valueIfNull);
        return result.toString();
    }

    public String getJavaTypeClass()
    {
        return "Boolean.TYPE";
    }

    public String getSetCollectionClass()
    {
        return "BooleanDirectSet";
    }

    public String getGetterPrefix()
    {
        return "is";
    }

    @Override
    public String getBeanSetter(BeanState beanState)
    {
        return "setI"+beanState.getIntCount()+"AsBoolean";
    }

    @Override
    public boolean isBoolean()
    {
        return true;
    }

    @Override
    public int getOffHeapSize()
    {
        return 1;
    }

    @Override
    public String getSqlTypeAsStringForNull()
    {
        return "dt.getNullableBooleanJavaSqlType()";
    }
}
