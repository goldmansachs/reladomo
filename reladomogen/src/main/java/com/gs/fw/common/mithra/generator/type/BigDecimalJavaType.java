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

import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;

import java.math.BigDecimal;


public class BigDecimalJavaType extends JavaType
{
    @Override
    public boolean isBigDecimal()
    {
        return true;
    }

    public String getTypeName()
    {
        return "BIGDECIMAL";
    }

    public String getJavaTypeString()
    {
        return "BigDecimal";
    }

    public String getJavaTypeStringPrimary()
    {
        return "BigDecimal";
    }

    public String getIoType()
    {
        return "Object";
    }

    public String getIoCast()
    {
        return "(BigDecimal)";
    }

    public String convertToObject(String name)
    {
        return name;
    }

    public String convertToPrimitive(String name)
    {
        return name;
    }

    public boolean isPrimitive()
    {
        return false;
    }

    public int getSqlType()
    {
        return java.sql.Types.DECIMAL;
    }

    public String getSqlTypeAsString()
    {
        return "java.sql.Types.DECIMAL";
    }

    public String getSqlDataType()
    {
        return "decimal";
    }

    public String getSqlDataType(CommonDatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForBigDecimal();
    }

    public String getDefaultInitialValue()
    {
        return "null";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isFloatingPoint()||node.isInteger();
    }

    public boolean isComparableTo(JavaType other)
    {
        return other instanceof BigDecimalJavaType;
    }

    public String getJavaTypeClass()
    {
        return "BigDecimal.class";
    }

    public String getObjectComparisonString(String obj1, String obj2)
    {
        return obj1+".equals("+obj2+")";
    }

    @Override
    public int getOffHeapSize()
    {
        throw new RuntimeException("not implemented");
    }
}

