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

import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

public class TimeJavaType extends JavaType
{
    public String getTypeName()
    {
        return "TIME";
    }

    public String getJavaTypeString()
    {
        return "Time";
    }

    public String getJavaTypeStringPrimary()
    {
        return "Time";
    }

    public String getIoType()
    {
        return "Object";
    }

    public String getIoCast()
    {
        return "(Time)";
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
        return java.sql.Types.TIME;
    }

    public String getSqlTypeAsString()
    {
        return "java.sql.Types.TIME";
    }

    public String getSqlDataType()
    {
        return "time";
    }

    public String getSqlDataType(CommonDatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForTime();
    }

    public String getDefaultInitialValue()
    {
        return "null";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isString();
    }

    public boolean isComparableTo(JavaType other)
    {
        return other instanceof TimeJavaType || other instanceof TimestampJavaType;
    }

    public String getJavaTypeClass()
    {
        return "Time.class";
    }

    public String getObjectComparisonString(String obj1, String obj2)
    {
        return obj1 + ".equals(" + obj2 + ")";
    }

    @Override
    public int getOffHeapSize()
    {
        return 8;
    }

    public boolean isTime()
    {
        return true;
    }
}
