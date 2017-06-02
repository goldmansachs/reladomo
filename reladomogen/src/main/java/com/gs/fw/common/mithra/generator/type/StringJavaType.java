


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

import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.computedattribute.type.StringType;
import com.gs.fw.common.mithra.generator.computedattribute.type.Type;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

/**
 * <b><code>StringJavaType</code></b>
 */
public class StringJavaType extends JavaType
{

    public String getTypeName()
    {
        return "STRING";
    }

    public String getJavaTypeString()
    {
        return "String";
    }

    public String getIoType()
    {
        return "Object";
    }

    public String getIoCast()
    {
        return "(String)";
    }

    public String getJavaTypeStringPrimary()
    {
        return "String";
    }

    public String getPrintableForm(String getter)
    {
        return "\"'\"+"+ getter +"+\"'\"";
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
        return java.sql.Types.VARCHAR;
    }

    public String getSqlTypeAsString()
    {
        return "java.sql.Types.VARCHAR";
    }

    public String getSqlDataType()
    {
        return "varchar";
    }

    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        return databaseType.getSqlDataTypeForString();
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
        return other instanceof StringJavaType;
    }

    public String getValueIfNull(String valueIfNull)
    {
        return "\"" + valueIfNull + "\"";
    }

    public String getJavaTypeClass()
    {
        return "String.class";
    }

    public String getArgumentForInOperation(String values, MithraObjectTypeWrapper fromObject)
    {
        return fromObject.getConstantStringSet(values);
    }

    public String getConnectionManagerClassName()
    {
        return "com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager";
    }

    public String getObjectComparisonString(String obj1, String obj2)
    {
        return obj1+".equals("+obj2+")";
    }

    public boolean canBePooled()
    {
        return true;
    }

    @Override
    public int getOffHeapSize()
    {
        return 4;
    }

    @Override
    public Type asComputedAttributeType()
    {
        return new StringType();
    }
}
