

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

/**
 * <b><code>StringJavaType</code></b>
 */
public class CharJavaType extends PrimitiveWrapperJavaType
{

    public String getTypeName()
    {
        return "CHAR";
    }

    public String getJavaTypeString()
    {
        return "char";
    }

    public String getResultSetName()
    {
        return "String";
    }

    public String getJavaTypeStringPrimary()
    {
        return "Char";
    }

    public String convertToObject(String name)
    {
        return this.getValueIfNull(name);
    }

    public String convertToPrimitive(String name)
    {
        return this.getValueIfNull(name);
    }

    public boolean isNumber()
    {
        return false;
    }

    public int getSqlType()
    {
        return java.sql.Types.CHAR;
    }

    public String getSqlTypeAsString()
    {
        return "java.sql.Types.CHAR";
    }

    public String getSqlDataType()
    {
        return "char";
    }

    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        return databaseType.getSqlDataTypeForChar();
    }

    public String getDefaultInitialValue()
    {
        return "' '";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isCharacter();
    }

    public boolean isComparableTo(JavaType other)
    {
        return other instanceof CharJavaType;
    }

    public String getValueIfNull(String valueIfNull)
    {
        if (valueIfNull.length() == 0) return "' '";
        return "'" + valueIfNull.substring(0, 1) + "'";
    }

    public String getJavaTypeClass()
    {
        return "Char.TYPE";
    }

    public String convertSqlParameter(String param)
    {
        return "String.valueOf("+param+")";
    }

    @Override
    public int getOffHeapSize()
    {
        return 2;
    }
}
