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

public class ByteJavaType
extends PrimitiveWrapperJavaType
{

    public String getTypeName()
    {
        return "BYTE";
    }

    public String getJavaTypeString()
    {
        return "byte";
    }

    public String getJavaTypeStringPrimary()
    {
        return "Byte";
    }

    public String convertToObject(String name)
    {
        return "new Byte(" + name + ")";
    }

    public String convertToPrimitive(String name)
    {
        return name + ".byteValue()";
    }

    public int getSqlType()
    {
        return java.sql.Types.TINYINT;
    }

    public String getSqlTypeAsString()
    {
        return "java.sql.Types.TINYINT";
    }

    public String getSqlDataType()
    {
        return "tinyint";
    }

    public String getSqlDataType(CommonDatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForByte();
    }

    public String getDefaultInitialValue()
    {
        return "0";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        if (node.isInteger())
        {
            int intValue = new Integer(node.getValue()).intValue();
            return ((intValue >= Byte.MIN_VALUE) && (intValue <= Byte.MAX_VALUE));
        }

        return false;
    }

    public String getJavaTypeClass()
    {
        return "Byte.TYPE";
    }

    @Override
    public String parseLiteralAndCast(String value)
    {
        return "(byte)"+value;
    }

    @Override
    public int getOffHeapSize()
    {
        return 1;
    }
}
