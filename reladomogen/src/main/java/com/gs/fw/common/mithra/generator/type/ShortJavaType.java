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
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

public class ShortJavaType
extends PrimitiveWrapperJavaType
{

    public String getTypeName()
    {
        return "SHORT";
    }

    public String getJavaTypeString()
    {
        return "short";
    }

    public String getJavaTypeStringPrimary()
    {
        return "Short";
    }

    public String convertToObject(String name)
    {
        return "new Short(" + name + ")";
    }

    public String convertToPrimitive(String name)
    {
        return name + ".shortValue()";
    }

    public int getSqlType()
    {
        return java.sql.Types.SMALLINT;
    }

    public String getSqlTypeAsString()
    {
        return "java.sql.Types.SMALLINT";
    }

    public String getSqlDataType()
    {
        return "smallint";
    }

    public String getSqlDataType(CommonDatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForShortJava();
    }

    public String getDefaultInitialValue()
    {
        return "0";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        if (node.isInteger())
        {
            int intValue = Integer.parseInt(node.getValue());
            return ((intValue >= Short.MIN_VALUE) && (intValue <= Short.MAX_VALUE));
        }

        return false;
    }

    public String getArgumentForInOperation(String values, MithraObjectTypeWrapper fromObject)
    {
        return fromObject.getConstantShortSet(values);
    }

    public String getJavaTypeClass()
    {
        return "Short.TYPE";
    }

    @Override
    public String parseLiteralAndCast(String value)
    {
        return "(short)"+value;
    }

    @Override
    public int getOffHeapSize()
    {
        return 2;
    }
}
