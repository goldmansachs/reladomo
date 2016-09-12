

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

/**
 * <b><code>IntJavaType</code></b>
 */
public class IntJavaType extends PrimitiveWrapperJavaType{

    public String getTypeName() {
        return "INT";
    }

    public String getJavaTypeString() {
        return "int";
    }
    public String getJavaTypeStringPrimary() {
        return "Integer";
    }

    public String convertToObject(String name) {
        return "Integer.valueOf("+ name +")";
    }

    public String convertToPrimitive(String name) {
        return name + ".intValue()";
    }

    public boolean isIntOrLong()
    {
        return true;
    }

    public int getSqlType() {
        return java.sql.Types.INTEGER;
    }

    public String getSqlTypeAsString() {
        return "java.sql.Types.INTEGER";
    }

    public String getSqlDataType()
    {
        return "integer";
    }

    public String getSqlDataType(CommonDatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForInt();
    }

    public String getDefaultInitialValue() {
        return "0";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isInteger();
    }

    public String getJavaTypeClass()
    {
        return "Integer.TYPE";
    }

    public String getArgumentForInOperation(String values, MithraObjectTypeWrapper fromObject)
    {
        return fromObject.getConstantIntSet(values);
    }

    public String getConnectionManagerClassName()
    {
        return "com.gs.fw.common.mithra.connectionmanager.IntSourceConnectionManager";
    }

    public boolean mayBeIdentity()
    {
        return true;
    }

    @Override
    public int getOffHeapSize()
    {
        return 4;
    }
}
