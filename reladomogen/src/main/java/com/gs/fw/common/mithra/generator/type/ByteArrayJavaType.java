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


public class ByteArrayJavaType extends JavaType
{
    @Override
    public String getResultSetName()
    {
        return "Bytes";
    }

    @Override
    public String getExtractionMethodName()
    {
        return "byteArrayValueOf";
    }

    @Override
    public String getValueSetterMethodName()
    {
        return "setByteArrayValue";
    }

    @Override
    public String getTypeName() {
        return "INT";
    }

    @Override
    public String getJavaTypeString() {
        return "byte[]";
    }
    @Override
    public String getJavaTypeStringPrimary() {
        return "ByteArray";
    }

    @Override
    public String convertToObject(String name) {
        return name;
    }

    @Override
    public String convertToPrimitive(String name) {
        throw new RuntimeException("byte[] is not primitive");
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public boolean isNumber()
    {
        return false;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.VARBINARY;
    }

    @Override
    public String getSqlTypeAsString() {
        return "java.sql.Types.VARBINARY";
    }

    @Override
    public String getSqlDataType()
    {
        throw new RuntimeException("not applicable");
    }

    @Override
    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        return databaseType.getSqlDataTypeForVarBinary(); 
    }

    @Override
    public String getDefaultInitialValue() {
        return "null";
    }

    @Override
    public boolean isComparableTo(ASTLiteral node)
    {
        // joins are not allowed on BLOBs
        return false;
    }

    @Override
    public boolean isComparableTo(JavaType other)
    {
        return other instanceof ByteArrayJavaType;
    }

    @Override
    public String getJavaTypeClass()
    {
        return "byte[].class";
    }

    @Override
    public String getObjectComparisonString(String obj1, String obj2)
    {
        return "Arrays.equals("+obj1+ ',' +obj2+ ')';
    }

    @Override
    public boolean isArray()
    {
        return true;
    }

    @Override
    public int getOffHeapSize()
    {
        throw new RuntimeException("not implemented");
    }
}
