


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
 * <b><code>TimestampJavaType</code></b>
 */
public class TimestampJavaType extends JavaType{

    public String getTypeName() {
        return "TIMESTAMP";
    }
    public String getJavaTypeString() {
        return "Timestamp";
    }
    public String getJavaTypeStringPrimary() {
        return "Timestamp";
    }

    public String getPrintableForm(String getter, String nullGetter, boolean isNullable)
    {
        return super.getPrintableForm(getter, nullGetter, true);
    }

    public String getPrintableForm(String getter)
    {
        return "PrintablePreparedStatement.timestampFormat.print("+ getter +".getTime())";
    }

    public String getIoType()
    {
        return "Object";
    }

    public String getIoCast()
    {
        return "(Timestamp)";
    }

    public String convertToObject(String name) {
        return name;
    }

    public String convertToPrimitive(String name) {
        return name;
    }

    public boolean isPrimitive() {
        return false;
    }

    public int getSqlType() {
        return java.sql.Types.TIMESTAMP;
    }

    public String getSqlTypeAsString() {
        return "java.sql.Types.TIMESTAMP";
    }

    public String getSqlDataType()
    {
        return "datetime";
    }

    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        return databaseType.getSqlDataTypeForTimestamp();
    }

    public String getDefaultInitialValue() {
        return "null";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        //todo: rezaem: check the date format
        return node.isString();
    }

    public boolean isComparableTo(JavaType other)
    {
        return other instanceof DateJavaType || other instanceof TimestampJavaType;
    }

    public String getJavaTypeClass()
    {
        return "Timestamp.class";
    }

    public String getObjectComparisonString(String obj1, String obj2)
    {
        return obj1+".equals("+obj2+")";
    }

    public boolean isTimestamp()
    {
        return true;
    }

    public boolean canBePooled()
    {
        return true;
    }

    @Override
    public int getOffHeapSize()
    {
        return 8;
    }
}
