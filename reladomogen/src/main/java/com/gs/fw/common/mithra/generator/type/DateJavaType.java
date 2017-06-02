


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
 * <b><code>DateJavaType</code></b>
 */
public class DateJavaType extends JavaType{

    public String getTypeName() {
        return "DATE";
    }
    public String getJavaTypeString() {
        return "Date";
    }
    public String getJavaTypeStringPrimary() {
        return "Date";
    }

    public String getPrintableForm(String getter)
    {
        return "PrintablePreparedStatement.dateFormat.print("+ getter +".getTime())";
    }

    public String getIoType()
    {
        return "Object";
    }

    public String getIoCast()
    {
        return "(Date)";
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
        return java.sql.Types.DATE;
    }

    public String getSqlTypeAsString() {
        return "java.sql.Types.DATE";
    }

    public String getSqlDataType()
    {
        return "date";
    }

    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        return databaseType.getSqlDataTypeForDateTime();
    }

    public String getDefaultInitialValue() {
        return "null";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        // todo: rezaem: parse the date and make sure it's a real date
        return node.isString();
    }

    public boolean isComparableTo(JavaType other)
    {
        return other instanceof DateJavaType || other instanceof TimestampJavaType;
    }

    public String getJavaTypeClass()
    {
        return "Date.class";
    }

    public String getObjectComparisonString(String obj1, String obj2)
    {
        return obj1+".equals("+obj2+")";
    }

    @Override
    public int getOffHeapSize()
    {
        return 8;
    }
}
