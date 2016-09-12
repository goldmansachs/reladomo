

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

import com.gs.fw.common.mithra.generator.BeanState;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

/**
 * <b><code>FloatJavaType</code></b>
 */
public class FloatJavaType extends PrimitiveWrapperJavaType
{

    public String getTypeName() {
        return "FLOAT";
    }
    public String getJavaTypeString() {
        return "float";
    }
    public String getJavaTypeStringPrimary() {
        return "Float";
    }

    public String convertToObject(String name) {
        return "new Float(" + name + ")";
    }

    public String convertToPrimitive(String name) {
        return name + ".floatValue()";
    }

    public int getSqlType() {
        return java.sql.Types.FLOAT;
    }

    public String getSqlDataType()
    {
        return "float";
    }

    public String getSqlDataType(CommonDatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForFloat();
    }

    public String getSqlTypeAsString() {
        return "java.sql.Types.FLOAT";
    }

    public String getDefaultInitialValue() {
        return "0.00F";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isFloatingPoint() || node.isInteger();
    }

    public String getJavaTypeClass()
    {
        return "Float.TYPE";
    }

    public String getPrimitiveComparisonString(String p1, String p2)
    {
        return "Math.abs("+p1+" - "+p2+") > toleranceForFloatingPointFields";
    }

    @Override
    public String parseLiteralAndCast(String value)
    {
        return "(float)"+value;
    }

    @Override
    public String getBeanSetter(BeanState beanState)
    {
        return "setI"+beanState.getIntCount()+"AsFloat";
    }

    @Override
    public int getOffHeapSize()
    {
        return 4;
    }
}
