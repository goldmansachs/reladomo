


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
 * <b><code>DoubleJavaType</code></b>
 */
public class DoubleJavaType extends PrimitiveWrapperJavaType
{

    public String getTypeName() {
        return "DOUBLE";
    }
    public String getJavaTypeString() {
        return "double";
    }
    public String getJavaTypeStringPrimary() {
        return "Double";
    }

    public String convertToObject(String name) {
        return "new Double(" + name + ")";
    }

    public String convertToPrimitive(String name) {
        return name + ".doubleValue()";
    }

    public int getSqlType() {
        return java.sql.Types.DOUBLE;
    }

    public String getSqlTypeAsString() {
        return "java.sql.Types.DOUBLE";
    }

    public String getSqlDataType()
    {
        return "DOUBLE";
    }

    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        return databaseType.getSqlDataTypeForDouble();
    }

    public String getDefaultInitialValue() {
        return "0.00D";
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        return node.isFloatingPoint() || node.isInteger();
    }

    public String getJavaTypeClass()
    {
        return "Double.TYPE";
    }

    public String getPrimitiveComparisonString(String p1, String p2)
    {
        return "Math.abs("+p1+" - "+p2+") > toleranceForFloatingPointFields";
    }

    @Override
    public String getBeanGetter(int intCount, int longCount, int objectCount)
    {
        return "getL"+longCount+"AsDouble";
    }

    @Override
    public boolean isBeanIntType()
    {
        return false;
    }

    @Override
    public boolean isBeanLongType()
    {
        return true;
    }

    @Override
    public String getBeanSetter(BeanState beanState)
    {
        return "setL"+beanState.getLongCount()+"AsDouble";
    }

    @Override
    public int getOffHeapSize()
    {
        return 8;
    }
}
