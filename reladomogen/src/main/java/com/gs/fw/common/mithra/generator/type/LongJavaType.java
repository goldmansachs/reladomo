


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
import com.gs.fw.common.mithra.generator.util.StringUtility;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

/**
 * <b><code>LongJavaType</code></b>
 */
public class LongJavaType extends PrimitiveWrapperJavaType
{

    public String getTypeName() {
        return "LONG";
    }

    public String getJavaTypeString() {
        return "long";
    }
    public String getJavaTypeStringPrimary() {
        return "Long";
    }

    public String convertToObject(String name) {
        return "new Long(" + name + ")";
    }

    public String convertToPrimitive(String name) {
        return name + ".longValue()";
    }

    public boolean isIntOrLong()
    {
        return true;
    }

    public int getSqlType() {
        return java.sql.Types.BIGINT;
    }

    public String getSqlTypeAsString() {
        return "java.sql.Types.BIGINT";
    }

    public String getSqlDataType()
    {
        return "bigint";
    }

    public String getSqlDataType(CommonDatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForLong();
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
        return "Long.TYPE";
    }

    public boolean mayBeIdentity()
    {
        return true;
    }

    @Override
    public String getBeanGetter(int intCount, int longCount, int objectCount)
    {
        return "getL"+longCount+"AsLong";
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
        return "setL"+beanState.getLongCount()+"AsLong";
    }

    @Override
    public int getOffHeapSize()
    {
        return 8;
    }
}
