
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

import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;

/**
 * {@link JavaType} representing Java 5+ <tt>Enum</tt>s. This class is a hybrid: on the Java side, it represents itself
 * as an <tt>Enum</tt> type, but on the database side it represents itself as a <tt>char</tt>, <tt>int</tt>, or
 * <tt>String</tt> type, depending on how the <tt>Enum</tt> is mapped.
 */
public class EnumerationJavaType extends JavaType
{

    private final String wrappedEnum;
    private final JavaType delegate;

    protected EnumerationJavaType(String wrappedEnum, JavaType delegate)
    {
        //TODO: ledav verify wrappedEnum is an Enum
        this.wrappedEnum = wrappedEnum;
        this.delegate = delegate;
    }

    public boolean isPrimitive()
    {
        return false;
    }

    public boolean isNumber()
    {
        return false;
    }

    public boolean isIntOrLong()
    {
        return false;
    }

    public boolean isTimestamp()
    {
        return false;
    }

    @Override
    public String getBeanGetter(int intCount, int longCount, int objectCount)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getOffHeapSize()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isComparableTo(JavaType other)
    {
        return other instanceof EnumerationJavaType;
    }

    public boolean isComparableTo(ASTLiteral node)
    {
        //TODO: ledav modify ASTLiteral to support Enum syntax
        return false;
    }

    public String getTypeName()
    {
        return "ENUM";
    }

    public String getJavaTypeClass()
    {
        return "Enum.class";
    }

    public String getJavaTypeString()
    {
        return this.wrappedEnum;
    }

    public String getJavaTypeStringPrimary()
    {
        return this.getJavaTypeString();
    }

    public String getFinderAttributeType()
    {
        return "EnumAttribute<" + this.wrappedEnum + ">";
    }

    public String getSetCollectionClass()
    {
        //TODO: ledav genericize this
        return "EnumSet";
    }

    public String getIoCast()
    {
        return "(" + this.getJavaTypeString() + ")";
    }

    public String getIoType()
    {
        return "Object";
    }

    public String getDefaultInitialValue()
    {
        return "null";
    }

    public String getValueIfNull(String valueIfNull)
    {
        //TODO: ledav look into this
        return this.delegate.getValueIfNull(valueIfNull);
    }

    public String getObjectComparisonString(String obj1, String obj2)
    {
        return obj1 + " == "+obj2;
    }

    public String convertToPrimitive(String name)
    {
        return name;
    }

    public String convertToObject(String name)
    {
        return name;
    }

    public int getSqlType()
    {
        return this.delegate.getSqlType();
    }

    public String getSqlTypeAsString()
    {
        return this.delegate.getSqlTypeAsString();
    }

    public String getSqlDataType()
    {
        return this.delegate.getSqlDataType();
    }

    public String getSqlDataType(CommonDatabaseType databaseType, boolean nullable)
    {
        return this.delegate.getSqlDataType(databaseType, true);
    }

    public String getResultSetName()
    {
        return this.delegate.getResultSetName();
    }

    public String getArgumentForInOperation(String values, MithraObjectTypeWrapper fromObject)
    {
        return this.delegate.getArgumentForInOperation(values, fromObject);
    }

    public String convertSqlParameter(String param)
    {
        return this.delegate.convertSqlParameter(param);
    }
}
