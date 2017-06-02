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
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.computedattribute.type.Type;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.util.StringUtility;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;


public abstract class JavaType
{

    public abstract String getTypeName();

    public abstract String getJavaTypeString();

    public abstract String getJavaTypeStringPrimary();

    public String getResultSetName()
    {
        return this.getJavaTypeString();
    }

    public String getIoType()
    {
        return StringUtility.firstLetterToUpper(this.getJavaTypeString());
    }

    public String getIoCast()
    {
        return "";
    }

    public String getPrintableForm(String getter, String nullGetter, boolean isNullable)
    {
        if (isNullable)
        {
            return "("+nullGetter+" ? \"null\" : "+this.getPrintableForm(getter)+")";
        }
        return this.getPrintableForm(getter);
    }

    protected String getPrintableForm(String getter)
    {
        return "(\"\"+"+ getter +")";
    }

    public abstract String convertToObject(String name);

    public abstract String convertToPrimitive(String name);

    public abstract boolean isPrimitive();

    public boolean isNumber()
    {
        return false;
    }

    public boolean isBigDecimal()
    {
        return false;
    }

    public boolean isIntOrLong()
    {
        return false;
    }

    public abstract int getSqlType();

    public abstract String getSqlTypeAsString();

    public abstract String getSqlDataType();

    public abstract String getSqlDataType(CommonDatabaseType databaseType, boolean nullable);

    public abstract String getDefaultInitialValue();

    public abstract boolean isComparableTo(ASTLiteral node);

    public abstract boolean isComparableTo(JavaType other);

    public String getExtractionMethodName()
    {
        return StringUtility.firstLetterToLower(this.getJavaTypeString())+"ValueOf";
    }

    public static JavaType create(String javaTypeName) throws JavaTypeException
    {
        if (javaTypeName.equalsIgnoreCase("INT")) return new IntJavaType();
        if (javaTypeName.equalsIgnoreCase("STRING")) return new StringJavaType();
        if (javaTypeName.equalsIgnoreCase("DOUBLE")) return new DoubleJavaType();
        if (javaTypeName.equalsIgnoreCase("FLOAT")) return new FloatJavaType();
        if (javaTypeName.equalsIgnoreCase("BOOLEAN")) return new BooleanJavaType();
        if (javaTypeName.equalsIgnoreCase("DATE")) return new DateJavaType();
        if (javaTypeName.equalsIgnoreCase("TIME")) return new TimeJavaType();
        if (javaTypeName.equalsIgnoreCase("TIMESTAMP")) return new TimestampJavaType();
        if (javaTypeName.equalsIgnoreCase("CHAR")) return new CharJavaType();
        if (javaTypeName.equalsIgnoreCase("LONG")) return new LongJavaType();
        if (javaTypeName.equalsIgnoreCase("BYTE")) return new ByteJavaType();
        if (javaTypeName.equalsIgnoreCase("SHORT")) return new ShortJavaType();
        if (javaTypeName.equalsIgnoreCase("BYTE[]")) return new ByteArrayJavaType();
        if (javaTypeName.equalsIgnoreCase("BIGDECIMAL")) return new BigDecimalJavaType();

        // nothing found so we have a bad type
        throw new JavaTypeException("Invalid JavaType " + javaTypeName);
    }

    public static JavaType createEnumeration(String wrappedEnum, String javaTypeName) throws JavaTypeException
    {
        return new EnumerationJavaType(wrappedEnum, JavaType.create(javaTypeName));
    }

    public String getFinderAttributeType()
    {
        return this.getJavaTypeStringPrimary()+"Attribute";
    }

    public boolean equals(Object obj)
    {
        if ((obj != null) && (obj instanceof JavaType))
        {
            return getTypeName().equals(((JavaType) obj).getTypeName());
        }
        return false;
    }

    public int hashCode()
    {
        return getTypeName().hashCode();
    }

    public String createEquals(String value1, String value2)
    {
        if (isPrimitive())
        {
            return value1 + " == " + value2;
        }
        else
        {
            return value1 + ".equals(" + value2 + ")";
        }
    }

    public String getValueIfNull(String valueIfNull)
    {
        return valueIfNull;
    }

    public String getGetterPrefix()
    {
        return "get";
    }

    public abstract String getJavaTypeClass();

    public String getValueSetterMethodName()
    {
        return "set"+StringUtility.firstLetterToUpper(this.getJavaTypeString())+"Value";
    }

	public String getArgumentForInOperation(String values, MithraObjectTypeWrapper fromObject)
	{
		throw new RuntimeException("in operation not supported for " + this.getJavaTypeString());
	}

    public String getSetCollectionClass()
    {
        if (this.isPrimitive())
        {
            return StringUtility.firstLetterToUpper(this.getJavaTypeString())+"OpenHashSet";
        }
        else
        {
            return "HashSet";
        }
    }

	public String getConnectionManagerClassName()
	{
		throw new RuntimeException("operation not suppported for " + this.getJavaTypeString());
	}

    public String convertSqlParameter(String param)
    {
        return param; // char will override
    }

    public String getFinderAttributeSuperClassType()
    {
        return "SingleColumn"+this.getJavaTypeStringPrimary()+"Attribute";
    }

    public String getObjectComparisonString(String obj1, String obj2)
    {
        throw new RuntimeException("operation not suppported for " + this.getJavaTypeString());
    }

    public String getPrimitiveComparisonString(String p1, String p2)
    {
        throw new RuntimeException("operation not suppported for " + this.getJavaTypeString());
    }

    public boolean isTimestamp()
    {
        return false;
    }

    public boolean canBePooled()
    {
        return false;
    }

    public boolean mayBeIdentity()
    {
        return false;
    }

    public boolean isArray()
    {
        return false;
    }

    public String parseLiteralAndCast(String value)
    {
        return value;
    }

    public String getBeanGetter(int intCount, int longCount, int objectCount)
    {
        return "getO"+objectCount+"As"+ StringUtility.firstLetterToUpper(this.getJavaTypeStringPrimary());
    }

    public boolean isBeanIntType()
    {
        return false;
    }

    public boolean isBeanLongType()
    {
        return false;
    }

    public boolean isBeanObjectType()
    {
        return true;
    }

    public String getBeanSetter(BeanState beanState)
    {
        return "setO"+beanState.getObjectCount();
    }

    public abstract int getOffHeapSize();

    public boolean isBoolean()
    {
        return false;
    }

    public Type asComputedAttributeType()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isTime()
    {
        return false;
    }

    public String getSqlTypeAsStringForNull()
    {
        return getSqlTypeAsString();
    }
}