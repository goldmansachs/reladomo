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

package com.gs.fw.common.mithra.generator.databasetype;

import java.util.HashMap;
import java.util.Map;

public class H2DatabaseType implements CommonDatabaseType
{
    private static final H2DatabaseType instance = new H2DatabaseType();
    private static final Map<String, String> sqlToJavaTypes;

    static
    {
        sqlToJavaTypes = new HashMap<String, String>();
        sqlToJavaTypes.put("integer", "int");
        sqlToJavaTypes.put("smallint", "short");
        sqlToJavaTypes.put("tinyint", "byte");
        sqlToJavaTypes.put("float", "float");
        sqlToJavaTypes.put("double precision", "double");
        sqlToJavaTypes.put("double precis", "double");
        sqlToJavaTypes.put("double", "double");
        sqlToJavaTypes.put("smallmoney", "not implemented");
        sqlToJavaTypes.put("money", "not implemented");
        sqlToJavaTypes.put("char", "char");
        sqlToJavaTypes.put("varchar", "String");
        sqlToJavaTypes.put("text", "String");
        sqlToJavaTypes.put("image", "byte[]");
        sqlToJavaTypes.put("datetime", "Timestamp");
        sqlToJavaTypes.put("smalldatetime", "Timestamp");
        sqlToJavaTypes.put("timestamp", "Timestamp");
        sqlToJavaTypes.put("bit", "boolean");
        sqlToJavaTypes.put("binary", "not implemented");
        sqlToJavaTypes.put("varbinary", "not implemented");
        sqlToJavaTypes.put("decimal", "BigDecimal");
        sqlToJavaTypes.put("real", "float");
        sqlToJavaTypes.put("date", "Timestamp");
//        sqlToJavaTypes.put("time", "Timestamp");
        sqlToJavaTypes.put("time", "Time");

    }
    /**
     * Singleton. Protected visibility to allow test harness to subclass.
     */
    protected H2DatabaseType()
    {
    }

    public static H2DatabaseType getInstance()
    {
        return instance;
    }

    public String getSqlDataTypeForBoolean()
    {
        return "boolean";
    }

    public String getSqlDataTypeForTimestamp()
    {
        return "datetime";
    }

    @Override
    public String getSqlDataTypeForTime()
    {
        return "time";
    }

    public String getSqlDataTypeForTinyInt()
    {
        return "tinyint";
    }

    public String getSqlDataTypeForVarBinary()
    {
        return "binary";
    }

    public String getSqlDataTypeForByte()
    {
        return "tinyint";
    }

    public String getSqlDataTypeForChar()
    {
        return "varchar(1)";
    }

    public String getSqlDataTypeForDateTime()
    {
        return "timestamp";
    }

    public String getSqlDataTypeForDouble()
    {
        return "double";
    }

    public String getSqlDataTypeForFloat()
    {
        return "double";
    }

    public String getSqlDataTypeForInt()
    {
        return "int";
    }

    public String getSqlDataTypeForLong()
    {
        return "bigint";
    }

    public String getSqlDataTypeForShortJava()
    {
        return "smallint";
    }

    public String getSqlDataTypeForString()
    {
        return "varchar";
    }

    public String getSqlDataTypeForBigDecimal()
    {
        return "decimal";
    }

    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        return sqlToJavaTypes.get(sql);
    }
}
