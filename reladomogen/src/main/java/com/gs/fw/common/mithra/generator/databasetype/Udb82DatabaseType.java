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

public class Udb82DatabaseType implements CommonDatabaseType
{
    private static final Udb82DatabaseType instance = new Udb82DatabaseType();

    private static final Map<String, String> sqlToJavaTypes;

    static
    {
        sqlToJavaTypes = new HashMap<String, String>();
        sqlToJavaTypes.put("blob", "not implemented");
        sqlToJavaTypes.put("bigint", "long");
        sqlToJavaTypes.put("integer", "int");
        sqlToJavaTypes.put("smallint", "short");
        sqlToJavaTypes.put("tinyint", "short");
        sqlToJavaTypes.put("double", "double");
        sqlToJavaTypes.put("character", "String");
        sqlToJavaTypes.put("char", "String");
        sqlToJavaTypes.put("varchar", "String");
        sqlToJavaTypes.put("blob", "String");
        sqlToJavaTypes.put("clob", "String");
        sqlToJavaTypes.put("date", "Timestamp");
        sqlToJavaTypes.put("time", "Time");
        sqlToJavaTypes.put("timestamp", "Timestamp");
        sqlToJavaTypes.put("bit", "byte");
        sqlToJavaTypes.put("decimal", "BigDecimal");
        sqlToJavaTypes.put("real", "float");
    }

    /** Extendable Singleton */
    protected Udb82DatabaseType()
    {
    }

    public static Udb82DatabaseType getInstance()
    {
        return instance;
    }

    public String getSqlDataTypeForBoolean()
    {
        return "smallint";
    }

    public String getSqlDataTypeForTimestamp()
    {
        return "timestamp";
    }

    public String getSqlDataTypeForTime()
    {
        return "time";
    }

    public String getSqlDataTypeForTinyInt()
    {
        return "smallint";
    }

    public String getSqlDataTypeForVarBinary()
    {
        return "blob";
    }

    public String getSqlDataTypeForByte()
    {
        return "smallint";
    }

    public String getSqlDataTypeForChar()
    {
        return "character";
    }

    public String getSqlDataTypeForDateTime()
    {
        return "date";
    }

    public String getSqlDataTypeForDouble()
    {
        return "double";
    }

    public String getSqlDataTypeForFloat()
    {
        return "real";
    }

    public String getSqlDataTypeForInt()
    {
        return "integer";
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
