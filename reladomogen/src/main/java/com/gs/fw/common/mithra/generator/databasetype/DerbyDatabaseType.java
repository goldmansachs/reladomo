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

public class DerbyDatabaseType implements CommonDatabaseType
{
    private static final DerbyDatabaseType instance = new DerbyDatabaseType();

    public static DerbyDatabaseType getInstance()
    {
        return instance;
    }

    @Override
    public String getSqlDataTypeForNullableBoolean()
    {
        return "smallint";
    }

    public String getSqlDataTypeForBoolean()
    {
        return "smallint";
    }

    public String getSqlDataTypeForTimestamp()
    {
        return "timestamp";
    }

    @Override
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
        return "char";
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
        return "float";
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
        return null;  //todo
    }
}
