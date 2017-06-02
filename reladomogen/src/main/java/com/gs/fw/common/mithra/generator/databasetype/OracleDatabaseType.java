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

public class OracleDatabaseType implements CommonDatabaseType
{
    private static final OracleDatabaseType instance = new OracleDatabaseType();
    /** Singleton */
    protected OracleDatabaseType()
    {
    }

    public static OracleDatabaseType getInstance()
    {
        return instance;
    }

    @Override
    public String getSqlDataTypeForNullableBoolean()
    {
        return "number(1)";
    }

    public String getSqlDataTypeForBoolean()
    {
        return "number(1)";
    }

    public String getSqlDataTypeForTimestamp()
    {
        return "timestamp";
    }

    public String getSqlDataTypeForTime()
    {
        return "interval day (0) to second (5)";
    }

    public String getSqlDataTypeForTinyInt()
    {
        return "number(3)";
    }

    public String getSqlDataTypeForVarBinary()
    {
        return "blob";
    }

    public String getSqlDataTypeForByte()
    {
        return "number(3)";
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
        return "binary_double";
    }

    public String getSqlDataTypeForFloat()
    {
        return "binary_float";
    }

    public String getSqlDataTypeForInt()
    {
        return "number(10)";
    }

    public String getSqlDataTypeForLong()
    {
        return "number(19)";
    }

    public String getSqlDataTypeForShortJava()
    {
        return "number(6)";
    }

    public String getSqlDataTypeForString()
    {
        return "varchar";
    }

    public String getSqlDataTypeForBigDecimal()
    {
        return "number";
    }

    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        return null;  //todo
    }
}
