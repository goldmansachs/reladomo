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

public class MariaDatabaseType implements CommonDatabaseType
{
    private static final MariaDatabaseType instance = new MariaDatabaseType();
    /** Singleton */
    protected MariaDatabaseType()
    {
    }


    public static MariaDatabaseType getInstance()
    {
        return instance;
    }

    public String getSqlDataTypeForBoolean()
    {
        return "boolean";
    }

    public String getSqlDataTypeForTimestamp()
    {
        return "DATETIME(3)";
    }

    public String getSqlDataTypeForTime()
    {
        return "TIME(3)";
    }

    public String getSqlDataTypeForTinyInt()
    {
        return "tinyint";
    }

    public String getSqlDataTypeForVarBinary()
    {
        return "blob";
    }

    public String getSqlDataTypeForByte()
    {
        return "tinyint";
    }

    public String getSqlDataTypeForChar()
    {
        return "char";
    }

    public String getSqlDataTypeForDateTime()
    {
        return "datetime";
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
        return null;  //todo
    }

}
