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

public class MsSqlDatabaseType implements CommonDatabaseType
{
    private static final Map<String, String> sqlToJavaTypes;

    private static final MsSqlDatabaseType instance = new MsSqlDatabaseType();

    static
    {
        sqlToJavaTypes = new HashMap<String, String>();
        sqlToJavaTypes.put("integer", "int");
        sqlToJavaTypes.put("smallint", "short");
        sqlToJavaTypes.put("tinyint", "byte");
        sqlToJavaTypes.put("float", "float");
        sqlToJavaTypes.put("double precision", "double");
        sqlToJavaTypes.put("double precis", "double");
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
        sqlToJavaTypes.put("time", "Time");

    }

    public static MsSqlDatabaseType getInstance()
    {
        return instance;
    }

    @Override
    public String getSqlDataTypeForNullableBoolean()
    {
        return "tinyint";
    }

    public String getSqlDataTypeForBoolean()
    {
        return "bit";
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
        return "image";
    }

    public String getSqlDataTypeForByte()
    {
        return "tinyint";
    }

    public String getSqlDataTypeForChar()
    {
        return "char(1)";
    }

    public String getSqlDataTypeForDateTime()
    {
        return "datetime";
    }

    public String getSqlDataTypeForDouble()
    {
        return "double precision";
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
        return "numeric(19,0)";
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
        return "numeric";
    }

    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        String javaType = sqlToJavaTypes.get(sql);

        if (sql.equals("numeric"))
        {
            if (decimal != 0)
            {
                javaType =  "double";
            }
            else if (precision <= 8)
            {
                javaType =  "int";
            }
            else
            {
               javaType =  "long";
            }
        }
        if("char".equals(sql))
        {
            if(precision > 1)
            {
                javaType = "String";
            }
        }
        return javaType;
    }
}
