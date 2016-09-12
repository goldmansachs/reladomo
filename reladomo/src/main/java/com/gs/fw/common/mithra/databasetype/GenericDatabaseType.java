
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

package com.gs.fw.common.mithra.databasetype;

import com.gs.fw.common.mithra.finder.SqlQuery;

import java.sql.SQLException;


public class GenericDatabaseType extends AbstractDatabaseType
{
    public static final int MAX_CLAUSES = 240;

    private static final GenericDatabaseType instance = new GenericDatabaseType();

    public static GenericDatabaseType getInstance()
    {
        return instance;
    }

    protected boolean isRetriableWithoutRecursion(SQLException exception)
    {
        return false;
    }

    protected boolean isTimedOutWithoutRecursion(SQLException exception)
    {
        return false;
    }

    public String getSqlDataTypeForBoolean()
    {
        return "boolean";
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
        return "tinyint";
    }

    public String getSqlDataTypeForVarBinary()
    {
        return "binary";
    }

    public String getSqlDataTypeForByte()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForChar()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForDateTime()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForDouble()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForFloat()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForInt()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForLong()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForShortJava()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForString()
    {
        return "not implemented";
    }

    public String getSqlDataTypeForBigDecimal()
    {
         return "not implemented";
    }

    public String getFullyQualifiedTableName(String schema, String tableName)
    {
        throw new RuntimeException("Schemas not supported");
    }

    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        return null;
    }

    protected boolean hasSelectUnionMultiInsert()
    {
        return false;
    }

    protected boolean hasValuesMultiInsert()
    {
        return false;
    }

    public int getMultiInsertBatchSize(int columnsToInsert)
    {
        return 0;
    }

    public String getDelete(SqlQuery query, int rowCount)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getLastIdentitySql(String tableName)
    {
        throw new RuntimeException("not implemented");
    }

    public String getIdentityTableCreationStatement()
    {
        throw new RuntimeException("not implemented");
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getConversionFunctionIntegerToString(String expression)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getConversionFunctionStringToInteger(String expression)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getSqlExpressionForDateYear(String columnName)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getSqlExpressionForDateMonth(String columnName)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getSqlExpressionForDateDayOfMonth(String columnName)
    {
        throw new RuntimeException("not implemented");
    }
}
