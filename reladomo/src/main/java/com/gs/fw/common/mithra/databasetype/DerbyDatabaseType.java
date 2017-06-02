
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

import java.sql.SQLException;
import java.sql.Types;


public class DerbyDatabaseType extends AbstractDatabaseType
{

    public static final int MAX_CLAUSES = 240;

    private static final DerbyDatabaseType instance = new DerbyDatabaseType();

    public static DerbyDatabaseType getInstance()
    {
        return instance;
    }

    protected boolean hasRowLevelLocking()
    {
        return true;
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    protected boolean hasSelectUnionMultiInsert()
    {
        return false;
    }

    // derby has values multi insert, but for test purposes, we'll use it as if it didn't
    protected boolean hasValuesMultiInsert()
    {
        return false;
    }

    public int getMultiInsertBatchSize(int columnsToInsert)
    {
        return 0;
    }

    public String getLastIdentitySql(String tableName)
    {
        return "values IDENTITY_VAL_LOCAL()";
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public String getDeleteStatementForTestTables()
    {
        return "delete from ";
    }

    @Override
    public String getConversionFunctionIntegerToString(String expression)
    {
        return "cast("+expression+" as char(11))";
    }

    @Override
    public String getConversionFunctionStringToInteger(String expression)
    {
        return "cast("+expression+" as int)";
    }

    @Override
    public String getSqlExpressionForDateYear(String columnName)
    {
        return "YEAR("+columnName+")";
    }

    @Override
    public String getSqlExpressionForDateMonth(String columnName)
    {
        return "MONTH("+columnName+")";
    }

    @Override
    public String getSqlExpressionForDateDayOfMonth(String columnName)
    {
        return "DAY("+columnName+")";
    }

    @Override
    public int getNullableBooleanJavaSqlType()
    {
        return Types.SMALLINT;
    }
}
