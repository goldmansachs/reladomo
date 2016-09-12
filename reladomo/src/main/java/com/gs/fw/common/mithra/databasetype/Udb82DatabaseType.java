
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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import com.gs.fw.common.mithra.util.MutableDouble;

import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;


public class Udb82DatabaseType extends AbstractDatabaseType
{

    private static final String DEADLOCK_OR_TIMEOUT1 = "57033";
    private static final String DEADLOCK_OR_TIMEOUT2 = "40001";
    private static final int DEADLOCK_OR_TIMEOUT_ERROR_CODE = -911;
    private static final int DISCONNECT_ERROR_CODE = -4499;
    private static final int CONNECTION_NON_EXISTANT_ERROR_CODE = -1024;
    private static final int DUPLICATE_INSERT_UPDATE = -803;
    private static final int CONNECTION_NON_EXISTANT_ERROR_CODE2 = -900;
//    private static final String DEADLOCK_REASON_CODE = "00C90088";
    private static final String TIMEOUT_REASON_CODE = "00C9008E";
    private static final String READ_ONLY = " FOR READ ONLY";
    private static final String SHARED_LOCKS = " WITH RR USE AND KEEP SHARE LOCKS";
    private static final String WITHOUT_LOCKS = " WITH CS";

    private static final int MAX_CLAUSES = 1000;

    private String tempSchemaName = "SESSION";
    private String tableSpace = null;

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

    public void setTempSchemaName(String tempSchemaName)
    {
        this.tempSchemaName = tempSchemaName;
    }

    public String getTableSpace()
    {
        return tableSpace;
    }

    public void setTableSpace(String tableSpace)
    {
        this.tableSpace = tableSpace;
    }

    @Override
    public boolean hasTopQuery()
    {
        return true;
    }

    @Override
    public String getSelect(String columns, SqlQuery query, String groupBy, boolean isInTransaction, int rowCount)
    {
        StringBuilder builder = this.getSelectAsStringBuilder(columns, query, groupBy, isInTransaction);
        if (rowCount > 0)
        {
            builder.append(" FETCH FIRST ").append(rowCount+1).append(" ROWS ONLY");
        }
        builder.append(READ_ONLY);
        if (isInTransaction)
        {
            MithraObjectPortal portal = query.getAnalyzedOperation().getOriginalOperation().getResultObjectPortal();
            if (portal.getTxParticipationMode().mustLockOnRead())
            {
                builder.append(SHARED_LOCKS);
            }
            else
            {
                builder.append(WITHOUT_LOCKS);
            }
        }
        return builder.toString();
    }

    @Override
    public String getSelect(String columns, String fromClause, String whereClause, boolean lock)
    {
        StringBuilder selectClause = new StringBuilder("select ").append(columns).append(" from ").append(fromClause);
        if (whereClause != null)
        {
            selectClause.append(" where ").append(whereClause);
        }

        selectClause.append(READ_ONLY);
        if (lock)
        {
            selectClause.append(SHARED_LOCKS);
        }
        else
        {
            selectClause.append(WITHOUT_LOCKS);
        }
        return selectClause.toString();
    }

    @Override
    public String getDelete(SqlQuery query, int rowCount)
    {
        StringBuilder deleteClause = new StringBuilder("delete from ");
        String whereClause = query.getWhereClauseAsString(0);
        String fromClause = query.getFromClauseAsString();
        if(rowCount > 0)
        {
            deleteClause.append(" (select 1 from ").append(fromClause);
            if (whereClause.trim().length() > 0)
            {
                deleteClause.append(" where ").append(whereClause);
            }
            deleteClause.append(" fetch first ").append(rowCount).append(" rows only) ");
        }
        else
        {
            deleteClause.append(fromClause);
            if (whereClause.trim().length() > 0)
            {
                deleteClause.append(" where ").append(whereClause);
            }
        }
        return deleteClause.toString();
    }

    @Override
    protected boolean hasRowLevelLocking()
    {
        return true;
    }

    @Override
    public boolean isRetriableWithoutRecursion(SQLException e)
    {
        return DEADLOCK_OR_TIMEOUT_ERROR_CODE == e.getErrorCode()
               || DEADLOCK_OR_TIMEOUT1.equals(e.getSQLState())
               || DEADLOCK_OR_TIMEOUT2.equals(e.getSQLState())
                || (e.getMessage() != null && e.getMessage().trim().endsWith("-911"));
    }

    @Override
    protected boolean isTimedOutWithoutRecursion(SQLException e)
    {
        return DEADLOCK_OR_TIMEOUT_ERROR_CODE == e.getErrorCode() && e.getMessage().contains(TIMEOUT_REASON_CODE);
    }

    @Override
    public boolean violatesUniqueIndexWithoutRecursion(SQLException e)
    {
        return DUPLICATE_INSERT_UPDATE == e.getErrorCode();
    }

    @Override
    public int getMaxClauses()
    {
        return MAX_CLAUSES;
    }

    @Override
    public int getMaxPreparedStatementBatchCount(int parametersPerStatement)
    {
        return 1000;
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

    @Override
    public String getFullyQualifiedTableName(String schema, String tableName)
    {
        return schema != null ? schema + '.' + tableName : tableName;
    }

    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        return sqlToJavaTypes.get(sql);
    }

    @Override
    protected boolean hasSelectUnionMultiInsert()
    {
        return false;
    }

    @Override
    protected boolean hasValuesMultiInsert()
    {
        return true;
    }

    public int getMultiInsertBatchSize(int columnsToInsert)
    {
        return 200; // 200 is the best choice for thin and wide tables alike
    }

    public String getLastIdentitySql(String tableName)
    {
        return "SELECT IDENTITY_VAL_LOCAL() FROM "+tableName;
    }

    @Override
    public String getIdentityTableCreationStatement()
    {
        return " GENERATED ALWAYS AS IDENTITY";
    }

    @Override
    public String getAllowInsertIntoIdentityStatementFor(String tableName, String onOff)
    {
        return tableName + " OVERRIDING SYSTEM VALUE ";
    }

    @Override
    public boolean supportsMultiValueInClause()
    {
        // todo: the syntax does not work with prepared statements. Need to investigate more.
        return false;
    }

    @Override
    public String getPerStatementLock(boolean lock)
    {
        String result = READ_ONLY;
        if (lock)
        {
            result += SHARED_LOCKS;
        }
        else
        {
            result += WITHOUT_LOCKS;
        }
        return result;
    }

    @Override
    public boolean isConnectionDeadWithoutRecursion(SQLException e)
    {
        return (e.getErrorCode() == CONNECTION_NON_EXISTANT_ERROR_CODE || e.getErrorCode() == CONNECTION_NON_EXISTANT_ERROR_CODE2
                || e.getErrorCode() == DISCONNECT_ERROR_CODE);
    }

    @Override
    public void configureConnection(Connection con) throws SQLException
    {
        // the following does not seem to have any effect, so it's disabled
//        fullyExecute(con, "call sysproc.set_routine_opts('REOPT ONCE')");
    }

    @Override
    public String getTableNameForNonSharedTempTable(String nominalName)
    {
        return "SESSION"+"."+nominalName;
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        return "CREATE TABLE "+this.tempSchemaName+"."+nominalTableName;
    }

    @Override
    public String getSqlPostfixForNonSharedTempTableCreation()
    {
        return " NOT LOGGED ON COMMIT PRESERVE ROWS";
    }

    @Override
    public void setSchemaOnConnection(Connection con, String schema) throws SQLException
    {
        Statement statement = null;
        try
        {
            statement = con.createStatement();
            statement.execute("set schema " + schema);
        }
        finally
        {
            if (statement != null) statement.close();
        }
    }

    @Override
    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        sb.append("DECLARE GLOBAL TEMPORARY TABLE SESSION.");
        sb.append(tempTableName);
        return getTableNameForNonSharedTempTable(tempTableName);
    }

    @Override
    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        String tableName = this.tempSchemaName+"."+tempTableName;
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        return tableName;
    }

    @Override
    public String getDeleteStatementForTestTables()
    {
        return "delete from ";
    }

    @Override
    public String getConversionFunctionIntegerToString(String expression)
    {
        return "char("+expression+")";
    }

    @Override
    public String getConversionFunctionStringToInteger(String expression)
    {
        return "int("+expression+")";
    }

    @Override
    public String createNonSharedIndexSql(String fullTableName, CharSequence indexColumns)
    {
        return this.createIndexSql(fullTableName, indexColumns, "SESSION");
    }

    @Override
    protected String createIndexSql(String fullTableName, CharSequence indexColumns)
    {
        return this.createIndexSql(fullTableName, indexColumns, this.tempSchemaName);
    }

    protected String createIndexSql(String fullTableName, CharSequence indexColumns, String indexSchemaName)
    {
        final StringBuilder indexSql = new StringBuilder("CREATE UNIQUE INDEX ");

        String partialTableName;
        int lastDot = fullTableName.lastIndexOf('.');
        if (lastDot > 0)
        {
            partialTableName = fullTableName.substring(lastDot+2, fullTableName.length());
        }
        else
        {
            partialTableName = fullTableName.substring(1, fullTableName.length());
        }
        indexSql.append(indexSchemaName).append('.').append('I').append(partialTableName).append(" ON ").append(fullTableName).append(" (");

        indexSql.append(indexColumns);

        indexSql.append(')');

        return indexSql.toString();
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
    public void fixSampleStandardDeviation(MutableDouble obj, int count)
    {
        double sumOfSquares = obj.doubleValue() * obj.doubleValue() * count;
        obj.replace(Math.sqrt(sumOfSquares / (count - 1)));
    }

    @Override
    public void fixSampleVariance(MutableDouble obj, int count)
    {
        double sumOfSquares = obj.doubleValue() * count;
        obj.replace(sumOfSquares / (count - 1));
    }

    @Override
    public String getSqlExpressionForStandardDeviationPop(String columnName)
    {
        return "stddev(" + columnName + ")";
    }

    @Override
    public String getSqlExpressionForVariancePop(String columnName)
    {
        return "variance(" +columnName + ")";
    }

    @Override
    public void setTimestamp(PreparedStatement ps, int index, Timestamp timestamp, boolean forceAsString, TimeZone timeZone) throws SQLException
    {
        if (forceAsString)
        {
            String s = convertDateToString(timestamp);
            ps.setString(index, s);
        }
        else
        {
            Calendar c = getCalendarInstanceUtc();
            Timestamp ts = MithraTimestamp.zConvertTimeForWritingWithUtcCalendar(timestamp, timeZone);
            if (ts == timestamp)
            {
                ts = new Timestamp(timestamp.getTime());  // some versions of the db2 driver modify the timestamp!
            }
            ps.setTimestamp(index, ts, c);
        }
    }

    @Override
    public Timestamp getTimestampFromResultSet(ResultSet rs, int pos, TimeZone timeZone) throws SQLException
    {
        Calendar c = getCalendarInstanceUtc();
        Timestamp timestamp = rs.getTimestamp(pos, c);
        if (null != timestamp)
        {
            timestamp = new Timestamp(timestamp.getTime());
        }
        return MithraTimestamp.zConvertTimeForReadingWithUtcCalendar(timestamp, timeZone);
    }

    @Override
    public String getSqlPostfixForSharedTempTableCreation()
    {
        if (this.tableSpace != null)
        {
            return " IN " + this.tableSpace;
        }
        return "";
    }

    @Override
    public void appendTestTableCreationPostamble(StringBuilder sb)
    {
        if (this.tableSpace != null)
        {
            sb.append(" IN ").append(this.tableSpace);
        }
    }
}
