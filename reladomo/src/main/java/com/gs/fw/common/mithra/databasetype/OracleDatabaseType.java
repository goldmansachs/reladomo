
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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import com.gs.fw.common.mithra.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;


public class OracleDatabaseType extends AbstractDatabaseType
{
    private static final Logger logger = LoggerFactory.getLogger(OracleDatabaseType.class.getName());
    private static final String TIMEOUT_SQL_STATE = "HYT00";
    private static final int TIMEOUT_SQL_ERROR_CODE = 50200;
    private static final int DUPLICATE_ERROR_CODE = 23001;
    public static final int MAX_CLAUSES = 240;
    private static final OracleDatabaseType instance = new OracleDatabaseType();
    private String tempSchema = null;

    private static final int CODE_CONNECTION_INCONSISTENT = 17447;
    private static final int CODE_CONNECTION_CLOSED = 17008;
    private static final int CODE_STREAM_CLOSED = 17027;
    private static final int CODE_SOCKET_ERROR = 17410;

    public static Logger getLogger()
    {
        return logger;
    }

    /** Singleton */
    protected OracleDatabaseType()
    {
    }

    @Override
    public int getMaxPreparedStatementBatchCount(int parametersPerStatement)
    {
        return 100;
    }

    public void setTempSchema(String tempSchema)
    {
        this.tempSchema = tempSchema;
    }

    public String convertDateToString(java.util.Date date)
    {
        //todo: default oracle format is DD-MON-YY, e.g. '01-JAN-03 01:02:03.000' for a timestamp and '01-JAN-03' for a date
        //todo: currently, we don't differentiate between timestamp and date
        return super.convertDateToString(date);
    }

    public String getSelect(String columns, SqlQuery query, String groupBy, boolean isInTransaction, int rowCount)
    {
        String result = super.getSelect(columns, query, groupBy, isInTransaction, rowCount);
        if (isInTransaction)
        {
            MithraObjectPortal portal = query.getAnalyzedOperation().getOriginalOperation().getResultObjectPortal();
            if (portal.getTxParticipationMode().mustLockOnRead())
            {
                int commaIndex = columns.indexOf(',');
                if (commaIndex == -1)
                {
                    result += " FOR UPDATE OF " + columns;
                }
                else
                {
                    result += " FOR UPDATE OF " + columns.substring(0, commaIndex);
                }
            }
        }
        return result;
    }

    public String getSelect(String columns, String fromClause, String whereClause, boolean lock)
    {
        String result = super.getSelect(columns, fromClause, whereClause, lock);
        if (lock)
        {
            int commaIndex = columns.indexOf(',');
            if (commaIndex == -1)
            {
                result += " FOR UPDATE OF " + columns;
            }
            else
            {
                result += " FOR UPDATE OF " + columns.substring(0, commaIndex);
            }
        }
        return result;
    }

    public String getDelete(SqlQuery query, int rowCount)
    {
        StringBuilder deleteClause = new StringBuilder("delete ");

        deleteClause.append(" from ").append(query.getFromClauseAsString());
        String where = query.getWhereClauseAsString(0);
        boolean hasWhereClause = where.trim().length() > 0;
        boolean hasRowCount =  rowCount > 0;
        if (hasWhereClause || hasRowCount)
        {
            deleteClause.append(" where ");
        }
        if (hasWhereClause)
        {
            deleteClause.append(where);
            if(hasRowCount)
            {
                deleteClause.append(" and ");
            }
        }
        if(hasRowCount)
        {
            deleteClause.append(" ROWNUM <= ").append(rowCount);
        }

        return deleteClause.toString();
    }

    public static OracleDatabaseType getInstance()
    {
        return instance;
    }

    public String getPerStatementLock(boolean lock)
    {
        // note: include " OF + {columnName} "
        // if / when this is uncommented

//        if (lock)
//        {
//            return " FOR UPDATE";
//        }
        return "";
    }

    protected boolean hasRowLevelLocking()
    {
        return true;
    }

    protected boolean isRetriableWithoutRecursion(SQLException sqlException)
    {
        return sqlException.getErrorCode() == 40001;
    }

    protected boolean isTimedOutWithoutRecursion(SQLException exception)
    {
        return TIMEOUT_SQL_STATE.equals(exception.getSQLState()) && TIMEOUT_SQL_ERROR_CODE==exception.getErrorCode();
    }

    public boolean violatesUniqueIndexWithoutRecursion(SQLException exception)
    {
        return DUPLICATE_ERROR_CODE == exception.getErrorCode();
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        return "create global temporary table "+nominalTableName + getSqlPostfixForNonSharedTempTableCreation();
    }

    public String getSqlPostfixForNonSharedTempTableCreation()
    {
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            return " on commit DELETE ROWS";
        }
        return " on commit preserve rows";
    }

    public String getSqlPostfixForSharedTempTableCreation()
    {
        return "";
//        return " on commit preserve rows";
    }

    @Override
    public void setTime(PreparedStatement ps, int index, Time time) throws SQLException
    {
        if(time == null)
            ps.setNull(index, Types.VARCHAR);
        else
            ps.setString(index, "0 " + time.toString());
    }

    @Override
    public void setTimeNull(PreparedStatement ps, int index) throws SQLException
    {
        ps.setNull(index, Types.VARCHAR);
    }

    @Override
    public Time getTime(ResultSet resultSet, int position) throws SQLException
    {
        String string = resultSet.getString(position);

        if(string == null)
            return null;

        String newString = string.substring(1).trim();
        try
        {
            return parseStringAndSet(newString);
        }
        catch (ParseException e)
        {
            throw new RuntimeException("Could not parse string '"+newString+"'");
        }
    }

    public static Time parseStringAndSet(String value) throws ParseException
    {
        int hour = 0;
        int start = 0;
        int pos = start;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "hour", pos, c, ':')) break;
            hour = parseDigit(value, "hour", pos, hour, c);
            pos++;
        }
        start = pos + 1;
        pos = start;
        int min = 0;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "minutes", pos, c, ':')) break;
            min = parseDigit(value, "minutes", pos, min, c);
            pos++;
        }

        start = pos + 1;
        pos = start;
        int sec = 0;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "seconds", pos, c, '.')) break;
            sec = parseDigit(value, "seconds", pos, sec, c);
            pos++;
        }
        int milli = 0;
        pos++;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            milli = parseDigit(value, "milliseconds", pos, milli, c);
            pos++;
        }
        if (hour > 23)
        {
            throw new ParseException("Hour too large in "+value, 0);
        }
        return Time.withNanos(hour, min, sec, milli);
    }

    public static int parseDigit(String value, String timePartName, int pos, int sec, char c) throws ParseException
    {
        if (c >= '0' && c <= '9')
        {
            sec *= 10;
            sec += (c - '0');
        }
        else
        {
            throw new ParseException("Could not parse " + timePartName + " in " +value, pos);
        }
        return sec;
    }

    public static boolean checkEnd(String value, int start, String timePartName, int pos, char c, char end) throws ParseException
    {
        if (c == end)
        {
            if (pos == start)
            {
                throw new ParseException("Could not parse " + timePartName + " in " +value, pos);
            }
            return true;
        }
        return false;
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

    public String getCreateSchema(String schema)
    {
        return "CREATE SCHEMA IF NOT EXISTS "+schema+" AUTHORIZATION sa";
    }

    public TableColumnInfo getTableColumnInfo(Connection connection, String schema, String table) throws SQLException
    {
        if(schema == null) //ensure we treat null as default schema, not any schema.
        {
            schema = getDefaultSchema(connection);
        }
        return super.getTableColumnInfo(connection, schema, table);
    }

    private static String getDefaultSchema(Connection connection) throws SQLException
    {
        ResultSet schemas =  connection.getMetaData().getSchemas();
        while(schemas.next())
        {
            if(schemas.getBoolean("IS_DEFAULT"))
            {
                return schemas.getString("TABLE_SCHEM");
            }
        }
        return null;
    }

    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        return 1000;
    }

    public String getLastIdentitySql(String tableName)
    {
        return "select IDENTITY()";
    }

    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        sb.append("CREATE GLOBAL TEMPORARY TABLE ");
        sb.append(tempTableName);
        return tempTableName;
    }

    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        if (tempSchema != null)
        {
            tempTableName = tempSchema+"."+tempTableName;
        }
        sb.append("CREATE TABLE ");
        sb.append(tempTableName);
        return tempTableName;
    }

    @Override
    public int zGetTxLevel()
    {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public String getConversionFunctionIntegerToString(String expression)
    {
        return "to_char("+expression+")";
    }

    @Override
    public String getConversionFunctionStringToInteger(String expression)
    {
        return "to_number("+expression+")";
    }

    @Override
    public boolean dropTableAllowedInTransaction()
    {
        return false;
    }

    @Override
    public boolean isConnectionDeadWithoutRecursion(SQLException e)
    {
        String state = e.getSQLState();
        int code = e.getErrorCode();

        return (code == CODE_CONNECTION_INCONSISTENT ||
           code == CODE_CONNECTION_CLOSED ||
           code == CODE_STREAM_CLOSED ||
           code == CODE_SOCKET_ERROR
        );
    }

    @Override
    public boolean supportsAsKeywordForTableAliases()
    {
        return false;
    }

    @Override
    public boolean truncateBeforeDroppingTempTable()
    {
        return true;
    }

    @Override
    public String getSqlExpressionForDateYear(String columnName)
    {
        return "EXTRACT(YEAR FROM " + columnName + ")";
    }

    @Override
    public String getSqlExpressionForDateMonth(String columnName)
    {
        return "EXTRACT(MONTH FROM " + columnName + ")";
    }

    @Override
    public String getSqlExpressionForDateDayOfMonth(String columnName)
    {
        return "EXTRACT(DAY FROM " + columnName + ")";
    }

    @Override
    public int getNullableBooleanJavaSqlType()
    {
        return Types.NUMERIC;
    }
}
