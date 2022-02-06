
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

import com.gs.fw.common.mithra.GroupByAttribute;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import com.gs.fw.common.mithra.util.MutableDouble;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.WildcardParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;



public abstract class AbstractDatabaseType implements DatabaseType
{
    protected static final int MAX_CLAUSES = 240; // from sybase
    private static final ThreadLocal calendarInstance = new ThreadLocal();
    private static final ThreadLocal calendarInstanceUtc = new ThreadLocal();
    private static final char[] STANDARD_SQL_META_CHARS = {'=', '%', '_'};
    private int updateViaInsertAndJoinThreshold = -1;

    private static Logger getLogger()
    {
        if(logger == null)
        {
           logger = LoggerFactory.getLogger(AbstractDatabaseType.class);
        }
        return logger;
    }

    private static Logger logger;

    public String getSelect(String columns, SqlQuery query, String groupBy, boolean isInTransaction, int rowCount)
    {
        StringBuilder result = getSelectAsStringBuilder(columns, query, groupBy, isInTransaction);

        return result.toString();
    }

    //todo: this implementation does not use the rowCount. Need to find how to do this with Derby

    public String getDelete(SqlQuery query, int rowCount)
    {
        StringBuilder deleteClause = new StringBuilder("delete ");

        deleteClause.append(" from ").append(query.getFromClauseAsString());
        String where = query.getWhereClauseAsString(0);
        boolean hasWhereClause = where.trim().length() > 0;

        if (hasWhereClause)
        {
            deleteClause.append(" where ");
        }
        return deleteClause.toString();
    }

    protected StringBuilder getSelectAsStringBuilder(String columns, SqlQuery query, String groupBy, boolean isInTransaction)
    {
        StringBuilder selectWithoutWhere = new StringBuilder(20 + columns.length()+query.getWhereClauseLength());
        selectWithoutWhere.append("select ");
        if (query.requiresDistinct())
        {
            selectWithoutWhere.append(" distinct ");
        }
        selectWithoutWhere.append(columns).append(" from ");
        query.appendFromClause(selectWithoutWhere);

        if (isInTransaction && !this.hasRowLevelLocking())
        {
            getLogger().warn("Row level locking is not implemented in database type.");
        }

        int numberOfUnions = query.getNumberOfUnions();
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < numberOfUnions; i++)
        {
            if (i > 0)
            {
                result.append(" union ");
            }
            result.append(selectWithoutWhere);
            String whereClause = query.getWhereClauseAsString(i);
            if (whereClause != null && whereClause.length() > 0)
            {
                result.append(" where ").append(whereClause);
            }
        }

        if (groupBy != null)
        {
            result.append(" group by ").append(groupBy);
        }

        String orderBy = query.getOrderByClause();
        if (orderBy != null && orderBy.length() > 0)
        {
            result.append(" order by ").append(orderBy);
        }
        return result;
    }

    /**
     * Override to return <code>true</code> if the database type supports row-level locking.
     * @return <code>true</code> if the database type supports row-level locking.
     */
    protected boolean hasRowLevelLocking()
    {
        return false;
    }

    public String getSelect(String columns, String fromClause, String whereClause, boolean lock)
    {
        StringBuilder selectClause = new StringBuilder("select ").append(columns).append(" from ").append(fromClause);
        if (whereClause != null)
        {
            selectClause.append(" where ").append(whereClause);
        }

        return selectClause.toString();
    }

    public String getSelectForAggregatedData(SqlQuery query, List aggregateAttributes, List groupByAttributes)
    {
        StringBuilder selectClause = new StringBuilder("select ");
        int selectClauseInitialSize = selectClause.length();
        for(int i = 0; i < groupByAttributes.size(); i++)
        {
            GroupByAttribute attr = (GroupByAttribute)groupByAttributes.get(i);
            if(i != 0)
            {
                selectClause.append(" ,");
            }
            selectClause.append(attr.getAttribute().getFullyQualifiedLeftHandExpression(query));
        }

        for(int i = 0; i < aggregateAttributes.size(); i++)
        {
            MithraAggregateAttribute attr = (MithraAggregateAttribute)aggregateAttributes.get(i);
            if(selectClause.length() != selectClauseInitialSize)
            {
                selectClause.append(" , ");
            }
            selectClause.append(attr.getFullyQualifiedAggregateExpresion(query));
        }

        selectClause.append(" from ").append(query.getFromClauseAsString());
        String whereClause = query.getWhereClauseAsString(0);
        if (whereClause != null && whereClause.length() > 0)
        {
            selectClause.append(" where ").append(whereClause);
        }
        return selectClause.toString();
    }

    public boolean loopNestedExceptionForFlagAndDetermineState(int flag, SQLException e)
    {
        boolean retriable = this.executeForFlag(flag, e);
        SQLException recursed = e.getNextException();
        while(!retriable && recursed != null)
        {
            retriable = this.executeForFlag(flag, recursed);
            recursed = recursed.getNextException();
        }

        Throwable cause = e.getCause();
        while(!retriable && cause != null)
        {
            if (cause instanceof SQLException)
            {
                retriable = this.executeForFlag(flag, (SQLException)cause);
            }
            cause = cause.getCause();
        }
        return retriable;
    }

    private boolean executeForFlag(int flag, SQLException e)
    {
        switch(flag)
        {
            case 1:
                return this.isRetriableWithoutRecursion(e);
            case 2:
                return this.isTimedOutWithoutRecursion(e);
            default:
                return false;
        }
    }

    public boolean hasTopQuery()
    {
        return false;
    }

    public TableColumnInfo getTableColumnInfo(Connection connection, String schema, String table) throws SQLException
    {
        return TableColumnInfo.createTableMetadata(connection, schema, table, this.getFullyQualifiedTableName(schema, table));
    }

    public String getFullyQualifiedTableName(String schema, String tableName)
    {
        return schema == null ? tableName : schema + '.' + tableName;
    }

    @Deprecated
    public BulkLoader createBulkLoader(Connection connection, String user, String password, String hostName, int port) throws BulkLoaderException
    {
        throw new BulkLoaderException("BulkLoader is not supported");
    }

    @Override
    public BulkLoader createBulkLoader(String user, String password, String hostName, int port) throws BulkLoaderException
    {
        throw new RuntimeException("BulkLoader is not supported");
    }

    public int getMaxClauses()
    {
        return MAX_CLAUSES;
    }

    public boolean hasSetRowCount()
    {
        return false;
    }

    public int getMaxPreparedStatementBatchCount(int parametersPerStatement)
    {
        return -1;
    }

    public void setInfiniteRowCount(Connection con)
    {
        throw new RuntimeException("Not supported");
    }

    public void setRowCount(Connection con, int rowcount) throws SQLException
    {
        throw new RuntimeException("Not supported");
    }

    public String getCreateSchema(String schema)
    {
        return null;
    }

    public boolean hasMultiInsert()
    {
        return this.hasSelectUnionMultiInsert() || this.hasValuesMultiInsert();
    }

    public boolean supportsMultiValueInClause()
    {
        return false;
    }

    public String createMultiInsertParametersStatement(String questionMarksForColumns, int numberOfStatements)
    {
        StringBuilder result;
        if (this.hasSelectUnionMultiInsert())
        {
            result = new StringBuilder((questionMarksForColumns.length() + 18)*numberOfStatements + 10);
            result.append(" select ").append(questionMarksForColumns);
            for(int i=1;i<numberOfStatements;i++)
            {
                result.append(" union all select ").append(questionMarksForColumns);
            }
        }
        else if (this.hasValuesMultiInsert())
        {
            result = new StringBuilder((questionMarksForColumns.length() + 3)*numberOfStatements + 10);
            result.append(" values (").append(questionMarksForColumns);
            for(int i=1;i<numberOfStatements;i++)
            {
                result.append("),(").append(questionMarksForColumns);
            }
            result.append(")");
        }
        else
        {
            throw new RuntimeException("should not get here");
        }
        return result.toString();
    }

    public String getHostnameFromDataSource(DataSource ds)
    {
        return null;
    }

    public int getPortFromDataSource(DataSource ds)
    {
        return 0;
    }

    public String getHostnameFromUrl(String url)
    {
        return null;
    }

    public int getPortFromUrl(String url)
    {
        return 0;
    }

    @Override
    public Timestamp getTimestampFromResultSet(ResultSet rs, int pos, TimeZone timeZone) throws SQLException
    {
        Calendar c = getCalendarInstanceUtc();
        Timestamp timestamp = rs.getTimestamp(pos, c);
        return MithraTimestamp.zConvertTimeForReadingWithUtcCalendar(timestamp, timeZone);
    }

    public void configureConnection(Connection con) throws SQLException
    {
        // subclass should override
    }

    public String getTempDbSchemaName()
    {
        return null;
    }

    public boolean hasPerTableLock()
    {
        return false;
    }

    public String getPerTableLock(boolean lock)
    {
        return "";
    }

    public String getPerStatementLock(boolean lock)
    {
        return "";
    }

    public boolean hasBulkInsert()
    {
        return false;
    }

    public String getNullableColumnConstraintString()
    {
        return "";
    }

    public boolean isKilledConnectionException(Exception e)
    {
        return false;
    }

    public boolean isKilledConnection(SQLException e)
    {
        boolean killed = this.isKilledConnectionException(e);

        SQLException recursed = e.getNextException();
        while(!killed && recursed != null)
        {
            killed = this.isKilledConnectionException(recursed);
            recursed = recursed.getNextException();
        }

        Throwable cause = e.getCause();
        while(!killed && cause != null)
        {
            if (cause instanceof Exception)
            {
                killed = this.isKilledConnectionException((Exception) cause);
            }
            cause = cause.getCause();
        }
        return killed;
    }

    public boolean isConnectionDead(SQLException e)
    {
        boolean dead = this.isConnectionDeadWithoutRecursion(e);
        SQLException recursed = e.getNextException();
        while(!dead && recursed != null)
        {
            dead = this.isConnectionDeadWithoutRecursion(recursed);
            recursed = recursed.getNextException();
        }

        Throwable cause = e.getCause();
        while(!dead && cause != null)
        {
            if (cause instanceof SQLException)
            {
                recursed = (SQLException) cause;
                dead = this.isConnectionDeadWithoutRecursion(recursed);
            }
            cause = cause.getCause();
        }

        return dead;
    }

    public boolean generateBetweenClauseForLargeInClause()
    {
        return false;
    }

    public String getTableNameForNonSharedTempTable(String nominalName)
    {
        return nominalName;
    }

    public String getSqlPostfixForNonSharedTempTableCreation()
    {
        return "";
    }

    public int getMaxSearchableArguments()
    {
        return this.getMaxClauses();
    }

    public int getMaxUnionCount()
    {
        return 1;
    }

    public String getModFunction(String fullyQualifiedLeftHandExpression, int divisor)
    {
        return "MOD("+fullyQualifiedLeftHandExpression+","+divisor+")";
    }

    @Override
    public String getCurrentSchema(Connection con) throws SQLException
    {
        return con.getCatalog();
    }

    public void setSchemaOnConnection(Connection con, String schema) throws SQLException
    {
        con.setCatalog(schema);
    }

    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        throw new RuntimeException("not implemented");
    }

    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String nominalTableName)
    {
        throw new RuntimeException("not implemented");
    }

    public String getSqlPostfixForSharedTempTableCreation()
    {
        return "";
    }

    public boolean dropTableAllowedInTransaction()
    {
        return true;
    }

    public boolean isConnectionDeadWithoutRecursion(SQLException e)
    {
        return false;
    }

    public boolean violatesUniqueIndex(SQLException e)
    {
        if (violatesUniqueIndexWithoutRecursion(e)) return true;
        SQLException next = e.getNextException();
        while(next != null)
        {
            if (violatesUniqueIndexWithoutRecursion(next)) return true;
            next = next.getNextException();
        }
        return false;
    }

    protected boolean violatesUniqueIndexWithoutRecursion(SQLException next)
    {
        return false;
    }

    protected void fullyExecute(Connection con, String sql) throws SQLException
    {
        Statement cs = con.createStatement();
        int updateCount = -100;
        boolean isResultSet = cs.execute(sql);
        while (updateCount != -1 || isResultSet)
        {
            if (isResultSet)
            {
                readFullResultSet(cs.getResultSet());
            }
            else
            {
                updateCount = cs.getUpdateCount();
            }
            isResultSet = cs.getMoreResults();
        }
        cs.close();
    }

    protected void readFullResultSet(ResultSet resultSet) throws SQLException
    {
        while(resultSet.next());
        resultSet.close();
    }

    protected abstract boolean isRetriableWithoutRecursion(SQLException exception);
    protected abstract boolean isTimedOutWithoutRecursion(SQLException exception);

    protected abstract boolean hasSelectUnionMultiInsert();

    protected abstract boolean hasValuesMultiInsert();

    public String getIdentityTableCreationStatement()
    {
        return " identity";
    }

    public String getAllowInsertIntoIdentityStatementFor(String tableName, String onOff)
    {
        return "";
    }

    public boolean createTempTableAllowedInTransaction()
    {
        return true;
    }

    public String getDeleteStatementForTestTables()
    {
        return "truncate table ";
    }

    private static Calendar getCalendarInstance()
    {
        Calendar c = (Calendar) calendarInstance.get();
        if (c == null)
        {
            c = Calendar.getInstance();
            calendarInstance.set(c);
        }
        return c;
    }

    protected static Calendar getCalendarInstanceUtc()
    {
        Calendar c = (Calendar) calendarInstanceUtc.get();
        if (c == null)
        {
            c = Calendar.getInstance();
            c.setTimeZone(MithraTimestamp.UtcTimeZone);
            calendarInstanceUtc.set(c);
        }
        return c;
    }

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
            timestamp = MithraTimestamp.zConvertTimeForWritingWithUtcCalendar(timestamp, timeZone);
            ps.setTimestamp(index, timestamp, c);
        }
    }

    public void setDate(PreparedStatement ps, int index, java.util.Date date, boolean forceAsString) throws SQLException
    {
        if (forceAsString)
        {
            String s = convertDateOnlyToString(date);
            ps.setString(index, s);
        }
        else
        {
            if (date instanceof java.sql.Date)
            {
                ps.setDate(index, (java.sql.Date)date);
            }
            else
            {
                ps.setDate(index, new java.sql.Date(date.getTime()));
            }
        }
    }

    public void setTime(PreparedStatement ps, int index, Time time) throws SQLException
    {
        ps.setTime(index, time.convertToSql());
    }

    public void setTimeNull(PreparedStatement ps, int index) throws SQLException
    {
        ps.setNull(index, Types.TIME);
    }

    public int getNullableBooleanJavaSqlType()
    {
        return Types.BOOLEAN;
    }

    public Time getTime(ResultSet resultSet, int position) throws SQLException
    {
       return Time.withSqlTime(resultSet.getTime(position));
    }

    public String convertDateToString(java.util.Date date)
    {
        if (date == null) return null;
        Calendar cal = getCalendarInstance();
        cal.setTimeInMillis(date.getTime());
        StringBuilder sb = new StringBuilder(23);
        sb.append(cal.get(Calendar.YEAR)).append("-");
        setAsTwoDigit(sb, cal.get(Calendar.MONTH) + 1).append("-");
        setAsTwoDigit(sb, cal.get(Calendar.DAY_OF_MONTH)).append(" ");
        setAsTwoDigit(sb, cal.get(Calendar.HOUR_OF_DAY)).append(":");
        setAsTwoDigit(sb, cal.get(Calendar.MINUTE)).append(":");
        setAsTwoDigit(sb, cal.get(Calendar.SECOND));
        int millis = cal.get(Calendar.MILLISECOND);
        sb.append(".");
        if (millis < 100)
        {
            sb.append("0");
        }
        setAsTwoDigit(sb, millis);
        return sb.toString();
    }

    @Override
    public String convertDateOnlyToString(java.util.Date date)
    {
        if (date == null) return null;
        Calendar cal = getCalendarInstance();
        cal.setTimeInMillis(date.getTime());
        StringBuilder sb = new StringBuilder(10);
        sb.append(cal.get(Calendar.YEAR)).append('-');
        setAsTwoDigit(sb, cal.get(Calendar.MONTH) + 1).append('-');
        setAsTwoDigit(sb, cal.get(Calendar.DAY_OF_MONTH));
        return sb.toString();
    }

    private static StringBuilder setAsTwoDigit(StringBuilder sb, int datePart)
    {
        if (datePart < 10)
        {
            sb.append("0");
        }
        return sb.append(datePart);
    }

    public int getDefaultPrecision()
    {
        return 5;
    }

    public int getMaxPrecision()
    {
        return 31;
    }

    public int getDeleteViaInsertAndJoinThreshold()
    {
        return -1;
    }

    @Override
    public int getUpdateViaInsertAndJoinThreshold()
    {
        return this.updateViaInsertAndJoinThreshold;
    }

    @Override
    public void setUpdateViaInsertAndJoinThreshold(int updateViaInsertAndJoinThreshold)
    {
        this.updateViaInsertAndJoinThreshold = updateViaInsertAndJoinThreshold;
    }


    public String createSubstringExpression(String stringExpression, int start, int end)
    {
        if (end < 0) return "substr("+stringExpression+","+(start+1)+")";
        return "substr("+stringExpression+","+(start+1)+","+(end - start)+")";
    }

    public int zGetTxLevel()
    {
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    public int getUseTempTableThreshold()
    {
        return 4*this.getMaxClauses();
    }

    public boolean indexRequiresSchemaName()
    {
        return true;
    }

    public boolean nonSharedTempTablesAreDroppedAutomatically()
    {
        return false;
    }

    public String createNonSharedIndexSql(String fullTableName, CharSequence indexColumns)
    {
        return this.createIndexSql(fullTableName, indexColumns);
    }

    public String createSharedIndexSql(String fullTableName, CharSequence indexColumns)
    {
        return this.createIndexSql(fullTableName, indexColumns);
    }

    public String getIndexableSqlDataTypeForBoolean()
    {
        return this.getSqlDataTypeForBoolean();
    }

    public boolean useBigDecimalValuesInRangeOperations()
    {
        return true;
    }

    @Override
    public boolean dropTempTableSyncAfterTransaction()
    {
        return false;
    }

    protected String createIndexSql(String fullTableName, CharSequence indexColumns)
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
        indexSql.append('I').append(partialTableName).append(" ON ").append(fullTableName).append(" (");

        indexSql.append(indexColumns);

        indexSql.append(')');

        return indexSql.toString();
    }

    @Override
    public double getSysLogPercentFull(Connection connection, String schemaName) throws SQLException
    {
        return 0.0;
    }

    @Override
    public String getUpdateTableStatisticsSql(String tableName)
    {
        return null;
    }

    @Override
    public boolean supportsSharedTempTable()
    {
        return true;
    }

    @Override
    public boolean supportsAsKeywordForTableAliases()
    {
        return true;
    }

    @Override
    public boolean truncateBeforeDroppingTempTable()
    {
        return false;
    }

    protected char[] getLikeMetaChars()
    {
        return STANDARD_SQL_META_CHARS;
    }

    @Override
    public String escapeLikeMetaChars(String parameter)
    {
        return WildcardParser.escapeLikeMetaChars(parameter, this.getLikeMetaChars());
    }

    @Override
    public String getSqlLikeExpression(WildcardParser parser)
    {
        return parser.getSqlLikeExpression(this.getLikeMetaChars());
    }

    protected boolean elideTimeZoneConversion(int conversion, TimeZone dbTimeZone)
    {
        return conversion == TimestampAttribute.CONVERT_NONE
                || conversion == TimestampAttribute.CONVERT_TO_UTC && TimeZone.getDefault().getID().equals("UTC")
                || conversion == TimestampAttribute.CONVERT_TO_DATABASE && dbTimeZone.getID().equals(TimeZone.getDefault().getID());
    }

    @Override
    public String getSqlExpressionForTimestampYear(String columnName, int conversion, TimeZone dbTimeZone) throws MithraBusinessException
    {
        if (elideTimeZoneConversion(conversion, dbTimeZone))
        {
            return getSqlExpressionForDateYear(columnName);
        }
        return getSqlExpressionForTimestampYearWithConversion(columnName, conversion, dbTimeZone);
    }

    protected String getSqlExpressionForTimestampYearWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        throw new MithraBusinessException("timezone conversion not supported by "+this.getClass().getName());
    }

    @Override
    public String getSqlExpressionForTimestampMonth(String columnName, int conversion, TimeZone dbTimeZone) throws MithraBusinessException
    {
        if (elideTimeZoneConversion(conversion, dbTimeZone))
        {
            return getSqlExpressionForDateMonth(columnName);
        }
        return getSqlExpressionForTimestampMonthWithConversion(columnName, conversion, dbTimeZone);
    }

    protected String getSqlExpressionForTimestampMonthWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        throw new MithraBusinessException("timezone conversion not supported by "+this.getClass().getName());
    }

    @Override
    public String getSqlExpressionForTimestampDayOfMonth(String columnName, int conversion, TimeZone dbTimeZone) throws MithraBusinessException
    {
        if (elideTimeZoneConversion(conversion, dbTimeZone))
        {
            return getSqlExpressionForDateDayOfMonth(columnName);
        }
        return getSqlExpressionForTimestampDayOfMonthWithConversion(columnName, conversion, dbTimeZone);
    }

    protected String getSqlExpressionForTimestampDayOfMonthWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        throw new MithraBusinessException("timezone conversion not supported by "+this.getClass().getName());
    }

    @Override
    public String getSqlExpressionForStandardDeviationSample(String columnName)
    {
        return "stddev(" + columnName + ")";
    }

    @Override
    public String getSqlExpressionForStandardDeviationPop(String columnName)
    {
        return "stddev_pop(" + columnName + ")";
    }

    public void fixSampleStandardDeviation(MutableDouble obj, int count)
    {
        //do nothing
    }

    public void fixSampleVariance(MutableDouble obj, int count)
    {
        //do nothing
    }

    @Override
    public String getSqlExpressionForVarianceSample(String columnName)
    {
        return "variance(" +columnName+ ")";
    }

    @Override
    public String getSqlExpressionForVariancePop(String columnName)
    {
        return "var_pop(" +columnName+ ")";
    }

    @Override
    public void setMultiUpdateViaJoinQuery(
            Object source,
            List updates,
            Attribute[] prototypeArray,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            MithraObjectPortal mithraObjectPortal,
            String fullyQualifiedTableNameGenericSource,
            StringBuilder builder)
    {
        this.startUpdateViaJoinQuery(fullyQualifiedTableNameGenericSource, builder);
        builder.append(" t0 set ");

        for (int i = 0; i < updates.size(); i++)
        {
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) updates.get(i);
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append("t0.").append(wrapper.getSetAttributeSql());
        }
        builder.append(" WHERE EXISTS (SELECT 1 FROM ");
        appendTempTableRightSideJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
        builder.append(")");
    }

    @Override
    public void setBatchUpdateViaJoinQuery(
            Object source,
            List updates,
            Attribute[] prototypeArray,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            MithraObjectPortal mithraObjectPortal,
            String fullyQualifiedTableNameGenericSource,
            StringBuilder builder)
    {
        this.startUpdateViaJoinQuery(fullyQualifiedTableNameGenericSource, builder);
        builder.append(" t0 set (");
        for (int i = 0; i < updates.size(); i++)
        {
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) updates.get(i);
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append("t0.").append(wrapper.getAttribute().getColumnName());
        }

        builder.append(") = (select ");

        for (int i = 0; i < updates.size(); i++)
        {
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append("t1.c").append(i + pkAttributeCount);
        }

        builder.append(" from ");

        appendTempTableRightSideJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
        builder.append(")");
        builder.append(" WHERE EXISTS (SELECT 1 FROM ");
        appendTempTableRightSideJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
        builder.append(")");
    }

    @Override
    public boolean canCombineOptimisticWithBatchUpdates()
    {
        return true;
    }

    protected void startUpdateViaJoinQuery(String fullyQualifiedTableNameGenericSource, StringBuilder builder)
    {
        builder.append("update ").append(fullyQualifiedTableNameGenericSource);
    }

    protected void appendTempTableRightSideJoin(
            Object source,
            Attribute[] prototypeAttributes,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            MithraObjectPortal mithraObjectPortal,
            StringBuilder builder)
    {
        builder.append(tempContext.getFullyQualifiedTableName(source, mithraObjectPortal.getPersisterId()));
        builder.append(" t1 where ");
        this.constructJoin(prototypeAttributes, nullAttributes, pkAttributeCount, builder);
    }

    protected void constructJoin(Attribute[] prototypeAttributes, MithraFastList<Attribute> nullAttributes, int pkAttributeCount, StringBuilder builder)
    {
        for (int i = 0; i < pkAttributeCount; i++)
        {
            if (!prototypeAttributes[i].isSourceAttribute())
            {
                if (i > 0)
                {
                    builder.append(" and ");
                }
                builder.append("t0.").append(prototypeAttributes[i].getColumnName()).append(" = t1.c").append(i);
            }
        }
        for (Attribute nullAttribute : nullAttributes)
        {
            builder.append(" and t0.").append(nullAttribute.getColumnName()).append(" IS NULL");
        }
    }

    protected void appendTempTableJoin(
            Object source,
            Attribute[] prototypeAttributes,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            MithraObjectPortal mithraObjectPortal,
            String fullyQualifiedTableNameGenericSource,
            StringBuilder builder)
    {
        builder.append(" from ").append(fullyQualifiedTableNameGenericSource);
        builder.append(" t0, ");
        this.appendTempTableRightSideJoin(source, prototypeAttributes, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
    }

    @Override
    public void appendTestTableCreationPostamble(StringBuilder sb)
    {
        //do nothing
    }

    @Override
    public boolean varBinaryHasLength()
    {
        return false;
    }
}
