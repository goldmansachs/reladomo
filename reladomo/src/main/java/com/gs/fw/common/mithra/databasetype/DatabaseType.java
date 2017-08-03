
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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.CommonDatabaseType;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MutableDouble;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.WildcardParser;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.TimeZone;


public interface DatabaseType extends CommonDatabaseType
{
    public static final int RETRIABLE_FLAG = 1;
    public static final int TIMED_OUT_FLAG = 2;

    String getSelect(String columns, SqlQuery query, String groupBy, boolean isInTransaction, int rowCount);
    String getSelect(String columns, String fromClause, String whereClause, boolean lock);
    String getSelectForAggregatedData(SqlQuery query, List aggregateAttributes, List groupByAttributes);
    String getDelete(SqlQuery query, int rowCount);
    boolean loopNestedExceptionForFlagAndDetermineState(int flag, SQLException e);

    boolean isKilledConnection(SQLException e);

    boolean hasTopQuery();

    int getMaxClauses();

    boolean hasSetRowCount();

    String getLastIdentitySql(String tableName);

    public String getIdentityTableCreationStatement();

    public String getAllowInsertIntoIdentityStatementFor(String tableName, String onOff);
    /**
     * @return the maximum number of statements that can be batched in one statement. a value less than zero means infinite.
     * @param parametersPerStatement
     */
    int getMaxPreparedStatementBatchCount(int parametersPerStatement);

    void setInfiniteRowCount(Connection con);

    void setRowCount(Connection con, int rowcount) throws SQLException;

    String getFullyQualifiedTableName(String schema, String tableName);

    String getCreateSchema(String schemaName);

    boolean hasMultiInsert();
    int getMultiInsertBatchSize(int columnsToInsert);

    boolean supportsMultiValueInClause();

    String createMultiInsertParametersStatement(String questionMarksForColumns, int numberOfStatements);
    /**
     * This method is deprecated. Use the similar method without a connection object.
     * <p>Creates a {@link BulkLoader} for this <code>DatabaseType</code>.</p>
     * <p><code>BulkLoader</code>s should be created once per bulk loading operation.</p>
     * @param connection A connection to the database.
     * @param user The user to log into the database (e.g. if the bulk loader has to start an external process).
     * @param password The password for the user.
     * @return A BulkLoader implementation for this database type.
     * @throws com.gs.fw.common.mithra.bulkloader.BulkLoaderException if there was a problem creating the bulk loader.
     */
    @Deprecated
    BulkLoader createBulkLoader(Connection connection, String user, String password, String hostName, int port) throws BulkLoaderException;

    /**
     * <p>Creates a {@link BulkLoader} for this <code>DatabaseType</code>.</p>
     * <p><code>BulkLoader</code>s should be created once per bulk loading operation.</p>
     * @param user The user to log into the database (e.g. if the bulk loader has to start an external process).
     * @param password The password for the user.
     * @return A BulkLoader implementation for this database type.
     * @throws com.gs.fw.common.mithra.bulkloader.BulkLoaderException if there was a problem creating the bulk loader.
     */
    BulkLoader createBulkLoader(String user, String password, String hostName, int port) throws BulkLoaderException;

    /**
     * <p>Gets hold of the {@link TableColumnInfo} for a given table in a given schema.</p>
     * @param connection The connection to use to fetch the table information from.
     * @param schema The name of the schema in which the table resides (may be <code>null</code>).
     * @param table The name of the table to get the metadata about.
     * @return The table metadata for the given table, or <code>null</code> if the table cannot be found.
     * @throws SQLException if there was a problem looking up the table metadata.
     */
    TableColumnInfo getTableColumnInfo(Connection connection, String schema, String table) throws SQLException;

    public String getHostnameFromDataSource(DataSource ds);
    public int getPortFromDataSource(DataSource ds);

    public String getHostnameFromUrl(String url);
    public int getPortFromUrl(String url);

    public Timestamp getTimestampFromResultSet(ResultSet rs, int pos, TimeZone timeZone) throws SQLException;

    public void configureConnection(Connection con) throws SQLException;

    public String getTempDbSchemaName();

    public boolean hasPerTableLock();

    public String getPerTableLock(boolean lock);

    public String getPerStatementLock(boolean lock);

    public boolean hasBulkInsert();

    public String getNullableColumnConstraintString();

    public boolean isConnectionDead(SQLException e);

    public boolean violatesUniqueIndex(SQLException e);

    public boolean generateBetweenClauseForLargeInClause();

    //todo: consolidate this with appendNonSharedTempTableCreatePreamble
    public String getTableNameForNonSharedTempTable(String nominalName);

    //todo: consolidate this with appendNonSharedTempTableCreatePreamble
    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName);

    public String getSqlPostfixForNonSharedTempTableCreation();

    public int getMaxSearchableArguments();

    public int getMaxUnionCount();

    public String getModFunction(String fullyQualifiedLeftHandExpression, int divisor);

    public void setSchemaOnConnection(Connection con, String schema) throws SQLException;

    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName);

    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String nominalTableName);

    public String getSqlPostfixForSharedTempTableCreation();

    public boolean dropTableAllowedInTransaction();

    public boolean createTempTableAllowedInTransaction();

    public String getDeleteStatementForTestTables();

    public void setTimestamp(PreparedStatement ps, int index, Timestamp timestamp, boolean forceAsString, TimeZone timeZone) throws SQLException;

    public void setDate(PreparedStatement ps, int index, java.util.Date date, boolean forceAsString) throws SQLException;

    public void setTime(PreparedStatement ps, int index, Time time) throws SQLException;

    public void setTimeNull(PreparedStatement ps, int index) throws SQLException;

    public int getNullableBooleanJavaSqlType();

    public Time getTime(ResultSet rs, int position) throws SQLException;

    public String convertDateToString(java.util.Date date);

    public String convertDateOnlyToString(java.util.Date date);

    public int getDefaultPrecision();

    public int getMaxPrecision();

    /**
     * threshold of when updates should become insert into temp table + update original via join
     * @return -1 if updates should never use insert + join
     */
    public int getUpdateViaInsertAndJoinThreshold();

    public void setUpdateViaInsertAndJoinThreshold(int updateViaInsertAndJoinThreshold);

    /**
     * threshold of when purges/deletes should become insert into temp table + purge/delete original via join
     * @return -1 if purges/deletes should never use insert + join
     */
    public int getDeleteViaInsertAndJoinThreshold();

    public String createSubstringExpression(String stringExpression, int start, int end);

    public int zGetTxLevel();

    public int getUseTempTableThreshold();

    public boolean indexRequiresSchemaName();

    public boolean nonSharedTempTablesAreDroppedAutomatically();

    public String createNonSharedIndexSql(String fullTableName, CharSequence indexColumns);

    public String createSharedIndexSql(String fullTableName, CharSequence indexColumns);

    public String getIndexableSqlDataTypeForBoolean();

    public boolean useBigDecimalValuesInRangeOperations();

    public String getConversionFunctionIntegerToString(String expression);

    public String getConversionFunctionStringToInteger(String expression);

    public boolean dropTempTableSyncAfterTransaction();

    public double getSysLogPercentFull(Connection connection, String schemaName) throws SQLException;

    public String getUpdateTableStatisticsSql(String tableName);

    public boolean supportsSharedTempTable();

    public boolean supportsAsKeywordForTableAliases();

    public boolean truncateBeforeDroppingTempTable();

    public String escapeLikeMetaChars(String parameter);

    public String getSqlLikeExpression(WildcardParser parser);

    public String getSqlExpressionForDateYear(String columnName);

    public String getSqlExpressionForDateMonth(String columnName);

    public String getSqlExpressionForDateDayOfMonth(String columnName);

    public String getSqlExpressionForTimestampYear(String columnName, int conversion, TimeZone dbTimeZone) throws MithraBusinessException;

    public String getSqlExpressionForTimestampMonth(String columnName, int conversion, TimeZone dbTimeZone) throws MithraBusinessException;

    public String getSqlExpressionForTimestampDayOfMonth(String columnName, int conversion, TimeZone dbTimeZone) throws MithraBusinessException;

    public String getSqlExpressionForStandardDeviationSample(String columnName);

    public String getSqlExpressionForStandardDeviationPop(String columnName);

    public void fixSampleStandardDeviation(MutableDouble obj, int count);

    public void fixSampleVariance(MutableDouble obj, int count);

    public String getSqlExpressionForVarianceSample(String columnName);

    public String getSqlExpressionForVariancePop(String columnName);

    public void appendTestTableCreationPostamble(StringBuilder sb);

    public void setMultiUpdateViaJoinQuery(
            Object source,
            List updates,
            Attribute[] prototypeArray,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            MithraObjectPortal mithraObjectPortal,
            String fullyQualifiedTableNameGenericSource,
            StringBuilder builder);

    public void setBatchUpdateViaJoinQuery(
            Object source,
            List updates,
            Attribute[] prototypeArray,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            MithraObjectPortal mithraObjectPortal,
            String fullyQualifiedTableNameGenericSource,
            StringBuilder builder);

    public boolean canCombineOptimisticWithBatchUpdates();
}
