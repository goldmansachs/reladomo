
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import com.gs.fw.common.mithra.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MariaDatabaseType extends AbstractDatabaseType
{
    private static final Logger logger = LoggerFactory.getLogger(MariaDatabaseType.class.getName());
    private static final int DEADLOCK_SQL_STATE = 40001;
    private static final int DUPLICATE_ERROR_CODE = 1062;
    private static final int TIMEOUT_ERROR_CODE = 1205;
    public static final int MAX_CLAUSES = 240;
    private static final MariaDatabaseType instance = new MariaDatabaseType();
    private String tempSchema = null;
    private static final char[] MARIA_SQL_LIKE_META_CHARS = {'%', '_', '\\'};

    public static Logger getLogger()
    {
        return logger;
    }

    /** Singleton */
    protected MariaDatabaseType()
    {
    }

    @Override
    public int getMaxPreparedStatementBatchCount(int parametersPerStatement)
    {
        return 100;
    }

    public boolean hasTopQuery()
    {
        return true;
    }

    public void setTempSchema(String tempSchema)
    {
        this.tempSchema = tempSchema;
    }

    public String convertDateToString(java.util.Date date)
    {
        return super.convertDateToString(date);
    }

    public String getSelect(String columns, SqlQuery query, String groupBy, boolean isInTransaction, int rowCount)
    {
        String result = super.getSelect(columns, query, groupBy, isInTransaction, rowCount);
        if (rowCount > 0)
        {
            result += " LIMIT "+rowCount + 1;
        }
        if (isInTransaction)
        {
            MithraObjectPortal portal = query.getAnalyzedOperation().getOriginalOperation().getResultObjectPortal();
            if (portal.getTxParticipationMode().mustLockOnRead())
            {
                result += " LOCK IN SHARE MODE";
            }
        }
        return result;
    }

    public String getSelect(String columns, String fromClause, String whereClause, boolean lock)
    {
        String result = super.getSelect(columns, fromClause, whereClause, lock);
        if (lock)
        {
            result += " LOCK IN SHARE MODE";
        }
        return result;
    }

    public String getDelete(SqlQuery query, int rowCount)
    {
        StringBuilder deleteClause = new StringBuilder("delete ");

        String fromTable = query.getFromClauseAsString();
        deleteClause.append(" from ").append(fromTable);
        String where = query.getWhereClauseAsString(0);
        boolean hasWhereClause = where.trim().length() > 0;
        if (hasWhereClause)
        {
            deleteClause.append(" where ");
            deleteClause.append(where);
        }
        if (rowCount > 0)
        {
            deleteClause.append(" limit ").append(rowCount);
        }
        return deleteClause.toString();
    }

    public static MariaDatabaseType getInstance()
    {
        return instance;
    }

    public String getPerStatementLock(boolean lock)
    {
        return "";
    }

    protected boolean hasRowLevelLocking()
    {
        return true;
    }

    @Override
    public boolean dropTableAllowedInTransaction()
    {
        return false;
    }

    protected boolean isRetriableWithoutRecursion(SQLException sqlException)
    {
        return DEADLOCK_SQL_STATE == sqlException.getErrorCode(); //|| DEADLOCK_SQL_STATE_2.equalsIgnoreCase(sqlException.getSQLState());
    }

    protected boolean isTimedOutWithoutRecursion(SQLException exception)
    {
        return TIMEOUT_ERROR_CODE == exception.getErrorCode();
    }

    public boolean violatesUniqueIndexWithoutRecursion(SQLException exception)
    {
        return DUPLICATE_ERROR_CODE == exception.getErrorCode();
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        return "create temporary table "+nominalTableName + getSqlPostfixForNonSharedTempTableCreation();
    }

    public String getSqlPostfixForNonSharedTempTableCreation()
    {
        return "";
    }

    public String getSqlPostfixForSharedTempTableCreation()
    {
        return "";
//        return " on commit preserve rows";
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
        sb.append("CREATE TEMPORARY TABLE ");
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

    public boolean indexRequiresSchemaName()
    {
        return false;
    }

    public boolean nonSharedTempTablesAreDroppedAutomatically()
    {
        return true;
    }

    @Override
    public boolean useBigDecimalValuesInRangeOperations()
    {
        return false;
    }

    @Override
    public void setTime(PreparedStatement ps, int index, com.gs.fw.common.mithra.util.Time time) throws SQLException
    {
        if(time == null)
            ps.setNull(index, Types.VARCHAR);
        else
            ps.setString(index, time.toString());
    }

    @Override
    public void setTimeNull(PreparedStatement ps, int index) throws SQLException
    {
        ps.setNull(index, Types.VARCHAR);
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

    public String getUpdateTableStatisticsSql(String tableName)
    {
        return "ANALYZE TABLE " + tableName;
    }

//    @Override
//    public String getLikeEscape()
//    {
//        return "'\\\\'";
//    }

    protected char[] getLikeMetaChars()
    {
        return MARIA_SQL_LIKE_META_CHARS;
    }

    @Override
    public String getSqlExpressionForStandardDeviationSample(String columnName)
    {
        return "stddev_samp(" +columnName +")";
    }

    @Override
    public String getSqlExpressionForStandardDeviationPop(String columnName)
    {
        return "stddev_pop(" +columnName +")";
    }

    @Override
    public String getSqlExpressionForVarianceSample(String columnName)
    {
        return "var_samp(" +columnName +")";
    }

    @Override
    public String getSqlExpressionForVariancePop(String columnName)
    {
        return "var_pop(" +columnName +")";
    }

    @Override
    public String getSqlExpressionForDateYear(String columnName)
    {
        return "YEAR(" + columnName + ")";
    }

    @Override
    public String getSqlExpressionForDateMonth(String columnName)
    {
        return "MONTH(" + columnName + ")";
    }

    @Override
    public String getSqlExpressionForDateDayOfMonth(String columnName)
    {
        return "DAY(" + columnName + ")";
    }

    @Override
    protected String getSqlExpressionForTimestampYearWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        int rawOffset = dbTimeZone.getRawOffset(); //millis to add
        long minute1 = (rawOffset / (1000 * 60)) % 60;
        long hour1 = (rawOffset / (1000 * 60 * 60)) % 24;
        String initialTimeZone;
        if(hour1 > 0)
        {
            initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "+00:00" : "+"+hour1+":"+minute1;
        }
        else
        {
            initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "+00:00" : hour1+":"+minute1;
        }
        int rawOffset2 = TimeZone.getDefault().getRawOffset();
        long minute = (rawOffset2 / (1000 * 60)) % 60;
        long hour = (rawOffset2 / (1000 * 60 * 60)) % 24;
        return "YEAR(CONVERT_TZ("+columnName+",'"+ initialTimeZone+"', '"+ hour+":"+minute +"'))";
    }

    @Override
    protected String getSqlExpressionForTimestampMonthWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        int rawOffset = dbTimeZone.getRawOffset(); //millis to add
        long minute1 = (rawOffset / (1000 * 60)) % 60;
        long hour1 = (rawOffset / (1000 * 60 * 60)) % 24;
        String initialTimeZone;
        if(hour1 > 0)
        {
            initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "+00:00" : "+"+hour1+":"+minute1;
        }
        else
        {
            initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "+00:00" : hour1+":"+minute1;
        }
        int rawOffset2 = TimeZone.getDefault().getRawOffset();
        long minute = (rawOffset2 / (1000 * 60)) % 60;
        long hour = (rawOffset2 / (1000 * 60 * 60)) % 24;
        return "MONTH(CONVERT_TZ("+columnName+",'"+ initialTimeZone+"', '"+ hour+":"+minute +"'))";
    }

    @Override
    protected String getSqlExpressionForTimestampDayOfMonthWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        int rawOffset = dbTimeZone.getRawOffset(); //millis to add
        long minute1 = (rawOffset / (1000 * 60)) % 60;
        long hour1 = (rawOffset / (1000 * 60 * 60)) % 24;
        String initialTimeZone;
        if(hour1 > 0)
        {
            initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "+00:00" : "+"+hour1+":"+minute1;
        }
        else
        {
            initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "+00:00" : hour1+":"+minute1;
        }
        int rawOffset2 = TimeZone.getDefault().getRawOffset();
        long minute = (rawOffset2 / (1000 * 60)) % 60;
        long hour = (rawOffset2 / (1000 * 60 * 60)) % 24;
        return "DAY(CONVERT_TZ("+columnName+",'"+ initialTimeZone+"', '"+ hour+":"+minute +"'))";
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

        builder.append(" t0 ");
        appendTempTableJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
        builder.append(" set ");
        for (int i = 0; i < updates.size(); i++)
        {
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) updates.get(i);
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append(wrapper.getSetAttributeSql());
        }
        builder.append(" where ");
        this.constructJoin(prototypeArray, nullAttributes, pkAttributeCount, builder);
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
        builder.append(" t0 ");
        appendTempTableJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
        builder.append(" set ");
        for (int i = 0; i < updates.size(); i++)
        {
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) updates.get(i);
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append(wrapper.getAttribute().getColumnName()).append(" = t1.c");
            builder.append(pkAttributeCount + i);
        }
        builder.append(" where ");
        this.constructJoin(prototypeArray, nullAttributes, pkAttributeCount, builder);
    }

    private void appendTempTableJoin(
            Object source,
            Attribute[] prototypeAttributes,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            MithraObjectPortal mithraObjectPortal,
            StringBuilder builder)
    {
        builder.append(" JOIN ");
        builder.append(tempContext.getFullyQualifiedTableName(source, mithraObjectPortal.getPersisterId()));
        builder.append(" t1 ON ");
        this.constructJoin(prototypeAttributes, nullAttributes, pkAttributeCount, builder);
    }
}
