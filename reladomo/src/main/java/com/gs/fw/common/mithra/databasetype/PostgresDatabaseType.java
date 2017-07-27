
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
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public class PostgresDatabaseType extends AbstractDatabaseType
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresDatabaseType.class.getName());
    private static final String DEADLOCK_SQL_STATE = "40P01";
    private static final String DEADLOCK_SQL_STATE_2 = "40001";
    private static final String DUPLICATE_ERROR_CODE = "23505";
    public static final int MAX_CLAUSES = 240;
    private static final PostgresDatabaseType instance = new PostgresDatabaseType();
    private String tempSchema = null;
    private static final char[] POSTGRES_SQL_META_CHARS = {'=', '%', '_', '\\'};
    private static final Map<String, String> sqlToJavaTypes;

    public static Logger getLogger()
    {
        return logger;
    }

    static
    {
        sqlToJavaTypes = new HashMap<String, String>();
        sqlToJavaTypes.put("int4", "int");
        sqlToJavaTypes.put("int2", "short");
        sqlToJavaTypes.put("tinyint", "short");
        sqlToJavaTypes.put("float4", "float");
        sqlToJavaTypes.put("float8", "double");
        sqlToJavaTypes.put("smallmoney", "not implemented");
        sqlToJavaTypes.put("money", "not implemented");
        sqlToJavaTypes.put("char", "char");
        sqlToJavaTypes.put("varchar", "String");
        sqlToJavaTypes.put("text", "String");
        sqlToJavaTypes.put("bytea", "byte[]");
        sqlToJavaTypes.put("date", "Date");
        sqlToJavaTypes.put("datetime", "Timestamp");
        sqlToJavaTypes.put("smalldatetime", "Timestamp");
        sqlToJavaTypes.put("timestamp", "Timestamp");
        sqlToJavaTypes.put("bool", "boolean");
        sqlToJavaTypes.put("binary", "byte[]");
        sqlToJavaTypes.put("varbinary", "byte[]");
        sqlToJavaTypes.put("decimal", "BigDecimal");
        sqlToJavaTypes.put("real", "float");
        sqlToJavaTypes.put("time", "Time");
        sqlToJavaTypes.put("int8", "long");
    }

    /** Singleton */
    protected PostgresDatabaseType()
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
                result += " FOR SHARE OF t0";
            }
        }
        return result;
    }

    public String getSelect(String columns, String fromClause, String whereClause, boolean lock)
    {
        String result = super.getSelect(columns, fromClause, whereClause, lock);
        if (lock)
        {
            result += " FOR SHARE of t0";
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
        if (rowCount > 0)
        {
            deleteClause.append(" where ");
            deleteClause.append("ctid  = any (array(select ctid from ").append(fromTable);
            if (hasWhereClause)
            {
                deleteClause.append(" where ").append(where);
            }
            deleteClause.append(" limit ").append(rowCount).append("))");
        }
        else if (hasWhereClause)
        {
            deleteClause.append(" where ");
            deleteClause.append(where);
        }
        return deleteClause.toString();
    }

    public static PostgresDatabaseType getInstance()
    {
        return instance;
    }

    public String getPerStatementLock(boolean lock)
    {
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

    @Override
    public boolean dropTableAllowedInTransaction()
    {
        return false;
    }

    protected boolean isRetriableWithoutRecursion(SQLException sqlException)
    {
        return DEADLOCK_SQL_STATE.equalsIgnoreCase(sqlException.getSQLState()) || DEADLOCK_SQL_STATE_2.equalsIgnoreCase(sqlException.getSQLState());
    }

    protected boolean isTimedOutWithoutRecursion(SQLException exception)
    {
        return false;
    }

    public boolean violatesUniqueIndexWithoutRecursion(SQLException exception)
    {
        return DUPLICATE_ERROR_CODE.equals(exception.getSQLState());
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        return "create temporary table "+nominalTableName + getSqlPostfixForNonSharedTempTableCreation();
    }

    public String getSqlPostfixForNonSharedTempTableCreation()
    {
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            return " on commit drop";
        }
        return " on commit preserve rows";
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
        return "timestamp";
    }

    public String getSqlDataTypeForTime()
    {
        return "time";
    }

    public String getSqlDataTypeForTinyInt()
    {
        return "numeric(3)";
    }

    public String getSqlDataTypeForVarBinary()
    {
        return "bytea";
    }

    public String getSqlDataTypeForByte()
    {
        return "smallint";
    }

    public String getSqlDataTypeForChar()
    {
        return "varchar(1)";
    }

    public String getSqlDataTypeForDateTime()
    {
        return "date";
    }

    public String getSqlDataTypeForDouble()
    {
        return "float8";
    }

    public String getSqlDataTypeForFloat()
    {
        return "float4";
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
        return "int2";
    }

    public String getSqlDataTypeForString()
    {
        return "varchar";
    }

    public String getSqlDataTypeForBigDecimal()
    {
        return "numeric";
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
        String javaType = sqlToJavaTypes.get(sql);

        if ("numeric".equalsIgnoreCase(sql))
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
        return "ANALYZE " + tableName;
    }

//    @Override
//    public String getLikeEscape()
//    {
//        return "'\\\\'";
//    }

    protected char[] getLikeMetaChars()
    {
        return POSTGRES_SQL_META_CHARS;
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
    protected String getSqlExpressionForTimestampYearWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        String initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "UTC" : dbTimeZone.getID();
        return "extract(year from "+columnName+" at time zone '"+ initialTimeZone+"' at time zone '"+ TimeZone.getDefault().getID() +"')";
    }

    @Override
    protected String getSqlExpressionForTimestampMonthWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        String initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "UTC" : dbTimeZone.getID();
        return "extract(month from "+columnName+" at time zone '"+ initialTimeZone+"' at time zone '"+ TimeZone.getDefault().getID() +"')";
    }

    @Override
    protected String getSqlExpressionForTimestampDayOfMonthWithConversion(String columnName, int conversion, TimeZone dbTimeZone)
    {
        String initialTimeZone = conversion == TimestampAttribute.CONVERT_TO_UTC ? "UTC" : dbTimeZone.getID();
        return "extract(day from "+columnName+" at time zone '"+ initialTimeZone+"' at time zone '"+ TimeZone.getDefault().getID() +"')";
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
            builder.append(wrapper.getSetAttributeSql());
        }
        builder.append(" from ");
        this.appendTempTableRightSideJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
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
        builder.append(" t0 set ");

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
        builder.append(" from ");
        appendTempTableRightSideJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, builder);
    }
}
