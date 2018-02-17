
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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.databasetype;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;


public class H2DatabaseType extends AbstractDatabaseType
{
    private static final Logger logger = LoggerFactory.getLogger(H2DatabaseType.class.getName());
    private static final String TIMEOUT_SQL_STATE = "HYT00";
    private static final int TIMEOUT_SQL_ERROR_CODE = 50200;
    private static final int DUPLICATE_ERROR_CODE = 23001;  // Older H2 error code
    private static final int DUPLICATE_KEY_1_ERROR_CODE = 23505;  // Newer H2 error code
    public static final int MAX_CLAUSES = 240;
    private static final H2DatabaseType instance = new H2DatabaseType();
    private static final IntIntHashMap sybaseToJDBCTypes;
    private static final Map<String, String> sqlToJavaTypes;
    private static final char[] H2_SQL_META_CHARS = {'=', '%', '_', '\\'};

    public static Logger getLogger()
    {
        return logger;
    }

    static
    {
        // http://jtds.sourceforge.net/typemap.html defines some of the mappings below.
        sybaseToJDBCTypes = new IntIntHashMap();
        sybaseToJDBCTypes.put(1, Types.CHAR);           // char
        sybaseToJDBCTypes.put(2, Types.VARCHAR);        // varchar
        sybaseToJDBCTypes.put(3, Types.BINARY);         // binary
        sybaseToJDBCTypes.put(4, Types.VARBINARY);      // varbinay
        sybaseToJDBCTypes.put(5, Types.TINYINT);        // tinyint
        sybaseToJDBCTypes.put(6, Types.SMALLINT);       // smallint
        sybaseToJDBCTypes.put(7, Types.INTEGER);        // int
        sybaseToJDBCTypes.put(8, Types.FLOAT);          // float
        sybaseToJDBCTypes.put(10, Types.NUMERIC);       // numeric
        sybaseToJDBCTypes.put(11, Types.DECIMAL);       // money
        sybaseToJDBCTypes.put(12, Types.TIMESTAMP);     // datetime
        sybaseToJDBCTypes.put(13, Types.INTEGER);       // intn
        sybaseToJDBCTypes.put(14, Types.FLOAT);         // floatn
        sybaseToJDBCTypes.put(15, Types.TIMESTAMP);     // datetimn
        sybaseToJDBCTypes.put(16, Types.BOOLEAN);       // bit
        sybaseToJDBCTypes.put(17, Types.DECIMAL);       // moneyn
        sybaseToJDBCTypes.put(19, Types.CLOB);          // text
        sybaseToJDBCTypes.put(20, Types.BLOB);          // image
        sybaseToJDBCTypes.put(21, Types.DECIMAL);       // smallmoney
        sybaseToJDBCTypes.put(22, Types.TIMESTAMP);     // smalldatetime
        sybaseToJDBCTypes.put(23, Types.REAL);          // real
        sybaseToJDBCTypes.put(24, Types.CHAR);          // nchar
        sybaseToJDBCTypes.put(25, Types.VARCHAR);       // nvarchar
        sybaseToJDBCTypes.put(26, Types.DECIMAL);       // decimal
        sybaseToJDBCTypes.put(27, Types.DECIMAL);       // decimaln
        sybaseToJDBCTypes.put(28, Types.NUMERIC);       // numericn
        sybaseToJDBCTypes.put(34, Types.CHAR);          // unichar
        sybaseToJDBCTypes.put(35, Types.VARCHAR);       // univarchar
        sybaseToJDBCTypes.put(37, Types.DATE);          // date
        sybaseToJDBCTypes.put(38, Types.TIME);          // time
        sybaseToJDBCTypes.put(39, Types.DATE);          // daten
        sybaseToJDBCTypes.put(40, Types.TIME);          // timen
        sybaseToJDBCTypes.put(80, Types.TIMESTAMP);     // timestamp

        sqlToJavaTypes = new HashMap<String, String>();
        sqlToJavaTypes.put("integer", "int");
        sqlToJavaTypes.put("smallint", "short");
        sqlToJavaTypes.put("tinyint", "byte");
        sqlToJavaTypes.put("float", "float");
        sqlToJavaTypes.put("double precision", "double");
        sqlToJavaTypes.put("double precis", "double");
        sqlToJavaTypes.put("double", "double");
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
//        sqlToJavaTypes.put("time", "Timestamp");
        sqlToJavaTypes.put("time", "Time");

    }

    private boolean useMultiValueInsert = false;
    private boolean quoteTableName = false;

    /**
     * Singleton. Protected visibility to allow test harness to subclass.
     */
    protected H2DatabaseType()
    {
    }

    public String getSelect(String columns, SqlQuery query, String groupBy, boolean isInTransaction, int rowCount)
    {
        String result = super.getSelect(columns, query, groupBy, isInTransaction, rowCount);
        if (isInTransaction)
        {
            MithraObjectPortal portal = query.getAnalyzedOperation().getOriginalOperation().getResultObjectPortal();
//            if (portal.getTxParticipationMode().mustLockOnRead())
//            {
//                result += " FOR UPDATE";
//            }
        }
        return result;
    }

    public String getSelect(String columns, String fromClause, String whereClause, boolean lock)
    {
        String result = super.getSelect(columns, fromClause, whereClause, lock);
//        if (lock)
//        {
//            result += " FOR UPDATE";
//        }
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
            deleteClause.append(" ROWNUM() <= ").append(rowCount);
        }

        return deleteClause.toString();
    }

    public static H2DatabaseType getInstance()
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
        return DUPLICATE_KEY_1_ERROR_CODE == exception.getErrorCode() || DUPLICATE_ERROR_CODE == exception.getErrorCode();
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        return "create global temporary table "+nominalTableName;
    }

    public String getSqlDataTypeForBoolean()
    {
        return "boolean";
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
        return "binary";
    }

    public String getSqlDataTypeForByte()
    {
        return "tinyint";
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
        return "double";
    }

    public String getSqlDataTypeForFloat()
    {
        return "double";
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
        return sqlToJavaTypes.get(sql);
    }

    protected boolean hasSelectUnionMultiInsert()
    {
        return false;
    }

    public void setUseMultiValueInsert(boolean useMultiValueInsert)
    {
        this.useMultiValueInsert = useMultiValueInsert;
    }

    protected boolean hasValuesMultiInsert()
    {
        return this.useMultiValueInsert;
    }

    public int getMultiInsertBatchSize(int columnsToInsert)
    {
        return 100;
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
        sb.append("CREATE GLOBAL TEMPORARY TABLE ");
        sb.append(tempTableName);
        return tempTableName;
    }

    @Override
    public Timestamp getTimestampFromResultSet(ResultSet rs, int pos, TimeZone timeZone) throws SQLException
    {
        Timestamp result = super.getTimestampFromResultSet(rs, pos, timeZone);
        if (result instanceof ImmutableTimestamp)
        {
            int nanos = result.getNanos();
            result = new Timestamp(result.getTime());
            result.setNanos(nanos);
        }
        return result;
    }

    @Override
    public boolean dropTableAllowedInTransaction()
    {
        return false;
    }

    @Override
    public boolean createTempTableAllowedInTransaction()
    {
        return false;
    }

    @Override
    public String getConversionFunctionIntegerToString(String expression)
    {
        return "convert("+expression+", char(11))";
    }

    @Override
    public String getConversionFunctionStringToInteger(String expression)
    {
        return "convert("+expression+", int)";
    }

    protected char[] getLikeMetaChars()
    {
        return H2_SQL_META_CHARS;
    }

    public boolean isQuoteTableName()
    {
        return quoteTableName;
    }

    public void setQuoteTableName(boolean quoteTableName)
    {
        this.quoteTableName = quoteTableName;
    }

    @Override
    public String getFullyQualifiedTableName(String schema, String tableName)
    {
        if (quoteTableName)
        {
            return (schema == null ? '"' + tableName + '"' : schema + '.' + '"' + tableName + '"');
        }
        else
        {
            return super.getFullyQualifiedTableName(schema, tableName);
        }
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
}
