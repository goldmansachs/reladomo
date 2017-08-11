
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

import com.gs.collections.impl.map.mutable.primitive.IntIntHashMap;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.MithraFastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


public class MsSqlDatabaseType extends AbstractDatabaseType
{

    private static Logger logger;

    private static final char[] MS_LIKE_META_CHARS = {'=', '%', '_', '[', ']'};
    public static final HashSet<String> TIMEOUT_STATE;
    public static final IntHashSet TIMEOUT_ERROR;
    private static final IntIntHashMap sybaseToJDBCTypes = new IntIntHashMap();
    private static final IntObjectHashMap<String> sybaseTypeNames = new IntObjectHashMap<String>();

    private static final int MAX_CLAUSES = 2000;
    private static final int MAX_SEARCHABLE_ARGUMENTS = 2000;
    private static final String UNION = " union ";
    private static final String UNION_ALL = " union all ";

    private static final Map<String, String> sqlToJavaTypes;

    private static final MsSqlDatabaseType instance = new MsSqlDatabaseType();

    private static final String JDBC_MS_SQL = "jdbc:sqlserver://";
    private static final int CODE_DUPLICATE = 2601;
    private static final int CODE_DEADLOCK = 1205;
    private static final int CODE_DEADLOCK_VICTIM = 1211;
    private static final String STATE_IO_EXCEPTION = "08S01";
    private static final String STATE_DISCONNECT = "01002";
    private static final String STATE_NOT_OPEN = "08003";

    static
    {
        registerSybaseType(1, "char", Types.CHAR);
        registerSybaseType(2, "varchar", Types.VARCHAR);        // varchar
        registerSybaseType(3, "binary", Types.BINARY);         // binary
        registerSybaseType(4, "varbinary", Types.VARBINARY);      // varbinay
        registerSybaseType(5, "tinyint", Types.TINYINT);        // tinyint
        registerSybaseType(6, "smallint", Types.SMALLINT);       // smallint
        registerSybaseType(7, "int", Types.INTEGER);        // int
        registerSybaseType(8, "float", Types.FLOAT);          // float
        registerSybaseType(10, "numeric", Types.NUMERIC);       // numeric
        registerSybaseType(11, "money", Types.DECIMAL);       // money
        registerSybaseType(12, "datetime", Types.TIMESTAMP);     // datetime
        registerSybaseType(13, "int", Types.INTEGER);       // intn
        registerSybaseType(14, "float", Types.FLOAT);         // floatn
        registerSybaseType(15, "datetime", Types.TIMESTAMP);     // datetimn
        registerSybaseType(16, "bit", Types.BOOLEAN);       // bit
        registerSybaseType(17, "money", Types.DECIMAL);       // moneyn
        registerSybaseType(18, "sysname", 0);
        registerSybaseType(19, "text", Types.CLOB);          // text
        registerSybaseType(20, "image", Types.BLOB);          // image
        registerSybaseType(21, "smallmoney", Types.DECIMAL);       // smallmoney
        registerSybaseType(22, "smalldatetime", Types.TIMESTAMP);     // smalldatetime
        registerSybaseType(23, "real", Types.REAL);          // real
        registerSybaseType(24, "nchar", Types.CHAR);          // nchar
        registerSybaseType(25, "nvarchar", Types.VARCHAR);       // nvarchar
        registerSybaseType(26, "decimal", Types.DECIMAL);       // decimal
        registerSybaseType(27, "decimal", Types.DECIMAL);       // decimaln
        registerSybaseType(28, "numeric", Types.NUMERIC);       // numericn
        registerSybaseType(34, "unichar", Types.CHAR);          // unichar
        registerSybaseType(35, "univarchar", Types.VARCHAR);       // univarchar
        registerSybaseType(37, "date", Types.DATE);          // date
        registerSybaseType(38, "time", Types.TIME);          // time
        registerSybaseType(39, "date", Types.DATE);          // daten
        registerSybaseType(40, "time", Types.TIME);          // timen
        registerSybaseType(80, "timestamp", Types.TIMESTAMP);     // timestamp
    }

    protected static void registerSybaseType(int sybaseType, String sybaseTypeName, int javaSqlType)
    {
        sybaseToJDBCTypes.put(sybaseType, javaSqlType);
        sybaseTypeNames.put(sybaseType, sybaseTypeName);
    }

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
        sqlToJavaTypes.put("nchar", "char");
        sqlToJavaTypes.put("nvarchar", "String");
        sqlToJavaTypes.put("unichar", "char");
        sqlToJavaTypes.put("univarchar", "String");
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

    private static final int CODE_LOCK_TIMEOUT = 1222;
    private static final int CODE_NO_MORE_LOCKS = 1204;

    static
    {
        //todo
        TIMEOUT_STATE = new HashSet<String>();
//        TIMEOUT_STATE.add(STATE_IO_READ_TIMEOUT);
//        TIMEOUT_STATE.add(STATE_IO_WRITE_TIMEOUT);
//        TIMEOUT_STATE.add(STATE_READ_TIMEOUT);
//        TIMEOUT_STATE.add(STATE_WRITE_TIMEOUT);

        TIMEOUT_ERROR = new IntHashSet();
        TIMEOUT_ERROR.add(CODE_NO_MORE_LOCKS);
        TIMEOUT_ERROR.add(CODE_LOCK_TIMEOUT);
    }

    /** Extendable Singleton */
    protected MsSqlDatabaseType()
    {
    }

    private static Logger getLogger()
    {
        if(logger == null)
        {
            logger = LoggerFactory.getLogger(MsSqlDatabaseType.class.getName());
        }
        return logger;
    }


    public static MsSqlDatabaseType getInstance()
    {
        return instance;
    }

    public boolean hasTopQuery()
    {
        return true;
    }

    public String getSelect(String columns, SqlQuery query, String groupBy, boolean isInTransaction, int rowCount)
    {
        StringBuilder selectWithoutWhere = new StringBuilder("select ");
        String union = UNION_ALL;
        if (query.requiresDistinct())
        {
            selectWithoutWhere.append(" distinct ");
        }
        if (rowCount > 0)
        {
            selectWithoutWhere.append(" top ").append(rowCount+1).append(' ');
        }
        selectWithoutWhere.append(columns).append(" from ");

        if (isInTransaction)
        {
            query.appendFromClauseWithPerTableLocking(selectWithoutWhere, "with (serializable)", "with (nolock)");
        }
        else
        {
            query.appendFromClause(selectWithoutWhere);
        }

        int numberOfUnions = query.getNumberOfUnions();
        StringBuilder result = new StringBuilder();
        if (numberOfUnions > 1 && query.requiresUnionWithoutAll())
        {
            union = UNION;
        }
        for (int i = 0; i < numberOfUnions; i++)
        {
            if (i > 0)
            {
                result.append(union);
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

        return result.toString();
    }

    public String getSelect(String columns, String fromClause, String whereClause, boolean lock)
    {
        StringBuilder selectClause = new StringBuilder("select ").append(columns).append(" from ").append(fromClause);
        if (lock)
        {
            selectClause.append(" with (serializable)");
        }
        else
        {
            selectClause.append(" with (nolock)");
        }
        if (whereClause != null)
        {
            selectClause.append(" where ").append(whereClause);
        }
        return selectClause.toString();
    }

    public String getDelete(SqlQuery query, int rowCount)
    {
        StringBuilder deleteClause = new StringBuilder("delete ");
        if(rowCount > 0)
        {
            deleteClause.append(" top (").append(rowCount).append(")");
        }
        deleteClause.append(" from ").append(query.getFromClauseAsString());
        String where = query.getWhereClauseAsString(0); // zero is the union number is which is disabled for deletes.
        if (where.trim().length() > 0)
        {
            deleteClause.append(" where ").append(where);
        }

        return deleteClause.toString();
    }

    protected boolean isRetriableWithoutRecursion(SQLException sqlException)
    {
        int code = sqlException.getErrorCode();
        return (code == CODE_DEADLOCK || code == CODE_DEADLOCK_VICTIM );
//
//        if (!retriable && STATE_BATCH_ERROR.equals(state))
//        {
//            retriable = sqlException.getMessage().indexOf("encountered a deadlock situation. Please re-run your command.") >= 0;
//        }
//        if (!retriable)
//        {
//            retriable = JTDS_IO_ERROR.equals(state) && sqlException.getMessage().contains("DB server closed connection");
//        }
    }

    protected boolean isTimedOutWithoutRecursion(SQLException sqlException)
    {
        return (TIMEOUT_STATE.contains(sqlException.getSQLState()) || TIMEOUT_ERROR.contains(sqlException.getErrorCode()));
    }

    public boolean violatesUniqueIndexWithoutRecursion(SQLException sqlException)
    {
        return CODE_DUPLICATE == sqlException.getErrorCode();
    }

    public void setRowCount(Connection con, int rowcount) throws SQLException
    {
        PreparedStatement stm = con.prepareStatement("set rowcount ?");
        stm.setInt(1, rowcount);
        stm.executeUpdate();
        stm.close();
    }

    public void setInfiniteRowCount(Connection con)
    {
        try
        {
            this.setRowCount(con, 0);
        }
        catch (SQLException e)
        {
            getLogger().error("Could not reset row count! This is very bad, as the connection will now be foobared in the pool", e);
        }
    }

    public int getMaxClauses()
    {
        return MAX_CLAUSES;
    }

    public boolean hasSetRowCount()
    {
        return true;
    }

    @Override
    public String getIndexableSqlDataTypeForBoolean()
    {
        return "tinyint";
    }

    @Override
    public String getConversionFunctionIntegerToString(String expression)
    {
        return "convert(char(11), "+expression+")";
    }

    @Override
    public String getConversionFunctionStringToInteger(String expression)
    {
        return "convert(int, "+expression+")";
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

    public String getFullyQualifiedTableName(String schema, String tableName)
    {
        String fqTableName;
        if (schema != null)
        {
            if (schema.indexOf('.') > 0)
            {
                fqTableName = schema + '.' + tableName;
            }
            else
            {
                fqTableName = schema + ".dbo." + tableName;
            }
        }
        else
        {
            fqTableName = tableName;
        }

        return fqTableName;
    }

    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        String javaType = sqlToJavaTypes.get(sql);

        if (sql.equals("numeric"))
        {
            if (decimal != 0)
            {
                javaType =  "BigDecimal";
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
        return true;
    }

    public int getMultiInsertBatchSize(int columnsToInsert)
    {
        return Math.min(900, MAX_CLAUSES/columnsToInsert);
    }

    public String getLastIdentitySql(String tableName)
    {
        return "select @@identity";
    }

    public String getAllowInsertIntoIdentityStatementFor(String tableName, String onOff)
    {
        return "SET IDENTITY_INSERT "+  tableName + onOff;
    }

    public String getHostnameFromDataSource(DataSource ds)
    {
        try
        {
            Method method = ds.getClass().getMethod("getServerName", (Class<Object>[]) null);
            return (String) method.invoke(ds, (Object[]) null);
        }
        catch (NoSuchMethodException e)
        {
            //ignore
        }
        catch (InvocationTargetException e)
        {
            //ignore
        }
        catch (IllegalAccessException e)
        {
            //ignore
        }

        return null;
    }

    public int getPortFromDataSource(DataSource ds)
    {
        try
        {
            Method method = ds.getClass().getMethod("getPortNumber", (Class<Object>[]) null);
            return (Integer) method.invoke(ds, (Object[]) null);
        }
        catch (NoSuchMethodException e)
        {
            //ignore
        }
        catch (InvocationTargetException e)
        {
            //ignore
        }
        catch (IllegalAccessException e)
        {
            //ignore
        }

        return 0;
    }

    public String getHostnameFromUrl(String url)
    {
        if (url.toLowerCase().startsWith(JDBC_MS_SQL))
        {
            StringTokenizer stok = new StringTokenizer(url, "\\:/?&");
            stok.nextToken(); // jdbc
            stok.nextToken(); // sqlserver
            return stok.nextToken();
        }
        return null;
    }

    public int getPortFromUrl(String url)
    {
        if (url.toLowerCase().startsWith(JDBC_MS_SQL))
        {
            int colon = url.lastIndexOf(':');
            if (colon > 0)
            {
                StringTokenizer stok = new StringTokenizer(url.substring(colon), "\\:/?&");
                String portString = stok.nextToken();
                try
                {
                    return Integer.parseInt(portString);
                }
                catch (NumberFormatException e)
                {
                    getLogger().error("Could not parse port number in url "+url);
                }
            }
        }
        return 0;
    }

    public boolean hasPerTableLock()
    {
        return true;
    }

    public String getPerTableLock(boolean lock)
    {
        if (lock)
        {
            return "with (serializable)";
        }
        else
        {
            return "with (nolock)";
        }
    }

    public boolean hasBulkInsert()
    {
        return false;
    }

    public String getNullableColumnConstraintString()
    {
        return " null ";
    }

    public boolean isConnectionDeadWithoutRecursion(SQLException e)
    {
        String msg = e.getMessage();
        String state = e.getSQLState();
        if (STATE_IO_EXCEPTION.equals(state) || STATE_DISCONNECT.equals(state) || STATE_NOT_OPEN.equals(state) ||
                (msg != null && (msg.contains("closed") || msg.toLowerCase().contains("connection reset"))))
        {
            return true;
        }
        return false;
    }

    public boolean generateBetweenClauseForLargeInClause()
    {
        return true;
    }

    public String getTableNameForNonSharedTempTable(String nominalName)
    {
        return "#"+nominalName;
    }

    public String getSqlPrefixForNonSharedTempTableCreation(String nominalTableName)
    {
        return "create table #"+nominalTableName;
    }

    public String getSqlPostfixForNonSharedTempTableCreation()
    {
        return "";
    }

    public String getSqlPostfixForSharedTempTableCreation()
    {
        return "";
    }

    public int getMaxSearchableArguments()
    {
        return MAX_SEARCHABLE_ARGUMENTS;
    }

    public String getModFunction(String fullyQualifiedLeftHandExpression, int divisor)
    {
        return "("+fullyQualifiedLeftHandExpression+" % "+divisor+")";
    }

    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        sb.append("create table #").append(tempTableName);
        return "#"+tempTableName;
//        return appendSharedTempTableCreatePreamble(sb, tempTableName);
    }

    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String nominalTableName)
    {
        sb.append("create table ##").append(nominalTableName);
        return "##"+nominalTableName;
    }

    @Override
    public boolean createTempTableAllowedInTransaction()
    {
        return true;
    }

    public boolean dropTableAllowedInTransaction()
    {
        return true;
    }

    public int getDefaultPrecision()
    {
        return 18;
    }

    public int getMaxPrecision()
    {
        return 38;
    }

    public String createSubstringExpression(String stringExpression, int start, int end)
    {
        int length = end - start;
        if (end < 0) length = Integer.MAX_VALUE;
        return "substring("+stringExpression+","+(start+1)+","+length+")";
    }

    @Override
    public boolean isKilledConnectionException(Exception e)
    {
        String message = e.getMessage();
        //todo
        return false; // message != null && message.contains("SybConnectionDeadException");
    }

    @Override
    protected char[] getLikeMetaChars()
    {
        return MS_LIKE_META_CHARS;
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
    public com.gs.fw.common.mithra.util.Time getTime(ResultSet resultSet, int position) throws SQLException
    {
        String string = resultSet.getString(position);

        if(string == null)
            return null;

        try
        {
            return parseStringAndSet(string);
        }
        catch (ParseException e)
        {
            throw new RuntimeException("Could not parse string '"+string+"'");
        }
    }

    public com.gs.fw.common.mithra.util.Time parseStringAndSet(String value) throws ParseException
    {
        int hour = 0;
        int start = 0;
        int pos = start;
        while (pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "hour", pos, c, ':')) break;
            hour = parseDigit(value, "hour", pos, hour, c);
            pos++;
        }
        start = pos + 1;
        pos = start;
        int min = 0;
        while (pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "minutes", pos, c, ':')) break;
            min = parseDigit(value, "minutes", pos, min, c);
            pos++;
        }

        start = pos + 1;
        pos = start;
        int sec = 0;
        while (pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "seconds", pos, c, '.')) break;
            sec = parseDigit(value, "seconds", pos, sec, c);
            pos++;
        }
        int milli = 0;
        pos++;
        int end = pos+3;
        while (pos < end)
        {
            char c = value.charAt(pos);
            milli = parseDigit(value, "milliseconds", pos, milli, c);
            pos++;
        }
        if (hour > 23)
        {
            throw new ParseException("Hour too large in " + value, 0);
        }
        return com.gs.fw.common.mithra.util.Time.withMillis(hour, min, sec, milli);
    }

    private int parseDigit(String value, String timePartName, int pos, int sec, char c) throws ParseException
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

    private boolean checkEnd(String value, int start, String timePartName, int pos, char c, char end) throws ParseException
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
    public String getSqlExpressionForStandardDeviationSample(String columnName)
    {
        return "stdev(" + columnName + ")";
    }

    @Override
    public String getSqlExpressionForStandardDeviationPop(String columnName)
    {
        return "stdevp(" + columnName + ")";
    }

    @Override
    public String getSqlExpressionForVarianceSample(String columnName)
    {
        return "var(" + columnName +")";
    }

    @Override
    public String getSqlExpressionForVariancePop(String columnName)
    {
        return "varp(" +columnName+")";
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
        this.appendTempTableJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, fullyQualifiedTableNameGenericSource, builder);
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

        this.appendTempTableJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, mithraObjectPortal, fullyQualifiedTableNameGenericSource, builder);
    }

    @Override
    public int getNullableBooleanJavaSqlType()
    {
        return Types.TINYINT;
    }
}
