
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
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.bulkloader.BcpBulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.finder.MapperStackImpl;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import com.gs.fw.common.mithra.util.Time;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;


public class SybaseDatabaseType extends AbstractDatabaseType
{

    private static Logger logger;

    private static final String BULK_INSERT_TYPE_KEY = "com.gs.fw.common.mithra.databasetype.SybaseDatabaseType.bulkInsertMethod";
    protected static final String JDBC_SYBASE_TDS = "jdbc:sybase:tds:";

    private static final char[] SYBASE_LIKE_META_CHARS = {'=', '%', '_', '[', ']'};

    public static final String STATE_CONNECTION_CLOSED = "JZ0C0";
    public static final String STATE_CONNECTION_CLOSED2 = "JZ0C1";
    public static final String STATE_EMPTY_QUERY = "JZ0S4";
    public static final String STATE_INCORRECT_ESCAPE_SEQUENCE = "JZ0S8";
    public static final String STATE_INPUT_PARAMETER_NOT_SET = "JZ0SA";
    public static final String STATE_UNEXPECTED_INPUT_PARAMETER = "JZ0SF";
    public static final String STATE_UNSUPPORTED_SQL_TYPE = "JZ0SM";
    public static final String STATE_IO_READ_TIMEOUT = "JZ0I1";
    public static final String STATE_IO_WRITE_TIMEOUT = "JZ0I2";
    public static final String STATE_READ_TIMEOUT = "JZ0T3";
    public static final String STATE_WRITE_TIMEOUT = "JZ0T4";
    public static final String STATE_ILLEGAL_TYPE_CONVERSION = "JZ0TC";
    public static final String STATE_INVALID_COLUMN_NAME = "S0022";
    public static final String STATE_USER_NAME_TOO_LONG = "JZ001";
    public static final String STATE_PASSWORD_TOO_LONG = "JZ002";
    public static final String STATE_INCORRECT_URL_FORMAT = "JZ003";
    public static final String STATE_IO_EXCEPTION = "JZ006";
    public static final String STATE_BATCH_ERROR ="JZ0BE";
    public static final String STATE_METADATA_NOT_FOUND = "JZ0SJ";
    public static final String JTDS_IO_ERROR = "08S01";

    public static final int CODE_OBJECT_NOT_FOUND = 208; // this seems to happen spuriously or on a bad connection
    public static final int CODE_RUN_OUT_OF_LOCKS = 1204;
    public static final int CODE_DEADLOCK = 1205;
    public static final int CODE_DUPLICATE = 2601;
    public static final int CODE_SCHEMA_CHANGE = 540;
    public static final int CODE_REQUEST_TIMEOUT = 13468;
    public static final int CODE_CONN_TIMEOUT = 13507;
    public static final int SESSION_ACQUIRE_LOCK_TIMEOUT = 12205;
    public static final int TABLE_ACQUIRE_LOCK_TIMEOUT = 12207;

    // use only JDK and trove collections in this class for generator dependency reasons
    public static final HashSet<String> TIMEOUT_STATE;
    public static final IntHashSet TIMEOUT_ERROR;
    private static final HashSet<String> typesWithLength;
    private static final IntIntHashMap sybaseToJDBCTypes = new IntIntHashMap();
    private static final IntObjectHashMap<String> sybaseTypeNames = new IntObjectHashMap<String>();

    private static int maxClauses = 420;
    private static int maxSearchableArguments = 102;
    private static final String UNION = " union ";
    private static final String UNION_ALL = " union all ";

    private static final SybaseDatabaseType instance = new SybaseDatabaseType();
    private static final SybaseDatabaseType sybase15Instance = new SybaseDatabaseType(1, 420, 102);

    public static final String TIMESTAMP_FORMAT = "MMM dd yyyy HH:mm:ss:SSS";
    public static final String DATE_FORMAT = "MMM dd yyyy";

    // these values were arrived at using a series of test, by inserting into tables of 1, 8, 16, and 30 columns
    private static final double OPTIMAL_INSERT_PARAMETERS = 160;
    private static final int MAX_UNIONS_IN_INSERT = 60;

    private int maxParallelDegree = -1;
    private boolean forceFile = false;

    private static final Map<String, String> sqlToJavaTypes;
    private static final HashSet<String> numericSybaseTypes;

    private static final ThreadLocal calendarInstance = new ThreadLocal();
    private int maxUnions = 1;
    private Constructor<?> bulkConstructor;

    static
    {
        // http://jtds.sourceforge.net/typemap.html defines some of the mappings below.

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
        registerSybaseType(43, "bigint", Types.BIGINT);      // bigint
        registerSybaseType(80, "timestamp", Types.TIMESTAMP);     // timestamp
    }

    protected static void registerSybaseType(int sybaseType, String sybaseTypeName, int javaSqlType)
    {
        sybaseToJDBCTypes.put(sybaseType, javaSqlType);
        sybaseTypeNames.put(sybaseType, sybaseTypeName);
    }

    static
    {
        numericSybaseTypes = new HashSet<String>();
        numericSybaseTypes.add("int");
        numericSybaseTypes.add("smallint");
        numericSybaseTypes.add("tinyint");
        numericSybaseTypes.add("float");
        numericSybaseTypes.add("numeric");
        numericSybaseTypes.add("bit");
        numericSybaseTypes.add("binary");
        numericSybaseTypes.add("varbinary");
        numericSybaseTypes.add("decimal");
        numericSybaseTypes.add("real");
        numericSybaseTypes.add("bigint");
    }

    static
    {
        sqlToJavaTypes = new HashMap<String, String>();
        sqlToJavaTypes.put("integer", "int");
        sqlToJavaTypes.put("smallint", "short");
        sqlToJavaTypes.put("tinyint", "short");
        sqlToJavaTypes.put("float", "float");
        sqlToJavaTypes.put("double precision", "double");
        sqlToJavaTypes.put("double precis", "double");
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
        sqlToJavaTypes.put("binary", "byte[]");
        sqlToJavaTypes.put("varbinary", "byte[]");
        sqlToJavaTypes.put("decimal", "BigDecimal");
        sqlToJavaTypes.put("real", "float");
        sqlToJavaTypes.put("date", "Timestamp");
        sqlToJavaTypes.put("time", "Time");
        sqlToJavaTypes.put("bigint", "long");
    }

    static
    {
        typesWithLength = new HashSet<String>();
        typesWithLength.add("char");
        typesWithLength.add("nchar");
        typesWithLength.add("varchar");
        typesWithLength.add("nvarchar");
        typesWithLength.add("binary");
        typesWithLength.add("varbinary");
        typesWithLength.add("unichar");
        typesWithLength.add("univarchar");

    }

    static
    {
        TIMEOUT_STATE = new HashSet<String>();
        TIMEOUT_STATE.add(STATE_IO_READ_TIMEOUT);
        TIMEOUT_STATE.add(STATE_IO_WRITE_TIMEOUT);
        TIMEOUT_STATE.add(STATE_READ_TIMEOUT);
        TIMEOUT_STATE.add(STATE_WRITE_TIMEOUT);

        TIMEOUT_ERROR = new IntHashSet();
        TIMEOUT_ERROR.add(CODE_CONN_TIMEOUT);
        TIMEOUT_ERROR.add(CODE_REQUEST_TIMEOUT);
        TIMEOUT_ERROR.add(SESSION_ACQUIRE_LOCK_TIMEOUT);
        TIMEOUT_ERROR.add(TABLE_ACQUIRE_LOCK_TIMEOUT);
    }

    /** Extendable Singleton */
    protected SybaseDatabaseType()
    {
        String bulkType = System.getProperty(BULK_INSERT_TYPE_KEY, "jtds");
        if (bulkType.equalsIgnoreCase("file"))
        {
            forceFile = true;
        }
        try
        {
            Class<?> aClass = Class.forName("com.gs.fw.common.mithra.bulkloader.JtdsBcpBulkLoader");
            this.bulkConstructor = aClass.getDeclaredConstructor(String.class, String.class, String.class, Integer.TYPE, SybaseDatabaseType.class, Boolean.TYPE);
        }
        catch (Exception e)
        {
            getLogger().info("Sybase bulk loader not found. Bulk loading disabled.");
        }

    }

    protected SybaseDatabaseType(int maxUnions)
    {
        this();
        this.maxUnions = maxUnions;
    }

    protected SybaseDatabaseType(int maxUnions, int maxClauses, int maxSearchableArguments)
    {
        this();
        this.maxUnions = maxUnions;
        this.maxClauses = maxClauses;
        this.maxSearchableArguments = maxSearchableArguments;
    }

    private static Logger getLogger()
    {
        if(logger == null)
        {
            logger = LoggerFactory.getLogger(SybaseDatabaseType.class.getName());
        }
        return logger;
    }


    public void setForceFileBcp(boolean forceFile)
    {
        this.forceFile = forceFile;
    }

    public static SybaseDatabaseType getInstance()
    {
        return instance;
    }

    public static SybaseDatabaseType getSybase15Instance()
    {
        return sybase15Instance;
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
            query.appendFromClauseWithPerTableLocking(selectWithoutWhere, "holdlock", "noholdlock");
        }
        else
        {
            query.appendFromClause(selectWithoutWhere);
        }

        int numberOfUnions = query.getNumberOfUnions();
        StringBuffer result = new StringBuffer();
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
            selectClause.append(" holdlock");
        }
        else
        {
            selectClause.append(" noholdlock");
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
            deleteClause.append(" top ").append(rowCount).append(' ');
        }
        String tableName = query.getTableName(query.getAnalyzedOperation().getAnalyzedOperation().getResultObjectPortal(), MapperStackImpl.EMPTY_MAPPER_STACK_IMPL);
        deleteClause.append(tableName);

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
        String state = sqlException.getSQLState();
        int code = sqlException.getErrorCode();
        boolean retriable = (code == CODE_DEADLOCK || code == CODE_SCHEMA_CHANGE || STATE_CONNECTION_CLOSED.equals(state));

        if (!retriable && STATE_BATCH_ERROR.equals(state))
        {
            retriable = sqlException.getMessage().indexOf("encountered a deadlock situation. Please re-run your command.") >= 0;
        }
        if (!retriable)
        {
            retriable = JTDS_IO_ERROR.equals(state) && sqlException.getMessage().contains("DB server closed connection");
        }
        return retriable;
    }

    protected boolean isTimedOutWithoutRecursion(SQLException sqlException)
    {
        return (TIMEOUT_STATE.contains(sqlException.getSQLState()) || TIMEOUT_ERROR.contains(sqlException.getErrorCode()));
    }

    public boolean violatesUniqueIndexWithoutRecursion(SQLException sqlException)
    {
        return CODE_DUPLICATE == sqlException.getErrorCode();
    }

    public int getMaxPreparedStatementBatchCount(int parametersPerStatement)
    {
        return (int)(OPTIMAL_INSERT_PARAMETERS/parametersPerStatement);
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
//        if (tempThreshold < maxClauses) return tempThreshold + 10;
        return maxClauses;
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

    public String getFullyQualifiedDboTableName(String schema, String tableName)
    {
        String fqTableName;
        if (schema != null)
        {
            int dotIndex = schema.indexOf('.');
            if (dotIndex > 0)
            {
                fqTableName = schema.substring(0, dotIndex) + ".dbo." + tableName;
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

    @Deprecated
    public BulkLoader createBulkLoader(Connection connection, String user, String password, String hostName, int port) throws BulkLoaderException
    {
        try
        {
            if (connection != null)
            {
                getLogger().error("Using deprecated method createBulkLoader with a connection object can lead to pool exhaustion!");
                connection.close();
                connection = null;
            }
        }
        catch (SQLException e)
        {
            getLogger().error("could not close connection", e);
        }
        return createBulkLoader(user, password, hostName, port);
    }

    public BulkLoader createBulkLoader(String user, String password, String hostName, int port)
    {
        return createBulkLoader(user, password, hostName, port, true);
    }

    public BulkLoader createBulkLoader(String user, String password, String hostName, int port, boolean dataModelMismatchIsFatal)
    {
        if (hostName == null || forceFile)
        {
            return new BcpBulkLoader(this, user, password);
        }
        else if (bulkConstructor != null)
        {
            try
            {
                return (BulkLoader) bulkConstructor.newInstance(user, password, hostName, port, this, dataModelMismatchIsFatal);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Could not instantiate bulk loader", e);
            }
        }
        return null;
    }

    /**
     * <p>Overridden to create the table metadata by hand rather than using the JDBC
     * <code>DatabaseMetadata.getColumns()</code> method. This is because the Sybase driver fails
     * when the connection is an XA connection unless you allow transactional DDL in tempdb.</p>
     */
    public TableColumnInfo getTableColumnInfo(Connection connection, String schema, String table) throws SQLException
    {
        if (schema == null || schema.length() == 0)
        {
            schema = connection.getCatalog();
        }
        PreparedStatement stmt = connection.prepareStatement("SELECT name,colid,length,usertype,prec,scale,status FROM "+getFullyQualifiedDboTableName(schema, "syscolumns")+" WHERE id=OBJECT_ID(?)");
        ResultSet results = null;

        ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
        try
        {
            String objectName = getFullyQualifiedTableName(schema , table);
            stmt.setString(1, objectName);

            results = stmt.executeQuery();

            while (results.next())
            {
                String name = results.getString("name");
                int ordinalPosition = results.getInt("colid");
                int size = results.getInt("length");
                int type = sybaseToJDBCTypes.get(results.getInt("usertype"));
                int precision = results.getInt("prec");
                int scale = results.getInt("scale");

                // http://www.sybase.com/detail?id=205883#syscol - How to Read syscolumns.status
                boolean nullable = (results.getInt("status") & 8) != 0;

                columns.add(new ColumnInfo(name, type, size, precision, scale, ordinalPosition, nullable));
            }
        }
        finally
        {
            closeResultSet(results, "Ignoring error whilst closing ResultSet that was used to query the DatabaseInfo");

            closeStatement(stmt, "Ignoring error whilst closing PreparedStatement that was used to query the DatabaseInfo");
        }
        Collections.sort(columns);
        return columns.isEmpty() ? null : new TableColumnInfo(null, schema, table, columns.toArray(new ColumnInfo[columns.size()]));
    }

    /**
     * Gets the <code>DatabaseInfo</code> for a particular connection.
     * @param connection The connection to lookup the database information from.
     * @return The database information.
     * @throws SQLException if there was a problem looking up the database information.
     */
    public DatabaseInfo getDatabaseInfo(Connection connection) throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement("select @@servername, db_name()");
        ResultSet results = null;
        DatabaseInfo info = null;

        try
        {
            results = statement.executeQuery();

            if (!results.next())
            {
                throw new SQLException("Query for database name and server name returned zero rows!");
            }

            info = new DatabaseInfo(results.getString(1), results.getString(2));
        }
        finally
        {
            closeResultSet(results, "Ignoring error whilst closing ResultSet that was used to query the DatabaseInfo");

            closeStatement(statement, "Ignoring error whilst closing PreparedStatement that was used to query the DatabaseInfo");
        }

        return info;
    }

    protected boolean hasSelectUnionMultiInsert()
    {
        return true;
    }

    protected boolean hasValuesMultiInsert()
    {
        return false;
    }

    public int getMultiInsertBatchSize(int columnsToInsert)
    {
        int result = (int) Math.round(OPTIMAL_INSERT_PARAMETERS / (double) columnsToInsert);
        if (result == 0) result = 1;
        if (result > MAX_UNIONS_IN_INSERT)
        {
            result = MAX_UNIONS_IN_INSERT;
        }
        return result;
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
            Method method = ds.getClass().getMethod("getServerName", null);
            return (String) method.invoke(ds, null);
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
            Method method = ds.getClass().getMethod("getPortNumber", null);
            return (Integer) method.invoke(ds, null);
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
        if (url.toLowerCase().startsWith(JDBC_SYBASE_TDS))
        {
            StringTokenizer stok = new StringTokenizer(url, ":/?&");
            stok.nextToken(); // jdbc
            stok.nextToken(); // sybase
            stok.nextToken(); // Tds
            return stok.nextToken();
        }
        return null;
    }

    public int getPortFromUrl(String url)
    {
        if (url.toLowerCase().startsWith(JDBC_SYBASE_TDS))
        {
            StringTokenizer stok = new StringTokenizer(url, ":/?&");
            stok.nextToken(); // jdbc
            stok.nextToken(); // sybase
            stok.nextToken(); // Tds
            stok.nextToken(); // server
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
        return 0;
    }

    @Override
    public Timestamp getTimestampFromResultSet(ResultSet rs, int pos, TimeZone timeZone) throws SQLException
    {
        Calendar c = getCalendarInstance();
        Timestamp timestamp = rs.getTimestamp(pos, c);
        return MithraTimestamp.zConvertTimeForReadingWithUtcCalendar(timestamp, timeZone);
    }

    private Calendar getCalendarInstance()
    {
        SingleInstanceGregorianCalendar c = (SingleInstanceGregorianCalendar) calendarInstance.get();
        if (c == null)
        {
            c = new SingleInstanceGregorianCalendar();
            calendarInstance.set(c);
        }
        return c;
    }

    protected static IntIntHashMap getSybaseToJDBCTypes()
    {
        return sybaseToJDBCTypes;
    }

    public static class SingleInstanceGregorianCalendar extends GregorianCalendar
    {
        protected static final ISOChronology ISO_CHRONOLOGY_UTC = ISOChronology.getInstance(DateTimeZone.UTC);

        private int year;
        private int month;
        private int dayOfMonth;
        private int hourOfDay;
        private int minute;
        private int second;
        private int millis;

        public Object clone()
        {
            year = month = dayOfMonth = hourOfDay = minute = second = millis = 0;
            return this;
        }

        public long getTimeInMillis()
        {
            try
            {
                return ISO_CHRONOLOGY_UTC.getDateTimeMillis(year, month + 1, dayOfMonth, hourOfDay, minute, second, millis);
            }
            catch (IllegalArgumentException e)
            {
                getLogger().warn(e.getMessage());
                return ISO_CHRONOLOGY_UTC.getDateTimeMillis(year, month + 1, dayOfMonth, hourOfDay + 1, minute, second, millis);
            }
        }

        public void set(int field, int value)
        {
            switch(field)
            {
                case Calendar.YEAR:
                    this.year = value;
                    break;
                case Calendar.MONTH:
                    this.month = value;
                    break;
                case Calendar.DAY_OF_MONTH:
                    this.dayOfMonth = value;
                    break;
                case Calendar.HOUR_OF_DAY:
                    this.hourOfDay = value;
                    break;
                case Calendar.MINUTE:
                    this.minute = value;
                    break;
                case Calendar.SECOND:
                    this.second = value;
                    break;
                case Calendar.MILLISECOND:
                    this.millis = value;
                    break;
                default:
                    throw new RuntimeException("unexpected set method for field "+field);
            }
        }

    }

    public String getCreateTableStatement(Connection connection, String schema, String tableName) throws SQLException
    {
        return createTableStatement(getColumnInfoList(connection, schema, tableName));
    }

    public List<ColumnInfo> getColumnInfoList(Connection connection, String schema, String tableName) throws SQLException
    {
        List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();

        if (schema == null)
        {
            throw new IllegalArgumentException("schema must not be null");
        }
        String sql =
                "SELECT c.colid, c.name,  t.usertype, c.status, c.length, c.prec, c.scale " +
                        "FROM "+getFullyQualifiedDboTableName(schema, "syscolumns")+" c, "+getFullyQualifiedDboTableName(schema,"systypes")+" t " +
                        "WHERE id=OBJECT_ID('"+getFullyQualifiedTableName(schema,tableName)+"') " +
                        "and ( (c.usertype = t.usertype and c.usertype < 100) or " +
                        "(c.type = t.type and c.usertype > 100 and t.usertype < 100 and t.name not in ('longsysname', 'nchar', 'nvarchar', 'sysname'))) "+
                        "order by c.colid";

        ResultSet results = null;
        Statement stmt = connection.createStatement();

        try
        {
            results = stmt.executeQuery(sql);
            while(results.next())
            {
                int columnId = results.getInt(1);
                String columnName = results.getString(2);
                int userType = results.getInt(3);
                String columnType = sybaseTypeNames.get(userType);
                if (columnType == null)
                {
                    throw new SQLException("could not resolve column type for column "+ columnName +" in table "+ getFullyQualifiedTableName(schema, tableName)
                            +" got a user type of "+ userType);
                }
                int status = results.getInt(4);
                if ((status & 128) != 0)
                {
                    // identity column, skip it
                    getLogger().warn("Skipping identity column "+columnName+" in table "+tableName);
                    continue;
                }
                boolean nullable = (status & 8) != 0;
                ColumnInfo columnInformation = new ColumnInfo(columnName, userType, results.getInt(5),
                        results.getInt(6), results.getInt(7), 0, nullable);
                columnInfoList.add(columnInformation);
            }
        }
        finally
        {
            closeResultSet(results, "Ignoring error whilst closing ResultSet that was used to query the DatabaseInfo");
            closeStatement(stmt, "Ignoring error whilst closing PreparedStatement that was used to query the DatabaseInfo");
        }

        return columnInfoList;
    }

    public String createTableStatement(List<ColumnInfo> columnInformationMap)
    {
        StringBuilder builder = new StringBuilder(32);
        builder.append('(');
        boolean added = false;

        for(ColumnInfo columnInfo: columnInformationMap)
        {
            if (added)
            {
                builder.append(',');
            }
            added = true;
            builder.append(columnInfo.getName());
            String columnType = getColumnType(columnInfo.getType());
            builder.append(' ').append(columnType);
            if (typesWithLength.contains(columnType))
            {
                int length = columnInfo.getSize();
                builder.append('(').append(length).append(')');
            }
            else if (columnType.equals("numeric") || columnType.equals("decimal"))
            {
                builder.append('(').append(columnInfo.getPrecision()).append(',');
                builder.append(columnInfo.getScale()).append(')');
            }
            if (columnInfo.isNullable())
            {
                builder.append(" null");
            }
            else
            {
                builder.append(" not null");
            }
        }
        builder.append(')');
        return builder.toString();
    }

    private void closeStatement(Statement stmt, String msg)
    {
        if (stmt != null)
        {
            try
            {
                stmt.close();
            }
            catch (SQLException e)
            {
                getLogger().warn(msg, e);
            }
        }
    }

    private void closeResultSet(ResultSet results, String msg)
    {
        if (results != null)
        {
            try
            {
                results.close();
            }
            catch (SQLException e)
            {
                getLogger().warn(msg, e);
            }
        }
    }

    public void setMaxParallelDegree(int maxParallelDegree)
    {
        this.maxParallelDegree = maxParallelDegree;
    }

    public void configureConnection(Connection con) throws SQLException
    {
        if (maxParallelDegree > 0)
        {
            fullyExecute(con, "set parallel_degree "+maxParallelDegree);
        }
    }

    public boolean hasPerTableLock()
    {
        return true;
    }

    public String getPerTableLock(boolean lock)
    {
        if (lock)
        {
            return "holdlock";
        }
        else
        {
            return "noholdlock";
        }
    }

    public String getTempDbSchemaName()
    {
        return "tempdb."; // see getFullyQualifiedTableName for explanantion of extra dot: we don't want
    }

    public boolean hasBulkInsert()
    {
        return bulkConstructor != null || forceFile;
    }

    public String getNullableColumnConstraintString()
    {
        return " null ";
    }

    public boolean isConnectionDeadWithoutRecursion(SQLException e)
    {
        String state = e.getSQLState();
        int code = e.getErrorCode();

        return (STATE_CONNECTION_CLOSED.equals(state) ||
                STATE_CONNECTION_CLOSED2.equals(state) ||
                STATE_IO_EXCEPTION.equals(state) ||
                STATE_METADATA_NOT_FOUND.equals(state) ||
                code == CODE_RUN_OUT_OF_LOCKS  ||
                code == CODE_OBJECT_NOT_FOUND
        );
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
        return " lock allpages";
    }

    public String getSqlPostfixForSharedTempTableCreation()
    {
        return " lock allpages";
    }

    public int getMaxSearchableArguments()
    {
        return maxSearchableArguments;
    }

    public int getMaxUnionCount()
    {
        return maxUnions;
    }

    public String getModFunction(String fullyQualifiedLeftHandExpression, int divisor)
    {
        return "("+fullyQualifiedLeftHandExpression+" % "+divisor+")";
    }

    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            // we use shared temp tables in sybase because of this error:
            // 'CREATE TABLE' command is not allowed within a multi-statement transaction in the 'tempdb' database.
            // (SQL code: 2762 SQL State: ZZZZZ)
            // google "sybase Error 2762" for more details.
            return appendSharedTempTableCreatePreamble(sb, tempTableName);
        }
        sb.append("create table #").append(tempTableName);
        return "#"+tempTableName;
    }

    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String nominalTableName)
    {
        String tableName = this.getTempDbSchemaName() + "." + nominalTableName;
        sb.append("create table ").append(tableName);
        return tableName;
    }

    @Override
    public boolean createTempTableAllowedInTransaction()
    {
        return false;  // see comment in appendNonSharedTempTableCreatePreamble
    }

    public boolean dropTableAllowedInTransaction()
    {
        return false;
    }

    //todo: rezaem: fix this for sybase. for millis = 999, Sybase rounds to 996 instead of 1000. maybe others
    public void xsetTimestamp(PreparedStatement ps, int index, Timestamp timestamp, boolean forceAsString) throws SQLException
    {
//        if (forceAsString)
//        {
//            super.setTimestamp(ps, index, timestamp, forceAsString, asdfsd);
//        }
//        else
//        {
//            long time = timestamp.getTime();
//            int lastDigit = (int) (time % 10);
//            switch(lastDigit)
//            {
//                case 0:
//                case 3:
//                case 6:
//                    ps.setTimestamp(index, timestamp);
//                    break;
//                case 1:
//                    ps.setTimestamp(index, new Timestamp(time - 1));
//                    break;
//                case 2:
//                    ps.setTimestamp(index, new Timestamp(time + 1));
//                    break;
//                case 4:
//                    ps.setTimestamp(index, new Timestamp(time - 1));
//                    break;
//                case 5:
//                    ps.setTimestamp(index, new Timestamp(time + 1));
//                    break;
//                case 7:
//                    ps.setTimestamp(index, new Timestamp(time - 1));
//                    break;
//                case 8:
//                    ps.setTimestamp(index, new Timestamp(time - 2));
//                    break;
//                case 9:
//                    ps.setTimestamp(index, new Timestamp(time + 1));
//                    break;
//            }
//        }
    }

    protected Time createOrReturnTimeWithAnyRequiredRounding(Time time)
    {
        // This behaviour is encapsulated in a protected method so that subclasses (e.g. IQ) can override it
        return time.createOrReturnTimeWithRoundingForSybaseJConnectCompatibility();
    }

    @Override
    public void setTime(PreparedStatement ps, int index, Time time) throws SQLException
    {
        super.setTime(ps, index, this.createOrReturnTimeWithAnyRequiredRounding(time));
    }

    public int getDefaultPrecision()
    {
        return 18;
    }

    public int getMaxPrecision()
    {
        return 38;
    }

    public int getDeleteViaInsertAndJoinThreshold()
    {
        return -1;
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
        return message != null && message.contains("SybConnectionDeadException");
    }

    @Override
    public double getSysLogPercentFull(Connection connection, String schemaName) throws SQLException
    {
        Statement statement = null;
        ResultSet resultSet = null;
        try
        {
            statement = connection.createStatement();

            resultSet = statement.executeQuery("select @@version_as_integer");
            resultSet.next();
            int version = resultSet.getInt(1);
            resultSet.close();
            if (schemaName != null && schemaName.indexOf('.') > 0)
            {
                schemaName = schemaName.substring(0, schemaName.indexOf('.'));
            }

            String syssegmentsFullyQualifiedName = getFullyQualifiedDboTableName(schemaName, "syssegments");
            String syslogsFullyQualifiedName = getFullyQualifiedDboTableName(schemaName, "syslogs");
            String sysindexesFullyQualifiedName = getFullyQualifiedDboTableName(schemaName, "sysindexes");
            String dbId = schemaName == null ? "" : "'"+schemaName+"'";

            if (version >= 15000)   // Sybase 15 or greater
            {
                String sql = "select sum(a.size)/(power(2,20)/@@maxpagesize) " +
                        "from master..sysusages a," + syssegmentsFullyQualifiedName + " b " +
                        "where a.dbid=db_id(" + dbId + ") " +
                        "and b.name='logsegment' " +
                        "and (a.segmap & power(2,b.segment)) != 0 ";
                getLogger().debug(sql);
                resultSet = statement.executeQuery(sql);
                resultSet.next();
                double total = resultSet.getDouble(1);
                resultSet.close();
                sql = "select data_pages(db_id(" + dbId + "),i.id)/(power(2,20)/@@maxpagesize) " +
                        "from " + sysindexesFullyQualifiedName + " i " +
                        "where i.id=object_id('" + syslogsFullyQualifiedName + "')";
                getLogger().debug(sql);
                resultSet = statement.executeQuery(sql);
                resultSet.next();
                return resultSet.getDouble(1) * 100.0 / total;
            }
            else
            {
                String sql = "select sum(a.size) " +
                        "from master..sysusages a," + syssegmentsFullyQualifiedName + " b " +
                        "where a.dbid=db_id(" + dbId + ") " +
                        "and b.name='logsegment' " +
                        "and (a.segmap & power(2,b.segment)) != 0 ";
                getLogger().debug(sql);
                resultSet = statement.executeQuery(sql);
                resultSet.next();
                double total = resultSet.getDouble(1);
                resultSet.close();
                sql = "select data_pgs(i.id,i.doampg) " +
                        "from " + sysindexesFullyQualifiedName + " i " +
                        "where i.id=object_id('" + syslogsFullyQualifiedName + "')";
                getLogger().debug(sql);
                resultSet = statement.executeQuery(sql);
                resultSet.next();
                return resultSet.getDouble(1) * 100.0 / total;
            }
        }
        finally
        {
            closeResultSet(resultSet, "Error when closing result set used to determine syslog percent.");
            closeStatement(statement, "Error when closing statement used to determine syslog percent.");
        }
    }

    public String getUpdateTableStatisticsSql(String tableName)
    {
        if (!MithraManagerProvider.getMithraManager().isInTransaction())
        {
            return "update statistics " + tableName;
        }
        return null;
    }

    public static boolean isColumnTypeNumeric(int userType)
    {
        return numericSybaseTypes.contains(getColumnType(userType));
    }

    public static String getColumnType(int userType)
    {
        return sybaseTypeNames.get(userType);
    }

    @Override
    protected char[] getLikeMetaChars()
    {
        return SYBASE_LIKE_META_CHARS;
    }

    @Override
    public String getSqlExpressionForDateYear(String columnName)
    {
        return "datepart(year, " + columnName + ")";
    }

    @Override
    public String getSqlExpressionForDateMonth(String columnName)
    {
        return "datepart(month, " + columnName + ")";
    }

    @Override
    public String getSqlExpressionForDateDayOfMonth(String columnName)
    {
        return "datepart(day, " + columnName + ")";
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
