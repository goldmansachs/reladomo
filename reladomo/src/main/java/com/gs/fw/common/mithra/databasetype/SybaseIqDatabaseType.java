
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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.bulkloader.SybaseIqBulkLoader;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import com.gs.fw.common.mithra.util.WildcardParser;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

public class SybaseIqDatabaseType extends SybaseDatabaseType
{
    private static Logger logger;
    private final static SybaseIqDatabaseType instance = new SybaseIqDatabaseType(true);
    private final static SybaseIqDatabaseType instanceUnsharedTempTable = new SybaseIqDatabaseType(false);
    private final static int MAX_CLAUSES = 2000;
    private final static int MAX_SEARCHABLE_ARGUMENTS = 2000;

    private static final IntIntHashMap sybaseToJDBCTypes;
    private static final Map<String, String> sqlToJavaTypes;
    private static final String TEMPDB_PREFIX = "tempdb_";

    private boolean supportsSharedTempTables = true;

    static
    {

        // from SYS.SYSDOMAIN:
    	sqlToJavaTypes = new UnifiedMap<String, String>();
        sybaseToJDBCTypes = new IntIntHashMap();
        registerSybaseType(1,"smallint", "short", Types.SMALLINT);//1,"smallint"
        registerSybaseType(2,"integer", "int", Types.INTEGER);//2,"integer"
        registerSybaseType(3,"numeric", "BigDecimal", Types.NUMERIC);//3,"numeric"
        registerSybaseType(4,"real", "float", Types.FLOAT);//4,"float"
        registerSybaseType(5,"float", "double", Types.DOUBLE);//5,"double"
        registerSybaseType(6,"date", "Timestamp", Types.DATE);//6,"date"
        registerSybaseType(7,"char", "String", Types.CHAR);//7,"char"
        registerSybaseType(8,"char", "String", Types.CHAR);//8,"char"
        registerSybaseType(9,"varchar", "String", Types.VARCHAR);//9,"varchar"
        registerSybaseType(10,"longvarchar", "String", Types.LONGVARCHAR);//10,"longvarchar"
        registerSybaseType(11,"binary", "not implemented", Types.BINARY);//11,"binary"
        registerSybaseType(12,"longbinary", "not implemented", Types.LONGVARBINARY);//12,"longbinary"
        registerSybaseType(13,"timestamp", "Timestamp", Types.TIMESTAMP);//13,"timestamp"
        registerSybaseType(19,"tinyint", "short", Types.TINYINT);//19,"tinyint"
        registerSybaseType(20,"bigint", "long", Types.BIGINT);//20,"bigint"
        registerSybaseType(21,"unsignedint", "unsignedint", Types.INTEGER);//21,"unsignedint"
        registerSybaseType(22,"unsignedsmallint", "unsignedshort", Types.SMALLINT);//22,"unsignedsmallint"
        registerSybaseType(23,"unsignedbigint", "unsignedlong", Types.BIGINT);//23,"unsignedbigint"
        registerSybaseType(24,"bit", "byte", Types.BIT);//24,"bit"
        registerSybaseType(25,"java.lang.Object", "java.lang.Object", Types.JAVA_OBJECT);//25,"java.lang.Object"
        registerSybaseType(26,"javaserialization", "javaserialization", Types.JAVA_OBJECT);//26,"javaserialization"
        registerSybaseType(27,"decimal", "BigDecimal", Types.DECIMAL);//27,"decimal"
        registerSybaseType(28,"varbinary", "varbinary", Types.VARBINARY);//28,"varbinary"
        registerSybaseType(29,"time", "Time", Types.TIME);//29,"time"
        registerSybaseType(-1,"smalldatetime", "Timestamp", Types.TIMESTAMP);
    }

    public SybaseIqDatabaseType()
    {
        this.setUpdateViaInsertAndJoinThreshold(10);
    }

    public SybaseIqDatabaseType(boolean supportsSharedTempTables)
    {
        this.supportsSharedTempTables = supportsSharedTempTables;
        this.setUpdateViaInsertAndJoinThreshold(10);
    }

    @Override
    public boolean supportsSharedTempTable()
    {
        return this.supportsSharedTempTables;
    }

    protected static void registerSybaseType(int sybaseType, String sybaseTypeName, String javaTypeName, int javaSqlType)
    {
        sybaseToJDBCTypes.put(sybaseType, javaSqlType);
        sqlToJavaTypes.put(sybaseTypeName, javaTypeName);
    }

    @Override
    public String getJavaTypeFromSql(String sql, Integer precision, Integer decimal)
    {
        return sqlToJavaTypes.get(sql);
    }

    public static SybaseIqDatabaseType getInstance()
    {
        return instance;
    }

    /*In previous override but not in Reladomo*/
    @Override
    public double getSysLogPercentFull(Connection connection, String schemaName) throws SQLException
    {
        //an override of the SybaseASE-specific behavior in SybaseDatabaseType
        return 0.0;
    }

    public static SybaseIqDatabaseType getInstanceWithoutSharedTempTables()
    {
        return instanceUnsharedTempTable;
    }

    public int getMaxClauses()
    {
        return MAX_CLAUSES;
    }

    public int getMaxSearchableArguments()
    {
        return MAX_SEARCHABLE_ARGUMENTS;
    }

    public int getMaxUnionCount()
    {
        return 1;
    }

    public boolean hasMultiInsert()
    {
        return false;
    }

    public boolean createTempTableAllowedInTransaction()
    {
        return true;
    }

    public boolean dropTableAllowedInTransaction()
    {
        return false;
    }

    @Override
    public boolean nonSharedTempTablesAreDroppedAutomatically()
    {
        return false;
    }

    @Override
    public boolean dropTempTableSyncAfterTransaction()
    {
        return true;
    }

    @Deprecated
    public BulkLoader createBulkLoader(Connection connection, String user, String password, String hostName, int port) throws BulkLoaderException
    {
        throw new RuntimeException("IQ requires a shared directory. Use the method with the extra parameters");
    }

    @Deprecated
    public BulkLoader createBulkLoader(Connection connection, String user, String password, String dbLoadDir, String appLoadDir) throws BulkLoaderException
    {
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                logger.error("could not close connection", e);
            }
        }
        return new SybaseIqBulkLoader(this, dbLoadDir, appLoadDir);
    }

    public BulkLoader createBulkLoader(String dbLoadDir, String appLoadDir) throws BulkLoaderException
    {
        return new SybaseIqBulkLoader(this, dbLoadDir, appLoadDir);
    }

    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        sb.append("create local temporary table ").append(tempTableName);
        return tempTableName;
    }

    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String nominalTableName)
    {
        String fullTableName = TEMPDB_PREFIX+nominalTableName;
        sb.append("create table ").append(fullTableName);
        return fullTableName;
    }

    public String getSqlPostfixForNonSharedTempTableCreation()
    {
        if (!MithraManagerProvider.getMithraManager().isInTransaction())
        {
            return " on commit preserve rows";
        }
        return "";
    }

    public String getSqlPostfixForSharedTempTableCreation()
    {
        return "";
    }

    /**
     * <p>Overridden to create the table metadata by hand rather than using the JDBC
     * <code>DatabaseMetadata.getColumns()</code> method. This is because the Sybase driver fails
     * when the connection is an XA connection unless you allow transactional DDL in tempdb.</p>
     */
    public TableColumnInfo getTableColumnInfo(Connection connection, String schema, String table) throws SQLException
    {
        if (schema == null || schema.length() == 0 || schema.equals(this.getTempDbSchemaName()))
        {
            schema = this.getCurrentSchema(connection);
        }
        return TableColumnInfo.createTableMetadataWithExtraSelect(connection, schema, table, this.getFullyQualifiedTableName(schema, table));
    }

    @Override
    public boolean indexRequiresSchemaName()
    {
        return false;
    }

    public String getModFunction(String fullyQualifiedLeftHandExpression, int divisor)
    {
        return "mod("+fullyQualifiedLeftHandExpression+","+divisor+")";
    }

    public String getFullyQualifiedTableName(String schema, String tableName)
    {
        String fqTableName;
        if (schema != null)
        {
            if (schema.equals(this.getTempDbSchemaName()))
            {
                return TEMPDB_PREFIX +tableName;
            }
            fqTableName = schema + '.' + tableName;
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
                fqTableName = schema.substring(0, dotIndex) + ".." + tableName;
            }
            else
            {
                fqTableName = schema + ".." + tableName;
            }
        }
        else
        {
            fqTableName = tableName;
        }

        return fqTableName;
    }

    private static Logger getLogger()
    {
        if(logger == null)
        {
            logger = LoggerFactory.getLogger(SybaseIqDatabaseType.class.getName());
        }
        return logger;
    }

    @Override
    public boolean violatesUniqueIndexWithoutRecursion(SQLException sqlException)
    {
        return "QGA03".equals(sqlException.getSQLState());
    }

    @Override
    public String createSubstringExpression(String stringExpression, int start, int end)
    {
        if (end < 0) return "substr("+stringExpression+","+(start+1)+")";
        return "substr("+stringExpression+","+(start+1)+","+(end - start)+")";
    }

    @Override
    public int getUseTempTableThreshold()
    {
        return 2*MAX_CLAUSES;
    }

    public String getUpdateTableStatisticsSql(String tableName)
    {
        return null;
    }

    @Override
    public String escapeLikeMetaChars(String parameter)
    {
        return WildcardParser.escapeLikeMetaCharsForIq(parameter);
    }

    @Override
    public String getSqlLikeExpression(WildcardParser parser)
    {
        return parser.getSqlLikeExpressionForIq();
    }

//    @Override
//    public String getSqlExpressionForDateYear(String columnName)
//    {
//        return "YEARS("+ columnName +")";
//    }
//
//    @Override
//    public String getSqlExpressionForDateMonth(String columnName)
//    {
//        return "MONTH("+ columnName +")";
//    }
//
//    @Override
//    public String getSqlExpressionForDateDayOfMonth(String columnName)
//    {
//        return "DAY("+ columnName +")";
//    }

    protected com.gs.fw.common.mithra.util.Time createOrReturnTimeWithAnyRequiredRounding(com.gs.fw.common.mithra.util.Time time)
    {
        // Unlike ASE, IQ time values do not require any rounding as (when using jConnect version 7.0 or later) they are stored with millisecond-level precision
        return time;
    }
}
