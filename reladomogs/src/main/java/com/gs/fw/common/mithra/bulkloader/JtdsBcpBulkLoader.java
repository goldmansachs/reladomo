

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

package com.gs.fw.common.mithra.bulkloader;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.attribute.SingleColumnIntegerAttribute;
import com.gs.fw.common.mithra.attribute.SingleColumnStringAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.connectionmanager.AbstractConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.ConnectionFactory;
import com.gs.fw.common.mithra.connectionmanager.MithraPoolableConnectionFactory;
import com.gs.fw.common.mithra.connectionmanager.ObjectPoolWithThreadAffinity;
import com.gs.fw.common.mithra.connectionmanager.PooledConnection;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.util.ColumnInfo;
import net.sourceforge.jtds.jdbc.BCP;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

public class JtdsBcpBulkLoader implements BulkLoader, ConnectionFactory
{
    private final static ConcurrentHashMap<TableKey, String> createTableMap = new ConcurrentHashMap<TableKey, String>();
    private final static ConcurrentHashMap<JtdsBcpBulkLoader, ObjectPoolWithThreadAffinity<PooledConnection>> JTDS_BULK_LOADER_MAP = new ConcurrentHashMap<JtdsBcpBulkLoader, ObjectPoolWithThreadAffinity<PooledConnection>>();
    private static int maxActiveConnectionsOnPool = 10000;
    private static int maxNumberOfIdleConnectionsToKeep = 20;
    private static int minNumberOfIdleConnectionsToKeep = 1;
    private static long timeBetweenEvictionRunsMillis = 1000L * 60L;
    private static long minEvictableIdleTimeMillis = 1000L * 60L * 10L;
    private static long softMinEvictableIdleTimeMillis = 1000L * 60L;

    private Logger logger = LoggerFactory.getLogger(JtdsBcpBulkLoader.class.getName());
    private Attribute[] reorderedAttributes;

    private String urlBase;
    private String hostname;
    private int port;
    private String user;
    private String password;
    private TimeZone timeZone = null;
    private SybaseDatabaseType factory;

    private ObjectPoolWithThreadAffinity<PooledConnection> bcpConnectionPool = null;
    private PooledConnection bcp = null;

    private boolean dead = false;
    protected static final String JTDS_JBCP_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

    private final boolean dataModelMismatchIsFatal;

    public JtdsBcpBulkLoader(String user, String password,
                             String hostName, int port, SybaseDatabaseType factory)
    {
        this(user, password, hostName, port, factory, true);
    }

    public JtdsBcpBulkLoader(String user, String password,
                             String hostName, int port, SybaseDatabaseType factory, boolean dataModelMismatchIsFatal)
    {
        this.user = user;
        this.password = password;
        this.hostname = hostName;
        this.port = port;
        this.urlBase = "jdbc:jbcp:sybase://" + hostName + ":" + port;
        this.factory = factory;
        this.dataModelMismatchIsFatal = dataModelMismatchIsFatal;
    }

    public void setFactory(SybaseDatabaseType factory)
    {
        this.factory = factory;
    }

    public void initialize(TimeZone dbTimeZone, String schema, String tableName, Attribute[] attributes,
            Logger logger, String tempTableName, String columnCreationStatement, Connection con) throws BulkLoaderException, SQLException

    {
        if (schema.endsWith("."))
        {
            schema = schema.substring(0, schema.length() - 1);
        }
        this.logger = logger;

        this.timeZone = dbTimeZone;

        List<ColumnInfo> columnInfoList = null;
        // Initialize BCP connection
        try
        {
            columnInfoList = initializeBcpAndCreateTable(schema, tableName, tempTableName, columnCreationStatement);
        }
        catch (SQLException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            zCloseBcpConnection();
            logger.error("Could not load bcp driver. Please make sure jtdsjbcp-1.2.0.16.jar or later is in the classpath", e);
            throw new BulkLoaderException("Could not load bcp driver. Please make sure jtdsjbcp-1.2.0.10.jar or later is in the classpath", e);
        }

        // Reorder the input columns
        if (columnCreationStatement == null)
        {
            ArrayList columnOrder = (ArrayList) this.getBCPConnection().getColumnInputOrder();
            if (columnOrder.size() < attributes.length)
            {
                String msg = "Database columns: ";
                for(int i=0;i<columnOrder.size();i++)
                {
                    msg += columnOrder.get(i)+",";
                }
                msg += " Java attributes: ";
                for (int j = 0; j < attributes.length; j++)
                {
                    msg += (attributes[j]).getColumnName();
                }

                throw new BulkLoaderException("could not get all columns for table "+tableName+" "+msg);
            }

            reorderedAttributes = new Attribute[columnOrder.size()];
            for (int i = 0; i < columnOrder.size(); i++)
            {
                String columnName = (String) columnOrder.get(i);

                boolean found = false;
                for (int j = 0; j < attributes.length; j++)
                {
                    if ((attributes[j]).getColumnName().equals(columnName))
                    {
                        reorderedAttributes[i] = attributes[j];
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    if(dataModelMismatchIsFatal)
                    {
                        throw new BulkLoaderException("could not find Java attribute for column "+columnName+" in table "+tableName);
                    }
                    else
                    {
                        reorderedAttributes[i] = getMissingColumnAttribute(getColumnInfo(columnName, columnInfoList, tableName));
                    }
                }
            }
        }
        else
        {
            reorderedAttributes = new Attribute[attributes.length];
            System.arraycopy(attributes, 0, reorderedAttributes, 0, attributes.length);
        }
    }

    private BCP getBCPConnection() {
        return (BCP)bcp.getUnderlyingConnection();
    }

    private ColumnInfo getColumnInfo(String columnName, List<ColumnInfo> columnInfoList, String tableName) throws BulkLoaderException
    {
        for(ColumnInfo info: columnInfoList)
        {
            if(info.getName().equals(columnName))
            {
                return info;
            }
        }
        throw new BulkLoaderException("could not find column type for column "+columnName+" in table "+tableName);
    }

    private boolean isMissingTable(SQLException e)
    {
        return e.getErrorCode() == 208 && "42S02".equals(e.getSQLState());
    }

    private List<ColumnInfo> initializeBcpAndCreateTable(String schema, String tableName, String tempTableName, String columnCreation)
            throws Exception {
        List<ColumnInfo> columnInfoList = null;
        int retry = 10;
        boolean dropTable = false;
        while(retry > 0)
        {
            String createStm = null;
            retry--;
            try
            {
                initializeBcp();
                String tempDbWithDots = this.getTempDbWithDots();
                if (dropTable)
                {
                    String dropSql = "if exists (select name from " + tempDbWithDots + "sysobjects where type = 'U' and name = '"+tempTableName+"') " +
                            "drop table " + tempDbWithDots + tempTableName;
                    if (logger.isDebugEnabled())
                    {
                        logger.debug(dropSql);
                    }
                    bcp.createStatement().executeUpdate(dropSql);
                }

                columnInfoList = this.factory.getColumnInfoList(bcp, schema, tableName);
                String createTableStatement = columnCreation == null ? getCreateTableStatement(schema, tableName, columnInfoList) : "("+columnCreation+")";
                createStm = "create table " + tempDbWithDots + tempTableName + " " + createTableStatement + " lock allpages";
                if (logger.isDebugEnabled())
                {
                    logger.debug(createStm);
                }
                bcp.createStatement().executeUpdate(createStm);
                this.getBCPConnection().bcpInit(tempTableName);
                break; // we're done
            }
            catch (SQLException e)
            {
                boolean isConnectionClosed = isConnectionClosed(bcp, e);
                if(isConnectionClosed)
                {
                    this.bcpConnectionPool.invalidateObject(this.bcp);
                }
                this.zCloseBcpConnection();
                if (SybaseDatabaseType.getInstance().loopNestedExceptionForFlagAndDetermineState(DatabaseType.RETRIABLE_FLAG, e) || isMissingTable(e) ||
                        isConnectionClosed)
                {
                    logger.warn("create table failed for statement " + createStm + "\n(SQL code: " + e.getErrorCode() + " SQL State: " + e.getSQLState() + ')', e);
                }
                else
                {
                    logger.error("could not get or initialize bcp connection for statement " + createStm + "\n(SQL code: " + e.getErrorCode() + " SQL State: " + e.getSQLState() + ')', e);
                    throw e;
                }
                dropTable = true;
            }
        }
        return columnInfoList;
    }

    private String getTempDbWithDots()
    {
        return this.factory.getTempDbSchemaName() + ".";
    }

    private boolean isConnectionClosed(Connection bcp, SQLException e)
    {
        if (e.getSQLState().equals("HY010") || e.getSQLState().equals("08S01"))
        {
            return true;
        }
        try
        {
            if (bcp.isClosed())
            {
                return true;
            }
        }
        catch (SQLException e2)
        {
            //ignore
        }
        return false;
    }

    private String getCreateTableStatement(String schema, String tableName, List<ColumnInfo> columnInfoList) throws SQLException
    {
        TableKey key = new TableKey(this.hostname, this.port, schema, tableName);
        String createTable = createTableMap.get(key);
        if (createTable == null)
        {
            createTable = this.factory.createTableStatement(columnInfoList);
            createTableMap.put(key, createTable);
        }
        return createTable;
    }

    private void initializeBcp() throws Exception {
        if (this.bcp == null)
        {
            this.bcpConnectionPool = getBcpConnectionPool(this);
            this.bcp = this.bcpConnectionPool.borrowObject();
        }
        this.bcp.setCatalog(this.getSchemaNameNoDot());
    }

    private String getSchemaNameNoDot()
    {
        String schemaName = this.factory.getTempDbSchemaName();
        return schemaName.substring(0, schemaName.lastIndexOf("."));
    }

    public void bindObjectsAndExecute(List mithraObjects, Connection con) throws BulkLoaderException, SQLException
    {
        Object[] array = new Object[reorderedAttributes.length];
        List columnsToBcp = Arrays.asList(array);

        try
        {
            BCP bcpConnection = this.getBCPConnection();

            for (int j=0;j<mithraObjects.size();j++)
            {
                MithraDataObject obj = ((MithraTransactionalObject) mithraObjects.get(j)).zGetTxDataForRead();

                for (int i = 0; i < reorderedAttributes.length; i++)
                {
                    Attribute attribute = this.reorderedAttributes[i];

                    if (attribute instanceof TimestampAttribute)
                    {
                        TimestampAttribute timestampAttribute = (TimestampAttribute) attribute;
                        array[i] = timestampAttribute.zConvertTimezoneIfNecessary((Timestamp) attribute.valueOf(obj), this.timeZone);
                    }
                    else
                    {
                        array[i] = attribute.isAttributeNull(obj) ? null : attribute.valueOf(obj);
                    }
                }

                bcpConnection.bcpSendRow(columnsToBcp);
            }

            int inserted = bcpConnection.bcpBatch();
            if (inserted != mithraObjects.size())
            {
                zCloseBcpConnection();
                throw new BulkLoaderException("Expected to insert "+ mithraObjects.size()+ " but only "+inserted+" were inserted!");
            }
        }
        catch (IOException e)
        {
            zCloseBcpConnection();
            throw new BulkLoaderException("bindObject error: ", e);
        }
        catch (SQLException e)
        {
            zCloseBcpConnection();
            if (logger.isDebugEnabled())
            {
                StringBuilder badInserts = new StringBuilder(100*mithraObjects.size());
                for (int i = 0; i < reorderedAttributes.length; i++)
                {
                    Attribute attribute = this.reorderedAttributes[i];
                    if (i > 0) badInserts.append(",");
                    badInserts.append(attribute.getAttributeName());
                }
                badInserts.append("\n");
                for (int j=0;j<mithraObjects.size();j++)
                {
                    MithraDataObject obj = ((MithraTransactionalObject) mithraObjects.get(j)).zGetTxDataForRead();

                    for (int i = 0; i < reorderedAttributes.length; i++)
                    {
                        Attribute attribute = this.reorderedAttributes[i];
                        if (i > 0) badInserts.append(",");
                        if (attribute.isAttributeNull(obj))
                        {
                            badInserts.append("null");
                        }
                        else if (attribute instanceof TimestampAttribute)
                        {
                            TimestampAttribute timestampAttribute = (TimestampAttribute) attribute;
                            badInserts.append("new Timestamp(");
                            badInserts.append(timestampAttribute.zConvertTimezoneIfNecessary((Timestamp) attribute.valueOf(obj), this.timeZone).getTime());
                            badInserts.append("L)");
                        }
                        else if (attribute instanceof DateAttribute)
                        {
                            DateAttribute dateAttribute = (DateAttribute) attribute;
                            badInserts.append("new Date(");
                            badInserts.append(dateAttribute.dateValueOf(obj).getTime());
                            badInserts.append("L)");
                        }
                        else if (attribute instanceof StringAttribute)
                        {
                            badInserts.append("\"");
                            badInserts.append(attribute.valueOf(obj));
                            badInserts.append("\"");
                        }
                        else
                        {
                            badInserts.append(attribute.valueOf(obj));
                        }
                    }
                    badInserts.append("\n");
                }
                logger.debug("bad inserts \n"+badInserts.toString());
            }
            throw e;
        }
    }

    public boolean isDead()
    {
        return dead;
    }

    public void dropTempTable(String tempTableName)
    {
        String fullyQualifiedTempTableName = this.getTempDbWithDots()+tempTableName;
        try
        {
            boolean close = false;
            if (bcp == null)
            {
                initializeBcp();
                close = true;
            }
            String dropStm = "drop table " + fullyQualifiedTempTableName;
            if (logger.isDebugEnabled())
            {
                logger.debug(dropStm);
            }
            bcp.createStatement().executeUpdate(dropStm);
            if (close)
            {
                bcp.close();
                bcp = null;
            }
        }
        catch (Exception e)
        {
            logger.error("could not drop temporary table " + fullyQualifiedTempTableName +" please drop this table manually.", e);
        }
    }

    public boolean createsTempTable()
    {
        return true;
    }

    public void destroy()
    {
        this.reorderedAttributes = null;
        this.zCloseBcpConnection();
    }

    public void zCloseBcpConnection()
    {
        dead = true;
        if (bcp != null)
        {
            try
            {
                bcp.close();
            }
            catch (SQLException e)
            {
                logger.error("could not close bcp connection", e);
            }
            bcp = null;
            bcpConnectionPool = null;
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final JtdsBcpBulkLoader that = (JtdsBcpBulkLoader) o;

        if (!urlBase.equals(that.urlBase)) return false;
        return user.equals(that.user);
    }

    public int hashCode()
    {
        int result;
        result = urlBase.hashCode();
        result = 29 * result + user.hashCode();
        return result;
    }

    private static class TableKey
    {
        private String hostName;
        private int port;
        private String schema;
        private String tableName;

        public TableKey(String hostName, int port, String schema, String tableName)
        {
            this.hostName = hostName;
            this.port = port;
            this.schema = schema;
            this.tableName = tableName;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final TableKey tableKey = (TableKey) o;

            if (port != tableKey.port) return false;
            if (!hostName.equals(tableKey.hostName)) return false;
            if (!schema.equals(tableKey.schema)) return false;
            if (!tableName.equals(tableKey.tableName)) return false;

            return true;
        }

        public int hashCode()
        {
            int result;
            result = hostName.hashCode();
            result = 29 * result + port;
            result = 29 * result + schema.hashCode();
            result = 29 * result + tableName.hashCode();
            return result;
        }
    }

    private Attribute getMissingColumnAttribute(ColumnInfo columnInfo) throws BulkLoaderException
    {
        String columnName = columnInfo.getName();
        if(columnInfo.isNullable())
        {
            return new SkippingNullAttribute(columnName);
        }
        else if(SybaseDatabaseType.isColumnTypeNumeric(columnInfo.getType()))
        {
            return new SkippingIntegerAttribute(columnName);
        }
        else
        {
            throw new BulkLoaderException("could not initialize attribute for a NOT nullable column " +
                    columnName + " of type " + SybaseDatabaseType.getColumnType(columnInfo.getType()));
        }
    }

    @Override
    public BCP createConnection() throws SQLException
    {
        try
        {
            Class.forName(JTDS_JBCP_DRIVER).newInstance();
            return (BCP) DriverManager.getConnection(urlBase + "/;appName=" + AbstractConnectionManager.getApplicationName() + "jTDS", user, password);
        }
        catch (ClassNotFoundException e)
        {
            throw new SQLException(e);
        }
        catch (InstantiationException e)
        {
            throw new SQLException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new SQLException(e);
        }
    }

    public static int getMaxActiveConnectionsOnPool()
    {
        return maxActiveConnectionsOnPool;
    }

    public static void setMaxActiveConnectionsOnPool(int maxActiveConnectionsOnPool)
    {
        JtdsBcpBulkLoader.maxActiveConnectionsOnPool = maxActiveConnectionsOnPool;
    }

    public static int getMaxNumberOfIdleConnectionsToKeep()
    {
        return maxNumberOfIdleConnectionsToKeep;
    }

    public static void setMaxNumberOfIdleConnectionsToKeep(int maxNumberOfIdleConnectionsToKeep)
    {
        JtdsBcpBulkLoader.maxNumberOfIdleConnectionsToKeep = maxNumberOfIdleConnectionsToKeep;
    }

    public static int getMinNumberOfIdleConnectionsToKeep()
    {
        return minNumberOfIdleConnectionsToKeep;
    }

    public static void setMinNumberOfIdleConnectionsToKeep(int minNumberOfIdleConnectionsToKeep)
    {
        JtdsBcpBulkLoader.minNumberOfIdleConnectionsToKeep = minNumberOfIdleConnectionsToKeep;
    }

    public static long getTimeBetweenEvictionRunsMillis()
    {
        return timeBetweenEvictionRunsMillis;
    }

    public static void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis)
    {
        JtdsBcpBulkLoader.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public static long getMinEvictableIdleTimeMillis()
    {
        return minEvictableIdleTimeMillis;
    }

    public static void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis)
    {
        JtdsBcpBulkLoader.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    public static long getSoftMinEvictableIdleTimeMillis()
    {
        return softMinEvictableIdleTimeMillis;
    }

    public static void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis)
    {
        JtdsBcpBulkLoader.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
    }

    private static ObjectPoolWithThreadAffinity<PooledConnection> getBcpConnectionPool(JtdsBcpBulkLoader loader) throws Exception
    {
        ObjectPoolWithThreadAffinity<PooledConnection> pool = JTDS_BULK_LOADER_MAP.get(loader);

        if (pool == null)
        {
            synchronized (JTDS_BULK_LOADER_MAP)
            {
                pool = JTDS_BULK_LOADER_MAP.get(loader);
                if (pool == null)
                {
                    pool = new ObjectPoolWithThreadAffinity(
                            new MithraPoolableConnectionFactory(loader, 0), //factory
                            getMaxActiveConnectionsOnPool(),   // max num of connection to allow to borrow
                            0,     // max wait if no object to borrow - 0 means create immediately
                            getMaxNumberOfIdleConnectionsToKeep(),     // max number of connections to keep around when idle
                            getMinNumberOfIdleConnectionsToKeep(),     // min number of connections to keep around when idle
                            true,  // test connection when borrow
                            false, // test connection on return
                            getTimeBetweenEvictionRunsMillis(),       // time between eviction runs millis
                            getMinEvictableIdleTimeMillis(), // the minimum number of milliseconds an object can sit idle in the pool before it is eligible for eviction
                            getSoftMinEvictableIdleTimeMillis()        // the minimum number of milliseconds an object can sit idle in the pool before it is eligible for eviction with the extra condition that at least "minIdle" amount of object remain in the pool
                    );

                    JTDS_BULK_LOADER_MAP.put(loader, pool);
                }
            }
        }

        return pool;
    }

    public static void zClearBulkLoaderPools() throws Exception
    {
        synchronized (JTDS_BULK_LOADER_MAP)
        {
            Iterator<ObjectPoolWithThreadAffinity<PooledConnection>> iterator = JTDS_BULK_LOADER_MAP.values().iterator();
            while (iterator.hasNext())
            {
                ObjectPoolWithThreadAffinity<PooledConnection> pool = iterator.next();
                iterator.remove();
                pool.close();
            }
        }
    }

    private static class SkippingIntegerAttribute extends SingleColumnIntegerAttribute
    {
        private final String columnName;

        private SkippingIntegerAttribute(String columnName)
        {
            this.columnName = columnName;
        }

        @Override
        public String getAttributeName()
        {
            return "Placeholder attribute for " + columnName;
        }
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public int intValueOf(Object o) { return 0; }
        public void setIntValue(Object o, int newValue) {}
        public boolean hasSameVersion(MithraDataObject first, MithraDataObject second) { return false; }
        public boolean isSequenceSet(Object o) { return false; }
    }

    private static class SkippingNullAttribute extends SingleColumnStringAttribute
    {
        private final String columnName;
        private SkippingNullAttribute(String columnName)
        {
            this.columnName = columnName;
        }

        @Override
        public String getAttributeName()
        {
            return "Placeholder attribute for " + columnName;
        }
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        @Override
        public void setValueNull(Object o) {}
        @Override
        public boolean isAttributeNull(Object o) { return false; }
        public String stringValueOf(Object o) { return null; }
        public void setStringValue(Object o, String newValue) {}
    }
}