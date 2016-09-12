
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

package com.gs.fw.common.mithra.test;

import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.connectionmanager.*;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.DerbyDatabaseType;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class ConnectionManagerForTests extends AbstractMithraTestConnectionManager
        implements SourcelessConnectionManager, IntSourceConnectionManager, ObjectSourceConnectionManager, SchemaManager
{

    private static final Logger logger = LoggerFactory.getLogger(ConnectionManagerForTests.class.getName());

    public static ConnectionManagerForTests instance = new ConnectionManagerForTests();

    private Map<Object, XAConnectionManager> connectionManagerMap = new ConcurrentHashMap<Object, XAConnectionManager>();

    private TimeZone timeZone = TimeZone.getTimeZone("America/New_York");
    private DatabaseType databaseType = H2DatabaseType.getInstance();
    private boolean isPeerToPeer;

    private static final Map<String, ConnectionManagerForTests> configToPoolMap = new ConcurrentHashMap<String, ConnectionManagerForTests>();
    private static final String RESOURCE_NAME_PROPERTY = "resourceName";
    private static ConnectionManagerFactory connectionManagerFactory = new DefaultConnectionManagerFactory();

    protected ConnectionManagerForTests()
    {
        this("");
    }

    protected ConnectionManagerForTests(String dbName)
    {
        this.setDefaultSource(dbName);
    }

    public static void setConnectionManagerFactory(ConnectionManagerFactory factory)
    {
        connectionManagerFactory = factory;
    }

    /**
     * @return the ConnectionManagerForTests instance
     */
    public static ConnectionManagerForTests getInstance()
    {
        return instance;
    }

    /**
     * This method returns an instance of a ConnectionManagerForTests. The properties must contain a property with name "resourceName".
     * The value of this property is the database name used by the connection manager when creating a Connection.
     *
     * @param properties Properties use to create the instance of the ConnectionManagerForTests
     * @return An instance of a ConnectionManagerForTests
     */
    public static ConnectionManagerForTests getInstance(Properties properties)
    {
        //return instance;
        if (properties == null)
        {
            throw new RuntimeException("Invalid properties.");
        }
        String resourceName = properties.getProperty(RESOURCE_NAME_PROPERTY);
        if (resourceName == null)
        {
            return instance;
        }
        return getInstanceForDbName(resourceName);
    }

    /**
     * This method returns an instance of a ConnectionManagerForTests.
     *
     * @param resourceName The database name used by the connection manager when creating a Connection.
     * @return An instance of a ConnectionManagerForTests
     */
    public static ConnectionManagerForTests getInstance(String resourceName)
    {
        if (resourceName == null)
        {
            logger.error("Invalid resource name: " + resourceName);
            throw new RuntimeException("Invalid resourceName: " + resourceName);
        }
        return getInstanceForDbName(resourceName);
    }


    /**
     * This method returns an instance of a ConnectionManagerForTests.
     *
     * @param dbName The database name used by the connection manager when creating a Connection.
     * @return An instance of a ConnectionManagerForTests
     */

    public static ConnectionManagerForTests getInstanceForDbName(String dbName)
    {

        synchronized (configToPoolMap)
        {
            ConnectionManagerForTests existing = configToPoolMap.get(dbName);
            if (existing == null)
            {
                existing = connectionManagerFactory.createConnectionManager(dbName);
                existing.setDatabaseType(H2DatabaseType.getInstance());
                existing.setDatabaseTimeZone(TimeZone.getTimeZone("America/New_York"));
                existing.setConnectionManagerIdentifier(dbName);
                configToPoolMap.put(dbName, existing);
            }
            existing.setDefaultSource(dbName);
            return existing;
        }
    }

    protected Map<Object, XAConnectionManager> getConnectionManagerMap()
    {
        return connectionManagerMap;
    }

    public boolean hasConnectionManagerForSource(String source)
    {
        return this.connectionManagerMap.containsKey(source);
    }

    // Return value of true means that a connection manager was added; false means it was already present
    public boolean addConnectionManagerForSource(String schemaName)
    {
        return this.addConnectionManagerForSource(schemaName, schemaName);
    }

    private String getAdditionalH2Arguments()
    {
        return System.getProperty("h2.additionalArguments", "");
    }

    // Return value of true means that a connection manager was added; false means it was already present
    public boolean addConnectionManagerForSource(Object sourceId, String schemaName)
    {
        Object key = getConnectionManagerSourceKey(sourceId, schemaName);
        if (this.connectionManagerMap.containsKey(key))
        {
//            logger.info("Connection manager for source exists: " + schemaName);
            return false;
        }

        XAConnectionManager connectionManager = new XAConnectionManager();
        connectionManager.setInitialSize(3);
        connectionManager.setPoolSize(17);
        connectionManager.setPoolName("Test database pool");
        connectionManager.setLdapName("localhost");
        connectionManager.setDefaultSchemaName(schemaName);

        if (this.databaseType instanceof DerbyDatabaseType)
        {
            connectionManager.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
            connectionManager.setJdbcConnectionString("jdbc:derby:" + schemaName + ";create=true;");
            connectionManager.setJdbcUser("");
            connectionManager.setJdbcPassword("");
        }
        else
        {
            connectionManager.setDriverClassName("org.h2.Driver");

            if (this.isPeerToPeer)
            {
                connectionManager.setJdbcConnectionString("jdbc:h2:tcp://localhost/mem:" + schemaName + this.getAdditionalH2Arguments());
            }
            else
            {
                connectionManager.setJdbcConnectionString("jdbc:h2:mem:" + schemaName + this.getAdditionalH2Arguments());
            }

            connectionManager.setJdbcUser("sa");
            connectionManager.setJdbcPassword("");
        }
        connectionManager.setUseStatementPooling(true);
        connectionManager.initialisePool();

        this.connectionManagerMap.put(key, connectionManager);
        return true;
    }

    public Object getConnectionManagerSourceKey(Object sourceId, String schemaName)
    {
        Object key = sourceId;
        if (key == null)
        {
            key = schemaName;
        }
        return key;
    }

    @Override
    protected void ensureAllDatabasesRegisteredAndTablesExist(MithraTestResource mtr, Set<TestDatabaseConfiguration> testDbConfigs)
    {
        Set<Object> nonRegisteredKeys = new UnifiedSet<Object>();

        for (TestDatabaseConfiguration testDbConfig : testDbConfigs)
        {
            Object key = testDbConfig.getConnectionManagerSourceKey(this);
            boolean keyNotRegistered = testDbConfig.addToConnectionManager(this); // it's harmless to repeat this if the source is already present

            if (keyNotRegistered || nonRegisteredKeys.contains(key))
            {
                // If the connection manager was not registered, then it was probably removed by fullyShutdown(), which means that the H2 database was shut down and all tables are gone.
                // We need to recreate the tables belonging to this config and any other config which shares the same source key.

                logger.info("Found non-registered TestDatabaseConfiguration - recreating all tables for " + testDbConfig.toString());
                testDbConfig.recreateTables(this, mtr);
                nonRegisteredKeys.add(key);
            }
        }
    }

    public void createSchema(String databaseName, String schemaName, Class sourceAttributeType)
    {
        Connection con = null;
        Statement stm = null;
        DatabaseType dt = this.getDatabaseType(databaseName);
        String schemaStmt = dt.getCreateSchema(this.getSchema(schemaName));

        if (schemaStmt != null)
        {
            try
            {
                if (sourceAttributeType == null)
                {
                    con = this.getConnection();
                }
                else if (sourceAttributeType == Integer.TYPE)
                {
                    con = this.getConnection(Integer.parseInt(databaseName));
                }
                else
                {
                    con = this.getConnection(databaseName);
                }

                stm = con.createStatement();
                stm.execute(schemaStmt);
            }
            catch (SQLException e)
            {
                logger.error("Create schema failed", e);
                throw new RuntimeException("create schema failed " + e.getMessage());
            }
            finally
            {
                try
                {
                    if (stm != null)
                    {
                        stm.close();
                    }
                    if (con != null)
                    {
                        con.close();
                    }
                }
                catch (SQLException e)
                {
                    logger.error("Error while closing statement and connection.", e);
                    throw new RuntimeException("Error while closing statement and connection.", e);
                }
            }
        }
    }

    public XAConnectionManager getDefaultConnectionManager()
    {
        if (this.getDefaultSource() == null)
        {
            throw new NullPointerException("Default connection manager is not set");
        }

        XAConnectionManager connectionManager = this.connectionManagerMap.get(this.getDefaultSource());

        if (connectionManager == null)
        {
            throw new NullPointerException("No connection manager found for source '" + this.getDefaultSource() + "'");
        }

        return connectionManager;
    }

    public DatabaseType getDatabaseType()
    {
        return this.databaseType;
    }

    public DatabaseType getDatabaseType(int sourceAttribute)
    {
        return this.getDatabaseType();
    }

    public String getDatabaseIdentifier(int sourceAttribute)
    {
        return this.getDatabaseIdentifier(Integer.valueOf(sourceAttribute));
    }

    public String getDatabaseIdentifier(Object sourceAttribute)
    {
        XAConnectionManager connectionManager = this.getUnderlyingConnectionManager(sourceAttribute);
        return connectionManager.getLdapName() + ":" + connectionManager.getDefaultSchemaName();
    }

    public String getDatabaseIdentifier()
    {
        XAConnectionManager connectionManager = this.getDefaultConnectionManager();
        return connectionManager.getLdapName() + ":" + connectionManager.getDefaultSchemaName();
    }

    public DatabaseType getDatabaseType(Object sourceAttribute)
    {
        return this.getDatabaseType();
    }

    public void setDatabaseType(DatabaseType databaseType)
    {
        this.databaseType = databaseType;
    }

    public void setPeerToPeer(boolean peerToPeer)
    {
        this.isPeerToPeer = peerToPeer;
    }

    private void setIsolationLevel(Connection connection)
    {
        try
        {
            if (connection.getTransactionIsolation() != Connection.TRANSACTION_SERIALIZABLE)
            {
                connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            }
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    public int getNumberOfActiveConnections()
    {
        return this.getDefaultConnectionManager().getNumberOfActiveConnections();
    }

    public int getNumberOfIdleConnection()
    {
        return this.getDefaultConnectionManager().getNumberOfIdleConnections();
    }

    @Override
    protected Collection<XAConnectionManager> getAllConnectionManagers()
    {
        return connectionManagerMap.values();
    }


    private XAConnectionManager getUnderlyingConnectionManager(Object sourceAttribute)
    {
        XAConnectionManager connectionManager = this.connectionManagerMap.get(sourceAttribute);
        if (connectionManager == null)
        {
            long curThread = Thread.currentThread().getId();
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            ThreadInfo threadInfo = threadMXBean.getThreadInfo(curThread, Integer.MAX_VALUE);
            StackTraceElement[] stackTrace = threadInfo.getStackTrace();
            String requestingClass = null;
            boolean hasGenericSource = false;
            for (int i = 0; i < stackTrace.length; i++)
            {
                String className = stackTrace[i].getClassName();
                if (className.endsWith("DatabaseObjectAbstract"))
                {
                    hasGenericSource = stackTrace[i].getMethodName().contains("GenericSource");
                    requestingClass = className.substring(0, className.length() - "DatabaseObjectAbstract".length());
                    break;
                }
            }
            String msg = "";
            if (requestingClass != null)
            {
                msg = "Could not find connection for class " + requestingClass;
                msg += " Make sure the class is added to the test xml file. Double check that it's added to the correct connection mananger. ";
                msg += " If the object has a source attribute, also ensure that the operation correctly specifies a value for it. ";
            }
            msg += "No connection manager found." + (sourceAttribute == null ? "" : " for database: " + sourceAttribute);
            throw new NullPointerException(msg);
        }
        return connectionManager;
    }

    public Connection getConnection()
    {
        return this.getDefaultConnectionManager().getConnection();
    }

    public Connection getConnection(int sourceAttribute)
    {
        return this.getConnection(Integer.valueOf(sourceAttribute));
    }

    public Connection getConnection(Object sourceAttribute)
    {
        Connection connection = this.getUnderlyingConnectionManager(sourceAttribute).getConnection();
        this.setIsolationLevel(connection);
        return connection;
    }

    public void setDatabaseTimeZone(TimeZone timeZone)
    {
        this.timeZone = timeZone;
    }

    public TimeZone getDatabaseTimeZone(Object sourceAttribute)
    {
        return this.timeZone;
    }

    public TimeZone getDatabaseTimeZone(int sourceAttribute)
    {
        return this.timeZone;
    }

    public TimeZone getDatabaseTimeZone()
    {
        return this.timeZone;
    }

    public String getSchema(String name)
    {
        return name;
    }

    public BulkLoader createBulkLoader() throws BulkLoaderException
    {
        DatabaseType databaseType = this.getDatabaseType();
        return databaseType.createBulkLoader(this.getLoginUser(databaseType), "", "", -1);
    }

    public BulkLoader createBulkLoader(int sourceAttribute) throws BulkLoaderException
    {
        DatabaseType databaseType = this.getDatabaseType(sourceAttribute);
        return databaseType.createBulkLoader(this.getLoginUser(databaseType), "", "", -1);
    }

    public BulkLoader createBulkLoader(Object sourceAttribute) throws BulkLoaderException
    {
        DatabaseType databaseType = this.getDatabaseType(sourceAttribute);
        return databaseType.createBulkLoader(this.getLoginUser(databaseType), "", "", -1);
    }

    private String getLoginUser(DatabaseType databaseType)
    {
        return (databaseType instanceof DerbyDatabaseType) ? "" : "sa";
    }

    private static class DefaultConnectionManagerFactory implements ConnectionManagerFactory
    {
        public ConnectionManagerForTests createConnectionManager(String dbName)
        {
            return new ConnectionManagerForTests(dbName);
        }
    }

    @Override
    public void fullyShutdown()
    {
        super.fullyShutdown();
        for (Iterator<XAConnectionManager> it = connectionManagerMap.values().iterator(); it.hasNext();)
        {
            XAConnectionManager connectionManager = it.next();
            if (connectionManager.getDatabaseType() instanceof H2DatabaseType)
            {
                try
                {
                    connectionManager.getConnection().createStatement().execute("SHUTDOWN");
                    connectionManager.shutdown();
                }
                catch (Throwable t)
                {
                    logger.error("Could not shutdown database", t);
                }
                it.remove();
            }
        }
    }
}