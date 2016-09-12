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

package com.gs.fw.common.mithra.connectionmanager;

import com.gs.fw.common.mithra.databasetype.*;
import com.gs.fw.common.mithra.util.WrappedConnection;

import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Enumeration;


public class XAConnectionManager extends AbstractConnectionManager
{
    private LdapDataSourceProvider ldapDataSourceProvider;
    private DatabaseType databaseType;

    private String hostName = null;
    private int port = -1;

    public void setLdapDataSourceProvider(LdapDataSourceProvider ldapDataSourceProvider)
    {
        this.ldapDataSourceProvider = ldapDataSourceProvider;
    }

    protected DataSource createPoolingDataSource(ObjectPoolWithThreadAffinity objectPool)
    {
        return new XAConnectionPoolingDataSource(objectPool, this.databaseType);
    }

    public DatabaseType getDatabaseType()
    {
        return this.databaseType;
    }

    protected ConnectionFactory createConnectionFactory(Properties loginProperties)
    {
        if (this.getJdbcConnectionString() != null)
        {
            this.setDatabaseTypeFromClassName(this.getDriver().getClass().getName());
            if (this.getDatabaseType() != null)
            {
                this.setHostName(this.getDatabaseType().getHostnameFromUrl(this.getJdbcConnectionString()));
                this.setPort(this.getDatabaseType().getPortFromUrl(this.getJdbcConnectionString()));
            }
            return new DriverConnectionFactoryWithSchema(this.getDriver(), this.getJdbcConnectionString(),
                    loginProperties, this.getDefaultSchemaName(), this.getDatabaseType());
        }
        else
        {
            DataSource ds = createDataSource(loginProperties);
            return new DataSourceConnectionFactoryWithSchema(ds, this.getJdbcUser(),
                    this.getJdbcPassword(), this.getDefaultSchemaName(), this.getDatabaseType(), this.getLdapName());
        }
    }

    protected DataSource recreateDataSource()
    {
        return createDataSource(this.createLoginProperties());
    }

    private DataSource createDataSource(Properties loginProperties)
    {
        DataSource ds;
        String ldapName = this.getLdapName();
        try
        {
            ds = createLdapDataSource(loginProperties, ldapName);
            this.invokeDataSourcePropertySetters(ds, loginProperties);
            this.setDatabaseTypeFromClassName(ds.getClass().getName());
            if (this.getDatabaseType() != null)
            {
                this.setHostName(this.getDatabaseType().getHostnameFromDataSource(ds));
                this.setPort(this.getDatabaseType().getPortFromDataSource(ds));
            }
        }
        catch (NamingException e)
        {
            throw new RuntimeException("unable to lookup datasource for server: " + ldapName, e);
        }
        return ds;
    }

    private DataSource createLdapDataSource(Properties loginProperties, String ldapName) throws NamingException
    {
        if (this.ldapDataSourceProvider == null)
        {
            try
            {
                this.ldapDataSourceProvider = (LdapDataSourceProvider) Class.forName("com.gs.fw.common.mithra.connectionmanager.JndiJdbcLdapDataSourceProvider").newInstance();
            }
            catch (Exception e)
            {
                throw new RuntimeException("could not create LDAP data source ", e);
            }
        }
        return this.ldapDataSourceProvider.createLdapDataSource(loginProperties, ldapName);
    }

    /**
     * set the mithra database type for this connection manager. If the value is not set, it will be automtically
     * set after initializePool is called.
     * @param databaseType
     */
    public void setDatabaseType(DatabaseType databaseType)
    {
        this.databaseType = databaseType;
    }

    private void setDatabaseTypeFromClassName(String name)
    {
        if (this.databaseType == null)
        {
            if (name.indexOf(".sybase.") >= 0)
            {
                this.databaseType = SybaseDatabaseType.getInstance();
            }
            else if (name.indexOf(".derby.") >=0)
            {
                this.databaseType = DerbyDatabaseType.getInstance();
            }
            else if (name.indexOf(".ibm.") >= 0)
            {
                this.databaseType = Udb82DatabaseType.getInstance();
            }
            else if (name.indexOf("oracle.") >= 0)
            {
                this.databaseType = OracleDatabaseType.getInstance();
            }
            else if (name.indexOf(".postgresql.") >= 0)
            {
                this.databaseType = PostgresDatabaseType.getInstance();
            }
            else if (name.indexOf(".microsoft.sqlserver.") >= 0)
            {
                this.databaseType = MsSqlDatabaseType.getInstance();
            }
            else if (name.indexOf(".h2.") >= 0)
            {
                this.databaseType = H2DatabaseType.getInstance();
            }
            else if (name.indexOf(".mariadb.") >= 0 || name.indexOf(".mysql.") >= 0)
            {
                this.databaseType = MariaDatabaseType.getInstance();
            }
        }
    }

    private class DataSourceConnectionFactoryWithSchema implements ConnectionFactory
    {
        private String schemaName;
        private DatabaseType databaseType;
        private String ldapName;
        private long lastDataSourceLookup;
        protected String user = null;
        protected String password = null;
        protected DataSource dataSource = null;

        public DataSourceConnectionFactoryWithSchema(DataSource dataSource, String user, String password, String schemaName,
                DatabaseType databaseType, String ldapName)
        {
            this.dataSource = dataSource;
            this.user = user;
            this.password = password;
            this.schemaName = schemaName;
            this.databaseType = databaseType;
            this.ldapName = ldapName;
            this.lastDataSourceLookup = System.currentTimeMillis();
        }

        private Connection createDataSourceConnection() throws SQLException
        {
            if (null == user && null == password)
            {
                return dataSource.getConnection();
            }
            else
            {
                return dataSource.getConnection(user, password);
            }
        }

        public Connection createConnection() throws SQLException
        {
            Connection connection = null;
            try
            {
                connection = new CatalogCachingConnection(createDataSourceConnection());
            }
            catch (SQLException e)
            {
                String msg = "Error creating connection to database: " + this.ldapName +
                        " with schema " + this.schemaName + " for userId: " + this.user +"; "+e.getMessage();
                if (lastDataSourceLookup < System.currentTimeMillis() - 5*60*1000) // retry ldap lookup every 5 minutes
                {
                    msg += ". Will retry LDAP lookup.";
                    try
                    {
                        this.lastDataSourceLookup = System.currentTimeMillis();
                        this.dataSource = recreateDataSource();
                        msg += " LDAP lookup succeeded.";
                        getLogger().warn(msg, e);
                        return createConnection();
                    }
                    catch (Exception e1)
                    {
                        msg += " LDAP lookup failed with "+e1.getClass().getName()+": "+e1.getMessage();
                    }
                }
                SQLException moreInfoException = new SQLException(msg);
                moreInfoException.initCause(e);
                throw moreInfoException;
            }
            try
            {
                configureConnection(connection);
            }
            catch (SQLException e)
            {
                closeConnection(connection);
                String msg = "Error configuring connection to database: " + this.ldapName +
                        " with schema " + this.schemaName + " for userId: " + this.user+"; "+e.getMessage();
                SQLException moreInfoException = new SQLException(msg);
                moreInfoException.initCause(e);
                throw moreInfoException;
            }
            return connection;
        }

        private void configureConnection(Connection connection) throws SQLException
        {
            if (databaseType != null)
            {
                databaseType.configureConnection(connection);
            }
            if (this.schemaName != null)
            {
                if (databaseType != null)
                {
                    databaseType.setSchemaOnConnection(connection, this.schemaName);
                }
                else
                {
                    connection.setCatalog(this.schemaName);
                }
            }
        }
    }

    protected void closeConnection(Connection connection)
    {
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                if ("Already closed.".equals(e.getMessage()))
                {
                    getLogger().debug("Connection already closed", e);
                }
                else
                {
                    getLogger().error("Could not close connection", e);
                }
            }
        }
    }

    public void setHostName(String hostName)
    {
        this.hostName = hostName;
    }

    public String getHostName()
    {
        return hostName;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public int getPort()
    {
        return port;
    }

    protected static class DriverConnectionFactoryWithSchema implements ConnectionFactory
    {
        private String schemaName;
        private DatabaseType databaseType;
        protected Driver driver = null;
        protected String connectString = null;
        protected Properties properties = null;

        public DriverConnectionFactoryWithSchema(Driver driver, String connectionString, Properties properties, String schemaName, DatabaseType databaseType)
        {
            this.driver = driver;
            this.connectString = connectionString;
            this.properties = properties;
            this.schemaName = schemaName;
            this.databaseType = databaseType;
        }

        private Connection createDriverConnection() throws SQLException
        {
            return driver.connect(connectString, properties);
        }

        public Connection createConnection() throws SQLException
        {
            Connection connection = null;
            try
            {
                connection = new CatalogCachingConnection(createDriverConnection());
                if (databaseType != null)
                {
                    databaseType.configureConnection(connection);
                }
                if (this.schemaName != null)
                {
                    if (databaseType != null)
                    {
                        databaseType.setSchemaOnConnection(connection, this.schemaName);
                    }
                    else
                    {
                        connection.setCatalog(this.schemaName);
                    }
                }

            }
            catch (SQLException e)
            {
                SQLException moreInfoException = new SQLException("error creating connnection to database: " + this.schemaName+
                " for connection string: "+this.connectString);
                moreInfoException.initCause(e);
                throw moreInfoException;
            }
            return connection;
        }
    }

    protected static class CatalogCachingConnection extends WrappedConnection implements ClosableConnection
    {
        private static final String NOT_A_CATALOG = "! not a real catalog . . .";
        private String currentCatalog = NOT_A_CATALOG;
        private static final int NOT_AN_ISOLATION_LEVEL = -12234455;
        private int currentIsolationLevel = NOT_AN_ISOLATION_LEVEL;
        private long lastClosedCheck = 0;
        private int autoCommitMode = -1;
        private boolean isDead = false;

        public CatalogCachingConnection(Connection c)
        {
            super(c);
        }

        public void setDead()
        {
            isDead = true;
            try
            {
                this.getUnderlyingConnection().rollback();
            }
            catch (Exception e)
            {
                //ignore -- we're forcing a rollback even if we think the connection is dead
            }
            try
            {
                this.getUnderlyingConnection().close();
            }
            catch (Exception e)
            {
                //ignore -- we're forcing a close even if we think the connection is dead
            }
        }

        public boolean getAutoCommit() throws SQLException
        {
            if (this.autoCommitMode < 0)
            {
                this.autoCommitMode = super.getAutoCommit() ? 1 : 0;
            }
            return this.autoCommitMode == 1;
        }

        public void setAutoCommit(boolean autoCommit) throws SQLException
        {
            if (this.autoCommitMode < 0 || autoCommit != this.getAutoCommit())
            {
                try
                {
                    super.setAutoCommit(autoCommit);
                }
                catch (SQLException e)
                {
                    this.autoCommitMode = -1;
                    throw e;
                }
                this.autoCommitMode = autoCommit ? 1 : 0;
            }
        }

        public int getTransactionIsolation() throws SQLException
        {
            if (this.currentIsolationLevel == NOT_AN_ISOLATION_LEVEL)
            {
                this.currentIsolationLevel = super.getTransactionIsolation();
            }
            return this.currentIsolationLevel;
        }

        public void setTransactionIsolation(int level) throws SQLException
        {
            if (this.currentIsolationLevel != level)
            {
                try
                {
                    super.setTransactionIsolation(level);
                }
                catch (SQLException e)
                {
                    this.currentIsolationLevel = NOT_AN_ISOLATION_LEVEL;
                    throw e;
                }
                this.currentIsolationLevel = level;
            }
        }

        public boolean isClosed() throws SQLException
        {
            if (isDead) return true;
            long now = System.currentTimeMillis();
            if (this.lastClosedCheck < now - 1000)
            {
                this.lastClosedCheck = now;
                return super.isClosed();
            }
            return false;
        }

        public void close() throws SQLException
        {
            if (isDead) return;
            this.getUnderlyingConnection().close();
        }

        public String getCatalog() throws SQLException
        {
            if (this.currentCatalog == NOT_A_CATALOG)
            {
                this.currentCatalog = this.getUnderlyingConnection().getCatalog();
            }
            return this.currentCatalog;
        }

        public void setCatalog(String catalog) throws SQLException
        {
            if (this.currentCatalog != null && this.currentCatalog.equals(catalog))
            {
                return;
            }
            this.currentCatalog = NOT_A_CATALOG;
            this.getUnderlyingConnection().setCatalog(catalog);
            this.currentCatalog = catalog;
        }
    }
}
