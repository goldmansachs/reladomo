
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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.SybaseIqDatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collection;
import java.util.TimeZone;

public class SybaseIqTestConnectionManager extends AbstractMithraTestConnectionManager
implements SourcelessConnectionManager, ObjectSourceConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(SybaseIqTestConnectionManager.class);
    private final static SybaseIqTestConnectionManager instance = new SybaseIqTestConnectionManager();

    public static SybaseIqTestConnectionManager getInstance()
    {
        return instance;
    }

    private XAConnectionManager connectionManager;
    private TimeZone databaseTimeZone;
    private SybaseIqDatabaseType databaseType;

    protected SybaseIqTestConnectionManager()
    {
        readCredentials();
        connectionManager = new XAConnectionManager();
        connectionManager.setLdapName(getCredential("sybaseiq_ldapName"));
        connectionManager.setJdbcUser(getCredential("sybaseiq_user"));
        connectionManager.setJdbcPassword(getCredential("sybaseiq_password"));
        connectionManager.setDefaultSchemaName(getCredential("sybaseiq_schemaName"));
        connectionManager.setPoolName(getCredential("sybaseiq_ldapName")+" connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.setProperty("com.gs.fw.aig.jdbc.global.DataSourceImpl","com.sybase.jdbc4.jdbc.SybDataSource");
        connectionManager.setProperty("com.gs.fw.aig.jdbc.global.ConnectionPoolDataSourceImpl","com.sybase.jdbc4.jdbc.SybConnectionPoolDataSource");
        connectionManager.setProperty("com.sybase.jdbc4.jdbc.SybDataSource.REPEAT_READ", "false");
        connectionManager.initialisePool();

        this.databaseType = SybaseIqDatabaseType.getInstance();
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        String dbLoadDir = System.getProperty("dbLoadDir");
        String appLoadDir = System.getProperty("appLoadDir");
        if (dbLoadDir == null)
        {
            dbLoadDir = getCredential("sybaseiq_dbloaddir");
        }
        if (appLoadDir == null)
        {
            String os = System.getProperty("os.name");
            if (os.toLowerCase().contains("windows"))
            {
                appLoadDir = getCredential("sybaseiq_unix_apploaddir");
            }
            else
            {
                appLoadDir = getCredential("sybaseiq_windows_apploaddir");
            }
        }
        return this.getDatabaseType().createBulkLoader(
                dbLoadDir,
                    appLoadDir);
    }

    public Connection getConnection()
    {
        return this.connectionManager.getConnection();
    }

    public SybaseIqDatabaseType getDatabaseType()
    {
        return this.databaseType;
    }

    public void setDatabaseType(SybaseIqDatabaseType databaseType)
    {
        this.databaseType = databaseType;
    }

    public TimeZone getDatabaseTimeZone()
    {
        return this.databaseTimeZone;
    }

    public void setDatabaseTimeZone(TimeZone databaseTimeZone)
    {
        this.databaseTimeZone = databaseTimeZone;
    }

    public String getDatabaseIdentifier()
    {
        return null;
    }

    public BulkLoader createBulkLoader(Object sourceAttribute) throws BulkLoaderException
    {
        return createBulkLoader();
    }

    public Connection getConnection(Object sourceAttribute)
    {
        return getConnection();
    }

    public DatabaseType getDatabaseType(Object sourceAttribute)
    {
        return getDatabaseType();
    }

    public TimeZone getDatabaseTimeZone(Object sourceAttribute)
    {
        return getDatabaseTimeZone();
    }

    public String getDatabaseIdentifier(Object sourceAttribute)
    {
        return getDatabaseIdentifier();
    }

    @Override
    protected Collection<XAConnectionManager> getAllConnectionManagers()
    {
        return FastList.newListWith(this.connectionManager);
    }
}
