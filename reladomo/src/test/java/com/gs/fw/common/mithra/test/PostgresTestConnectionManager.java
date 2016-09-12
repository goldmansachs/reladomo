
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
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.OracleDatabaseType;
import com.gs.fw.common.mithra.databasetype.PostgresDatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collection;
import java.util.TimeZone;

public class PostgresTestConnectionManager extends AbstractMithraTestConnectionManager
implements SourcelessConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresTestConnectionManager.class);
    private final static PostgresTestConnectionManager instance = new PostgresTestConnectionManager();

    public static PostgresTestConnectionManager getInstance()
    {
        return instance;
    }

    private XAConnectionManager connectionManager;
    private TimeZone databaseTimeZone;

    protected PostgresTestConnectionManager()
    {
        readCredentials();
        connectionManager = new XAConnectionManager();
        connectionManager.setDriverClassName("org.postgresql.Driver");
        //jdbc:postgresql://host:port/database
        connectionManager.setJdbcConnectionString("jdbc:postgresql://"+getCredential("postgres_hostName")+":"+getCredential("postgres_port")+"/"+getCredential("postgres_databaseName"));
        connectionManager.setJdbcUser(getCredential("postgres_user"));
        connectionManager.setJdbcPassword(getCredential("postgres_password"));
        connectionManager.setDefaultSchemaName(getCredential("postgres_schemaName"));
        connectionManager.setPoolName(getCredential("postgres_hostName") + ":"+getCredential("postgres_databaseName")+":"+getCredential("postgres_schemaName")+" connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.initialisePool();

        PostgresDatabaseType.getInstance().setTempSchema(getCredential("postgres_schemaName"));
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        return this.getDatabaseType().createBulkLoader(
                getCredential("postgres_user"),
                getCredential("postgres_password"),
                    this.connectionManager.getHostName(),
                    this.connectionManager.getPort());
    }

    public Connection getConnection()
    {
        return this.connectionManager.getConnection();
    }

    public DatabaseType getDatabaseType()
    {
        return PostgresDatabaseType.getInstance();
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
        return getCredential("postgres_hostName")+getCredential("postgres_databaseName");
    }

    @Override
    protected Collection<XAConnectionManager> getAllConnectionManagers()
    {
        return FastList.newListWith(this.connectionManager);
    }
}