
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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.PostgresDatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresTestConnectionManager extends VendorTestConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresTestConnectionManager.class);
    private final static PostgresTestConnectionManager instance = new PostgresTestConnectionManager();

    public static PostgresTestConnectionManager getInstance()
    {
        return instance;
    }

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
        this.setDatabaseType(PostgresDatabaseType.getInstance());
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        return null;
    }

    public String getDatabaseIdentifier()
    {
        return getCredential("postgres_hostName")+getCredential("postgres_databaseName");
    }
}