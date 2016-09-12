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
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.util.ListFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

public class SybaseTestConnectionManager extends AbstractMithraTestConnectionManager
implements SourcelessConnectionManager
{
    private final static SybaseTestConnectionManager instance = new SybaseTestConnectionManager();

    public static SybaseTestConnectionManager getInstance()
    {
        return instance;
    }

    private XAConnectionManager connectionManager;
    private TimeZone databaseTimeZone;
    private boolean isDataModelMismatchIsFatal = true;

    protected SybaseTestConnectionManager()
    {
        readCredentials();
        connectionManager = new XAConnectionManager();
        connectionManager.setJdbcConnectionString(getCredential("sybase_url"));
        connectionManager.setDriverClassName("com.sybase.jdbc4.jdbc.SybDriver");
        connectionManager.setJdbcUser(getCredential("sybase_username"));
        connectionManager.setJdbcPassword(getCredential("sybase_password"));
        connectionManager.setDefaultSchemaName(getCredential("sybase_schema"));
        connectionManager.setPoolName(getCredential("sybase_ldapname") +" connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.setDatabaseType(SybaseDatabaseType.getSybase15Instance());
        connectionManager.setProperty("com.gs.fw.aig.jdbc.global.DataSourceImpl","com.sybase.jdbc4.jdbc.SybDataSource");
        connectionManager.setProperty("com.gs.fw.aig.jdbc.global.ConnectionPoolDataSourceImpl","com.sybase.jdbc4.jdbc.SybConnectionPoolDataSource");
        connectionManager.setProperty("com.sybase.jdbc4.jdbc.SybDataSource.REPEAT_READ", "false");
        connectionManager.initialisePool();
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        return ((SybaseDatabaseType) getDatabaseType()).createBulkLoader(
                getCredential("sybase_username"),
                getCredential("sybase_password"),
                        this.connectionManager.getHostName(),
                        this.connectionManager.getPort(),
                        this.isDataModelMismatchIsFatal);
    }

    public void setDataModelMismatchIsFatal(boolean isFatal)
    {
        isDataModelMismatchIsFatal = isFatal;
    }

    public Connection getConnection()
    {
        return this.connectionManager.getConnection();
    }

    public DatabaseType getDatabaseType()
    {
        return SybaseDatabaseType.getSybase15Instance();
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

    public List getConnectionManagers()
    {
        return ListFactory.create(this.connectionManager);
    }

    @Override
    protected Collection<XAConnectionManager> getAllConnectionManagers()
    {
        return FastList.newListWith(this.connectionManager);
    }
}
