
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
import com.gs.fw.common.mithra.databasetype.MsSqlDatabaseType;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.util.ListFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

public class MsSqlTestConnectionManager extends AbstractMithraTestConnectionManager
implements SourcelessConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(MsSqlTestConnectionManager.class);
    private final static MsSqlTestConnectionManager instance = new MsSqlTestConnectionManager();

    public static MsSqlTestConnectionManager getInstance()
    {
        return instance;
    }

    private XAConnectionManager connectionManager;
    private TimeZone databaseTimeZone;

    protected MsSqlTestConnectionManager()
    {
        readCredentials();
        connectionManager = new XAConnectionManager();
        connectionManager.setLdapName(getCredential("mssql_ldapName"));
        connectionManager.setJdbcUser(getCredential("mssql_user"));
        connectionManager.setJdbcPassword(getCredential("mssql_password"));
        connectionManager.setDefaultSchemaName(getCredential("mssql_schemaName"));
        connectionManager.setPoolName(getCredential("mssql_ldapName") + " connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.setProperty("com.gs.fw.aig.jdbc.global.DataSourceImpl","com.microsoft.sqlserver.jdbc.SQLServerDataSource");
        connectionManager.initialisePool();
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        return null;
    }

    public Connection getConnection()
    {
        return this.connectionManager.getConnection();
    }

    public DatabaseType getDatabaseType()
    {
        return MsSqlDatabaseType.getInstance();
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
