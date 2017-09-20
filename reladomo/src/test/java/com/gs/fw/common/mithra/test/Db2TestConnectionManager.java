
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
import com.gs.fw.common.mithra.databasetype.Udb82DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collection;
import java.util.TimeZone;

public class Db2TestConnectionManager extends VendorTestConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(Db2TestConnectionManager.class);
    private final static Db2TestConnectionManager instance = new Db2TestConnectionManager();

    public static Db2TestConnectionManager getInstance()
    {
        return instance;
    }

    protected Db2TestConnectionManager()
    {
        readCredentials();
        Udb82DatabaseType.getInstance().setTempSchemaName(getCredential("db2_schemaName"));
        Udb82DatabaseType.getInstance().setTableSpace(getCredential("db2_tablespace"));
        connectionManager = new XAConnectionManager();
        connectionManager.setDriverClassName("com.ibm.db2.jcc.DB2Driver");
        connectionManager.setJdbcConnectionString("jdbc:db2://"+getCredential("db2_hostName")+":"+getCredential("db2_port")+"/"+getCredential("db2_databaseName"));
        connectionManager.setJdbcUser(getCredential("db2_user"));
        connectionManager.setJdbcPassword(getCredential("db2_password"));
        connectionManager.setDefaultSchemaName(getCredential("db2_schemaName"));
        connectionManager.setPoolName(getCredential("db2_hostName") + ":"+getCredential("db2_databaseName")+":"+getCredential("db2_schemaName")+" connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.initialisePool();

        this.setDatabaseType(Udb82DatabaseType.getInstance());
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        return null;
    }
    public String getDatabaseIdentifier()
    {
        return getCredential("db2_hostName")+getCredential("db2_databaseName");
    }
}
