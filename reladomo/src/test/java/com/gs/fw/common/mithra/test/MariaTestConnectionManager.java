
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

import java.io.IOException;
import java.sql.Connection;
import java.util.Collection;
import java.util.Properties;
import java.util.TimeZone;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.MariaDatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MariaTestConnectionManager extends VendorTestConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(MariaTestConnectionManager.class);
    private final static MariaTestConnectionManager instance = new MariaTestConnectionManager();

    public static MariaTestConnectionManager getInstance()
    {
        return instance;
    }

    private XAConnectionManager connectionManager;
    private TimeZone databaseTimeZone;

    protected MariaTestConnectionManager()
    {
        readCredentials();
        connectionManager = new XAConnectionManager();
        connectionManager.setDriverClassName("org.mariadb.jdbc.Driver");
        //jdbc:mariadb://<host>:<port>/<database>?<key1>=<value1>&<key2>=<value2>...
        connectionManager.setJdbcConnectionString("jdbc:mariadb://"+getCredential("maria_hostname")+":"+getCredential("maria_port")+"/"+getCredential("maria_databaseName"));
        connectionManager.setJdbcUser(getCredential("maria_user"));
        connectionManager.setJdbcPassword(getCredential("maria_password"));
        connectionManager.setDefaultSchemaName(getCredential("maria_schemaName"));
        //todo
        connectionManager.setPoolName(getCredential("maria_hostName") + ":"+getCredential("maria_databaseName")+":"+getCredential("maria_schemaName")+" connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.initialisePool();

        MariaDatabaseType.getInstance().setTempSchema(getCredential("maria_schemaName"));
        this.setDatabaseType(MariaDatabaseType.getInstance());
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        return null;
    }

    public String getDatabaseIdentifier()
    {
        return getCredential("maria_hostName")+getCredential("maria_databaseName");
    }
}