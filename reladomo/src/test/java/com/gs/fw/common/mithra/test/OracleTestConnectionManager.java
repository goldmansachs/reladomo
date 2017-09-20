
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
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import org.mortbay.util.SingletonList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collection;
import java.util.TimeZone;

public class OracleTestConnectionManager extends VendorTestConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(OracleTestConnectionManager.class);
    private final static OracleTestConnectionManager instance = new OracleTestConnectionManager();

    public static OracleTestConnectionManager getInstance()
    {
        return instance;
    }

    protected OracleTestConnectionManager()
    {
        readCredentials();
        connectionManager = new XAConnectionManager();
        connectionManager.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        connectionManager.setJdbcConnectionString("jdbc:oracle:thin:@"+getCredential("oracle_hostName")+":"+getCredential("oracle_port")+":"+getCredential("oracle_ssid"));
        connectionManager.setJdbcUser(getCredential("oracle_user"));
        connectionManager.setJdbcPassword(getCredential("oracle_password"));
        connectionManager.setDefaultSchemaName(getCredential("oracle_schemaName"));
        connectionManager.setDatabaseType(OracleDatabaseType.getInstance());
        connectionManager.setPoolName(getCredential("oracle_ssid")+" connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.initialisePool();

        OracleDatabaseType.getInstance().setTempSchema(getCredential("oracle_schemaName"));
        this.setDatabaseType(OracleDatabaseType.getInstance());
    }

    public String getSchemaName()
    {
        return getCredential("oracle_schemaName");
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        return null;
    }

    public String getDatabaseIdentifier()
    {
        return getCredential("oracle_hostName")+getCredential("oracle_schemaName");
    }
}