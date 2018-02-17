
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
import com.gs.fw.common.mithra.databasetype.SybaseIqDatabaseType;
import com.gs.fw.common.mithra.databasetype.SybaseIqNativeDatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SybaseIqNativeTestConnectionManager extends VendorTestConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(SybaseIqNativeTestConnectionManager.class);
    private final static SybaseIqNativeTestConnectionManager instance = new SybaseIqNativeTestConnectionManager();

    public static SybaseIqNativeTestConnectionManager getInstance()
    {
        return instance;
    }

    protected SybaseIqNativeTestConnectionManager()
    {
        readCredentials();
        this.databaseType = SybaseIqNativeDatabaseType.getInstanceWithoutSharedTempTables();
        SybaseIqNativeDatabaseType.loadNativeDrivers();

        String url = "jdbc:sqlanywhere:"+params("UID", getCredential("sybaseiq_user")) + ";" + params("PWD", getCredential("sybaseiq_password")) + ";"+
                params("HOST", "nyfipls3017a.ny.fw.gs.com:2740");

        connectionManager = new XAConnectionManager();
        connectionManager.setDriverClassName("sap.jdbc4.sqlanywhere.IDriver");
        connectionManager.setDatabaseType(SybaseIqNativeDatabaseType.getInstanceWithoutSharedTempTables());
        connectionManager.setJdbcConnectionString(url);
        connectionManager.setJdbcUser("");
        connectionManager.setJdbcPassword("");
        connectionManager.setDefaultSchemaName(getCredential("sybaseiq_schemaName"));
        connectionManager.setPoolName(getCredential("sybaseiq_ldapName")+" connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.initialisePool();

    }

    private String params(String key, String value)
    {
        return key+"="+value;
    }

    public BulkLoader createBulkLoader()
    throws BulkLoaderException
    {
        String appLoadDir = System.getProperty("appLoadDir");
        if (appLoadDir == null)
        {
            appLoadDir = System.getProperty("java.io.tmpdir");
        }
        return this.getDatabaseType().createBulkLoader(null, appLoadDir);
    }

    public SybaseIqDatabaseType getDatabaseType()
    {
        return (SybaseIqDatabaseType) this.databaseType;
    }

}
