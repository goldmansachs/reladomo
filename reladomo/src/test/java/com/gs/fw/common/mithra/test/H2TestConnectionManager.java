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

import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;

public class H2TestConnectionManager
        extends VendorTestConnectionManager
{
    private static final H2TestConnectionManager INSTANCE = new H2TestConnectionManager();

    public static H2TestConnectionManager getInstance()
    {
        return INSTANCE;
    }

    protected H2TestConnectionManager()
    {
        connectionManager = new XAConnectionManager();
        connectionManager.setDriverClassName("org.h2.Driver");
        connectionManager.setJdbcConnectionString("jdbc:h2:mem:testdb");
        connectionManager.setJdbcUser("sa");
        connectionManager.setJdbcPassword("");
        connectionManager.setPoolName("h2-schema connection pool");
        connectionManager.setInitialSize(1);
        connectionManager.setPoolSize(100);
        connectionManager.setUseStatementPooling(true);
        connectionManager.initialisePool();

        this.setDatabaseType(H2DatabaseType.getInstance());
    }

    public BulkLoader createBulkLoader()
    {
        return null;
    }

    public String getDatabaseIdentifier()
    {
        return "h2-schema";
    }
}
