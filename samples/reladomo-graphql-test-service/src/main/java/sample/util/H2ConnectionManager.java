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

package sample.util;

import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;
import org.h2.tools.RunScript;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.TimeZone;

public class H2ConnectionManager implements SourcelessConnectionManager
{
    private static H2ConnectionManager instance;
    private static final String MAX_POOL_SIZE_KEY = "maxPoolSize";
    private final static int DEFAULT_MAX_WAIT = 500;
    private static final int DEFAULT_POOL_SIZE = 10;
    private static final TimeZone NEW_YORK_TIMEZONE = TimeZone.getTimeZone("America/New_York");
    private final String driverClassname = "org.h2.Driver";
    private final String connectionString = "jdbc:h2:mem:";
    private final String schemaName = "testdb";
    private final String userName = "sa";

    private XAConnectionManager xaConnectionManager;

    public static synchronized H2ConnectionManager getInstance()
    {
        if (instance == null)
        {
            instance = new H2ConnectionManager();
        }
        return instance;
    }

    private H2ConnectionManager()
    {
        this.createConnectionManager();
    }

    private XAConnectionManager createConnectionManager()
    {
        xaConnectionManager = new XAConnectionManager();
        xaConnectionManager.setDriverClassName(driverClassname);
        xaConnectionManager.setMaxWait(DEFAULT_MAX_WAIT);
        xaConnectionManager.setJdbcConnectionString(connectionString + schemaName);
        xaConnectionManager.setJdbcUser(userName);
        xaConnectionManager.setJdbcPassword("");
        xaConnectionManager.setPoolName("myproj connection pool");
        xaConnectionManager.setInitialSize(1);
        xaConnectionManager.setPoolSize(DEFAULT_POOL_SIZE);
        xaConnectionManager.initialisePool();
        return xaConnectionManager;
    }

    public Connection getConnection()
    {
        return xaConnectionManager.getConnection();
    }

    public DatabaseType getDatabaseType()
    {
        return H2DatabaseType.getInstance();
    }

    public TimeZone getDatabaseTimeZone()
    {
        return NEW_YORK_TIMEZONE;
    }

    public BulkLoader createBulkLoader() throws BulkLoaderException
    {
        throw new RuntimeException("BulkLoader is not supported");
    }

    public String getDatabaseIdentifier()
    {
        return schemaName;
    }

    // Load ddl/idx files under generated-db/sql in-memory to make the sample app self-contained.
    // This is not typically done in production app.
    public void prepareTables() throws Exception
    {
        Path ddlPath = Paths.get(ClassLoader.getSystemResource("sql").toURI());

        try (Connection conn = xaConnectionManager.getConnection();)
        {
            Files.walk(ddlPath, 1)
                    .filter(path -> !Files.isDirectory(path)
                            && (path.toString().endsWith("ddl")
                            || path.toString().endsWith("idx")))
                    .forEach(path -> {
                try
                {
                    RunScript.execute(conn, Files.newBufferedReader(path));
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Exception at table initialization", e);
                }
            });
        }
    }
}
