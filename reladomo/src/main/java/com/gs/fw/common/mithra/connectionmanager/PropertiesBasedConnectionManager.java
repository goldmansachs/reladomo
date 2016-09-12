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

package com.gs.fw.common.mithra.connectionmanager;


import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import com.gs.fw.common.mithra.bulkloader.*;
import com.gs.fw.common.mithra.databasetype.*;
import com.gs.fw.common.mithra.overlap.*;
import org.slf4j.*;

public class PropertiesBasedConnectionManager implements SourcelessConnectionManager, ObjectSourceConnectionManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesBasedConnectionManager.class.getName());

    private static PropertiesBasedConnectionManager instance;

    private final XAConnectionManager xaConnectionManager;

    public PropertiesBasedConnectionManager(Properties properties)
    {
        this.xaConnectionManager = this.createConnectionManager(properties);
    }

    public static synchronized PropertiesBasedConnectionManager getInstance()
    {
        if (instance == null)
        {
            instance = createConnectionManager(System.getProperty("connectionManagerPropertiesFile"));
        }
        return instance;
    }

    private static PropertiesBasedConnectionManager createConnectionManager(String propertiesFileName)
    {
        InputStream is = PropertiesBasedConnectionManager.class.getClassLoader().getResourceAsStream(propertiesFileName);
        try
        {
            Properties properties = new Properties();
            properties.load(is);
            return new PropertiesBasedConnectionManager(properties);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error trying to open properties file " + propertiesFileName, e);
        }
        finally
        {
            try
            {
                is.close();
            }
            catch (IOException e)
            {
                LOGGER.error("Error closing properties file input stream", e);
            }
        }
    }

    public XAConnectionManager getXaConnectionManager()
    {
        return this.xaConnectionManager;
    }

    public void shutdown()
    {
        this.xaConnectionManager.shutdown();
    }

    @Override
    public BulkLoader createBulkLoader(Object sourceAttribute) throws BulkLoaderException
    {
        return this.createBulkLoader();
    }

    @Override
    public Connection getConnection(Object sourceAttribute)
    {
        return this.getConnection();
    }

    @Override
    public DatabaseType getDatabaseType(Object sourceAttribute)
    {
        return this.getDatabaseType();
    }

    @Override
    public TimeZone getDatabaseTimeZone(Object sourceAttribute)
    {
        return this.getDatabaseTimeZone();
    }

    @Override
    public String getDatabaseIdentifier(Object sourceAttribute)
    {
        return this.getDatabaseIdentifier();
    }

    @Override
    public BulkLoader createBulkLoader() throws BulkLoaderException
    {
        throw new BulkLoaderException("Not implemented yet.");
    }

    @Override
    public Connection getConnection()
    {
        return this.xaConnectionManager.getConnection();
    }

    @Override
    public DatabaseType getDatabaseType()
    {
        return this.xaConnectionManager.getDatabaseType();
    }

    @Override
    public TimeZone getDatabaseTimeZone()
    {
        switch (Character.toUpperCase(this.xaConnectionManager.getLdapName().charAt(0)))
        {
            case 'L':
                return TimeZone.getTimeZone("Europe/London");
            case 'T':
                return TimeZone.getTimeZone("Asia/Tokyo");
            default:
                return TimeZone.getTimeZone("America/New_York");
        }
    }

    @Override
    public String getDatabaseIdentifier()
    {
        return this.xaConnectionManager.getLdapName() + ':' + this.xaConnectionManager.getDefaultSchemaName();
    }

    private XAConnectionManager createConnectionManager(Properties properties)
    {
        XAConnectionManager connectionManager = new XAConnectionManager();
        initFromProperties(connectionManager, properties);
        connectionManager.setPoolSize(10);
        connectionManager.initialisePool();
        return connectionManager;
    }

    private void initFromProperties(XAConnectionManager connectionManager, Properties properties)
    {
        for (Object key : properties.keySet())
        {
            String keyString = key.toString();
            String methodName = "set" + Character.toUpperCase(keyString.charAt(0)) + keyString.substring(1);
            try
            {
                Method method = connectionManager.getClass().getMethod(methodName, String.class);
                method.invoke(connectionManager, properties.getProperty(keyString));
            }
            catch (Exception e)
            {
                // Ignore
            }
        }
    }
}
