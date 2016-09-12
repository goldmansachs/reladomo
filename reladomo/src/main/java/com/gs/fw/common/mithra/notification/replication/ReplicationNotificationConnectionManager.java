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

package com.gs.fw.common.mithra.notification.replication;

import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.connectionmanager.IntSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.IntSourceSchemaManager;
import com.gs.fw.common.mithra.connectionmanager.SchemaManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;


public class ReplicationNotificationConnectionManager implements IntSourceConnectionManager, IntSourceSchemaManager
{
    private List connectionManagerList = new ArrayList();
    private List schemaHandlers = new ArrayList();
    private static ReplicationNotificationConnectionManager instance = new ReplicationNotificationConnectionManager();


    private ReplicationNotificationConnectionManager()
    {

    }

    public static ReplicationNotificationConnectionManager getInstance()
    {
        return instance;
    }

    public BulkLoader createBulkLoader(int sourceAttribute) throws BulkLoaderException
    {
       return null;
    }

    public void addConnectionManager(Object connectionManager)
    {
        this.connectionManagerList.add(connectionManager);
        this.schemaHandlers.add(DefaultSchemaHandler.getInstance());

    }

    public void addConnectionManager(Object connectionManager, String schema)
    {
        this.connectionManagerList.add(connectionManager);
        this.schemaHandlers.add(new PlainSchemaHandler(schema));

    }

    public void addConnectionManager(Object connectionManager, String schema, boolean getFromConnectionManager)
    {
        this.connectionManagerList.add(connectionManager);
        this.schemaHandlers.add(new DelegatingSchemaHandler(schema,((SchemaManager) connectionManager)));

    }

    public Connection getConnection(int sourceAttribute)
    {
        SourcelessConnectionManager connectionManager = (SourcelessConnectionManager) this.connectionManagerList.get(sourceAttribute);
        return connectionManager.getConnection();
    }

    public DatabaseType getDatabaseType(int sourceAttribute)
    {
        return ((SourcelessConnectionManager)this.connectionManagerList.get(sourceAttribute)).getDatabaseType();
    }

    public TimeZone getDatabaseTimeZone(int sourceAttribute)
    {
        return ((SourcelessConnectionManager)this.connectionManagerList.get(sourceAttribute)).getDatabaseTimeZone();
    }

    public String getDatabaseIdentifier(int sourceAttribute)
    {
        return ((SourcelessConnectionManager)this.connectionManagerList.get(sourceAttribute)).getDatabaseIdentifier();
    }

    public List getConnectionManagerList()
    {
        return this.connectionManagerList;
    }

    public String getSchema(String name, int sourceAttribute)
    {
        SchemaHandler handler = (SchemaHandler) this.schemaHandlers.get(sourceAttribute);
        return handler.getSchema();
    }

    private interface SchemaHandler
    {
        public String getSchema();
    }

    public static class DefaultSchemaHandler implements SchemaHandler
    {
        private static DefaultSchemaHandler instance = new DefaultSchemaHandler();
        private DefaultSchemaHandler()
        {
        }

        public static DefaultSchemaHandler  getInstance()
        {
             return instance;
        }


        public String getSchema()
        {
            return null;
        }
    }

    private static class PlainSchemaHandler implements SchemaHandler
    {
        String schema;

        public PlainSchemaHandler(String schema)
        {
            this.schema = schema;
        }

        public String getSchema()
        {
            return this.schema;
        }
    }

    private static class DelegatingSchemaHandler implements SchemaHandler
    {
        SchemaManager schemaManager;
        String schema;

        public DelegatingSchemaHandler(String schema, SchemaManager connectionManager)
        {
            this.schemaManager = connectionManager;
            this.schema = schema;
        }

        public String getSchema()
        {
            return this.schemaManager.getSchema(this.schema);
        }
    }
}
