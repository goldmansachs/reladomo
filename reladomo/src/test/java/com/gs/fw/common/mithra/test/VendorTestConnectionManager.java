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
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.SybaseIqDatabaseType;

import java.sql.Connection;
import java.util.Collection;
import java.util.TimeZone;

public abstract class VendorTestConnectionManager  extends AbstractMithraTestConnectionManager
implements SourcelessConnectionManager, ObjectSourceConnectionManager
{
    protected XAConnectionManager connectionManager;
    protected TimeZone databaseTimeZone;
    protected DatabaseType databaseType;

    public Connection getConnection()
    {
        return this.connectionManager.getConnection();
    }

    public DatabaseType getDatabaseType()
    {
        return this.databaseType;
    }

    public void setDatabaseType(DatabaseType databaseType)
    {
        this.databaseType = databaseType;
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

    public BulkLoader createBulkLoader(Object sourceAttribute) throws BulkLoaderException
    {
        return createBulkLoader();
    }

    public Connection getConnection(Object sourceAttribute)
    {
        return getConnection();
    }

    public DatabaseType getDatabaseType(Object sourceAttribute)
    {
        return getDatabaseType();
    }

    public TimeZone getDatabaseTimeZone(Object sourceAttribute)
    {
        return getDatabaseTimeZone();
    }

    public String getDatabaseIdentifier(Object sourceAttribute)
    {
        return getDatabaseIdentifier();
    }

    @Override
    protected Collection<XAConnectionManager> getAllConnectionManagers()
    {
        return FastList.newListWith(this.connectionManager);
    }

}
