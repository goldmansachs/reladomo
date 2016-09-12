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


import java.sql.*;
import java.util.*;

import com.gs.fw.common.mithra.bulkloader.*;
import com.gs.fw.common.mithra.databasetype.*;

public class FixedObjectSourceConnectionManager implements SourcelessConnectionManager
{
    private ObjectSourceConnectionManager oscm;
    private Object fixedSource;

    public FixedObjectSourceConnectionManager(ObjectSourceConnectionManager oscm, Object fixedSource)
    {
        this.oscm = oscm;
        this.fixedSource = fixedSource;
    }

    @Override
    public BulkLoader createBulkLoader() throws BulkLoaderException
    {
        return this.oscm.createBulkLoader(this.fixedSource);
    }

    @Override
    public Connection getConnection()
    {
        return this.oscm.getConnection(this.fixedSource);
    }

    @Override
    public DatabaseType getDatabaseType()
    {
        return this.oscm.getDatabaseType(this.fixedSource);
    }

    @Override
    public TimeZone getDatabaseTimeZone()
    {
        return this.oscm.getDatabaseTimeZone(this.fixedSource);
    }

    @Override
    public String getDatabaseIdentifier()
    {
        return this.oscm.getDatabaseIdentifier(this.fixedSource);
    }
}
