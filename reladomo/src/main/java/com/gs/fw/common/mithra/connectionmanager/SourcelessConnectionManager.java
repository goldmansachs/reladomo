
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

import java.sql.Connection;
import java.util.TimeZone;

import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.databasetype.DatabaseType;

/**
 * This class represents a connection manager for objects that have no source attribute. See the Mithra tutorial
 * to see a possible implementation, which uses Mithra's XaConnectionManager.
 */
public interface SourcelessConnectionManager
{

    /**
     * This method should return the Sybase bulk loader if the database is Sybase. null, otherwise.
     * @return An implementation of BulkLoader
     * @throws BulkLoaderException
     */
    BulkLoader createBulkLoader() throws BulkLoaderException;

    /**
     * returns a transactionally tied connection. Usually, this is just delegated to the Mithra utility class (XaConnectionManager).
     * @return a valid, transactionally coordinated connection.
     */
    Connection getConnection();

    /**
     * This is usually SybaseDatabaseType.getInstance() or Udb82DatabaseType.getInstance(). For test cases, this can
     * be H2DatabaseType.getInstance() or DerbyDatabaseType.getInstance().
     * @return an implemenation of the DatabaseType interface
     */
    DatabaseType getDatabaseType();

    /**
     * returns the timezone the database server is located in.
     * @return timezone
     */
    TimeZone getDatabaseTimeZone();

    /**
     * The database identifier should be a unique string that identifies this database. This information is used
     * for notification purposes. This can be as simple as the database url plus the schema/catalog. See the Mithra
     * tutorial for an implementation that uses the Mithra utility class (XaConnectionManager). 
     * @return unique database identifier
     */
    String getDatabaseIdentifier();
}
