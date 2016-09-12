
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

package com.gs.fw.common.mithra.bulkloader;

import org.slf4j.Logger;
import com.gs.fw.common.mithra.attribute.Attribute;

import java.sql.*;
import java.util.List;
import java.util.TimeZone;


public interface BulkLoader
{

    /**
     * <p>Initializes the bulk loader for bulk inserting objects managed by <code>MithraDatabaseObject</code>.</p>
     *
     * @param dbTimeZone The <code>TimeZone</code> of the database.
     * @param schema The schema of the table to bulk load to.
     * @param tableName The name of the table to bulk load to.
     * @param attributes The attributes
     * @param logger Logger to write out any messages to.
     * @param tempTableName temporary table to BCP data into
     * @param columnCreationStatement
     * @param con
     * @throws com.gs.fw.common.mithra.bulkloader.BulkLoaderException if there was a problem initializing the bulk loader.
     */

    void initialize(TimeZone dbTimeZone, String schema, String tableName, Attribute[] attributes,
            Logger logger, String tempTableName, String columnCreationStatement, Connection con) throws BulkLoaderException, SQLException;


    void bindObjectsAndExecute(List mithraObjects, Connection con) throws SQLException, BulkLoaderException;

    /**
     * Does any cleanup that is needed following either a failure or successful execute.
     */
    void destroy();

    void dropTempTable(String tempTableName);

    boolean createsTempTable();

}