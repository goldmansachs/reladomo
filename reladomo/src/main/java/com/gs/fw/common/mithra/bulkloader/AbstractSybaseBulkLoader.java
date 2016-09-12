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
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.databasetype.DatabaseInfo;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.util.TableColumnInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;


public abstract class AbstractSybaseBulkLoader implements BulkLoader
{

    protected final SybaseDatabaseType sybase;
    protected Logger logger;
    protected DatabaseInfo databaseInfo;
    protected TableColumnInfo tableMetadata;
    protected TimeZone dbTimeZone;
    protected Attribute[] attributes;

    public AbstractSybaseBulkLoader(SybaseDatabaseType sybaseDatabaseType)
    {
        this.sybase = sybaseDatabaseType;
    }

    public abstract void bindObject(MithraObject object) throws BulkLoaderException;

    public abstract void execute(Connection connection) throws BulkLoaderException, SQLException;

    protected DatabaseInfo createDatabaseInfo(Connection connection)
            throws BulkLoaderException
    {
        try
        {
            return this.sybase.getDatabaseInfo(connection);
        }
        catch (SQLException e)
        {
            throw new BulkLoaderException("Cannot lookup database metadata", e);
        }
    }

    protected TableColumnInfo createTableMetaData(String schema, String tableName, Connection connection)
            throws BulkLoaderException
    {
        try
        {
            return this.sybase.getTableColumnInfo(connection, schema, tableName);
        }
        catch (SQLException e)
        {
            throw new BulkLoaderException("Error whilst looking up table metadata", e);
        }
    }

    protected TableColumnInfo getTableMetadata()
    {
        return tableMetadata;
    }

    public void destroy()
    {
    }

    public void initialize(TimeZone dbTimeZone, String schema, String tableName, Attribute[] attributes,
            Logger logger, String tempTableName, String columnCreationStatement, Connection con) throws BulkLoaderException

    {
        this.logger = logger;
        this.dbTimeZone = dbTimeZone;

        this.databaseInfo = createDatabaseInfo(con);
        this.tableMetadata = createTableMetaData(schema, tableName, con);
        if (this.tableMetadata == null)
        {
            throw new BulkLoaderException("Cannot find the table metadata for table '" + tableName + "' in schema '" + schema + "'.");
        }
        this.attributes = new Attribute[attributes.length];
        System.arraycopy(attributes, 0, this.attributes, 0, attributes.length);

        Arrays.sort(this.attributes, new AttributeByOrdinalPositionComparator());

    }

    public void bindObjectsAndExecute(List mithraObjects, Connection con) throws BulkLoaderException, SQLException
    {
        for (Iterator it = mithraObjects.iterator(); it.hasNext();)
        {
            bindObject((MithraObject) it.next());
        }
        execute(con);
    }

    public void dropTempTable(String tempTableName)
    {
        // can't drop tables here!
    }

    public boolean createsTempTable()
    {
        return false;
    }

    protected class AttributeByOrdinalPositionComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            Attribute left = (Attribute) o1;
            Attribute right = (Attribute) o2;
            return tableMetadata.getColumn(left.getColumnName()).getOrdinalPosition() -
                    tableMetadata.getColumn(right.getColumnName()).getOrdinalPosition();
        }
    }
}