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

package com.gs.fw.common.mithra.database;

import org.slf4j.Logger;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.MapperStackImpl;
import com.gs.fw.common.mithra.finder.SqlQuery;

import java.io.IOException;
import java.io.ObjectInput;
import java.sql.*;
import java.util.List;
import java.util.TimeZone;



public interface MithraCodeGeneratedDatabaseObject
{
    // retrieve source attribute
    public Object getSourceAttributeValueForSelectedObjectGeneric(SqlQuery query, int queryNumber);

    public Object getSourceAttributeValueGeneric(SqlQuery query, MapperStackImpl mapperStack, int sourceNumber);

    public Object getSourceAttributeValueFromObjectGeneric(MithraDataObject object);

    // Connection Manager
    public DatabaseType getDatabaseTypeGenericSource(Object source);

    public TimeZone getDatabaseTimeZoneGenericSource(Object source);

    public String getDatabaseIdentifierGenericSource(Object source);

    public Connection getConnectionGenericSource(Object source);

    public BulkLoader createBulkLoaderGenericSource(Object source) throws BulkLoaderException;

    // Schema Manager
    public String getSchemaGenericSource(Object source);

    // Generic
    public Logger getLogger();

    public Logger getSqlLogger();

    public Logger getBatchSqlLogger();

    public Logger getTestSqlLogger();

    // Meta Data
    public boolean hasNullablePrimaryKeys();

    public boolean hasOptimisticLocking();

    public boolean hasSourceAttribute();

    public String getPrimaryKeyWhereSqlWithDefaultAlias();

    public String getPrimaryKeyWhereSqlWithNullableAttribute(MithraDataObject dataObj);

    public String getPrimaryKeyWhereSqlWithNullableAttributeWithDefaultAlias(MithraDataObject dataObj);

    public String getColumnListWithPk(String databaseAlias);

    public String getColumnListWithoutPk();

    public String getColumnListWithoutPkWithAlias();

    public int getTotalColumnsInResultSet();

    public int getTotalColumnsInInsert();

    public String getFullyQualifiedFinderClassName();

    public void analyzeAndWrapSqlExceptionGenericSource(String msg, SQLException e, Object source, Connection con) throws MithraDatabaseException;

    public MithraDataObject inflateDataGenericSource(ResultSet rs, Object source, DatabaseType dt) throws SQLException;

    public MithraDataObject inflatePkDataGenericSource(ResultSet rs, Object source, DatabaseType dt) throws SQLException;

    public void inflateNonPkDataGenericSource(MithraDataObject data, ResultSet rs, Object source, DatabaseType dt) throws SQLException;

    public String getTableNameGenericSource(Object source) throws MithraDatabaseException;

    public String getPrimaryKeyIndexColumns();

    public void analyzeChangeForReload(PrimaryKeyIndex fullUniqueIndex, MithraDataObject data, List newDataList, List updatedDataList);

    public void setPrimaryKeyAttributes(PreparedStatement stm, int pos, MithraDataObject dataObj,
                                        TimeZone databaseTimeZone, DatabaseType dt) throws SQLException;

    public int setPrimaryKeyAttributesWithoutOptimistic(PreparedStatement stm, int pos, MithraDataObject dataObj,
            TimeZone databaseTimeZone, DatabaseType dt) throws SQLException;

    public void setIdentity(Connection conn, Object source, MithraDataObject mithraDataObject) throws SQLException;
    
    public MithraDataObject deserializeFullData(ObjectInput in) throws IOException, ClassNotFoundException;

    public abstract void deserializeAsOfAttributes(ObjectInput in, Timestamp[] asof) throws IOException, ClassNotFoundException;
}
