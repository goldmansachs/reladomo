
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

package com.gs.fw.common.mithra.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.collections.impl.map.mutable.UnifiedMap;

import java.sql.*;
import java.util.*;


public class TableColumnInfo
{

    private static final Logger logger = LoggerFactory.getLogger(TableColumnInfo.class.getName());

    private final String catalog;
    private final String schema;
    private final String name;
    private final ColumnInfo[] columns;

    private final Map columnsByName;

    /**
     * Creates the <code>TableColumnInfo</code> using the standard JDBC table metadata mechanisms.
     *
     * @param connection A connection to lookup the <code>TableColumnInfo</code> data on.
     * @param schema The schema of the table, or <code>null</code> if none should be used to lookup the table.
     * @param tableName The name of the table.
     * @param fullyQualifiedTableName
     * @return The metadata about the table.
     * @throws SQLException if there was a problem looking up the metadata.
     */
    public static TableColumnInfo createTableMetadata(Connection connection, String schema, String tableName, String fullyQualifiedTableName)
    throws SQLException
    {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = null;

        // Get hold of the table catalog and schema separately from the column metadata in case we have a zero-column table/view
        String catalog = null;
        String dbSchema = null;
        String dbName = null;
        boolean tableExists = false;

        try
        {
            resultSet = metaData.getTables(null, schema, tableName, null);

            if (resultSet.next())
            {
                catalog = resultSet.getString("TABLE_CAT");
                dbSchema = resultSet.getString("TABLE_SCHEM");
                dbName = resultSet.getString("TABLE_NAME");

                // TABLE_SCHEM could be null so use the passed in schema name if this is the case
                if (dbSchema == null)
                {
                    dbSchema = schema;
                }

                // Make sure we didn't get too many tables
                if (resultSet.next())
                {
                    throw new SQLException("Too many tables matching tablename '" + tableName + "' and schema '" + schema + "'.");
                }

                tableExists = true;
            }
        }
        finally
        {
            close(resultSet);
        }

        // Only continue if the table exists
        if (! tableExists)
        {
            return null;
        }

        // Get hold of the column metadata
        List columns = new ArrayList();
        try
        {
            resultSet = metaData.getColumns(null, schema, tableName, null);

            while (resultSet.next())
            {
                String columnName = resultSet.getString("COLUMN_NAME");
                int dataType = resultSet.getInt("DATA_TYPE");
                int columnSize = resultSet.getInt("COLUMN_SIZE");
                int scale = resultSet.getInt("DECIMAL_DIGITS");
                int ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
                int nullable = resultSet.getInt("NULLABLE");
                ColumnInfo columnInfo = new ColumnInfo(
                        columnName,
                        dataType,
                        columnSize,
                        columnSize,
                        scale,
                        ordinalPosition,
                        nullable == DatabaseMetaData.columnNullable
                );

                columns.add(columnInfo);
            }
        }
        finally
        {
            close(resultSet);
        }

        return new TableColumnInfo(catalog, dbSchema, dbName, (ColumnInfo[]) columns.toArray(new ColumnInfo[columns.size()]));
    }

    public static TableColumnInfo createTableMetadataWithExtraSelect(Connection connection, String schema, String tableName, String fullyQualifiedTableName)
    throws SQLException
    {
        TableColumnInfo tableInfo = createTableMetadata(connection, schema, tableName, fullyQualifiedTableName);

        if (tableInfo == null || tableInfo.getColumns().length == 0)
        {
            List columns = new ArrayList();
            ResultSet resultSet = null;

            // Get hold of the table catalog and schema separately from the column metadata in case we have a zero-column table/view
            String catalog = null;
            String dbSchema = null;
            String dbName = null;
            Statement stm = null;
            try
            {
                stm = connection.createStatement();
                resultSet = stm.executeQuery("select * from "+fullyQualifiedTableName+" where 0 = 1");
                resultSet.next();
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int columnCount = resultSetMetaData.getColumnCount();
                dbName = resultSetMetaData.getTableName(1);
                dbSchema = resultSetMetaData.getSchemaName(1);
                if ("".equals(dbSchema))
                {
                    dbSchema = null;
                }
                for(int i=0;i< columnCount; i++)
                {
                    int col = i + 1;
                    ColumnInfo columnInfo = new ColumnInfo(resultSetMetaData.getColumnName(col),
                            resultSetMetaData.getColumnType(col),
                            resultSetMetaData.getColumnDisplaySize(col),
                            resultSetMetaData.getPrecision(col),
                            resultSetMetaData.getScale(col),
                            col,
                            resultSetMetaData.isNullable(col) == DatabaseMetaData.columnNullable);
                    columns.add(columnInfo);
                }
            }
            catch(SQLException e)
            {
                //ignore this; it's acceptable for the table to not be found.
            }
            finally
            {
                close(stm);
                close(resultSet);
            }
            if (columns.size() == 0)
            {
                return null;
            }
            tableInfo = new TableColumnInfo(catalog, dbSchema, dbName, (ColumnInfo[]) columns.toArray(new ColumnInfo[columns.size()]));
        }
        return tableInfo;
    }

    public TableColumnInfo(String catalog, String schema, String name, ColumnInfo[] columns)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;

        // Sort the columns by the ordinal position
        this.columns = new ColumnInfo[columns.length];
        System.arraycopy(columns, 0, this.columns, 0, columns.length);

        Arrays.sort(this.columns, new Comparator()
        {
            public int compare(Object o1, Object o2)
            {
                return ((ColumnInfo) o1).getOrdinalPosition() - ((ColumnInfo) o2).getOrdinalPosition();
            }
        });

        // Also index the columns by name
        this.columnsByName = new UnifiedMap(this.columns.length);
        for (int i = 0; i < columns.length; i++)
        {
            ColumnInfo column = columns[i];
            this.columnsByName.put(column.getName(), column);
        }
    }

    /**
     * @return The catalog that the table resides in (this may be <code>null</code>).
     */
    public String getCatalog()
    {
        return this.catalog;
    }

    /**
     * @return The schema that the table resides in (this may be <code>null</code>).
     */
    public String getSchema()
    {
        return this.schema;
    }

    /**
     * @return The name of the table.
     */
    public String getName()
    {
        return this.name;
    }

    /**
     * @return The columns in the table (ordered by ordinal position).
     */
    public ColumnInfo[] getColumns()
    {
        return this.columns;
    }

    /**
     * @param name The name of the column to lookup.
     * @return The information about the column, or <code>null</code> if there is no column matching <code>name</code>.
     */
    public ColumnInfo getColumn(String name)
    {
        return (ColumnInfo) this.columnsByName.get(name);
    }

    private static void close(ResultSet resultSet)
    {
        if (resultSet != null)
        {
            try
            {
                resultSet.close();
            }
            catch (SQLException e)
            {
                logger.error("Could not close ResultSet", e);
            }
        }
    }

    private static void close(Statement stm)
    {
        if (stm != null)
        {
            try
            {
                stm.close();
            }
            catch (SQLException e)
            {
                logger.error("Could not close ResultSet", e);
            }
        }
    }

    /**
     * @return The number of columns that the table has.
     */
    public int getNumberOfColumns()
    {
        return this.columns.length;
    }

    public boolean hasColumn(SingleColumnAttribute attr)
    {
        ColumnInfo info = (ColumnInfo) this.columnsByName.get(attr.getColumnName().toUpperCase());
        if (info == null)
        {
            info = (ColumnInfo) this.columnsByName.get(attr.getColumnName());
        }
        return info != null && attr.verifyColumn(info);
    }

    /**
     * Does the table have a column matching the <code>name</code>, <code>type</code>, <code>size</code>
     * and <code>isNullable</code> status?
     * @param columnName The name of the column.
     * @param type The JDBC {@link Types} type.
     * @param size The size of the column.
     * @param isNullable <code>true</code> if the column should be nullable.
     * @return <code>true</code> if a matching column can be found, otherwise <code>false</code>.
     */
    public boolean hasColumn(String columnName, int type, int size, boolean isNullable)
    {
        ColumnInfo info = (ColumnInfo) this.columnsByName.get(columnName.toUpperCase());
        if (info == null)
        {
            info = (ColumnInfo) this.columnsByName.get(columnName);
        }
        if (info != null)
        {
            if (info.isNullable() != isNullable) return false;

            type = normalizeType(type, -1);
            int infoType = normalizeType(info.getType(), info.getSize());

            if (type == Types.VARCHAR && size != info.getSize())
            {
                return false;
            }
            if (type == Types.DOUBLE && info.getType() == Types.FLOAT)
            {
                return info.getSize() == 8;
            }

            if (type == Types.DOUBLE && (info.getType() == Types.DECIMAL || info.getType() == Types.NUMERIC))
            {
                return true;
            }

            if (type == Types.BIGINT)
            {
                return infoType == type || infoType == Types.NUMERIC;
            }

            return infoType == type;
        }

        return false;
    }

    private int normalizeType(int type, int size)
    {
        switch (type)
        {
            case Types.CHAR:
                type = Types.VARCHAR;
                break;
            case Types.BIT:
            case Types.BOOLEAN:
            case Types.TINYINT:
                type = Types.SMALLINT;
                break;
            case Types.FLOAT:
                type = Types.DOUBLE;
                break;
        }
        return type;
    }
}
