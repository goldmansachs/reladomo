

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

package com.gs.fw.common.mithra.generator.objectxmlgenerator;

import java.sql.*;
import java.util.*;

import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.generator.util.StringUtility;
import org.apache.tools.ant.BuildException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableInfo
{
    protected static final Logger logger = LoggerFactory.getLogger(MithraObjectXmlGenerator.class);

    private String tableName;
    private Map<String, ColumnInfo> columnMap = new HashMap<String, ColumnInfo>();
    private List<ColumnInfo> asOfList = new ArrayList<ColumnInfo>();

    private List<ColumnInfo> pkList = new ArrayList<ColumnInfo>();
    private Map<String, ForeignKey> foreignKeys = new HashMap<String, ForeignKey>();
    
    private Map<String, IndexInfo> indexMap = new HashMap<String, IndexInfo>();

    private List<AsOfAttributeNamePair> asOfNamePairs;

    public TableInfo()
    {
        asOfNamePairs = new ArrayList<AsOfAttributeNamePair>();
        asOfNamePairs.add(new AsOfAttributeNamePair("IN_Z", "OUT_Z", "processingDate"));
        asOfNamePairs.add(new AsOfAttributeNamePair("FROM_Z", "THRU_Z", "businessDate"));
        asOfNamePairs.add(new AsOfAttributeNamePair("IN_TMSTMP", "OUT_TMSTMP", "processingDate"));
        asOfNamePairs.add(new AsOfAttributeNamePair("EFF_TMSTMP", "EXP_TMSTMP", "businessDate"));
        asOfNamePairs.add(new AsOfAttributeNamePair("FROM_DATE_Z", "THRU_DATE_Z", "businessDate"));
        asOfNamePairs.add(new AsOfAttributeNamePair("IN_DATE_Z", "OUT_DATE_Z", "processingDate"));
        asOfNamePairs.add(new AsOfAttributeNamePair("IN_UTC", "OUT_UTC", "processingDate"));
    }

    public String getTableName()
    {
        return tableName;
    }

    public void removeForeignKeysForMissingTables(Set<String> tableNames)
    {
        for (Iterator<Map.Entry<String, ForeignKey>> iterator = this.getForeignKeys().entrySet().iterator(); iterator.hasNext();)
        {
            Map.Entry<String, ForeignKey> entry = iterator.next();
            if (!tableNames.contains(entry.getValue().getRefTableName()))
            {
                iterator.remove();
            }
        }
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public ColumnInfo findColumnInfo(String columnName)
    {
        return this.columnMap.get(columnName);
    }

    public Map<String, ColumnInfo> getColumnMap()
    {
        return columnMap;
    }

    public List<ColumnInfo> getAsOfList()
    {
        return asOfList;
    }

    public List<ColumnInfo> getPkList()
    {
        if (pkList == null)
        {
            pkList = new ArrayList<ColumnInfo>();
        }
        return pkList;
    }

    public Map<String, IndexInfo> getIndexMap()
    {
        if (indexMap == null)
        {
            indexMap = new HashMap<String, IndexInfo>();
        }
        return indexMap;
    }

    public void addToColumnList(ColumnInfo columnInfo)
    {
        /* Check if this completes a pair of AsOfAttributes */
        for (Iterator it = asOfNamePairs.iterator(); it.hasNext();)
        {
            AsOfAttributeNamePair pair = (AsOfAttributeNamePair) it.next();

            if (pair.isAnAsOfAttribute(columnInfo))
            {
                columnInfo.setAsOfAttribute(true);
            }

            if (pair.completesPair(columnInfo))
            {
                ColumnInfo startColumn = pair.getStartColumn();
                ColumnInfo endColumn = pair.getEndColumn();

                startColumn.setAsOfAttribute(true);
                startColumn.setRelatedAsOfColumn(endColumn);
                startColumn.setAsOfAttributeName(pair.getDateType());
                startColumn.setAsOfAttributeFrom(true);

                asOfList.add(startColumn);
            }
        }
        columnMap.put(columnInfo.getColumnName(), columnInfo);
    }

    public void populateTableInfoWithMetaData(DatabaseMetaData metaData, String catalog, String schema, DatabaseType dbType, boolean excludeAsOfAttributesFromDbIndex)
    {
        this.populatePrimaryKey(metaData, catalog, schema);
        this.populateColumnList(metaData, catalog, schema, dbType);
        this.populateIndices(metaData, catalog, schema, excludeAsOfAttributesFromDbIndex);
        this.populateForeignKeys(metaData, catalog, schema);
    }

    public void populateColumnList(DatabaseMetaData metaData, String catalog, String schema, DatabaseType dbType)
    {
        ResultSet rs = null;

        try
        {
            rs = metaData.getColumns(catalog, schema, this.tableName, "%");
            ResultSetMetaData tableMetadata = this.getTableMetadata(metaData, dbType, catalog, schema);
            int columnCount = 1;
            while (rs.next())
            {
                ColumnInfo columnInfo = this.generateColumnListing(rs);
                columnInfo.setIdentity(tableMetadata.isAutoIncrement(columnCount++));
                this.addToColumnList(columnInfo);
            }
        }
        catch (SQLException e)
        {
            logger.error("", e);
            throw new BuildException(e);
        }
        finally
        {
            this.closeResultSet(rs);
        }
        for (AsOfAttributeNamePair pair : this.asOfNamePairs)
        {
            pair.markIncompleteAsNormalAttribute();
        }
    }

    public String getClassName()
    {
        return StringUtility.toCamelCaseIgnoringLastChar(this.getTableName(), "_", true);
    }

    private void populateForeignKeys(DatabaseMetaData metaData, String catalog, String schema)
    {
        ResultSet rs = null;
        try
        {
            rs = metaData.getImportedKeys(catalog, schema, tableName);
            while (rs.next())
            {
                String pkTableName = rs.getString("PKTABLE_NAME");
                String pkColName = rs.getString("PKCOLUMN_NAME");
                String fkColName = rs.getString("FKCOLUMN_NAME");
                String name = rs.getString("FK_NAME");
                ForeignKey fk = foreignKeys.get(name);
                if (fk == null)
                {
                    fk = new ForeignKey(this, name, pkTableName);
                    foreignKeys.put(name, fk);
                }
                fk.addtRefColumnsAsString(pkColName);
                fk.getColumns().add(this.columnMap.get(fkColName));
            }
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Could not retrieve foreign keys", e);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    //ignore
                }
            }
        }
    }
    
    private void populateIndices(DatabaseMetaData metaData, String catalog, String schema, boolean excludeAsOfAttributesFromDbIndex)
    {
        ResultSet rs = null;

        try
        {
            rs = metaData.getIndexInfo(catalog, schema, tableName, false, false);

            while (rs.next())
            {
                String indexName = rs.getString("INDEX_NAME");

                /* This is an index of type tableIndexStatistic  */
                if (indexName == null)
                {
                    continue;
                }

                String columnName = rs.getString("COLUMN_NAME");
                boolean indexUnique = !rs.getBoolean("NON_UNIQUE");
                ColumnInfo columnInfo = this.columnMap.get(columnName);

                if (!excludeAsOfAttributesFromDbIndex || !columnInfo.isAsOfAttribute())
                {
                    /* Check if this is the first column in a new index or part of an existing index */
                    if (indexMap.containsKey(indexName))
                    {
                        indexMap.get(indexName).getColumnInfoList().add(columnInfo);
                    }
                    else
                    {
                        IndexInfo indexInfo = new IndexInfo();
                        indexInfo.setName(indexName);
                        indexInfo.setUnique(indexUnique);
                        ArrayList<ColumnInfo> indexColumnList = indexInfo.getColumnInfoList();

                        indexColumnList.add(columnInfo);

                        if (!alreadyInIndices(indexInfo) && !alreadyInPk(indexInfo))
                        {
                            indexMap.put(indexName, indexInfo);
                        }
                    }
                }
            }

            closeResultSet(rs);

            /* If there's no primary key, make it the first unique index */
            if (pkList.isEmpty())
            {
                for (Iterator mapIt = indexMap.values().iterator(); mapIt.hasNext();)
                {
                    IndexInfo indexInfo = (IndexInfo) mapIt.next();

                    if (indexInfo.isUnique())
                    {
                        pkList = indexInfo.getColumnInfoList();
                        for(int i=0;i<pkList.size();i++)
                        {
                            pkList.get(i).setPartOfPk(true);
                        }
                        mapIt.remove();

                        break;
                    }
                }
            }
            else
            {
                // Remove any indexes that exactly match the primary key
                for (Iterator indexIterator = indexMap.values().iterator(); indexIterator.hasNext();)
                {
                    IndexInfo indexInfo = (IndexInfo) indexIterator.next();

                    List<ColumnInfo> indexColumns = indexInfo.getColumnInfoList();
                    if (indexColumns.size() == pkList.size())
                    {
                        boolean match = true;
                        for(int i=0;i<pkList.size() && match;i++)
                        {
                            match = indexColumns.contains(pkList.get(i));
                        }
                        if (match)
                        {
                            indexIterator.remove();
                        }
                    }
                }

            }

            if (pkList.isEmpty())
            {
                logger.error("No primary key for " + tableName + "!");
            }

        }
        catch (SQLException e)
        {
            logger.error("", e);
            throw new BuildException(e);
        }
        finally
        {
            closeResultSet(rs);
        }
    }

    private void populatePrimaryKey(DatabaseMetaData metaData, String catalog, String schema)
    {
        ResultSet rs = null;

        try
        {
            rs = metaData.getPrimaryKeys(catalog, schema, tableName);
            while (rs.next())
            {
                String columnName = rs.getString("COLUMN_NAME");
                ColumnInfo columnInfo = new ColumnInfo(columnName, this, true);

                pkList.add(columnInfo);
            }

            closeResultSet(rs);
        }
        catch (SQLException e)
        {
            logger.error("", e);
            throw new BuildException(e);
        }
        finally
        {
            closeResultSet(rs);
        }
    }

    private boolean alreadyInList(IndexInfo indexInfo, List<ColumnInfo> list)
    {
        List<ColumnInfo> indexColumns = indexInfo.getColumnInfoList();

        if (list.size() != indexColumns.size())
        {
            return false;
        }

        int matchCount = 0;

        for (Iterator it = list.iterator(); it.hasNext();)
        {
            if (indexColumns.contains(it.next()))
            {
                matchCount++;
            }
        }
        return (list.size() == matchCount);
    }

    private boolean alreadyInPk(IndexInfo indexInfo)
    {
        return alreadyInList(indexInfo, pkList);
    }

    private boolean alreadyInIndices(IndexInfo indexInfo)
    {
        for (Iterator it = indexMap.values().iterator(); it.hasNext();)
        {
            if (alreadyInList(indexInfo, ((IndexInfo) it.next()).getColumnInfoList()))
            {
                return true;
            }
        }
        return false;
    }

    private void closeResultSet(ResultSet rs)
    {
        if (rs != null)
        {
            try
            {
                rs.close();
            }
            catch (SQLException e)
            {
                logger.error("", e);
                throw new BuildException();
            }
        }
    }

    private ColumnInfo generateColumnListing(ResultSet rs) throws SQLException
    {
        ColumnInfo columnInfo = new ColumnInfo(this);
        columnInfo.setResultSet(rs);

        if (this.pkList.contains(columnInfo))
        {
            columnInfo.setPartOfPk(true);
        }
        return columnInfo;
    }

    private ResultSetMetaData getTableMetadata(DatabaseMetaData metaData, DatabaseType dt, String catalog, String schemaName) throws SQLException
    {
        Connection connection = metaData.getConnection();
        connection.setCatalog(catalog);
        PreparedStatement ps = connection.prepareStatement("select * from " + dt.getFullyQualifiedTableName(schemaName, this.tableName) + " where 1=2");
        ResultSet rs = ps.executeQuery();
        return rs.getMetaData();        
    }

    public Map<String, ForeignKey> getForeignKeys()
    {
        return foreignKeys;
    }

    public boolean isManyToManyTable()
    {
        List<ColumnInfo> pk = getPkList();
        if (pk == null || pk.isEmpty() || pk.size() != getColumnMap().size())
        {
            return false;
        }
        List<ForeignKey> foreignKeys = new ArrayList<ForeignKey>();
        for (ForeignKey fkey : getForeignKeys().values())
        {
            foreignKeys.add(fkey);
            if (foreignKeys.size() > 2)
            {
                return false;
            }
        }
        if (foreignKeys.size() != 2)
        {
            return false;
        }
        Set<String> columns = new HashSet<String>();
        for (ColumnInfo column : getColumnMap().values())
        {
            columns.add(column.getColumnName());
        }

        for (ForeignKey foreignKey : getForeignKeys().values())
        {
            for (ColumnInfo fkColumn : foreignKey.getColumns())
            {
                columns.remove(fkColumn.getColumnName());
            }
            if (columns.isEmpty())
            {
                break;
            }
        }

        return columns.isEmpty();
    }
}
