
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

import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ForeignKey
{
    private String refTableName;
    private List<String> refColumnsAsString = new ArrayList<String>();
    private List<ColumnInfo> refColumns = new ArrayList<ColumnInfo>();
    private List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
    private boolean multipleRelationsBetweenTables;
    private TableInfo tableA;
    private TableInfo tableB;
    private TableInfo tableAB;

    private String name;

    public ForeignKey(TableInfo table, String name, String refTable)
    {
        super();
        this.tableA = table;
        this.name = name;
        this.refTableName = refTable;
        multipleRelationsBetweenTables = false;
    }

    public String getRefTableName()
    {
        return refTableName;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<ColumnInfo> getRefColumns()
    {
        return refColumns;
    }

    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    public TableInfo getTableB()
    {
        return tableB;
    }

    public void setTableB(TableInfo tableB)
    {
        this.tableB = tableB;
        for(String colName: refColumnsAsString)
        {
            refColumns.add(tableB.getColumnMap().get(colName));
        }
    }

    public void setTableAB(TableInfo tableAB)
    {
        this.tableAB = tableAB;
    }

    public void initMultipleRelationsBetweenTables()
    {
        for (ForeignKey fk : tableA.getForeignKeys().values())
        {
            if (fk == this || fk.getName().equals(this.getName()))  // Same foreign key
                continue;

            if (fk.getRefTableName().equals(this.getRefTableName()))
            {
                multipleRelationsBetweenTables = true;
                break;
            }
        }
    }

    public String getJoinOperation()
    {
		String ret = "";
		if(tableAB == null)
		{
			int fkColumns = getColumns().size();
			List<ColumnInfo> refColumns = getRefColumns();
			for(int i = 0;i < this.columns.size();i++)
			{
				ColumnInfo column = columns.get(i);
				ColumnInfo refColumn = refColumns.get(i);
				ret += "this." + column.getAttributeName() + "=" + tableB.getClassName() + "." + refColumn.getAttributeName();
				if(i != (fkColumns - 1))
					ret += " and ";
			}
		}
		else // Many-to-Many relationship
		{
			ForeignKey[] fks = tableAB.getForeignKeys().values().toArray(new ForeignKey[2]);
			String ret1 = fks[0].getJoinOperation().replace("this.", tableAB.getClassName() + ".")
						  .replace(tableA.getClassName() + ".", "this.");
			String ret2 = fks[1].getJoinOperation().replace("this.", tableAB.getClassName() + ".");
			ret = ret1 + " and " + ret2;
		}
        return ret;
    }

    private static String concatenateColumnNames(ForeignKey fk)
    {
        String ret = "";
        List<ColumnInfo> columns = fk.getColumns();
        for (ColumnInfo col : columns)
        {
            String attributeName = col.getAttributeName();
            int idIndex = attributeName.lastIndexOf("Id");
            if (idIndex != -1)
                attributeName = attributeName.substring(0, idIndex);
            ret += "_" + attributeName;
        }
        return ret;
    }

    // TableA as property name in TableB
    public String getReverseRelationshipName()
    {
        String ret = "";
        if (tableAB == null)
        {
            if (multipleRelationsBetweenTables)  // foreignKey will be non-null since it applies only to one-to-X relation
            {
                ret = concatenateColumnNames(this);
                ret = ret.substring(1) + "_"; // remove leading _ and add trailing _.
            }

            ret += StringUtility.firstLetterToLower(tableA.getClassName());
        }
        else
        {
            ForeignKey[] fks = tableAB.getForeignKeys().values().toArray(new ForeignKey[2]);
            ret = ForeignKey.concatenateColumnNames(fks[0]).substring(1);
        }
        String multiplicity = getTableAMultiplicity();
        if (!multiplicity.equals("one"))
            ret = StringUtility.englishPluralize(ret);

        return ret;
    }

    // TableB as property name in TableA
    public String getRelationshipName()
    {
        String ret;
        if (tableAB == null /* && multipleRelationsBetweenTables */)
        {
            ret = concatenateColumnNames(this);
            ret = ret.substring(1); // remove leading _.
        }
        else // Many-to-Many relationship
        {
            ForeignKey[] fks = tableAB.getForeignKeys().values().toArray(new ForeignKey[2]);
            ret = ForeignKey.concatenateColumnNames(fks[1]).substring(1);
        }

        String multiplicity = getTableBMultiplicity();
        if (!multiplicity.equals("one"))
            ret = StringUtility.englishPluralize(ret);
        return ret;
    }

    // If the referenced table for a foreign key of tableB is tableA and referenced columns of that foreign key of tableB is same as tableA PKs, this is one-to-one relationship.
    // Otherwise, it is a 1-1 relationship.
    public String getTableAMultiplicity()
    {
        String ret = "many";
        List<ColumnInfo> tableAPKs = tableA.getPkList();

        if (tableAPKs.size() == 0 || tableA.getTableName().equals(tableB.getTableName()))
        {
            return ret;
        }
        if (tableAB == null)
        {
            List<ColumnInfo> fkColumns = this.getColumns();
            Set<ColumnInfo> columnSet = new HashSet<ColumnInfo>();
            columnSet.addAll(fkColumns);
            columnSet.removeAll(tableAPKs);
            if (fkColumns.size() == tableAPKs.size() && columnSet.isEmpty())
            {
                ret = "one";
                return ret;
            }
        }
        for (ForeignKey tableBFK : tableB.getForeignKeys().values())
        {
            if (!tableBFK.getTableB().getTableName().equals(tableA.getTableName()))
                continue;

            List<ColumnInfo> tableBRefCols = tableBFK.getRefColumns();
            Set<ColumnInfo> columns = new HashSet<ColumnInfo>();
            columns.addAll(tableBRefCols);
            columns.removeAll(tableAPKs);
            if (columns.isEmpty())
            {
                ret = "one";
                break;
            }
        }
        return ret;
    }

    public String getTableBMultiplicity()
    {
        // Usually this is a RelationalOperation for one-to-many relationship.
        String ret = "one";
        // If foreignKey is null, this is a RelationalOperation for many-to-many relationship constructed from tableA and tableB instead of from foreignKey.
        if (tableAB != null) ret = "many";

        return ret;
    }

    public void addtRefColumnsAsString(String colName)
    {
        this.refColumnsAsString.add(colName);
    }
}
