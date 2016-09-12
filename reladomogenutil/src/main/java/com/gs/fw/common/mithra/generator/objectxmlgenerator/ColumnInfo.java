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

/*****************************************************************
 *
 *
 *
 *
 *
 * @(#) $Header$
 *****************************************************************/

package com.gs.fw.common.mithra.generator.objectxmlgenerator;

import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class ColumnInfo
{
    
    private TableInfo parentTable = null;

    private ColumnInfo relatedAsOfColumn = null;
    private ColumnInfo asOfColumnFrom = null;
    private ColumnInfo asOfColumnTo = null;

    private String columnName;
    private String attributeName;
    private String javaType;

    private int sqlType;
    private int columnSize = -1;
    private int precision = 0;
    private int decimal = 0;

    private boolean nullable = false;
    private boolean asOfAttribute = false;

    private boolean partOfPk = false;
    private boolean identity = false;

    private static Set<String> objectIdNameSet;
    private static Set<String> javaKeywordSet;

    private String asOfAttributeName;
    private boolean asOfAttributeFrom;

    static
    {
        objectIdNameSet = new HashSet<String>();
        objectIdNameSet.add("OBJECTID");
        objectIdNameSet.add("OBJECT_ID");

        javaKeywordSet = new HashSet<String>();
        javaKeywordSet.add("abstract");
        javaKeywordSet.add("continue");
        javaKeywordSet.add("for");
        javaKeywordSet.add("new");
        javaKeywordSet.add("switch");
        javaKeywordSet.add("assert");
        javaKeywordSet.add("default");
        javaKeywordSet.add("goto");
        javaKeywordSet.add("package");
        javaKeywordSet.add("synchronized");
        javaKeywordSet.add("boolean");
        javaKeywordSet.add("do");
        javaKeywordSet.add("if");
        javaKeywordSet.add("private");
        javaKeywordSet.add("this");
        javaKeywordSet.add("break");
        javaKeywordSet.add("double");
        javaKeywordSet.add("implements");
        javaKeywordSet.add("protected");
        javaKeywordSet.add("throw");
        javaKeywordSet.add("byte");
        javaKeywordSet.add("else");
        javaKeywordSet.add("import");
        javaKeywordSet.add("public");
        javaKeywordSet.add("throws");
        javaKeywordSet.add("case");
        javaKeywordSet.add("enum");
        javaKeywordSet.add("instanceof");
        javaKeywordSet.add("return");
        javaKeywordSet.add("transient");
        javaKeywordSet.add("catch");
        javaKeywordSet.add("extends");
        javaKeywordSet.add("int");
        javaKeywordSet.add("short");
        javaKeywordSet.add("try");
        javaKeywordSet.add("char");
        javaKeywordSet.add("final");
        javaKeywordSet.add("interface");
        javaKeywordSet.add("static");
        javaKeywordSet.add("void");
        javaKeywordSet.add("class");
        javaKeywordSet.add("finally");
        javaKeywordSet.add("long");
        javaKeywordSet.add("strictfp");
        javaKeywordSet.add("volatile");
        javaKeywordSet.add("const");
        javaKeywordSet.add("float");
        javaKeywordSet.add("native");
        javaKeywordSet.add("super");
        javaKeywordSet.add("while");
    }

    public ColumnInfo(TableInfo parentTable)
    {
        this.parentTable = parentTable;
    }

    public ColumnInfo(String columnName, TableInfo parentTable)
    {
        this.columnName = columnName;
        this.attributeName = getAttributeName(columnName);
        this.parentTable = parentTable;
    }

    public ColumnInfo(String columnName, TableInfo parentTable, boolean partOfPk)
    {
        this.columnName = columnName;
        this.attributeName = getAttributeName(columnName);
        this.partOfPk = partOfPk;
        this.parentTable = parentTable;
    }

    public String getAsOfAttributeName()
    {
        return this.asOfAttributeName;
    }

    public boolean isAsOfAttributeFrom()
    {
        return this.asOfAttributeFrom;
    }

    public void setAsOfAttributeFrom(boolean asOfAttributeFrom)
    {
        this.asOfAttributeFrom = asOfAttributeFrom;

        if (asOfAttributeFrom)
        {
            this.asOfColumnFrom = this;
            this.asOfColumnTo = this.relatedAsOfColumn;
        }
        else
        {
            this.asOfColumnTo = this;
            this.asOfColumnFrom = this.relatedAsOfColumn;
        }
    }

    public void setAsOfAttributeName(String asOfAttributeName)
    {
        this.asOfAttributeName = asOfAttributeName;
    }

    public ColumnInfo getAsOfColumnFrom()
    {
        return this.asOfColumnFrom;
    }

    public ColumnInfo getAsOfColumnTo()
    {
        return this.asOfColumnTo;
    }

    public String getColumnName()
    {
        return this.columnName;
    }

    public String getJavaType()
    {
        return this.javaType;
    }

    public int getSqlType()
    {
        return this.sqlType;
    }

    public ColumnInfo getRelatedAsOfColumn()
    {
        return this.relatedAsOfColumn;
    }

    public void setRelatedAsOfColumn(ColumnInfo relatedAsOfColumn)
    {
        this.relatedAsOfColumn = relatedAsOfColumn;
    }

    public boolean isPartOfPk()
    {
        return this.partOfPk;
    }

    public void setPartOfPk(boolean partOfPk)
    {
        this.partOfPk = partOfPk;
    }

    public boolean isIdentity()
    {
        return this.identity;
    }

    public void setIdentity(boolean identity)
    {
        this.identity = identity;
    }
    
    public int getColumnSize()
    {
        return this.columnSize;
    }

    public int getDecimal()
    {
        return this.decimal;
    }

    public boolean isNullable()
    {
        return this.nullable;
    }

    public boolean isAsOfAttribute()
    {
        return this.asOfAttribute;
    }

    public void setAsOfAttribute(boolean asOfAttribute)
    {
        this.asOfAttribute = asOfAttribute;
    }

    public void setResultSet(ResultSet rs) throws SQLException
    {
        this.javaType = rs.getString("TYPE_NAME");
        this.columnName = rs.getString("COLUMN_NAME");
        this.attributeName = this.getAttributeName(this.columnName);
        this.sqlType = rs.getInt("DATA_TYPE");
        this.nullable = (rs.getInt("NULLABLE") == 1);
        this.columnSize = rs.getInt("COLUMN_SIZE");
        this.precision = rs.getInt("COLUMN_SIZE");
        this.decimal = rs.getInt("DECIMAL_DIGITS");
        
        // In the Sybase DB, when the column is defined as "int identity", the type coming as is, instead of "int". 
        if(javaType.equalsIgnoreCase("int identity"))
        {
        	javaType = "integer";
        }
    }

    private String getAttributeName(String columnName)
    {
        String tableName = this.parentTable == null ? null : this.parentTable.getTableName();
        return getAttributeName(columnName, tableName);
    }

    public String getAttributeName(String columnName, String tableName)
    {
        if (objectIdNameSet.contains(columnName) && (tableName != null))
        {
            return StringUtility.toCamelCaseIgnoringLastChar(tableName + "_Id", "_", false);
        }
        else
        {
            String result = StringUtility.toCamelCaseIgnoringLastChar(columnName, "_", false);
            if (javaKeywordSet.contains(result))
            {
                result += "_FIXME";
            }
            return result;
        }
    }

    public int getPrecision()
    {
        return this.precision;
    }

    public String getAttributeName()
    {
        return this.attributeName;
    }

    public void setParentTable(TableInfo tableInfo)
    {
        this.parentTable = tableInfo;
    }

    public boolean equals(Object o)
    {
        return this.columnName.equals(((ColumnInfo) o).getColumnName());
    }

    public int hashCode()
    {
        return this.columnName.hashCode();
    }
}
