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


import java.io.FileNotFoundException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.MithraGenerator;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.type.StringJavaType;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

public class DatabaseTableValidator extends Task
{
    private static final double TEXT_DATA_SIZE = 2147483647;
    private final MithraObjectXmlGenerator xmlGenerator;
    private final Map<String, String> tablesMissingFromDb = new HashMap<String, String>();
    private final Map<String, String> missingColumnsViolations = new HashMap<String, String>();
    private final Map<String, String> columnTypeViolations = new HashMap<String, String>();
    private final Map<String, String> columnSizeViolations = new HashMap<String, String>();
    private final List<MithraObjectTypeWrapper> mithraObjectsWithNoTableInfo = new ArrayList();
    private String xml;
    private boolean executed = false;
    private HashMap<Integer, Integer> equivalentSqlTypeMap = new HashMap<Integer, Integer>();

    public DatabaseTableValidator()
    {
        this.initEquivalentSqlTypeMap();
        xmlGenerator = new MithraObjectXmlGenerator();
        xmlGenerator.setExcludeAsOfAttributesFromDbIndex(false);
    }

    private void initEquivalentSqlTypeMap()
    {
        equivalentSqlTypeMap.put(Types.BIGINT, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.INTEGER, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.SMALLINT, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.TINYINT, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.BIT, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.FLOAT, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.REAL, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.DOUBLE, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.DECIMAL, Types.NUMERIC);
        equivalentSqlTypeMap.put(Types.CHAR, Types.VARCHAR);
        equivalentSqlTypeMap.put(Types.LONGVARCHAR, Types.VARCHAR);
        equivalentSqlTypeMap.put(Types.DATE, Types.TIMESTAMP);
        equivalentSqlTypeMap.put(Types.TIME, Types.TIMESTAMP);
    }

    public String getXml()
    {
        return xml;
    }

    public void setXml(String xml)
    {
        this.xml = xml;
    }

    public void execute() throws BuildException
    {
        if (!this.executed)
        {
            this.validateTables();
            if (!this.tablesMissingFromDb.isEmpty())
            {
                this.log("The following tables are not found: " + this.tablesMissingFromDb.values(), Project.MSG_ERR);
            }
            if (!missingColumnsViolations.isEmpty())
            {
                this.log("Missing columns: " + this.missingColumnsViolations.values(), Project.MSG_ERR);
            }
            if (!columnTypeViolations.isEmpty())
            {
                this.log("Unmatched column type: " + this.columnTypeViolations.values(), Project.MSG_ERR);
            }
            if (!columnSizeViolations.isEmpty())
            {
                this.log("Unmatched column size: " + this.columnSizeViolations.values(), Project.MSG_ERR);
            }
            this.executed = true;
        }
    }

    public void validateTables()
    {
        Map<String, MithraObjectTypeWrapper> parsed = this.parseXmlAndGetMithraObjects(this.xml);
        this.validate(parsed.values());
    }

    public void validate(Collection<MithraObjectTypeWrapper> mithraObjectTypeWrapperList)
    {
        if (!mithraObjectTypeWrapperList.isEmpty())
        {
            List<MithraObjectTypeWrapper> typesForValidation = new ArrayList(mithraObjectTypeWrapperList);
            Map<String, TableInfo> tableInfo = this.generateTableInfo(typesForValidation);
            this.validateTable(typesForValidation, tableInfo);
            if (this.mithraObjectsWithNoTableInfo.size() > 0)
            {
                this.validateView(mithraObjectsWithNoTableInfo);
            }
        }
    }

    private Map<String, MithraObjectTypeWrapper> parseXmlAndGetMithraObjects(String xml)
    {
        MithraGenerator mithraGenerator = new MithraGenerator();
        mithraGenerator.setXml(xml);
        try
        {
            mithraGenerator.parseAndValidate();
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
        return mithraGenerator.getMithraObjects();
    }

    private void validateTable(List<MithraObjectTypeWrapper> typeWrappers, Map<String, TableInfo> tableInfo)
    {
        for (int i = 0; i < typeWrappers.size(); i++)
        {
            MithraObjectTypeWrapper typeWrapper = typeWrappers.get(i);
            String tableName = typeWrapper.getDefaultTable();
            TableInfo tableDetails = tableInfo.get(tableName);

            if (tableDetails == null)
            {
                //Add the object to the list to check against view
                mithraObjectsWithNoTableInfo.add(typeWrapper);
            }
            else
            {
                validateColumns(typeWrapper, tableDetails);
            }
        }
    }

    private void validateView(List<MithraObjectTypeWrapper> typeWrappers)
    {
        Map<String, TableInfo> viewInfo = this.xmlGenerator.processViewInfo();
        for (MithraObjectTypeWrapper typeWrapper : typeWrappers)
        {
            String tableName = typeWrapper.getDefaultTable();
            String mithraObjectClassName = typeWrapper.getClassName();
            TableInfo viewDetails = viewInfo.get(tableName);
            if (viewDetails == null)
            {
                tablesMissingFromDb.put(mithraObjectClassName, "Mithra object: " + mithraObjectClassName + ", missing table/view: " + tableName);
            }
            else
            {
                validateColumns(typeWrapper, viewDetails);
            }
        }
    }

    private void validateColumns(MithraObjectTypeWrapper typeWrapper, TableInfo viewDetails)
    {
        String className = typeWrapper.getClassName();

        Attribute[] attributes = typeWrapper.getAttributes();
        Map<String, ColumnInfo> columnInfoMap = viewDetails.getColumnMap();
        for (Attribute attribute : attributes)
        {
            String columnName = attribute.getPlainColumnName();
            ColumnInfo columnInfo = columnInfoMap.get(columnName);
            if (columnInfo == null)
            {
                missingColumnsViolations.put(className + "-" + columnName, "Missing column in object " + typeWrapper.getClassName() + " column " + columnName + " in attribute " + attribute.getName());
                continue;
            }
            //check column type
            int mithraSqlType = attribute.getType().getSqlType();
            int dbSqlType = columnInfo.getSqlType();
            if (mithraSqlType != dbSqlType)
            {
                //check equivalent sql type
                int equivalentMithraSqlType = normalizeType(mithraSqlType);
                int equivalentDbSqlType = normalizeType(dbSqlType);
                if (equivalentMithraSqlType != equivalentDbSqlType)
                {
                    String mithraColumnType = attribute.getAttributeType().getJavaType();
                    String dbColumnType = columnInfo.getJavaType();
                    this.columnTypeViolations.put(className + "-" + columnName, "Unmatched column type on object " + className + " column " + columnName + ". mithra: " + mithraColumnType + " db: " + dbColumnType);
                }
            }

            //Check column size
            int maxLength = attribute.getMaxLength();
            int dbColumnSize = columnInfo.getColumnSize();
            //Check if maxLength attribute is set for String type
            if(attribute.getType() instanceof StringJavaType && maxLength == 0)
            {
                this.columnSizeViolations.put(className + "-" + columnName, "MaxLength is missing for object " + className + " column " + columnName + " db column size: " + dbColumnSize);
            }
            //Check if maxLength matches column size
            else if (maxLength > 0
                    && ((dbColumnSize == TEXT_DATA_SIZE && maxLength > dbColumnSize)
                    || (dbColumnSize != TEXT_DATA_SIZE && maxLength != dbColumnSize)))
            {
                this.columnSizeViolations.put(className + "-" + columnName, "Unmatched column size on object " + className + " column " + columnName + ". mithra: " + maxLength + " db: " + dbColumnSize);
            }
        }
    }

    private int normalizeType(int mithraSqlType)
    {
        return equivalentSqlTypeMap.containsKey(mithraSqlType) ? equivalentSqlTypeMap.get(mithraSqlType) : mithraSqlType;
    }

    private Map<String, TableInfo> generateTableInfo(List<MithraObjectTypeWrapper> wrappers)
    {
        List<String> tableNames = new ArrayList<String>();
        for (int i = 0; i < wrappers.size(); i++)
        {
            tableNames.add(wrappers.get(i).getDefaultTable());
        }
        this.xmlGenerator.setIncludeList(tableNames);
        this.xmlGenerator.setGeneratedPackageName("test");
        return this.xmlGenerator.processTableInfo();
    }

    public Map<String, String> getTablesMissingFromDb()
    {
        return this.tablesMissingFromDb;
    }

    public Map<String, String> getMissingColumnsViolations()
    {
        return this.missingColumnsViolations;
    }

    public Map<String, String> getColumnTypeViolations()
    {
        return this.columnTypeViolations;
    }

    public Map<String, String> getColumnSizeViolations()
    {
        return this.columnSizeViolations;
    }

    public String getUserName()
    {
        return this.xmlGenerator.getUserName();
    }

    public void setUserName(String userName)
    {
        this.xmlGenerator.setUserName(userName);
    }

    public String getPassword()
    {
        return this.xmlGenerator.getPassword();
    }

    public void setPassword(String password)
    {
        this.xmlGenerator.setPassword(password);
    }

    public String getDatabaseType()
    {
        return this.xmlGenerator.getDatabaseType();
    }


    public void setDatabaseType(String databaseType)
    {
        this.xmlGenerator.setDatabaseType(databaseType);
    }

    public String getDriver()
    {
        return this.xmlGenerator.getDriver();
    }

    public void setDriver(String driver)
    {
        this.xmlGenerator.setDriver(driver);
    }

    public void setLdapName(String ldapName)
    {
        this.xmlGenerator.setLdapName(ldapName);
    }

    public String getLdapName()
    {
        return this.xmlGenerator.getLdapName();
    }

    public String getUrl()
    {
        return this.xmlGenerator.getUrl();
    }

    public void setUrl(String url)
    {
        this.xmlGenerator.setUrl(url);
    }

    public void setSchema(String schema)
    {
        this.xmlGenerator.setSchema(schema);
    }

    public String getSchema()
    {
        return this.xmlGenerator.getSchema();
    }
}
