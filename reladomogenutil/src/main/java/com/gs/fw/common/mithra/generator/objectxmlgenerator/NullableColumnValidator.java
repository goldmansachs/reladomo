
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

import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.MithraGenerator;
import com.gs.fw.common.mithra.generator.MithraGeneratorImport;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import java.io.*;
import java.util.*;

public class NullableColumnValidator
        extends Task
{
    private final MithraObjectXmlGenerator xmlGenerator;
    private String xml;
    private boolean executed = false;
    private List<MithraGeneratorImport> imports = new ArrayList<MithraGeneratorImport>();
    private final SortedMap<String, String> tablesMissingFromDb = new TreeMap<String, String>();
    private List<String> violations = new ArrayList<String>();
    private boolean autofixMithraNullableAttributes = false;

    public NullableColumnValidator()
    {
        xmlGenerator = new MithraObjectXmlGenerator();
        xmlGenerator.setExcludeAsOfAttributesFromDbIndex(false);
    }

    public boolean isAutofixMithraNullableAttributes()
    {
        return autofixMithraNullableAttributes;
    }

    public void setAutofixMithraNullableAttributes(boolean autofixMithraNullableAttributes)
    {
        this.autofixMithraNullableAttributes = autofixMithraNullableAttributes;
    }

    public void addImport(String dir, String fileName)
    {
        MithraGeneratorImport anImport = new MithraGeneratorImport();
        anImport.setDir(dir);
        anImport.setFilename(fileName);
        this.addConfiguredMithraImport(anImport);
    }

    public String getXml()
    {
        return xml;
    }

    public void setXml(String xml)
    {
        this.xml = xml;
    }

    public Map<String, String> getTablesMissingFromDb()
    {
        return tablesMissingFromDb;
    }

    public void execute() throws BuildException
    {
        if (!this.executed)
        {
            try
            {
                this.validateNullableColumns();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            if (!this.tablesMissingFromDb.isEmpty())
            {
                this.log("The following tables are not found: " + this.tablesMissingFromDb.values(), Project.MSG_ERR);
            }
            if (!violations.isEmpty())
            {
                throw new RuntimeException("The following violations are reported: " + this.violations);
            }

            this.executed = true;
        }
    }

    public void validateNullableColumns() throws IOException
    {
        Map<String, MithraObjectTypeWrapper> parsed = this.parseXmlAndGetMithraObjects(this.xml);
        validateNullableColumns(parsed.values()) ;

    }

    public void validateNullableColumns(Collection<MithraObjectTypeWrapper>  wrappers) throws IOException
    {
        List<MithraObjectTypeWrapper> typesForValidation = this.selectNotPureNotImported(wrappers);
        if (!typesForValidation.isEmpty())
        {
            Map<String, TableInfo> tableInfo = this.generateTableInfo(typesForValidation);
            this.validate(typesForValidation, tableInfo);
        }
    }

    public void addConfiguredMithraImport(MithraGeneratorImport imp)
    {
        this.imports.add(imp);
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

    private void validate(List<MithraObjectTypeWrapper> typeWrappers, Map<String, TableInfo> tableInfo) throws IOException
    {
        for (int i = 0; i < typeWrappers.size(); i++)
        {
            MithraObjectTypeWrapper typeWrapper = typeWrappers.get(i);
            String tableName = typeWrapper.getDefaultTable();
            TableInfo tableDetails = tableInfo.get(tableName);
            String mithraObjectClassName = typeWrapper.getClassName();
            if (tableDetails == null)
            {
                tablesMissingFromDb.put(mithraObjectClassName, "Mithra object: " + mithraObjectClassName + ", missing table: " + tableName);
            }
            else
            {
                Attribute[] attributes = typeWrapper.getAttributes();
                for (int j = 0; j < attributes.length; j++)
                {
                    Attribute nextAttribute = attributes[j];
                    ColumnInfo dbColumnInfo = tableDetails.findColumnInfo(nextAttribute.getPlainColumnName());
                    if (dbColumnInfo != null && dbColumnInfo.isNullable() != nextAttribute.isNullable())
                    {
                        this.violations.add("Mithra xml: " + mithraObjectClassName + " db table: " + tableName + " column: " + dbColumnInfo.getColumnName() + " mithra col name: " + nextAttribute.getName() + " is db nullable? " + dbColumnInfo.isNullable() + " is mithra nullable? " + nextAttribute.isNullable() + "\n");
                        if (this.autofixMithraNullableAttributes && nextAttribute.isNullable())
                        {
                            String fileName = this.createFileName(nextAttribute);

                            String text = this.readFile(fileName);
                            String columnName = "\"" + dbColumnInfo.getColumnName() + "\"";
                            text = text.replaceFirst(columnName, columnName + " nullable=\"" + dbColumnInfo.isNullable() + "\"");
                            this.writeFile(fileName, text);
                        }
                    }
                }
            }
        }
    }


    private Map<String, MithraObjectTypeWrapper> parseXmlAndGetMithraObjects(String xml)
    {
        MithraGenerator mithraGenerator = new MithraGenerator();
        mithraGenerator.setXml(xml);
        for (MithraGeneratorImport anImport : this.imports)
        {
            mithraGenerator.addConfiguredMithraImport(anImport);
        }
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


    private List<MithraObjectTypeWrapper> selectNotPureNotImported(Collection<MithraObjectTypeWrapper> values)
    {
        List<MithraObjectTypeWrapper> list = new ArrayList<MithraObjectTypeWrapper>();
        for (MithraObjectTypeWrapper wrapper : values)
        {
            if (!wrapper.isPure() && !wrapper.isImported() && !wrapper.isTablePerSubclassSuperClass())
            {
                list.add(wrapper);
            }
        }
        return list;
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

    public void setDatabaseType(String databaseType) throws BuildException
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

    private String extractOwnerClassName(Attribute missingLenAttribute)
    {
        return missingLenAttribute.getOriginalOwner() == null ?
               missingLenAttribute.getOwner().getClassName() : missingLenAttribute.getOriginalOwner().getClassName();
    }


    public String readFile(String filename) throws IOException
    {
        BufferedReader in = null;
        try
        {
            in = new BufferedReader(new FileReader(filename));
            String nextLine;
            StringWriter stringWriter = new StringWriter();
            PrintWriter writer = new PrintWriter(stringWriter);
            while ((nextLine = in.readLine()) != null)
            {
                writer.println(nextLine);
            }
            writer.flush();
            return stringWriter.toString();
        }
        finally
        {
            if (in != null)
            {
                in.close();
            }
        }
    }

    private String createFileName(Attribute missingLenAttribute)
    {
        return new File(getXml()).getParent() + File.separator + extractOwnerClassName(missingLenAttribute) + ".xml";
    }


    public void writeFile(String filename, String text) throws IOException
    {
        BufferedWriter writer = null;
        try
        {
            writer = new BufferedWriter(new FileWriter(filename, false));
            writer.write(text);
            writer.flush();
        }
        finally
        {
            if (writer != null)
            {
                writer.close();
            }
        }

    }
}
