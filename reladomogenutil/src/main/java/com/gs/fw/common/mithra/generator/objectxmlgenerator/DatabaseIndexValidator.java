
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

import com.gs.fw.common.mithra.generator.*;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import java.io.FileNotFoundException;
import java.util.*;

public class DatabaseIndexValidator
        extends Task
{
    private final MithraObjectXmlGenerator xmlGenerator;
    private String xml;
    private boolean ignorePackageNamingConvention = false;
    private boolean executed = false;
    private List<MithraGeneratorImport> imports = new ArrayList<MithraGeneratorImport>();
    private final SortedMap<String, String> tablesMissingFromDb = new TreeMap<String, String>();
    private final SortedMap<String, String> missingKeyViolations = new TreeMap<String, String>();


    public DatabaseIndexValidator()
    {
        xmlGenerator = new MithraObjectXmlGenerator();
        xmlGenerator.setExcludeAsOfAttributesFromDbIndex(false);
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

    public void setIgnorePackageNamingConvention(boolean ignorePackageNamingConvention)
    {
        this.ignorePackageNamingConvention = ignorePackageNamingConvention;
    }

    public Map<String, String> getTablesMissingFromDb()
    {
        return tablesMissingFromDb;
    }

    public Map<String, String> getMissingKeyViolations()
    {
        return missingKeyViolations;
    }

    public void execute() throws BuildException
    {
        if (!this.executed)
        {
            this.validateIndices();
            if (!this.tablesMissingFromDb.isEmpty())
            {
                this.log("The following tables are not found: " + this.tablesMissingFromDb.values(), Project.MSG_ERR);
            }
            if (!missingKeyViolations.isEmpty())
            {
                throw new RuntimeException("The following violations are reported: " + this.missingKeyViolations.values());
            }
            this.executed = true;
        }
    }

    public void validateIndices(Collection<MithraObjectTypeWrapper>  wrappers)
    {
        List<MithraObjectTypeWrapper> typesForValidation = this.selectNotPureNotImported(wrappers);
        if (!typesForValidation.isEmpty())
        {
            Map<String, TableInfo> tableInfo = this.generateTableInfo(typesForValidation);
            this.validate(typesForValidation, tableInfo);
        }
    }

    public void validateIndices()
    {
        Map<String, MithraObjectTypeWrapper> parsed = this.parseXmlAndGetMithraObjects(this.xml);
        this.validateIndices(parsed.values());
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

    private void validate(List<MithraObjectTypeWrapper> typeWrappers, Map<String, TableInfo> tableInfo)
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
                Set<String> pkColumnNamesInMithra = extractMithraPrimaryKey(typeWrapper);
                Iterable<Set<String>> dbPkCandidates = this.createPkCandidates(tableDetails);
                if (!matchFound(pkColumnNamesInMithra, dbPkCandidates))
                {
                    missingKeyViolations.put(mithraObjectClassName, "\n" + mithraObjectClassName + ": DB table: " + tableName +
                                                                         ", mithra pk: " + this.toDisplayString(pkColumnNamesInMithra) + ", pk candidates from db: " +
                                                                         this.toDisplayString(dbPkCandidates));
                }
            }
        }
    }

    private boolean matchFound(Set<String> pkColumnNamesInMithra, Iterable<Set<String>> dbPkCandidates)
    {
        boolean matchFound = false;
        Iterator<Set<String>> iterator = dbPkCandidates.iterator();
        while (iterator.hasNext() && !matchFound)
        {
            matchFound = iterator.next().containsAll(pkColumnNamesInMithra);
        }
        return matchFound;
    }

    private String toDisplayString(Iterable<Set<String>> iterable)
    {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<Set<String>> iterator = iterable.iterator();
        while (iterator.hasNext())
        {
            stringBuilder.append("[");
            stringBuilder.append(this.toDisplayString(iterator.next()));
            stringBuilder.append("] ");
        }
        return stringBuilder.toString();
    }

    private Iterable<Set<String>> createPkCandidates(TableInfo tableInfo)
    {
        Set<Set<String>> pkCandidates = new HashSet<Set<String>>();
        pkCandidates.add(this.getPkCandidateColumnList(tableInfo));
        for (IndexInfo indexInfo : tableInfo.getIndexMap().values())
        {
            if (indexInfo.isUnique())
            {
                ArrayList<ColumnInfo> columnInfoList = indexInfo.getColumnInfoList();
                Set<String> indexColumns = new HashSet<String>();
                for (ColumnInfo aColumnInfoList : columnInfoList)
                {
                    indexColumns.add(aColumnInfoList.getColumnName());
                }
                pkCandidates.add(indexColumns);
            }
        }
        return pkCandidates;
    }

    private Set<String> getPkCandidateColumnList(TableInfo tableInfo)
    {
        List<ColumnInfo> columnInfos = tableInfo.getPkList();
        Set<String> pkColumnNamesInDb = new HashSet<String>();
        for (int k = 0; k < columnInfos.size(); k++)
        {
            pkColumnNamesInDb.add(columnInfos.get(k).getColumnName());
        }
        return pkColumnNamesInDb;
    }


    private Set<String> extractMithraPrimaryKey(MithraObjectTypeWrapper typeWrapper)
    {
        Attribute[] pkAttributes = typeWrapper.getPrimaryKeyAttributes();
        Set<String> pkColumnNamesInMithra = new HashSet<String>();
        for (int j = 0; j < pkAttributes.length; j++)
        {
            pkColumnNamesInMithra.add(pkAttributes[j].getColumnName());
        }
        this.addAsOfAttributes(pkColumnNamesInMithra, typeWrapper);
        return pkColumnNamesInMithra;
    }

    private void addAsOfAttributes(Set<String> pkColumnNames, MithraObjectTypeWrapper typeWrapper)
    {
        AsOfAttribute[] asOfAttribute = typeWrapper.getAsOfAttributes();
        for (int i = 0; i < asOfAttribute.length; i++)
        {
            pkColumnNames.add(asOfAttribute[i].getToColumnName());
        }
    }

    private String toDisplayString(Collection<String> collection)
    {
        if (collection.isEmpty())
        {
            return "<NONE!>";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (String aCollection : collection)
        {
            if (stringBuilder.length() > 0)
            {
                stringBuilder.append(", ");
            }
            stringBuilder.append(aCollection);
        }
        return stringBuilder.toString();
    }

    private Map<String, MithraObjectTypeWrapper> parseXmlAndGetMithraObjects(String xml)
    {
        MithraGenerator mithraGenerator = new MithraGenerator();
        mithraGenerator.setXml(xml);
        mithraGenerator.setIgnorePackageNamingConvention(ignorePackageNamingConvention);
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

}
