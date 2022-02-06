
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

import java.io.*;
import java.util.*;

import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.MithraGenerator;
import com.gs.fw.common.mithra.generator.MithraGeneratorImport;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.type.StringJavaType;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

public class MaxLenValidator
        extends Task
{
    private final MithraObjectXmlGenerator xmlGenerator = new MithraObjectXmlGenerator();
    private String xml;
    private boolean executed = false;
    private List<MithraGeneratorImport> imports = new ArrayList();

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
            Map<String, MithraObjectTypeWrapper> parsed = this.parseXmlAndGetMithraObjects(this.xml);
            validateMaxLength(parsed.values());
            this.executed = true;
        }
    }

    public void validateMaxLength(Collection<MithraObjectTypeWrapper>  wrappers) {
        List<MithraObjectTypeWrapper> typesWithMissingMaxLength = this.selectNotPureNotImportedWithMissingMaxLength(wrappers);
        if (!typesWithMissingMaxLength.isEmpty())
        {
            Map<String, TableInfo> tableInfo = this.generateTableInfo(typesWithMissingMaxLength);
            this.fixMaxLengthAndAddTruncate(typesWithMissingMaxLength, tableInfo);
        }
    }

    public void addConfiguredMithraImport(MithraGeneratorImport imp)
    {
        this.imports.add(imp);
    }


    private Map<String, TableInfo> generateTableInfo(List<MithraObjectTypeWrapper> wrappers)
    {
        List<String> tableNames = new ArrayList();
        for (int i = 0; i < wrappers.size(); i++)
        {
            tableNames.add(wrappers.get(i).getDefaultTable());
        }
        this.xmlGenerator.setIncludeList(tableNames);
        this.xmlGenerator.setGeneratedPackageName("test");
        return this.xmlGenerator.processTableInfo();
    }

    private List<Attribute> validateForAmbiguousMaxLen(List<MithraObjectTypeWrapper> wrappersWithMissingMaxLength, Map<String, TableInfo> tableInfo)
    {
        final Map<Attribute, Set<Integer>> superClassAttributeMaxLen = new HashMap();
        for (Iterator<MithraObjectTypeWrapper> iterator = wrappersWithMissingMaxLength.iterator(); iterator.hasNext();)
        {
            MithraObjectTypeWrapper wrapper = iterator.next();
            Attribute[] attributes = wrapper.getAttributes();
            for (int i = 0; i < attributes.length; i++)
            {
                Attribute attribute = attributes[i];
                validateMaxLenForAttribute(wrapper, attribute, tableInfo, superClassAttributeMaxLen);
            }
        }
        return selectViolations(superClassAttributeMaxLen);
    }


    private Set<String> getDetailsForAttribute(Attribute attribute, List<MithraObjectTypeWrapper> wrappers)
    {
        Set<String> classNames = new HashSet();
        for (int i = 0; i < wrappers.size(); i ++)
        {
            MithraObjectTypeWrapper wrapper = wrappers.get(i);
            Attribute[] attributes = wrapper.getAttributes();
            for (int j = 0; j < attributes.length; j++)
            {
                Attribute nextAttribute = attributes[j];
                if (this.getKeyAttribute(nextAttribute).equals(attribute))
                {
                    classNames.add(this.createDisplayString(wrapper.getClassName(), attribute.getName()));
                }
            }
        }
        return classNames;
    }

    private void validateMaxLenForAttribute(MithraObjectTypeWrapper wrapper,
                                            Attribute attribute,
                                            Map<String, TableInfo> tableInfo,
                                            Map<Attribute, Set<Integer>> superClassAttributeMaxLen)
    {
        Attribute key = this.getKeyAttribute(attribute);
        if (isMissingMaxLen(attribute) && isDifferentOwner(attribute))
        {
            TableInfo info = tableInfo.get(wrapper.getDefaultTable());
            if (info != null)
            {
                ColumnInfo columnInfo = info.findColumnInfo(attribute.getPlainColumnName());
                if (columnInfo != null)
                {
                    int columnLen = columnInfo.getColumnSize();
                    Set<Integer> maxLen = superClassAttributeMaxLen.get(key);
                    if (maxLen == null)
                    {
                        maxLen = new HashSet();
                        superClassAttributeMaxLen.put(key, maxLen);
                    }
                    maxLen.add(columnLen);
                }
            }
        }
    }

    private Attribute getKeyAttribute(Attribute attribute)
    {
        Attribute key = attribute;
        if (attribute.getOriginalOwner() != null)
        {
            key = attribute.getOriginalOwner().getAttributeByName(attribute.getName());
        }
        return key;
    }

    private boolean isDifferentOwner(Attribute attribute)
    {
        return !extractOwnerClassName(attribute).equals(attribute.getOwner().getClassName());
    }

    private List<Attribute> selectViolations(Map<Attribute, Set<Integer>> superClassAttributeMaxLen)
    {
        final List<Attribute> violations = new ArrayList();

        for (Iterator<Attribute> iterator = superClassAttributeMaxLen.keySet().iterator(); iterator.hasNext();)
        {
            Attribute attribute = iterator.next();
            Set<Integer> maxLenFromDifferentTables = superClassAttributeMaxLen.get(attribute);
            if (maxLenFromDifferentTables.size() > 1)
            {
                violations.add(attribute);
            }
        }
        return violations;
    }

    private List<String> validateForMissingTables(List<MithraObjectTypeWrapper> wrappersWithMissingMaxLength, Collection<String> tableNames)
    {
        List<String> missingTableNames = new ArrayList();
        for (int i = 0; i < wrappersWithMissingMaxLength.size(); i++)
        {
            MithraObjectTypeWrapper nextWrapper = wrappersWithMissingMaxLength.get(i);
            if (!tableNames.contains(nextWrapper.getDefaultTable()))
            {
                missingTableNames.add(nextWrapper.getClassName());
            }
        }
        return missingTableNames;
    }

    private void fixMaxLengthAndAddTruncate(List<MithraObjectTypeWrapper> wrappersWithMissingMaxLength, Map<String, TableInfo> tableInfo)
    {
        final List<Attribute> ambiguousMaxLenViolations = this.validateForAmbiguousMaxLen(wrappersWithMissingMaxLength, tableInfo);

        final Collection<String> missingTablesViolations = this.validateForMissingTables(wrappersWithMissingMaxLength, tableInfo.keySet());
        AddMaxLenAndTruncateTrueBlock block = new AddMaxLenAndTruncateTrueBlock(tableInfo, ambiguousMaxLenViolations);

        for (int i = 0; i < wrappersWithMissingMaxLength.size(); i++)
        {
            MithraObjectTypeWrapper wrapper = wrappersWithMissingMaxLength.get(i);
            if (!missingTablesViolations.contains(wrapper.getClassName()))
            {
                List<Attribute> missingLenAttributes = selectMissingMaxLenAttributes(wrapper.getAttributes());

                for (int j = 0; j < missingLenAttributes.size(); j++)
                {
                    block.execute(missingLenAttributes.get(j), wrapper);
                }
            }
        }
        this.checkForExceptions(ambiguousMaxLenViolations,
                                missingTablesViolations,
                                block.getAttributesNotFound(),
                                block.getDbColumnNotFound(),
                                wrappersWithMissingMaxLength);
    }

    private String createDisplayString(String one, String two)
    {
        return one + ";" + two;
    }

    private List<Attribute> selectMissingMaxLenAttributes(Attribute[] attributes)
    {
        List<Attribute> resultAttributes = new ArrayList();
        for (int i = 0; i < attributes.length; i++)
        {
            Attribute nextAttribute = attributes[i];
            if (isMissingMaxLen(nextAttribute))
            {
                resultAttributes.add(nextAttribute);
            }
        }
        return resultAttributes;
    }

    private void checkForExceptions(Collection<Attribute> ambiguousMaxLenViolations,
                                    Collection<String> missingTablesViolations,
                                    Collection<String> attributesNotFound,
                                    Collection<String> dbColumnNotFound,
                                    List<MithraObjectTypeWrapper> wrappersWithMissingMaxLength)
    {
        String errors = "";

        if (!ambiguousMaxLenViolations.isEmpty())
        {
            List<String> formattedErrors = new ArrayList();
            for (Iterator<Attribute> iterator = ambiguousMaxLenViolations.iterator(); iterator.hasNext();)
            {
                Attribute nextAttribute = iterator.next();
                formattedErrors.add(createDisplayString("Problematic class: " + nextAttribute.getOwner().getClassName(),
                                                        "Classes that might've contributed to conflicting len: " + getDetailsForAttribute(nextAttribute, wrappersWithMissingMaxLength).toString()+ "\n"));
            }
            errors += "The following objects are sourced from tables that have different max len for the same attribute.\n"
                      + formattedErrors + "\n\n";
        }

        if (!missingTablesViolations.isEmpty())
        {
            errors += " Didn't find table(s) in database for the following object(s):\n " + missingTablesViolations + "\n\n";
        }

        if (!attributesNotFound.isEmpty())
        {
            errors += " Didn't find attributes in mithra xml for:\n" + attributesNotFound + "\n\n";
        }

        if (!dbColumnNotFound.isEmpty())
        {
            errors += " Didn't find db columns for:\n" + dbColumnNotFound;
        }

        if (errors.length() > 0)
        {
            throw new BuildException(errors);
        }
    }


    private Map<String, MithraObjectTypeWrapper> parseXmlAndGetMithraObjects(String xml)
    {
        MithraGenerator mithraGenerator = new MithraGenerator();
        mithraGenerator.setXml(xml);
        for (int i = 0; i < this.imports.size(); i ++)
        {
            mithraGenerator.addConfiguredMithraImport(this.imports.get(i));
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


    private List<MithraObjectTypeWrapper> selectNotPureNotImportedWithMissingMaxLength(Collection<MithraObjectTypeWrapper> values)
    {
        List<MithraObjectTypeWrapper> typesWithMissingMaxLen = new ArrayList();
        for (Iterator<MithraObjectTypeWrapper> iterator = values.iterator(); iterator.hasNext();)
        {
            MithraObjectTypeWrapper wrapper = iterator.next();
            if (!wrapper.isPure() && !wrapper.isImported() && !wrapper.isTablePerSubclassSuperClass() && this.hasMissingLenType(wrapper.getAttributes()))
            {
                typesWithMissingMaxLen.add(wrapper);
            }
        }
        return typesWithMissingMaxLen;
    }

    private boolean hasMissingLenType(Attribute[] attributes)
    {
        for (int i = 0; i < attributes.length; i++)
        {
            Attribute attribute = attributes[i];
            if (isMissingMaxLen(attribute))
            {
                return true;
            }
        }
        return false;
    }

    private boolean isMissingMaxLen(Attribute attribute)
    {
        return attribute.getType() instanceof StringJavaType && attribute.getMaxLength() == 0;
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

    private class AddMaxLenAndTruncateTrueBlock
    {
        private final Map<String, TableInfo> tableInfo;
        private final List<Attribute> ambiguousMaxLenViolations;

        final List<String> attributesNotFound = new ArrayList();
        final List<String> dbColumnNotFound = new ArrayList();
        final Set<Attribute> fixedAttributes = new HashSet();


        public AddMaxLenAndTruncateTrueBlock(Map<String, TableInfo> tableInfo, List<Attribute> ambiguousMaxLenViolations)
        {
            this.tableInfo = tableInfo;
            this.ambiguousMaxLenViolations = ambiguousMaxLenViolations;
        }

        public List<String> getAttributesNotFound()
        {
            return attributesNotFound;
        }

        public List<String> getDbColumnNotFound()
        {
            return dbColumnNotFound;
        }

        public void execute(Attribute attribute, MithraObjectTypeWrapper missingObjectTypeWrapper)
        {
            try
            {
                TableInfo info = this.tableInfo.get(missingObjectTypeWrapper.getDefaultTable());
                ColumnInfo columnInfo = info.findColumnInfo(attribute.getPlainColumnName());
                String attributeName = attribute.getName();

                if (columnInfo == null)
                {
                    this.dbColumnNotFound.add(createDisplayString(extractOwnerClassName(attribute), attributeName));
                    return;
                }


                Attribute keyAttribute = getKeyAttribute(attribute);
                if (!this.ambiguousMaxLenViolations.contains(keyAttribute) && !this.fixedAttributes.contains(keyAttribute))
                {
                    this.fixedAttributes.add(keyAttribute);
                    String columnName = "\"" + columnInfo.getColumnName() + "\"";
                    String fileName = this.createFileName(attribute);
                    String fileAsString = readFile(fileName);
                    if (fileAsString.contains(columnName))
                    {
                        fileAsString = fileAsString.replaceFirst(columnName, columnName + " maxLength=\"" + columnInfo.getColumnSize() + "\"" + " truncate=\"true\"");
                        writeFile(fileName, fileAsString);
                    }
                    else
                    {
                        this.attributesNotFound.add(createDisplayString(extractOwnerClassName(attribute), attributeName));
                    }
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        private String createFileName(Attribute missingLenAttribute)
        {
            return new File(getXml()).getParent() + File.separator + extractOwnerClassName(missingLenAttribute) + ".xml";
        }
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
