
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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraDatabaseObject;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SourceAttributeType;
import com.gs.fw.common.mithra.util.fileparser.BinaryCompressor;
import com.gs.fw.common.mithra.util.fileparser.MithraDelimitedDataParser;
import com.gs.fw.common.mithra.util.fileparser.MithraParsedData;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.Format;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TestDatabaseConfiguration
{
    private static final Logger logger = LoggerFactory.getLogger(TestDatabaseConfiguration.class.getName());

    private String databaseName;
    private Object sourceId;
    private Class sourceAttributeType;
    private List<TestDataFile> testDataFiles;
    private List<MithraDatabaseObject> databaseObjects;
    private Set<String> createdTableNames;
    private boolean configured;
    protected static final Class[] NO_PARAMS = {};
    protected static final Object[] NO_ARGS = {};
    private boolean shutdown;
    private boolean enableStrictParsing;
    private Charset charset;

    public TestDatabaseConfiguration(String databaseName, Object sourceId, Class sourceAttributeType)
    {
        this(databaseName, sourceId, sourceAttributeType, false);
    }

    public TestDatabaseConfiguration(String databaseName, Object sourceId, Class sourceAttributeType, boolean enableStrictParsing)
    {
        this.databaseName = databaseName;
        this.sourceId = sourceId;
        this.sourceAttributeType = sourceAttributeType;
        this.testDataFiles = new ArrayList<TestDataFile>(3);
        this.enableStrictParsing = enableStrictParsing;
    }

    public void setCharset(Charset charset)
    {
        this.charset = charset;
    }

    protected static Logger getLogger()
    {
        return logger;
    }

    private boolean isConfigured()
    {
        return configured;
    }

    public void setConfigured(boolean configured)
    {
        this.configured = configured;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
    }

    public Set<String> getCreatedTableNames()
    {
        return createdTableNames;
    }

    public void setCreatedTableNames(Set<String> createdTableNames)
    {
        this.createdTableNames = createdTableNames;
    }

    public List<MithraDatabaseObject> getDatabaseObjects()
    {
        return databaseObjects;
    }

    public void setDatabaseObjects(List<MithraDatabaseObject> databaseObjects)
    {
        this.databaseObjects = databaseObjects;
    }

    public void addMoreDatabaseObjects(MithraTestResource mtr, List<MithraDatabaseObject> databaseObjects)
    {
        if(this.databaseObjects == null)
        {
            this.databaseObjects = new ArrayList<MithraDatabaseObject>();
        }
        this.databaseObjects.addAll(databaseObjects);
        this.createTables(mtr, databaseObjects);
    }

    public Class getSourceAttributeType()
    {
        return sourceAttributeType;
    }

    public void setSourceAttributeType(Class sourceAttributeType)
    {
        this.sourceAttributeType = sourceAttributeType;
    }

    public Object getSourceId()
    {
        return sourceId;
    }

    public void setSourceId(Object sourceId)
    {
        this.sourceId = sourceId;
    }


    protected boolean isUsed(MithraTestResource mtr, String classname)
    {
        return mtr.isUsed(classname);
    }

    public TestDataFile parseTestDataStream(URL streamLocation, InputStream is)
    {
        TestDataFile parsedData = MithraTestResource.findObjectInList(new TestDataFile(streamLocation, null), testDataFiles);
        if (parsedData == null)
        {
            getLogger().debug("Parsing data from url: " + streamLocation.toString());
            List<MithraParsedData> results;
            if (streamLocation.toString().endsWith(".ccbf"))
            {
                BinaryCompressor compressor = new BinaryCompressor();
                results = compressor.decompress(streamLocation, is);
            }
            else
            {
                MithraTestDataParser parser = new MithraTestDataParser(streamLocation, is);
                parser.setCharset(this.charset);
                results = parser.getResults();
            }
            getLogger().debug("Finished parsing data from url: " + streamLocation.toString());
            parsedData = new TestDataFile(streamLocation, results);
            testDataFiles.add(parsedData);
        }
        return parsedData;
    }

    public TestDataFile parseTestDataFile(String testDataFilename)
    {
        TestDataFile parsedData = MithraTestResource.findObjectInList(new TestDataFile(testDataFilename, null), testDataFiles);
        if (parsedData == null)
        {
            getLogger().debug("Parsing data file: " + testDataFilename);
            List<MithraParsedData> results;
            if (testDataFilename.endsWith(".ccbf"))
            {
                BinaryCompressor binaryCompressor = new BinaryCompressor();
                results = binaryCompressor.decompress(testDataFilename);
            }
            else
            {
                MithraTestDataParser parser = new MithraTestDataParser(testDataFilename);
                parser.setCharset(this.charset);
                results = parser.getResults();
            }
            getLogger().debug("Finished parsing data file: " + testDataFilename);
            parsedData = new TestDataFile(testDataFilename, results);
            testDataFiles.add(parsedData);
        }
        return parsedData;
    }

    private TestDataFile parseBcpTestDataFile(String bcpFilename, String delimiter, List<Attribute> attributes, Format dateFormat)
    {
        TestDataFile parsedData = MithraTestResource.findObjectInList(new TestDataFile(bcpFilename, null), testDataFiles);
        if (parsedData == null)
        {
            getLogger().debug("Parsing data file: " + bcpFilename);
            MithraDelimitedDataParser parser = new MithraDelimitedDataParser(bcpFilename, delimiter, attributes, dateFormat);
            List<MithraParsedData> results = parser.getResults();
            getLogger().debug("Finished parsing data file: " + bcpFilename);
            parsedData = new TestDataFile(bcpFilename, results);
            testDataFiles.add(parsedData);
        }
        return parsedData;

    }

    public void parseAndInsertTestData(MithraTestResource mtr, MithraTestConnectionManager testConnectionManager, URL streamLocation, InputStream is, boolean isSetupCompleted)
    {
        TestDataFile parsedData = this.parseTestDataStream(streamLocation, is);
        if(parsedData != null && !parsedData.isInserted())
        {
            if(isConfigured() && isSetupCompleted)
            {
                this.insertParsedData(parsedData, mtr);
            }
        }
    }

    public void parseAndInsertTestData(MithraTestResource mtr, MithraTestConnectionManager testConnectionManager, String testDataFilename, boolean isSetupCompleted)
    {
        TestDataFile parsedData = this.parseTestDataFile(testDataFilename);
        if(parsedData != null && !parsedData.isInserted())
        {
            if(isConfigured() && isSetupCompleted)
            {
                this.insertParsedData(parsedData, mtr);
            }
        }
    }

    public void parseAndInsertBcpTestData(MithraTestResource mtr,
            String bcpFilename, String delimiter, List<Attribute> attributes, Format dateFormat,
            boolean isSetupCompleted)
    {
        TestDataFile parsedData = this.parseBcpTestDataFile(bcpFilename, delimiter, attributes, dateFormat);
        if(parsedData != null && !parsedData.isInserted())
        {
            if(isConfigured() && isSetupCompleted)
            {
                this.insertParsedData(parsedData, mtr);
            }
        }
    }

    public void insertData(MithraTestConnectionManager connectionManager, MithraTestResource mtr)
    {
        for(int i = 0; i <  testDataFiles.size(); i++)
        {
            TestDataFile testDataFile = testDataFiles.get(i);
            if(mtr.getTestDataFilesInUse().contains(testDataFile.getFilename()) && !testDataFile.isInserted())
            {
                insertParsedData(testDataFile, mtr);
            }
        }
    }

    private void insertParsedData(TestDataFile testDataFile, MithraTestResource mtr)
    {
        List<MithraParsedData> parsedDataList = testDataFile.getParsedData();
        for(int i = 0; i < parsedDataList.size(); i++)
        {
            MithraParsedData mithraParsedData = parsedDataList.get(i);
            List attributes = mithraParsedData.getAttributes();
            List dataObjects = mithraParsedData.getDataObjects();
            String currentClassName = mithraParsedData.getParsedClassName();
            String finderClassname = currentClassName + "Finder";
            MithraObjectPortal mithraObjectPortal;

            if (mtr.isConfigured(currentClassName) || MithraManagerProvider.getMithraManager().getConfigManager().isClassConfigured(currentClassName))
            {
                Method method = this.getMethod(finderClassname, "getMithraObjectPortal", NO_PARAMS);
                mithraObjectPortal = (MithraObjectPortal) this.invokeMethod(method, null, NO_ARGS);

                MithraDatabaseObject databaseObject = mithraObjectPortal.getDatabaseObject();
                if(isCompatibleWithConnectionManager(mithraObjectPortal) && isUsed(mtr, currentClassName) && databaseObject != null && dataObjects.size() > 0)
                {
                    Method insertDataMethod = this.getMethod(databaseObject, "insertData", new Class[]{List.class, List.class, Object.class});
                    this.invokeMethod(insertDataMethod, databaseObject, new Object[]{attributes, dataObjects, this.sourceId});
                }
            }
        }
        testDataFile.setInserted(true);
    }

    // If strict parsing is enabled this method will filter out objects which have a sourceId on a sourceless connection manager and vice versa
    private boolean isCompatibleWithConnectionManager(MithraObjectPortal mithraObjectPortal)
    {
        return !this.enableStrictParsing || this.sourceId == null ^ mithraObjectPortal.getFinder().getSourceAttribute() != null;
    }

    private Set<String> getSchemaNames(List<MithraDatabaseObject> databaseObjects)
    {
        Set<String> schemaNames = new UnifiedSet();

        if(databaseObjects == null)
        {
            return schemaNames;
        }

        for(int i = 0; i < databaseObjects.size(); i++)
        {
            MithraDatabaseObject databaseObject = databaseObjects.get(i);
            String defaultSchema = databaseObject.getDefaultSchema();
            if(defaultSchema != null)
                schemaNames.add(defaultSchema);
        }
        return schemaNames;
    }

    public void recreateTables(MithraTestConnectionManager connectionManager, MithraTestResource mtr)
    {
        if (this.createdTableNames != null)
        {
            this.createdTableNames.clear();
        }
        createTables(connectionManager, mtr);
    }

    public void createTables(MithraTestConnectionManager connectionManager, MithraTestResource mtr)
    {
        List<MithraDatabaseObject> loadedDatabaseObjects
                =  mtr.getDatabaseObjectPerConnectionManager().get(connectionManager);
        if(this.createdTableNames == null)
        {
            createdTableNames = new UnifiedSet();
        }

        if(this.databaseObjects == null)
        {
            databaseObjects = new ArrayList<MithraDatabaseObject>();
        }

        Set<String> schemaNames = this.getSchemaNames(loadedDatabaseObjects);
        for(Iterator<String> it = schemaNames.iterator(); it.hasNext();)
        {
            String schemaName = it.next();
            if(sourceId == null)
                connectionManager.createSchema(databaseName, schemaName, null);
            else
                connectionManager.createSchema(""+sourceId, schemaName, sourceAttributeType);
        }

        createTables(mtr, loadedDatabaseObjects);
        this.setConfigured(true);

    }

    private void createTables(MithraTestResource mtr, List<MithraDatabaseObject> loadedDatabaseObjects)
    {
        if(loadedDatabaseObjects == null)
        {
            return;
        }

        for(int i = 0; i < loadedDatabaseObjects.size(); i++)
        {
            MithraDatabaseObject databaseObject = loadedDatabaseObjects.get(i);
            String classname = getClassNameFromDatabaseObject(databaseObject);
            if(isInvalidSourceIdForDatabaseObject(classname))
            {
                continue;
            }
            String tableName = databaseObject.getFullyQualifiedTableNameGenericSource(this.sourceId);
            if(!createdTableNames.contains(tableName) && isUsed(mtr, classname))
            {
                verifyAndCreateTestTable(mtr, databaseObject, classname);
            }
        }
    }

    private boolean isInvalidSourceIdForDatabaseObject(String classname)
    {
        String finderClassname = classname + "Finder";
        Method sourceAttributeTypeMethod = this.getMethod(finderClassname, "getSourceAttributeType", NO_PARAMS);
        SourceAttributeType sourceAttributeType = (SourceAttributeType) this.invokeMethod(sourceAttributeTypeMethod, null, NO_ARGS);

        return (this.sourceId != null && sourceAttributeType == null) ||
                sourceAttributeType != null && (sourceAttributeType.getSourceAttributeUnderlyingClass() != this.sourceAttributeType);
    }

    private void verifyAndCreateTestTable( MithraTestResource mtr, MithraDatabaseObject databaseObject, String classname)
    {
        Object mdbe;
        Method goodTableMethod = this.getMethod(databaseObject, "verifyTable",new Class[]{Object.class} );
        Boolean goodTable = (Boolean) this.invokeMethod(goodTableMethod, databaseObject, new Object[]{this.sourceId});

        if (!goodTable)
        {
            Method dropTestTableMethod = this.getMethod(databaseObject, "dropTestTable", new Class[]{Object.class});
            mdbe = this.invokeMethod(dropTestTableMethod, databaseObject, new Object[]{this.sourceId} );
            Method createTestTableMethod = this.getMethod(databaseObject, "createTestTable", new Class[]{Object.class});
            try
            {
                this.invokeMethod(createTestTableMethod, databaseObject, new Object[]{this.sourceId} );
            }
            catch(Exception e)
            {
                if (mdbe != null)
                {
                    getLogger().error("drop table failed with exception", (Throwable) mdbe);
                }
                getLogger().error("Failed to invoke createTestTable in " + databaseObject.getClass().getName(), e);
                throw new MithraException("Failed to invoke createTestTable in " + databaseObject.getClass().getName(), e);
            }
            getLogger().debug("Creating a table with databaseObject: " + databaseObject.getClass().getName() + " in database " + databaseName + " with sourceAttribute " + this.sourceId);
            addToReplicatedObjects(mtr, databaseObject);
        }
        this.databaseObjects.add(databaseObject);
        String tableName = databaseObject.getFullyQualifiedTableNameGenericSource(this.sourceId);
        createdTableNames.add(tableName);
    }

    protected void addToReplicatedObjects(MithraTestResource mtr, MithraDatabaseObject databaseObject)
    {
        if (databaseObject.isReplicated())
        {
            mtr.getReplicatedChildQueueTables().add(databaseObject);
        }
    }

    public void tearDownDatabase(MithraTestConnectionManager connectionManager, MithraTestResource mtr)
    {
        MithraTestConnectionManager dead = null;
        for(int i = 0; i < databaseObjects.size(); i++)
        {
            MithraDatabaseObject databaseObject = databaseObjects.get(i);
            String classname = getClassNameFromDatabaseObject(databaseObject);
            if(isUsed(mtr, classname))
            {
                if (databaseObject.getConnectionManager() != dead)
                {
                    Method deleteAllRowsMethod = this.getMethod(databaseObject, "deleteAllRowsFromTestTable",new Class[]{Object.class} );
                    try
                    {
                        this.invokeMethod(deleteAllRowsMethod, databaseObject, new Object[]{this.sourceId});
                    }
                    catch(Throwable t)
                    {
                        shutdown = true;
                        getLogger().error("Could not tear down "+classname, t);
                        dead = (MithraTestConnectionManager)databaseObject.getConnectionManager();
                        dead.fullyShutdown();
                        break;
                    }
                }
            }
        }

        for(int i = 0; i < testDataFiles.size(); i ++)
        {
            TestDataFile testDataFile = testDataFiles.get(i);
            testDataFile.setInserted(false);
        }
    }

    private static Method getMethod(Class underlyingObjectClass, String methodName, Class[] parameterTypes)
    {
        try
        {
            return underlyingObjectClass.getMethod(methodName,parameterTypes);
        }
        catch (NoSuchMethodException e)
        {
            getLogger().error("Class " + underlyingObjectClass.getName() + " does not have method "+methodName);
            throw new MithraException("Class " + underlyingObjectClass.getName() + " does not have method "+methodName, e);
        }

    }

    protected static Method getMethod(Object underlyingObject, String methodName, Class[] parameterTypes)
    {
        return getMethod(underlyingObject.getClass(), methodName,parameterTypes);
    }

    protected static Method getMethod(String classname, String methodName, Class[] parameterTypes)
    {
        try
        {
            return getMethod(Class.forName(classname), methodName, parameterTypes);
        }
        catch (ClassNotFoundException e)
        {
            getLogger().error("Could not find class " + classname);
            throw new MithraException("Could not find class " + classname, e);
        }
    }


    protected static Object invokeMethod(Method method, Object underlyingObject, Object[] arguments)
    {
        try
        {
            return method.invoke(underlyingObject, arguments);
        }
        catch (IllegalAccessException e)
        {
            getLogger().error("Could not access method "+method.getName()+" in class " + underlyingObject.getClass().getName());
            throw new MithraException("Could not access method "+method.getName()+" in class " + underlyingObject.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            getLogger().error("Exception during the invocation of "+method.getName()+" in class " + method.getDeclaringClass().getName(), e.getTargetException());
            throw new MithraException("Exception during the invocation of "+method.getName()+" in class " + method.getDeclaringClass().getName(), e.getTargetException());
        }
    }

    protected static String getClassNameFromDatabaseObject(MithraDatabaseObject databaseObject)
    {
        String classname = databaseObject.getClass().getName();
        int index = classname.lastIndexOf("DatabaseObject");
        classname = classname.substring(0, index);
        return classname;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TestDatabaseConfiguration that = (TestDatabaseConfiguration) o;

        if (!databaseName.equals(that.databaseName)) return false;
        if (sourceAttributeType != null ? !sourceAttributeType.equals(that.sourceAttributeType) : that.sourceAttributeType != null)
            return false;
        return !(sourceId != null ? !sourceId.equals(that.sourceId) : that.sourceId != null);

    }

    public int hashCode()
    {
        int result;
        result = databaseName.hashCode();
        result = 29 * result + (sourceId != null ? sourceId.hashCode() : 0);
        result = 29 * result + (sourceAttributeType != null ? sourceAttributeType.hashCode() : 0);
        return result;
    }

    public boolean isShutdown()
    {
        return shutdown;
    }

    public void setShutdown()
    {
        this.shutdown = true;
    }

    public boolean addToConnectionManager(MithraTestConnectionManager connectionManager)
    {
        return connectionManager.addConnectionManagerForSource(this.getSourceId(), this.getDatabaseName());
    }

    public Object getConnectionManagerSourceKey(ConnectionManagerForTests connectionManager)
    {
        return connectionManager.getConnectionManagerSourceKey(this.getSourceId(), this.getDatabaseName());
    }

    @Override
    public String toString()
    {
        return "TestDatabaseConfiguration{" +
                "databaseName='" + databaseName + '\'' +
                ", sourceId=" + sourceId +
                ", sourceAttributeType=" + sourceAttributeType +
                '}';
    }
}
