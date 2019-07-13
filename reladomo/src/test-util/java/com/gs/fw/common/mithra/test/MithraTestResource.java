
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

import com.gs.fw.common.mithra.DefaultJtaProvider;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatabaseObject;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectDeserializer;
import com.gs.fw.common.mithra.MithraObjectFactory;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraRuntimeConfig;
import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.MithraReferenceThread;
import com.gs.fw.common.mithra.cache.PartialUniqueIndex;
import com.gs.fw.common.mithra.connectionmanager.IntSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.database.MithraAbstractDatabaseObject;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.DerbyDatabaseType;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.mithraruntime.MithraRuntimeType;
import com.gs.fw.common.mithra.notification.MithraReplicatedDatabaseObject;
import com.gs.fw.common.mithra.notification.RunsMasterQueueDatabaseObject;
import com.gs.fw.common.mithra.notification.RunsMasterQueueFinder;
import com.gs.fw.common.mithra.notification.replication.ReplicationNotificationConnectionManager;
import com.gs.fw.common.mithra.transaction.LocalTm;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MithraConfigurationManager;
import com.gs.fw.common.mithra.util.MultiHashMap;
import com.gs.fw.common.mithra.util.fileparser.MithraParsedData;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MithraTestResource
{

    private static final Logger logger = LoggerFactory.getLogger(MithraTestResource.class.getName());
    private String configFileName = "";
    private static final Class[] NO_PARAMS = {};
    private static final Object[] NO_ARGS = {};

    public static final String ROOT_KEY = "mithra.xml.root";

    private Set<String> restrictedClasses;

    private static final PartialUniqueIndex testDataIndex = new PartialUniqueIndex("", new Extractor[]{new TestDataFile.FilenameExtractor()});
    private List<TestDataFile> testPureFilesInUse;

    private boolean deleteOnCreate = true;

    private List<MithraRuntimeConfig> mithraRuntimeList = new ArrayList<MithraRuntimeConfig>();
    private Set<String> configuredObjects = new UnifiedSet<String>();
    private DatabaseType databaseType = H2DatabaseType.getInstance();
    private boolean testConnectionsOnTearDown = false;
    private boolean isStrictParsingEnabled = false;
    private boolean validateConnectionManagers = Boolean.parseBoolean(System.getProperty("mithraTestResource.validateConnectionManagers", Boolean.TRUE.toString()));

    private static final Map<MithraTestConnectionManager, List<TestDatabaseConfiguration>> configuredDatabasesPerConnectionManager =
            new HashMap<MithraTestConnectionManager, List<TestDatabaseConfiguration>>();

    private final Map<MithraTestConnectionManager, List<MithraDatabaseObject>> databaseObjectPerConnectionManager =
            new HashMap<MithraTestConnectionManager, List<MithraDatabaseObject>>(3);

    private final Set<MithraTestConnectionManager> connectionManagersInUse = new UnifiedSet<MithraTestConnectionManager>(3);
    private final Set<MithraTestConnectionManager> additionalConnectionManagersInUse = new UnifiedSet<MithraTestConnectionManager>(3);
    private final Set<String> testDataFilesInUse = new UnifiedSet<String>(3);

    private static final Map<String, MithraRuntimeType> parsedConfigurations = new UnifiedMap<String, MithraRuntimeType>();

    protected List<MithraObjectPortal> portals;
    protected Map<String, List<MithraObject>> testData = new UnifiedMap<String, List<MithraObject>>();
    protected Charset charset;

    public boolean isSetUpCompleted()
    {
        return setUpCompleted;
    }

    private boolean setUpCompleted;
    private MithraRuntimeType runtimeType;
    private boolean runtimeInitialized;

    public MithraTestResource(String configFilename)
    {
        this(configFilename, H2DatabaseType.getInstance());
    }

    protected MithraTestResource(String configFilename, MithraConfigurationManager manager)
    {
        this(configFilename, H2DatabaseType.getInstance(), manager);
    }

    public MithraTestResource(String configFilename, DatabaseType databaseType)
    {
        this(configFilename, databaseType, null);
    }

    protected MithraTestResource(String configFilename, DatabaseType databaseType, MithraConfigurationManager manager)
    {
        this.initialize(databaseType, manager);
        if (configFilename == null)
        {
            throw new MithraException("Could not construct MihtraTestResourceInstance, filename can not be null");
        }
        this.configFileName = configFilename;
        getLogger().debug("config file " + configFilename);

        this.runtimeType = this.loadConfigXml(configFilename);
    }

    public MithraTestResource(MithraRuntimeType mithraRuntimeType)
    {
        this(mithraRuntimeType, H2DatabaseType.getInstance(), null);
    }

    public MithraTestResource(MithraRuntimeType mithraRuntimeType, MithraConfigurationManager manager)
    {
        this(mithraRuntimeType, H2DatabaseType.getInstance(), manager);
    }

    public MithraTestResource(MithraRuntimeType mithraRuntimeType, DatabaseType databaseType, MithraConfigurationManager manager)
    {
        this.initialize(databaseType, manager);
        this.runtimeType = mithraRuntimeType;
    }

    public void setCharset(Charset charset)
    {
        this.charset = charset;
    }

    private void initializeRuntime()
    {
        if (!this.runtimeInitialized)
        {
            this.runtimeInitialized = true;
            initializeRuntimeConfig(this.runtimeType);
        }
    }

    private void initialize(DatabaseType databaseType, MithraConfigurationManager manager)
    {
        this.setDatabaseType(databaseType);
        try
        {
            startTestDatabaseServer(databaseType);
            this.initializeMithraManager(manager);
        }
        catch (Exception e)
        {
            getLogger().error("Unable to initialize MithraTestResource", e);
            throw new MithraException("Unable to initialize MithraTestResource", e);
        }
    }

    protected void startTestDatabaseServer(DatabaseType databaseType)
    {
        if (databaseType instanceof DerbyDatabaseType)
        {
            DerbyServer.getInstance().startDerbyServer();
        }
        else if (databaseType instanceof H2DatabaseType)
        {
            H2DbServer.getInstance().startH2DbServer();
        }
    }

    protected static Logger getLogger()
    {
        return logger;
    }

    public void setDeleteOnCreate(boolean deleteOnCreate)
    {
        this.deleteOnCreate = deleteOnCreate;
    }

    public boolean isDeleteOnCreate()
    {
        return this.deleteOnCreate;
    }

    public static List<MithraDatabaseObject> getReplicatedChildQueueTables()
    {
        return replicatedChildQueueTables;
    }

    public Set<String> getTestDataFilesInUse()
    {
        return testDataFilesInUse;
    }

    public Map<MithraTestConnectionManager, List<MithraDatabaseObject>> getDatabaseObjectPerConnectionManager()
    {
        return databaseObjectPerConnectionManager;
    }

    public void setDatabaseType(DatabaseType databaseType)
    {
        this.databaseType = databaseType;
    }

    public void setStrictParsingEnabled(boolean strictParsingEnabled)
    {
        this.isStrictParsingEnabled = strictParsingEnabled;
    }

    @Deprecated
    public void setIgnoreUnconfiguredObjects(boolean ignoreUnconfiguredObjects)
    {
        // not used
    }

    public void setValidateConnectionManagers(boolean validateConnectionManagers)
    {
        this.validateConnectionManagers = validateConnectionManagers;
    }

    public DatabaseType getDatabaseType()
    {
        return databaseType;
    }

    public void setTestConnectionsOnTearDown(boolean testConnectionsOnTearDown)
    {
        this.testConnectionsOnTearDown = testConnectionsOnTearDown;
    }

    public void setUp()
    {
        initializeRuntime();
        logger.debug(System.identityHashCode(this) + " MithraTestResource set up with: " + this.configFileName);
        MithraRuntimeConfig mithraRuntimeConfig;
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        configureTransactionManager(mithraManager);

        for (int i = 0; i < mithraRuntimeList.size(); i++)
        {
            mithraRuntimeConfig = mithraRuntimeList.get(i);
            MithraTestConnectionManager connectionManager = (MithraTestConnectionManager) mithraRuntimeConfig.getConnectionManager();
            if (connectionManager != null && !connectionManagersInUse.contains(connectionManager))
            {
                additionalConnectionManagersInUse.add(connectionManager);
            }
        }

        removeRestrictedClassesFromConfig();

        this.setUpDatabases();

        this.setUpPortals();

        try
        {
            if (this.restrictedClasses != null)
            {
                Set<String> notInUse = UnifiedSet.newSet();

                for (Iterator<MithraObjectPortal> it = portals.iterator(); it.hasNext(); )
                {
                    String classname = getClassNameFromFinder(it.next().getFinder());
                    if (!isUsed(classname))
                    {
                        notInUse.add(classname);
                        it.remove();
                    }
                }

                if(!notInUse.isEmpty())
                {
                    mithraManager.cleanUpRuntimeCacheControllers(notInUse);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Exception during MithraTestResource setup", e);
            throw new RuntimeException("Exception during MithraTestResource setup", e);
        }
        for (Iterator<List<MithraObject>> it = this.testData.values().iterator(); it.hasNext(); )
        {
            this.insertTestData(it.next());
        }
        mithraManager.loadMithraCache(portals, 2);
        loadPureObjects();


        this.setSetUpCompleted(true);
    }

    private void setUpPortals()
    {
        Map<String, MithraObjectPortal> allPortals = UnifiedMap.newMap();
        for (MithraRuntimeConfig config : this.mithraRuntimeList)
        {
            for (MithraObjectPortal portal : config.getObjectPortals())
            {
                String key = portal.getFinder().getFinderClassName();
                MithraObjectPortal existingPortal = allPortals.get(key);
                if (this.validateConnectionManagers && (existingPortal != null && !portalsHaveSameConnectionManager(portal, existingPortal)))
                {
                    throw new IllegalStateException(key + " must use same connection managers in all configurations");
                }
                allPortals.put(key, portal);
            }
        }
        this.portals = FastList.newList(allPortals.values());
    }

    private boolean portalsHaveSameConnectionManager(MithraObjectPortal thisPortal, MithraObjectPortal thatPortal)
    {
        Object thisConnectionManager = getConnectionManagerFromPortal(thisPortal);
        Object thatConnectionManager = getConnectionManagerFromPortal(thatPortal);
        return thisConnectionManager == null ? thatConnectionManager == null : thisConnectionManager.equals(thatConnectionManager);
    }

    private static Object getConnectionManagerFromPortal(MithraObjectPortal portal)
    {
        if (portal.getMithraObjectDeserializer() instanceof MithraDatabaseObject)
        {
            return portal.getDatabaseObject().getConnectionManager();
        }
        return null;
    }

    private void removeRestrictedClassesFromConfig()
    {
        if (this.restrictedClasses != null)
        {
            for (Iterator<String> it = this.configuredObjects.iterator(); it.hasNext(); )
            {
                if (!isUsed(it.next())) it.remove();
            }
        }
    }

    private void addConfigToConfiguredObjects(MithraRuntimeConfig mithraRuntimeConfig)
    {
        List<MithraConfigurationManager.Config> configs = mithraRuntimeConfig.getConfigs();
        for (MithraConfigurationManager.Config cfg : configs)
        {
            String className = cfg.getClassName();
            if (isUsed(className))
            {
                this.configuredObjects.add(className);
            }
        }
    }

    public boolean isConfigured(String className)
    {
        initializeRuntime();
        return this.configuredObjects.contains(className) && isUsed(className);
    }

    private void loadPureObjects()
    {
        if (testPureFilesInUse != null)
        {
            for (int i = 0; i < testPureFilesInUse.size(); i++)
            {
                insertPureParsedData(testPureFilesInUse.get(i));
            }
        }
    }

    public void addTestDataForPureObjects(String testDataFilename)
    {
        initializeRuntime();
        TestDataFile testDataFile = (TestDataFile) testDataIndex.get(testDataFilename);
        if (testDataFile == null)
        {
            getLogger().debug("Parsing data file: " + testDataFilename);
            MithraTestDataParser parser = new MithraTestDataParser(testDataFilename);
            parser.setCharset(this.charset);
            List<MithraParsedData> results = parser.getResults();
            getLogger().debug("Finished parsing data file: " + testDataFilename);
            testDataFile = new TestDataFile(testDataFilename, results);
            testDataIndex.put(testDataFile);
        }
        if (testPureFilesInUse == null)
        {
            testPureFilesInUse = new ArrayList<TestDataFile>();
        }
        this.testPureFilesInUse.add(testDataFile);
    }

    private void insertPureParsedData(TestDataFile testDataFile)
    {
        List<MithraParsedData> parsedDataList = testDataFile.getParsedData();
        for (int i = 0; i < parsedDataList.size(); i++)
        {
            MithraParsedData mithraParsedData = parsedDataList.get(i);
            List attributes = mithraParsedData.getAttributes();
            List dataObjects = mithraParsedData.getDataObjects();
            String currentClassName = mithraParsedData.getParsedClassName();
            if (this.isConfigured(currentClassName) && dataObjects.size() > 0)
            {
                String finderClassname = currentClassName + "Finder";
                MithraObjectPortal mithraObjectPortal;

                Method method = getMethod(finderClassname, "getMithraObjectPortal", NO_PARAMS);
                mithraObjectPortal = (MithraObjectPortal) invokeMethod(method, null, NO_ARGS);

                MithraObjectDeserializer deserializer = mithraObjectPortal.getMithraObjectDeserializer();
                if (deserializer instanceof MithraObjectFactory)
                {
                    for (int j = 0; j < dataObjects.size(); j++)
                    {
                        mithraObjectPortal.getCache().getObjectFromData((MithraDataObject) dataObjects.get(j));
                    }
                }
                else
                {
                    AsOfAttribute[] asOfAttributes = mithraObjectPortal.getFinder().getAsOfAttributes();
                    Timestamp[] timestamp = new Timestamp[asOfAttributes.length];
                    for (int j = 0; j < dataObjects.size(); j++)
                    {
                        MithraDataObject data = (MithraDataObject) dataObjects.get(j);
                        for (int t = 0; t < asOfAttributes.length; t++)
                        {
                            if (asOfAttributes[t].isToIsInclusive())
                            {
                                timestamp[t] = asOfAttributes[t].getToAttribute().timestampValueOf(data);
                            }
                            else
                            {
                                timestamp[t] = asOfAttributes[t].getFromAttribute().timestampValueOf(data);
                            }
                        }
                        mithraObjectPortal.getCache().getObjectFromData(data, timestamp);
                    }
                }
            }
        }
        testDataFile.setInserted(true);
    }

    private static Method getMethod(Class underlyingObjectClass, String methodName, Class[] parameterTypes)
    {
        try
        {
            return underlyingObjectClass.getMethod(methodName, parameterTypes);
        }
        catch (NoSuchMethodException e)
        {
            getLogger().error("Class " + underlyingObjectClass.getName() + " does not have method " + methodName);
            throw new MithraException("Class " + underlyingObjectClass.getName() + " does not have method " + methodName, e);
        }

    }

    protected static Method getMethod(Object underlyingObject, String methodName, Class[] parameterTypes)
    {
        return getMethod(underlyingObject.getClass(), methodName, parameterTypes);
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
            getLogger().error("Could not access method " + method.getName() + " in class " + underlyingObject.getClass().getName());
            throw new MithraException("Could not access method " + method.getName() + " in class " + underlyingObject.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            getLogger().error("Exception during the invocation of " + method.getName() + " in class " + underlyingObject.getClass().getName(), e);
            throw new MithraException("Exception during the invocation of " + method.getName() + " in class " + underlyingObject.getClass().getName(), e);
        }
    }

    private void configureTransactionManager(MithraManager mithraManager)
    {
        mithraManager.setJtaTransactionManagerProvider(new DefaultJtaProvider(new LocalTm()));
    }

    public boolean isUsed(String classname)
    {
        return restrictedClasses == null || restrictedClasses.contains(classname);
    }

    private void setSetUpCompleted(boolean setUpCompleted)
    {
        this.setUpCompleted = setUpCompleted;
    }

    private void setUpDatabases()
    {
        for (Iterator<MithraTestConnectionManager> it = additionalConnectionManagersInUse.iterator(); it.hasNext(); )
        {
            MithraTestConnectionManager connectionManager = it.next();
            connectionManager.setUpDatabases(this, true);
        }

        for (Iterator<MithraTestConnectionManager> it = connectionManagersInUse.iterator(); it.hasNext(); )
        {
            MithraTestConnectionManager connectionManager = it.next();
            connectionManager.setUpDatabases(this, false);
        }
        connectionManagersInUse.addAll(additionalConnectionManagersInUse);
    }

    public void tearDown()
    {
        logger.debug(System.identityHashCode(this) + " MithraTestResource tear down");
        MithraManager mithra = MithraManagerProvider.getMithraManager();
        try
        {
            while (mithra.isInTransaction())
            {
                logger.error("incomplete transaction. attempting rollback");
                mithra.getCurrentTransaction().rollback();
            }
        }
        catch (MithraTransactionException e)
        {
            logger.error("rollback failed. subsequent tests may cascade fail", e);
        }
        if (testConnectionsOnTearDown)
        {
            if (!ConnectionManagerForTests.getInstance().ensureAllConnectionsReturnedToPool())
            {
                logger.error("all connections were not returned to the pool", new Exception("for tracing"));
            }
        }
        this.tearDownDatabases();
        mithra.cleanUpPrimaryKeyGenerators();
        mithra.cleanUpRuntimeCacheControllers(this.configuredObjects);
        databaseObjectPerConnectionManager.clear(); //todo: make this an instance field.
        connectionManagersInUse.clear();
        tearDownPureObjects();
        restrictedClasses = null;
        testPureFilesInUse = null;
        mithraRuntimeList = null;
        this.configuredObjects.clear();
        additionalConnectionManagersInUse.clear();
        testDataFilesInUse.clear();
        if (portals != null)
        {
            portals.clear();
        }
        MithraReferenceThread.getInstance().runNow();
    }

    private void tearDownPureObjects()
    {
        if (testPureFilesInUse != null)
        {
            for (int i = 0; i < testPureFilesInUse.size(); i++)
            {
                List<MithraParsedData> parsedDataList = testPureFilesInUse.get(i).getParsedData();
                for (int p = 0; p < parsedDataList.size(); p++)
                {
                    MithraParsedData mithraParsedData = parsedDataList.get(p);
                    String className = mithraParsedData.getParsedClassName();
                    try
                    {
                        resetPortal(className);
                    }
                    catch (Exception e)
                    {
                        logger.error("Could not tear down portal for class " + className, e);
                    }
                }
            }
        }
    }

    private void tearDownDatabases()
    {
        for (Iterator<MithraTestConnectionManager> it = connectionManagersInUse.iterator(); it.hasNext(); )
        {
            MithraTestConnectionManager connectionManager = it.next();
            connectionManager.tearDownDatabases(this);
        }
    }

    public void createDatabaseForStringSourceAttributeWithTableSharding(ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        createDatabaseForStringSourceAttribute(connectionManager, sourceAttribute, null, true);
    }

    public void createDatabaseForStringSourceAttributeWithTableSharding(ObjectSourceConnectionManager connectionManager, String sourceAttribute, String testDataFilename)
    {
        this.createDatabaseForStringSourceAttribute(connectionManager, sourceAttribute, testDataFilename, true);
    }

    public void createDatabaseForStringSourceAttribute(ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        createDatabaseForStringSourceAttribute(connectionManager, sourceAttribute, null, false);
    }

    public void createDatabaseForStringSourceAttribute(ObjectSourceConnectionManager connectionManager, String sourceAttribute, String testDataFilename)
    {
        createDatabaseForStringSourceAttribute(connectionManager, sourceAttribute, testDataFilename, false);
    }

    private void createDatabaseForStringSourceAttribute(ObjectSourceConnectionManager connectionManager, String sourceAttribute, String testDataFilename, boolean isTableSharding)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(this.getConnectionManagerIdentifier(testConnectionManager, sourceAttribute, isTableSharding), sourceAttribute, String.class);

        createTestDatabase(testDbConfig, testConnectionManager, testDataFilename);
    }

    /**
     * @deprecated Use {@link MithraTestResource#createDatabaseForStringSourceAttribute(ObjectSourceConnectionManager connectionManager, String sourceAttribute, String testDataFilename)} instead.
     */
    public void createDatabaseForStringSourceAttribute(ObjectSourceConnectionManager connectionManager, String sourceAttribute, String resourceName, String testDataFilename)
    {
        initializeRuntime();
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(resourceName, sourceAttribute, String.class);

        createTestDatabase(testDbConfig, (MithraTestConnectionManager) connectionManager, testDataFilename);
    }

    public void createDatabaseForIntSourceAttribute(IntSourceConnectionManager connectionManager, int sourceAttribute)
    {
        this.createDatabaseForIntSourceAttribute(connectionManager, sourceAttribute, null, false);
    }

    public void createDatabaseForIntSourceAttribute(IntSourceConnectionManager connectionManager,
                                                    int sourceAttribute,
                                                    String testDataFilename)
    {
        this.createDatabaseForIntSourceAttribute(connectionManager, sourceAttribute, testDataFilename, false);
    }

    public void createDatabaseForIntSourceAttributeWithTableSharding(IntSourceConnectionManager connectionManager, int sourceAttribute)
    {
        this.createDatabaseForIntSourceAttribute(connectionManager, sourceAttribute, null, true);
    }

    public void createDatabaseForIntSourceAttributeWithTableSharding(IntSourceConnectionManager connectionManager,
                                                    int sourceAttribute,
                                                    String testDataFilename)
    {
        this.createDatabaseForIntSourceAttribute(connectionManager, sourceAttribute, testDataFilename, true);
    }

    private void createDatabaseForIntSourceAttribute(IntSourceConnectionManager connectionManager, int sourceAttribute, String testDataFilename, boolean isTableSharding)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(this.getConnectionManagerIdentifier(testConnectionManager, sourceAttribute, isTableSharding), Integer.valueOf(sourceAttribute), Integer.TYPE);

        createTestDatabase(testDbConfig, testConnectionManager, testDataFilename);
    }

    private String getConnectionManagerIdentifier(MithraTestConnectionManager testConnectionManager, Object sourceAttribute, boolean isTableSharding)
    {
        return isTableSharding ? testConnectionManager.getConnectionManagerIdentifier()
                               : testConnectionManager.getConnectionManagerIdentifier() + sourceAttribute;
    }

    private TestDatabaseConfiguration createTestDbConfig(String databaseName, Object sourceId, Class type)
    {
        TestDatabaseConfiguration configuration = new TestDatabaseConfiguration(databaseName, sourceId, type, this.isStrictParsingEnabled);
        configuration.setCharset(this.charset);
        return configuration;
    }

    /**
     * @deprecated Use {@link MithraTestResource#createDatabaseForIntSourceAttribute(IntSourceConnectionManager connectionManager, int sourceAttribute, String testDataFilename)} instead.
     */
    public void createDatabaseForIntSourceAttribute(IntSourceConnectionManager connectionManager,
                                                    int sourceAttribute,
                                                    String resourceName,
                                                    String testDataFilename)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(resourceName, Integer.valueOf(sourceAttribute), Integer.TYPE);

        createTestDatabase(testDbConfig, testConnectionManager, testDataFilename);
    }

    public void createSingleDatabase(SourcelessConnectionManager connectionManager)
    {
        createSingleDatabase(connectionManager, null);
    }

    public void createSingleDatabase(SourcelessConnectionManager connectionManager, String testDataFilename)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(testConnectionManager.getConnectionManagerIdentifier(), null, null);

        createTestDatabase(testDbConfig, testConnectionManager, testDataFilename);
    }

    public void createSingleDatabase(SourcelessConnectionManager connectionManager, URL streamLocation, InputStream is)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(testConnectionManager.getConnectionManagerIdentifier(), null, null);

        createTestDatabase(testDbConfig, testConnectionManager, streamLocation, is);
    }

    /**
     * @deprecated Use {@link MithraTestResource#createSingleDatabase(SourcelessConnectionManager connectionManager, String testDataFilename)} instead.
     */
    public void createSingleDatabase(SourcelessConnectionManager connectionManager, String resourceName, String testDataFilename)
    {
        initializeRuntime();
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(resourceName, null, null);

        createTestDatabase(testDbConfig, (MithraTestConnectionManager) connectionManager, testDataFilename);
    }

    private void createTestDatabase(TestDatabaseConfiguration testDbConfig,
                                    MithraTestConnectionManager testConnectionManager,
                                    String testDataFilename)
    {
        initializeRuntime();
        TestDatabaseConfiguration createdTestDatabaseConfiguration = registerDbConfigWithConnectionManager(testDbConfig, testConnectionManager);
        createdTestDatabaseConfiguration.createTables(testConnectionManager, this);

        if (testDataFilename != null)
        {
            createdTestDatabaseConfiguration.parseTestDataFile(testDataFilename);
            this.testDataFilesInUse.add(testDataFilename);
        }

        testConnectionManager.addTestDbConfiguration(createdTestDatabaseConfiguration);
        this.connectionManagersInUse.add(testConnectionManager);

    }

    private void createTestDatabase(TestDatabaseConfiguration testDbConfig,
                                    MithraTestConnectionManager testConnectionManager,
                                    URL streamLocation,
                                    InputStream is)
    {
        initializeRuntime();
        TestDatabaseConfiguration createdTestDatabaseConfiguration = registerDbConfigWithConnectionManager(testDbConfig, testConnectionManager);
        createdTestDatabaseConfiguration.createTables(testConnectionManager, this);

        if (streamLocation != null)
        {
            createdTestDatabaseConfiguration.parseTestDataStream(streamLocation, is);
            this.testDataFilesInUse.add(streamLocation.toString());
        }

        testConnectionManager.addTestDbConfiguration(createdTestDatabaseConfiguration);
        this.connectionManagersInUse.add(testConnectionManager);
    }

    private TestDatabaseConfiguration registerDbConfigWithConnectionManager(TestDatabaseConfiguration testDbConfig,
                                                                            MithraTestConnectionManager testConnectionManager)
    {
        List<TestDatabaseConfiguration> databasesPerConnectionManager = checkDatabasesPerConnectionManagerIfAbsentPutNew(testConnectionManager);
        int index = databasesPerConnectionManager.indexOf(testDbConfig);
        TestDatabaseConfiguration createdTestDatabaseConfiguration = index >= 0 ? databasesPerConnectionManager.get(index) : null;
        if (createdTestDatabaseConfiguration != null)
        {
            createdTestDatabaseConfiguration.setCharset(this.charset);
        }

        // Add the connection manager irrespective of any existing TestDatabaseConfiguration as it could have been cleared down by a previous fullyShutdown() event.
        boolean connectionManagerWasNotAlreadyRegistered = testDbConfig.addToConnectionManager(testConnectionManager);

        // Additionally, if the connection manager was not already registered then we want to ensure that any existing TestDatabaseConfiguration is replaced.
        // If we have an existing TestDatabaseConfiguration for a connection which was not already registered then the only known explanation is that the
        // connection has previously been shut down and de-registered by ConnectionManagerForTests.fullyShutdown(). This would mean that the database is now empty.
        // In this case, we need to ensure the new TestDatabaseConfiguration replaces any existing one so that all the tables will be recreated.
        if (createdTestDatabaseConfiguration == null || createdTestDatabaseConfiguration.isShutdown() || connectionManagerWasNotAlreadyRegistered)
        {
            if (createdTestDatabaseConfiguration != null)
            {
                databasesPerConnectionManager.set(index, testDbConfig);
            }
            else
            {
                databasesPerConnectionManager.add(testDbConfig);
            }
            createdTestDatabaseConfiguration = testDbConfig;
        }

        return createdTestDatabaseConfiguration;
    }

    public static <T> T findObjectInList(T key, List<T> list)
    {
        int size = list.size();
        for (int i = 0; i < size; i++)
        {
            T entry = list.get(i);
            if (entry.equals(key))
            {
                return entry;
            }
        }
        return null;
    }

    private List<TestDatabaseConfiguration> checkDatabasesPerConnectionManagerIfAbsentPutNew(MithraTestConnectionManager testConnectionManager)
    {
        List<TestDatabaseConfiguration> databasesPerConnectionManager = configuredDatabasesPerConnectionManager.get(testConnectionManager);
        if (databasesPerConnectionManager == null)
        {
            databasesPerConnectionManager = new ArrayList<TestDatabaseConfiguration>();
            configuredDatabasesPerConnectionManager.put(testConnectionManager, databasesPerConnectionManager);
        }
        return databasesPerConnectionManager;
    }

    /**
     * Load additional Mithra configuration to test resource.
     *
     * @param mithraConfigXml
     */
    public void loadMithraConfiguration(String mithraConfigXml)
    {
        MithraRuntimeType runtimeType = loadConfigXml(mithraConfigXml);
        this.loadMithraConfiguration(runtimeType);
    }

    public void loadMithraConfiguration(MithraRuntimeType runtimeType)
    {
        List<MithraRuntimeConfig> runtimeList = initializeRuntimeConfig(runtimeType);
        if (this.isSetUpCompleted())
        {
            for (int i = 0; i < runtimeList.size(); i++)
            {
                MithraRuntimeConfig mithraRuntimeConfig = runtimeList.get(i);
                MithraTestConnectionManager connectionManager = (MithraTestConnectionManager) mithraRuntimeConfig.getConnectionManager();
                if (!connectionManagersInUse.contains(connectionManager))
                {
                    additionalConnectionManagersInUse.add(connectionManager);
                }
                else
                {
                    connectionManager.addDatabaseObjectsToTestDatabases(mithraRuntimeConfig.getDatabaseObjects(), this);
                }
            }
        }
    }

    /**
     * @param testDataFilename
     * @param connectionManager
     * @param sourceAttribute
     */
    public void addTestDataToDatabase(String testDataFilename, ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        this.addTestDataToDatabase(testDataFilename, connectionManager, sourceAttribute, false);

    }

    public void addTestDataToDatabaseWithTableSharding(String testDataFilename, ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        this.addTestDataToDatabase(testDataFilename, connectionManager, sourceAttribute, true);

    }

    private void addTestDataToDatabase(String testDataFilename, ObjectSourceConnectionManager connectionManager, String sourceAttribute, boolean isTableSharding)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(this.getConnectionManagerIdentifier(testConnectionManager, sourceAttribute, isTableSharding),
                                        sourceAttribute, String.class);
        parseTestDataForTestDbConfig(testDataFilename, testDbConfig, testConnectionManager);
    }

    /**
     * @param testDataFilename
     * @param connectionManager
     * @param sourceAttribute
     */
    public void addTestDataToDatabase(String testDataFilename, IntSourceConnectionManager connectionManager, int sourceAttribute)
    {
        addTestDataToDatabase(testDataFilename, connectionManager, sourceAttribute, false);
    }

    public void addTestDataToDatabaseWithTableSharding(String testDataFilename, IntSourceConnectionManager connectionManager, int sourceAttribute)
    {
        addTestDataToDatabase(testDataFilename, connectionManager, sourceAttribute, true);
    }

    private void addTestDataToDatabase(String testDataFilename, IntSourceConnectionManager connectionManager, int sourceAttribute, boolean isTableSharding)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager)connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(this.getConnectionManagerIdentifier(testConnectionManager, sourceAttribute, isTableSharding),
                                        Integer.valueOf(sourceAttribute), Integer.TYPE);
        parseTestDataForTestDbConfig(testDataFilename, testDbConfig, testConnectionManager);
    }

    /**
     * @param testDataFilename
     * @param connectionManager
     */
    public void addTestDataToDatabase(String testDataFilename, SourcelessConnectionManager connectionManager)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(testConnectionManager.getConnectionManagerIdentifier(), null, null);
        parseTestDataForTestDbConfig(testDataFilename, testDbConfig, testConnectionManager);
        this.testDataFilesInUse.add(testDataFilename);
    }

    public void addTestDataToDatabase(URL streamLocation, InputStream is, ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        this.addTestDataToDatabase(streamLocation, is, connectionManager, sourceAttribute, false);

    }

    public void addTestDataToDatabaseWithTableSharding(URL streamLocation, InputStream is, ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        this.addTestDataToDatabase(streamLocation, is, connectionManager, sourceAttribute, true);

    }

    public void addTestDataToDatabase(URL streamLocation, InputStream is, IntSourceConnectionManager connectionManager, int sourceAttribute)
    {
        addTestDataToDatabase(streamLocation, is, connectionManager, sourceAttribute, false);
    }

    public void addTestDataToDatabaseWithTableSharding(URL streamLocation, InputStream is, IntSourceConnectionManager connectionManager, int sourceAttribute)
    {
        addTestDataToDatabase(streamLocation, is, connectionManager, sourceAttribute, true);
    }

    public void addTestDataToDatabase(URL streamLocation, InputStream is, SourcelessConnectionManager connectionManager)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(testConnectionManager.getConnectionManagerIdentifier(), null, null);
        parseTestDataForTestDbConfig(streamLocation, is, testDbConfig, testConnectionManager);
        this.testDataFilesInUse.add(streamLocation.toString());
    }

    private void addTestDataToDatabase(URL streamLocation, InputStream is, IntSourceConnectionManager connectionManager, int sourceAttribute, boolean isTableSharding)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager)connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(this.getConnectionManagerIdentifier(testConnectionManager, sourceAttribute, isTableSharding),
                        Integer.valueOf(sourceAttribute), Integer.TYPE);
        parseTestDataForTestDbConfig(streamLocation, is, testDbConfig, testConnectionManager);
    }

    private void addTestDataToDatabase(URL streamLocation, InputStream is, ObjectSourceConnectionManager connectionManager, String sourceAttribute, boolean isTableSharding)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(this.getConnectionManagerIdentifier(testConnectionManager, sourceAttribute, isTableSharding),
                        sourceAttribute, String.class);
        parseTestDataForTestDbConfig(streamLocation, is, testDbConfig, testConnectionManager);
    }

    private void parseTestDataForTestDbConfig(URL streamLocation, InputStream is, TestDatabaseConfiguration testDbConfig, MithraTestConnectionManager connectionManager)
    {
        List<TestDatabaseConfiguration> databasesPerConnectionManager = configuredDatabasesPerConnectionManager.get(connectionManager);
        if (databasesPerConnectionManager != null)
        {
            TestDatabaseConfiguration configuredTestDbConfig = findObjectInList(testDbConfig, databasesPerConnectionManager);
            if (configuredTestDbConfig != null)
            {
                configuredTestDbConfig.parseAndInsertTestData(this, connectionManager, streamLocation, is, this.isSetUpCompleted());
            }
        }
    }

    private void parseTestDataForTestDbConfig(String testDataFilename, TestDatabaseConfiguration testDbConfig, MithraTestConnectionManager connectionManager)
    {
        List<TestDatabaseConfiguration> databasesPerConnectionManager = configuredDatabasesPerConnectionManager.get(connectionManager);
        if (databasesPerConnectionManager != null)
        {
            TestDatabaseConfiguration configuredTestDbConfig = findObjectInList(testDbConfig, databasesPerConnectionManager);
            if (configuredTestDbConfig != null)
            {
                configuredTestDbConfig.parseAndInsertTestData(this, connectionManager, testDataFilename, this.isSetUpCompleted());
            }
        }
    }

    private void parseBcpTestDataForTestDbConfig(String bcpFilename, String delimiter, List<Attribute> attributes, Format dateFormat,
                                                 TestDatabaseConfiguration testDbConfig, MithraTestConnectionManager connectionManager)
    {
        List<TestDatabaseConfiguration> databasesPerConnectionManager = configuredDatabasesPerConnectionManager.get(connectionManager);
        if (databasesPerConnectionManager != null)
        {
            TestDatabaseConfiguration configuredTestDbConfig = findObjectInList(testDbConfig, databasesPerConnectionManager);
            if (configuredTestDbConfig != null)
            {
                configuredTestDbConfig.parseAndInsertBcpTestData(this, bcpFilename,
                                                                 delimiter, attributes, dateFormat, this.isSetUpCompleted());
            }
        }
    }

    private void resetPortal(String classname)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException
    {
        Class aClass = Class.forName(classname + "Finder");
        Method method = aClass.getMethod("zResetPortal", NO_PARAMS);
        method.invoke(null, NO_ARGS);
    }

    private static String getClassNameFromFinder(RelatedFinder finder)
    {
        String classname = finder.getClass().getName();
        int index = classname.indexOf("Finder$");
        classname = classname.substring(0, index);
        return classname;
    }

    private static String getClassNameFromDatabaseObject(MithraDatabaseObject databaseObject)
    {
        String classname = databaseObject.getClass().getName();
        int index = classname.lastIndexOf("DatabaseObject");
        classname = classname.substring(0, index);
        return classname;
    }

    protected InputStream getConfigXml(String fileName) throws FileNotFoundException
    {
        String xmlRoot = System.getProperty(ROOT_KEY);
        if (xmlRoot == null)
        {
            getLogger().debug("Could not find " + ROOT_KEY + " property. Will attempt to find " + fileName + " in classpath");
            InputStream result = this.getClass().getClassLoader().getResourceAsStream(fileName);
            if (result == null)
            {
                throw new RuntimeException("could not find " + fileName + " in classpath. Additionally, " + ROOT_KEY + " was not specified");
            }
            return result;
        }
        else
        {
            String fullPath = xmlRoot;
            if (!xmlRoot.endsWith(File.separator))
            {
                fullPath += File.separator;
            }
            return new FileInputStream(fullPath + fileName);
        }
    }

    private void initializeMithraManager(MithraConfigurationManager manager)
    {
        MithraManager mithra = MithraManagerProvider.getMithraManager();
        mithra.setTransactionTimeout(60); // reset to default
        this.initializeMithraManagerConfigManager(mithra, manager);
    }

    protected void initializeMithraManagerConfigManager(MithraManager mithra, MithraConfigurationManager manager)
    {
        if (manager != null)
        {
            mithra.setConfigManager(manager);
        }
        else
        {
            if (!mithra.getConfigManager().getClass().equals(MithraConfigurationManager.class))
            {
                mithra.setConfigManager(new MithraConfigurationManager());
            }
        }
    }

    private MithraRuntimeType loadConfigXml(String configXmlFilename)
    {
        try
        {
            MithraRuntimeType runtimeConfig = parsedConfigurations.get(configXmlFilename);
            if (runtimeConfig == null)
            {
                InputStream mithraXml = getConfigXml(configXmlFilename);
                runtimeConfig = MithraManagerProvider.getMithraManager().parseConfiguration(mithraXml);
                mithraXml.close();
                parsedConfigurations.put(configXmlFilename, runtimeConfig);
            }
            return runtimeConfig;
        }
        catch (FileNotFoundException e)
        {
            logger.error("Cannot find file " + configXmlFilename + " in the classpath", e);
            throw new RuntimeException("Cannot find file " + configXmlFilename + " in the classpath", e);
        }
        catch (IOException e)
        {
            logger.error("Error while closing InputStream", e);
            throw new RuntimeException("Error while closing InputStream", e);
        }
    }

    private List<MithraRuntimeConfig> initializeRuntimeConfig(MithraRuntimeType runtimeConfig)
    {
        if (this.isDeleteOnCreate())
        {
            runtimeConfig.setDestroyExistingPortal(true);
        }
        List<MithraRuntimeConfig> runtimeList = MithraManagerProvider.getMithraManager().initDatabaseObjects(runtimeConfig);
        populateDatabaseObjectPerConnectionManagerMap(runtimeList);
        mithraRuntimeList.addAll(runtimeList);
        for (MithraRuntimeConfig cfg : runtimeList)
        {
            addConfigToConfiguredObjects(cfg);
        }
        return runtimeList;
    }

    private void populateDatabaseObjectPerConnectionManagerMap(List<MithraRuntimeConfig> mithraRuntimeList)
    {
        for (int i = 0; i < mithraRuntimeList.size(); i++)
        {
            MithraRuntimeConfig runtimeConfig = mithraRuntimeList.get(i);
            List<MithraDatabaseObject> databaseObjectList = runtimeConfig.getDatabaseObjects();
            if (databaseObjectList != null)
            {
                List<MithraDatabaseObject> databaseObjects = new ArrayList<MithraDatabaseObject>();
                MithraTestConnectionManager connectionManager = (MithraTestConnectionManager) runtimeConfig.getConnectionManager();
                databaseObjects.addAll(databaseObjectList);
                List<MithraDatabaseObject> savedDatabaseObjects = databaseObjectPerConnectionManager.get(connectionManager);
                if (savedDatabaseObjects == null)
                {
                    databaseObjectPerConnectionManager.put(connectionManager, databaseObjects);
                }
                else
                {
                    savedDatabaseObjects.addAll(databaseObjects);
                }
            }

        }
    }

    public void setRestrictedClassList(Class[] classList)
    {
        if (classList != null)
        {
            restrictedClasses = new UnifiedSet<String>(classList.length);
            int length = classList.length;
            for (int i = 0; i < length; i++)
            {
                restrictedClasses.add(classList[i].getName());
            }
        }
    }

    private static Map<MasterQueueTableMapKey, MithraDatabaseObject> masterQueueTableMap = new UnifiedMap<MasterQueueTableMapKey, MithraDatabaseObject>(3);
    private static List<MithraDatabaseObject> replicatedChildQueueTables = new ArrayList<MithraDatabaseObject>(3);

    public static <T> T findObjectInCollection(T key, Collection<T> collection)
    {
        Iterator<T> it = collection.iterator();
        for (int i = 0; i < collection.size(); i++)
        {
            T obj = it.next();
            if (obj.equals(key))
            {
                return obj;
            }
        }
        return null;
    }

    public Set<MithraTestConnectionManager> getAdditionalConnectionManagersInUse()
    {
        return this.additionalConnectionManagersInUse;
    }

    /**
     * Loads the content of a delimited file into a test database.
     *
     * @param bcpFilename       The name of the delimited file.
     * @param delimiter         The delimiter used in the file
     * @param attributes        A list of Mithra Attributes that map to the fields in the file.
     * @param dateFormatString  A String that represents the format of Date or Timestamp fields in the delimited file.
     * @param connectionManager An instance of a ConnectionManager.
     */
    public void loadBcpFile(String bcpFilename, String delimiter, List<Attribute> attributes, String dateFormatString, SourcelessConnectionManager connectionManager)
    {
        loadBcpFile(bcpFilename, delimiter, attributes, connectionManager, dateFormatString != null ? new SimpleDateFormat(dateFormatString): null);
    }

    public void loadBcpFile(String bcpFilename, String delimiter, List<Attribute> attributes, SourcelessConnectionManager connectionManager, Format dateFormat)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(testConnectionManager.getConnectionManagerIdentifier(), null, null);
        parseBcpTestDataForTestDbConfig(bcpFilename, delimiter, attributes, dateFormat, testDbConfig, testConnectionManager);
        this.testDataFilesInUse.add(bcpFilename);
    }

    /**
     * Loads the content of a delimited file into a test database.
     *
     * @param bcpFilename       The name of the delimited file.
     * @param delimiter         The delimiter used in the file
     * @param attributes        A list of Mithra Attributes that map to the fields in the file.
     * @param dateFormatString  A String that represents the format of Date or Timestamp fields in the delimited file.
     * @param connectionManager An instance of a ConnectionManager.
     * @param sourceAttribute
     */
    public void loadBcpFile(String bcpFilename, String delimiter, List<Attribute> attributes, String dateFormatString,
                            ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        loadBcpFile(bcpFilename, delimiter, attributes, connectionManager, dateFormatString != null ? new SimpleDateFormat(dateFormatString) : null, sourceAttribute);
    }

    public void loadBcpFile(String bcpFilename, String delimiter, List<Attribute> attributes,
                            ObjectSourceConnectionManager connectionManager, Format dateFormat, String sourceAttribute)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(testConnectionManager.getConnectionManagerIdentifier() + sourceAttribute, sourceAttribute, String.class);
        parseBcpTestDataForTestDbConfig(bcpFilename, delimiter, attributes, dateFormat, testDbConfig, testConnectionManager);
        this.testDataFilesInUse.add(bcpFilename);
    }

    public TestDatabaseConfiguration getDatabaseConfigurationForDatabase(MithraTestConnectionManager connectionManager, String sourceAttribute)
    {
        return this.getDatabaseConfigurationForDatabase(connectionManager, sourceAttribute, String.class);
    }
      
    public TestDatabaseConfiguration getDatabaseConfigurationForDatabase(MithraTestConnectionManager connectionManager, Integer sourceAttribute)
    {
        return this.getDatabaseConfigurationForDatabase(connectionManager, sourceAttribute, Integer.class);
    }

    private TestDatabaseConfiguration getDatabaseConfigurationForDatabase(MithraTestConnectionManager connectionManager, Object sourceAttribute, Class sourceAttributeClass)
    {
        initializeRuntime();
        TestDatabaseConfiguration testDbConfig;
        if (sourceAttribute == null)
        {
            testDbConfig = this.createTestDbConfig(connectionManager.getConnectionManagerIdentifier(), null, null);
        }
        else
        {
            testDbConfig = this.createTestDbConfig(connectionManager.getConnectionManagerIdentifier() + sourceAttribute, sourceAttribute, sourceAttributeClass);
        }
        List<TestDatabaseConfiguration> databasesPerConnectionManager = configuredDatabasesPerConnectionManager.get(connectionManager);
        return databasesPerConnectionManager == null ? null : findObjectInList(testDbConfig, databasesPerConnectionManager);
    }

    /**
     * Loads the content of a delimited file into a test database.
     *
     * @param bcpFilename       The name of the delimited file.
     * @param delimiter         The delimiter used in the file
     * @param attributes        A list of Mithra Attributes that map to the fields in the file.
     * @param dateFormatString  A String that represents the format of Date or Timestamp fields in the delimited file.
     * @param connectionManager An instance of a ConnectionManager.
     * @param sourceAttribute
     */

    public void loadBcpFile(String bcpFilename, String delimiter, List<Attribute> attributes, String dateFormatString,
                            IntSourceConnectionManager connectionManager, int sourceAttribute)
    {
        loadBcpFile(bcpFilename, delimiter, attributes, connectionManager, dateFormatString != null ? new SimpleDateFormat(dateFormatString): null, sourceAttribute);
    }

    public void loadBcpFile(String bcpFilename, String delimiter, List<Attribute> attributes,
                            IntSourceConnectionManager connectionManager, Format dateFormat, int sourceAttribute)
    {
        initializeRuntime();
        MithraTestConnectionManager testConnectionManager = (MithraTestConnectionManager) connectionManager;
        TestDatabaseConfiguration testDbConfig =
                this.createTestDbConfig(testConnectionManager.getConnectionManagerIdentifier() + sourceAttribute, Integer.valueOf(sourceAttribute), int.class);
        parseBcpTestDataForTestDbConfig(bcpFilename, delimiter, attributes, dateFormat, testDbConfig, testConnectionManager);
        this.testDataFilesInUse.add(bcpFilename);
    }


    public void insertTestData(List<? extends MithraObject> testDataList)
    {
        initializeRuntime();
        List<MithraDataObject> dataObjects = getDataObjectsFromDomainObjects(testDataList);
        MithraObjectPortal portal = dataObjects.get(0).zGetMithraObjectPortal();
        Attribute sourceAttribute = portal.getFinder().getSourceAttribute();
        if (sourceAttribute != null)
        {
            List segregated = this.segregateBySourceAttribute(dataObjects, sourceAttribute);
            int segregatedSize = segregated.size();
            for (int i = 0; i < segregatedSize; i++)
            {
                List segregatedList = (List) segregated.get(i);
                Object source = sourceAttribute.valueOf(segregatedList.get(0));
                this.insertTestData(segregatedList, source);
            }
        }
        else
        {
            this.insertTestData(dataObjects, null);
        }
        portal.reloadCache();
    }

    /**
     * This method is used to insert test data into a test database. This method assumes that setUp() has been called in this instance
     * of MithraTestResource and that the MithraObject type has been included in the runtime configuration.
     *
     * @param dataObjects
     */
    private void insertTestData(List<? extends MithraDataObject> dataObjects, Object source)
    {
        if (dataObjects != null && !dataObjects.isEmpty())
        {
            MithraDataObject firstData = dataObjects.get(0);
            RelatedFinder finder = firstData.zGetMithraObjectPortal().getFinder();

            ((MithraAbstractDatabaseObject) finder.getMithraObjectPortal().getDatabaseObject()).insertData(
                    Arrays.asList(finder.getPersistentAttributes()), dataObjects, source
            );
            finder.getMithraObjectPortal().clearQueryCache();
        }
    }

    public void collectTestData(MithraObject mithraObject)
    {
        initializeRuntime();
        String clazz = mithraObject.getClass().getName();
        List<MithraObject> mithraObjectsForClazz = this.testData.get(clazz);
        if (mithraObjectsForClazz == null)
        {
            mithraObjectsForClazz = FastList.newList();
            this.testData.put(clazz, mithraObjectsForClazz);
        }
        mithraObjectsForClazz.add(mithraObject);
    }


    private List<MithraDataObject> getDataObjectsFromDomainObjects(List<? extends MithraObject> testDataList)
    {
        List<MithraDataObject> dataObjects = new ArrayList<MithraDataObject>(testDataList.size());
        for (int i = 0; i < testDataList.size(); i++)
        {
            dataObjects.add(testDataList.get(i).zGetCurrentData());
        }
        return dataObjects;
    }

    protected List<List> segregateBySourceAttribute(List<? extends MithraDataObject> mithraDataObjects, Attribute sourceAttribute)
    {
        MultiHashMap map = new MultiHashMap();
        for (int i = 0; i < mithraDataObjects.size(); i++)
        {
            map.put(sourceAttribute.valueOf(mithraDataObjects.get(i)), mithraDataObjects.get(i));
        }

        if (map.size() > 1)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.create((List) mithraDataObjects);
        }
    }

    private class MasterQueueTableMapKey
    {
        private final Object connectionManager;
        private final String schemaName;

        public MasterQueueTableMapKey(Object connectionManager, String schemaName)
        {
            this.connectionManager = connectionManager;
            this.schemaName = schemaName;
        }

        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final MasterQueueTableMapKey that = (MasterQueueTableMapKey) o;

            if (!connectionManager.equals(that.connectionManager))
            {
                return false;
            }
            return !(this.schemaName == null ? that.schemaName != null : !schemaName.equals(that.schemaName));
        }

        public int hashCode()
        {
            int result;
            result = connectionManager.hashCode();
            result = 29 * result + (schemaName != null ? schemaName.hashCode() : 0);
            return result;
        }
    }

    public void createReplicationNotificationTables(ReplicationNotificationConnectionManager replicationNotificationConnectionManager)
    {
        initializeRuntime();
        RunsMasterQueueDatabaseObject dbo = (RunsMasterQueueDatabaseObject) RunsMasterQueueFinder.getMithraObjectPortal().getDatabaseObject();
        List connectionManagerList = replicationNotificationConnectionManager.getConnectionManagerList();

        for (int i = 0; i < connectionManagerList.size(); i++)
        {
            Object connectionManager = connectionManagerList.get(i);
            String schemaName = replicationNotificationConnectionManager.getSchema("", i);
            MasterQueueTableMapKey key = new MasterQueueTableMapKey(connectionManager, schemaName);
            if (!masterQueueTableMap.containsKey(key))
            {
                dbo.createTestTable(Integer.valueOf(i));
                masterQueueTableMap.put(key, dbo);
            }
        }

        for (int i = 0; i < replicatedChildQueueTables.size(); i++)
        {
            MithraDatabaseObject obj = replicatedChildQueueTables.get(i);
            String className = getClassNameFromDatabaseObject(obj);
            if (!createdChildQueueTables.containsKey(className))
            {
                ((MithraReplicatedDatabaseObject) obj).createChildQueueTestTable();
                createdChildQueueTables.put(className, obj);
            }
        }
    }

    private static Map<String, MithraDatabaseObject> createdChildQueueTables = new UnifiedMap<String, MithraDatabaseObject>();

    public void tearDownReplicationNotificationTables(ReplicationNotificationConnectionManager replicationNotificationConnectionManager)
    {
        RunsMasterQueueDatabaseObject dbo = (RunsMasterQueueDatabaseObject) RunsMasterQueueFinder.getMithraObjectPortal().getDatabaseObject();
        List connectionManagerList = replicationNotificationConnectionManager.getConnectionManagerList();

        for (int i = 0; i < connectionManagerList.size(); i++)
        {
            Object connectionManager = connectionManagerList.get(i);
            String schemaName = replicationNotificationConnectionManager.getSchema("", i);
            MasterQueueTableMapKey key = new MasterQueueTableMapKey(connectionManager, schemaName);
            if (masterQueueTableMap.containsKey(key))
            {
                dbo.deleteAllRowsFromTestTable(Integer.valueOf(i));
                for (int j = 0; j < replicatedChildQueueTables.size(); j++)
                {
                    MithraReplicatedDatabaseObject obj = (MithraReplicatedDatabaseObject) replicatedChildQueueTables.get(j);
                    obj.deleteAllReplicationNotificationData();
                }
            }
        }
    }
}