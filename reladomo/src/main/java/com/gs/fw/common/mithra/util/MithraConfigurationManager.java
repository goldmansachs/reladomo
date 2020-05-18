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

package com.gs.fw.common.mithra.util;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.gs.fw.common.mithra.LoadOperationProvider;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDatabaseObject;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectDeserializer;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraPrimaryKeyGenerator;
import com.gs.fw.common.mithra.MithraPureObjectFactory;
import com.gs.fw.common.mithra.MithraRuntimeConfig;
import com.gs.fw.common.mithra.SimulatedSequenceInitValues;
import com.gs.fw.common.mithra.cache.offheap.MasterCacheService;
import com.gs.fw.common.mithra.cache.offheap.MasterCacheUplink;
import com.gs.fw.common.mithra.connectionmanager.ConnectionManagerWrapper;
import com.gs.fw.common.mithra.connectionmanager.IntSourceSchemaManager;
import com.gs.fw.common.mithra.connectionmanager.IntSourceTablePartitionManager;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceSchemaManager;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceTablePartitionManager;
import com.gs.fw.common.mithra.connectionmanager.SchemaManager;
import com.gs.fw.common.mithra.connectionmanager.TablePartitionManager;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.mithraruntime.ConnectionManagerType;
import com.gs.fw.common.mithra.mithraruntime.MasterCacheReplicationServerType;
import com.gs.fw.common.mithra.mithraruntime.MithraObjectConfigurationType;
import com.gs.fw.common.mithra.mithraruntime.MithraPureObjectConfigurationType;
import com.gs.fw.common.mithra.mithraruntime.MithraRuntimeType;
import com.gs.fw.common.mithra.mithraruntime.MithraRuntimeUnmarshaller;
import com.gs.fw.common.mithra.mithraruntime.MithraTemporaryObjectConfigurationType;
import com.gs.fw.common.mithra.mithraruntime.PropertyType;
import com.gs.fw.common.mithra.mithraruntime.PureObjectsType;
import com.gs.fw.common.mithra.mithraruntime.RemoteServerType;
import com.gs.fw.common.mithra.mithraruntime.SchemaType;
import com.gs.fw.common.mithra.notification.MithraReplicationNotificationManager;
import com.gs.fw.common.mithra.notification.replication.ReplicationNotificationConnectionManager;
import com.gs.fw.common.mithra.remote.RemoteMithraObjectConfig;
import com.gs.fw.common.mithra.remote.RemoteMithraService;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MithraConfigurationManager
{

    private static final Logger logger = LoggerFactory.getLogger(MithraConfigurationManager.class);
    private static final Class[] NO_PARAMS = {};
    private static final Object[] NO_ARGS = {};

    private static final PersisterId PURE_PERSISTER_ID = new PersisterId(0);
    private MithraReplicationNotificationManager replicationNotificationManager;
    private final Map<Object, PersisterId> connectionManagerMap = new IdentityHashMap();
    private final Map<Object, ConnectionManagerWrapper> connectionManagerWrapperMap = new IdentityHashMap();
    private AtomicInteger connectionManagerId = new AtomicInteger(0);
    private Set<MithraRuntimeCacheController> runtimeCacheControllerSet = new UnifiedSet<MithraRuntimeCacheController>();
    private final Set<RemoteMithraObjectConfig> threeTierConfigSet = new UnifiedSet<RemoteMithraObjectConfig>();
    private final Set<RemoteMithraObjectConfig> cacheReplicableConfigSet = new UnifiedSet<RemoteMithraObjectConfig>();
    private final ConcurrentHashMap<String, MasterCacheUplink> masterCacheUplinkMap = ConcurrentHashMap.newMap();
    private int defaultMinQueriesToKeep = 32;
    private int defaultRelationshipCacheSize = 10000;
    private final UnifiedMap uninitialized = new UnifiedMap();
    private final Set<String> initializedClasses = new UnifiedSet<String>();
    private static final Class[] GET_INSTANCE_PARAMETER_TYPES = new Class[] { Properties.class };

    /**
     * sets the value of minimum queries to keep per class. 32 is the default. This can be
     * overridden in a configuration file using &lt;MithraRuntime defaultMinQueriesToKeep="100"&gt;
     * or on a per object basis
     * &lt;MithraObjectConfiguration className="com.gs.fw.para.domain.desk.product.ProductScrpMap" cacheType="partial" minQueriesToKeep="100"/&gt;
     * @param defaultMinQueriesToKeep the default minimum queries to keep
     */
    public void setDefaultMinQueriesToKeep(int defaultMinQueriesToKeep)
    {
        this.defaultMinQueriesToKeep = defaultMinQueriesToKeep;
    }

    /**
     * sets the value of relationship cache per class. 10000 is the default. This can be
     * overridden in a configuration file using &lt;MithraRuntime defaultRelationshipCacheSize="100"&gt;
     * or on a per object basis
     * &lt;MithraObjectConfiguration className="com.gs.fw.para.domain.desk.product.ProductScrpMap" cacheType="partial" relationshipCacheSize="50000"/&gt;
     * @param defaultRelationshipCacheSize the default cache size to set
     */
    public void setDefaultRelationshipCacheSize(int defaultRelationshipCacheSize)
    {
        this.defaultRelationshipCacheSize = defaultRelationshipCacheSize;
    }

    public MasterCacheUplink getMasterCacheUplink(String masterCacheId)
    {
        return this.masterCacheUplinkMap.get(masterCacheId);
    }

    protected Object getConnectionManagerInstance(ConnectionManagerType connectionManagerType)
    {
        return this.getConnectionManagerInstance(connectionManagerType.getClassName(), connectionManagerType.getProperties());
    }

    protected Object getConnectionManagerInstance(String className, List propertyList)
    {
        Object instance;
        try
        {
            Class clazz = MithraManager.class.getClassLoader().loadClass(className);
            if (!propertyList.isEmpty())
            {
                Method method = getMethodByReflection(clazz, "getInstance", GET_INSTANCE_PARAMETER_TYPES);
                Properties properties = this.createProperties(propertyList);
                if (method == null)
                {
                    throw new MithraBusinessException(
                        "The connection manager class " + className
                        + " must have a public static getInstance(Properties) method. See the error *above*"
                    );
                }
                instance = method.invoke(null, new Object[] { properties} );
            }
            else
            {
                Method method = getMethodByReflection(clazz, "getInstance", NO_PARAMS);
                if (method == null)
                {
                    throw new MithraBusinessException(
                        "The connection manager class " + className
                        + " must have a public static getInstance() method. See the error *above*"
                    );
                }
                instance = method.invoke(null, NO_ARGS);
            }
        }
        catch (ClassNotFoundException e)
        {
            throw new MithraBusinessException("unable to find connection manager class: " + className, e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("unable to invoke getInstance() on connection manager class '" +
                    className + "' Is it declared public static? See the error *below* for details.", e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("unable to call getInstance() on connection manager class '" +
                    className + "' See the error *below* for the cause", e);
        }
        getOrCreateConnectionManagerId(instance);
        getOrCreateConnectionManagerWrapper(instance);

        return instance;
    }

    private Method getMethodByReflection(Class clazz, String methodName, Class[] params)
    {
        Method method = null;
        try
        {
            method = clazz.getMethod(methodName, params);
        }
        catch (NoSuchMethodException e)
        {
            //ignore
        }
        return method;
    }

    private PersisterId getOrCreateConnectionManagerId(Object instance)
    {
        PersisterId id;
        synchronized (connectionManagerMap)
        {
            id = connectionManagerMap.get(instance);
            if (id == null)
            {
                id = new PersisterId(this.connectionManagerId.incrementAndGet());
                connectionManagerMap.put(instance, id);
            }
        }
        return id;
    }

    private ConnectionManagerWrapper getOrCreateConnectionManagerWrapper(Object instance)
    {
        ConnectionManagerWrapper wrapper;
        synchronized (connectionManagerWrapperMap)
        {
            wrapper = connectionManagerWrapperMap.get(instance);
            if (wrapper == null)
            {
                wrapper = new ConnectionManagerWrapper(instance);
                connectionManagerWrapperMap.put(instance, wrapper);
            }
        }
        return wrapper;
    }

    public PersisterId getConnectionManagerId(Object key)
    {
        synchronized (connectionManagerMap)
        {
            return connectionManagerMap.get(key);
        }
    }

    public synchronized void cleanUpRuntimeCacheControllers()
    {
        this.resetAllInitializedClasses();
        this.runtimeCacheControllerSet.clear();
        this.threeTierConfigSet.clear();
        this.cacheReplicableConfigSet.clear();
        this.masterCacheUplinkMap.clear();
    }

    public synchronized void cleanUpRuntimeCacheControllers(Set<String> classesToCleanUp)
    {
        this.resetAllInitializedClasses(classesToCleanUp);
        for(Iterator<MithraRuntimeCacheController> it = this.runtimeCacheControllerSet.iterator(); it.hasNext();)
        {
            if (classesToCleanUp.contains(it.next().getClassName()))
            {
                it.remove();
            }
        }
    }

    /**
     * This method will load the cache of the object already initialized. A Collection is used
     * to keep track of the objects to load.
     * @param portals list of portals to load caches for
     * @param threads number of parallel threads to load
     * @throws MithraBusinessException if something goes wrong during the load
     */
    public void loadMithraCache(List<MithraObjectPortal> portals, int threads) throws MithraBusinessException
    {
        ThreadConservingExecutor executor = new ThreadConservingExecutor(threads);
        for(int i=0;i<portals.size();i++)
        {
            final MithraObjectPortal portal = portals.get(i);
            executor.submit(new PortalLoadCacheRunnable(portal));
        }
        executor.finish();
    }

    public void readConfiguration(InputStream mithraFileIs) throws MithraBusinessException
    {
        MithraRuntimeType mithraRuntimeType = parseConfiguration(mithraFileIs);
        initializeRuntime(mithraRuntimeType);
    }

    public void initializeRuntime(MithraRuntimeType mithraRuntimeType)
    {
        List<MithraRuntimeConfig> mithraRuntimeList = new ArrayList<MithraRuntimeConfig>();

        lazyInitObjectsWithCallback(mithraRuntimeType, mithraRuntimeList, null);

        List<String> mithraInitializationErrors = new ArrayList<String>();

        for(int i=0;i<mithraRuntimeList.size();i++)
        {
            MithraRuntimeConfig mrc = mithraRuntimeList.get(i);
            List<Config> configs = mrc.getConfigs();
            List<MithraObjectPortal> objectPortals = FastList.newList();
            mrc.setObjectPortals(objectPortals);
            MasterCacheUplink uplink = mrc.getMasterCacheUplink();
            if (uplink != null)
            {
                for(int j=0;j<configs.size();j++)
                {
                    Config config = configs.get(j);
                    MithraObjectPortal portal = this.initializeObject(config.className, mithraInitializationErrors);
                    objectPortals.add(portal);
                }
                checkForErrors(mithraInitializationErrors);
                uplink.startSyncAndWaitForInitialSync(objectPortals);
                this.masterCacheUplinkMap.put(uplink.getMasterCacheId(), uplink);
            }
            else
            {
                List<MithraObjectPortal> objectsForStartupLoad = new ArrayList<MithraObjectPortal>();
                for(int j=0;j<configs.size();j++)
                {
                    Config config = configs.get(j);
                    if (config.fullCache || config.isDbReplicated)
                    {
                        MithraObjectPortal portal = this.initializeObject(config.className, mithraInitializationErrors);
                        if (portal != null)
                        {
                            objectPortals.add(portal);
                            if (config.loadCacheOnStartup)
                            {
                                objectsForStartupLoad.add(portal);
                            }
                        }
                    }
                }
                checkForErrors(mithraInitializationErrors);
                this.loadMithraCache(objectsForStartupLoad, mrc.getLoaderThreads());
            }
        }
    }

    public List<MithraRuntimeConfig> initDatabaseObjects(InputStream mithraFileIs)
    throws MithraBusinessException
    {
        MithraRuntimeType mithraRuntimeType = parseConfiguration(mithraFileIs);
        return initDatabaseObjects(mithraRuntimeType);
    }

    private void lazyInitObjectsWithCallback(MithraRuntimeType mithraRuntimeType, List<MithraRuntimeConfig> mithraRuntimeList, PostInitializeHook hook)
    {
        this.lazyInitLocalObjects(mithraRuntimeType, mithraRuntimeList, hook);
        this.lazyInitRemoteObjects(mithraRuntimeType, mithraRuntimeList, hook);
        this.lazyInitPureObjects(mithraRuntimeType, mithraRuntimeList, hook);
        this.lazyInitReplicatedObjects(mithraRuntimeType, mithraRuntimeList, hook);
    }

    public void lazyInitObjectsWithCallback(MithraRuntimeType mithraRuntimeType, PostInitializeHook hook)
    {
        List<MithraRuntimeConfig> mithraRuntimeList = new ArrayList<MithraRuntimeConfig>();
        lazyInitObjectsWithCallback(mithraRuntimeType, mithraRuntimeList, hook);
    }

    public List<MithraRuntimeConfig> initDatabaseObjects(MithraRuntimeType mithraRuntimeType)
    {
        List<MithraRuntimeConfig> mithraRuntimeList = new ArrayList<MithraRuntimeConfig>();

        lazyInitObjectsWithCallback(mithraRuntimeType, mithraRuntimeList, null);

        List<String> mithraInitializationErrors = new ArrayList<String>();

        for(int i=0;i<mithraRuntimeList.size();i++)
        {
            MithraRuntimeConfig mrc = mithraRuntimeList.get(i);
            List<Config> configs = mrc.getConfigs();
            List<MithraDatabaseObject> databaseObjects = new ArrayList<MithraDatabaseObject>(configs.size());
            List<MithraObjectPortal> objectPortals = new ArrayList<MithraObjectPortal>(configs.size());
            mrc.setDatabaseObjects(databaseObjects);
            mrc.setObjectPortals(objectPortals);
            for(int j=0;j<configs.size();j++)
            {
                Config config = configs.get(j);
                MithraObjectPortal portal = this.initializeObject(config.className, mithraInitializationErrors);
                if (portal != null)
                {
                    objectPortals.add(portal);
                    if (config.isLocal())
                    {
                        databaseObjects.add(portal.getDatabaseObject());
                    }
                }
            }
        }

        checkForErrors(mithraInitializationErrors);
        return mithraRuntimeList;
    }

    private void lazyInitRemoteObjects(MithraRuntimeType mithraRuntimeType, List<MithraRuntimeConfig> mithraRuntimeList, PostInitializeHook hook)
    {
        List<RemoteServerType> remoteServerList = mithraRuntimeType.getRemoteServers();
        for (int serverIndex = 0; serverIndex < remoteServerList.size(); serverIndex++)
        {
            RemoteServerType remoteServerType = remoteServerList.get(serverIndex);

            MithraRuntimeConfig mithraRuntimeConfig = new MithraRuntimeConfig(remoteServerType.getInitialLoaderThreads());
            mithraRuntimeList.add(mithraRuntimeConfig);
            List<Config> configs = new ArrayList<Config>();
            mithraRuntimeConfig.setConfigs(configs);

            List<MithraObjectConfigurationType> localOverrrides = remoteServerType.getMithraObjectConfigurations();
            Map<String, MithraObjectConfigurationType> localOverrideMap = new HashMap<String, MithraObjectConfigurationType>();
            for (int o = 0; o < localOverrrides.size(); o++)
            {
                MithraObjectConfigurationType mithraObjectConfigurationType = localOverrrides.get(o);
                localOverrideMap.put(mithraObjectConfigurationType.getClassName(), mithraObjectConfigurationType);
            }

            RemoteMithraService remoteMithraService = getRemoteServiceInstance(remoteServerType);
            RemoteMithraObjectConfig[] remoteMithraObjectConfigs = remoteMithraService.getObjectConfigurations();
            for (int r = 0; r < remoteMithraObjectConfigs.length; r++)
            {
                String className = remoteMithraObjectConfigs[r].getClassName();
                RemoteObjectConfig config = new RemoteObjectConfig();
                config.className = className;
                config.postInitializeHook = hook;
                config.remoteMithraService = remoteMithraService;
                config.remoteSerialId = remoteMithraObjectConfigs[r].getSerialVersion();
                config.persisterId = remoteMithraObjectConfigs[r].getPersisterId();

                MithraObjectConfigurationType localOverride = localOverrideMap.get(className);
                config.minQueriesToKeep = remoteMithraObjectConfigs[r].getMinQueriesToKeep();
                if (remoteServerType.isOverrideMinQueriesToKeepSet())
                {
                    config.minQueriesToKeep = remoteServerType.getOverrideMinQueriesToKeep();
                }

                if (localOverride != null && localOverride.isMinQueriesToKeepSet())
                {
                    config.minQueriesToKeep = localOverride.getMinQueriesToKeep();
                }

                config.relationshipCacheSize = remoteMithraObjectConfigs[r].getRelationshipCacheSize();
                if (remoteServerType.isOverrideRelationshipCacheSizeSet())
                {
                    config.relationshipCacheSize = remoteServerType.getOverrideRelationshipCacheSize();
                }

                if (localOverride != null && localOverride.isRelationshipCacheSizeSet())
                {
                    config.relationshipCacheSize = localOverride.getRelationshipCacheSize();
                }
                config.isPure = remoteMithraObjectConfigs[r].isPure();
                config.factoryParameter = remoteMithraObjectConfigs[r].getFactoryParameter();
                config.pureNotificationId = remoteMithraObjectConfigs[r].getPureNotificationId();
                config.fullCache = localOverride != null && localOverride.getCacheType().isFull();
                config.offHeapFullCache = config.fullCache && localOverride.isOffHeapFullCache();
                if (config.offHeapFullCache)
                {
                    StringPool.getInstance().enableOffHeapSupport();
                }
                config.disableCache = localOverride != null && localOverride.getCacheType().isNone();
                config.isParticipatingInTx = localOverride == null ? true : localOverride.getTxParticipation().isFull();

                config.cacheTimeToLive = remoteMithraObjectConfigs[r].getCacheTimeToLive();
                if (localOverride != null && localOverride.isCacheTimeToLiveSet())
                {
                    config.cacheTimeToLive = localOverride.getCacheTimeToLive();
                }

                config.relationshipCacheTimeToLive = remoteMithraObjectConfigs[r].getRelationshipCacheTimeToLive();
                if (localOverride != null && localOverride.isRelationshipCacheTimeToLiveSet())
                {
                    config.relationshipCacheTimeToLive = localOverride.getRelationshipCacheTimeToLive();
                }

                if (config.relationshipCacheTimeToLive != 0 && config.cacheTimeToLive == 0)
                {
                    throw new RuntimeException("relationshipCacheTimeToLive cannot be set without cacheTimeToLive being set for object " + config.className);
                }

                config.useMultiUpdate = remoteMithraObjectConfigs[r].useMultiUpdate();

                config.threeTierExport = overrideBoolean(false, remoteServerType.isThreeTierExportSet(), remoteServerType.isThreeTierExport());
                if (localOverride != null)
                {
                    config.threeTierExport = overrideBoolean(config.threeTierExport, localOverride.isThreeTierExportSet(), localOverride.isThreeTierExport());
                }
                configs.add(config);
                addUnitialized(config, mithraRuntimeType.isDestroyExistingPortal());
            }
        }
    }

    private void lazyInitReplicatedObjects(MithraRuntimeType mithraRuntimeType, List<MithraRuntimeConfig> mithraRuntimeList, PostInitializeHook hook)
    {
        List<MasterCacheReplicationServerType> masterReplicationServers = mithraRuntimeType.getMasterCacheReplicationServers();
        for (int serverIndex = 0; serverIndex < masterReplicationServers.size(); serverIndex++)
        {
            MasterCacheReplicationServerType masterReplicationServer = masterReplicationServers.get(serverIndex);

            MithraRuntimeConfig mithraRuntimeConfig = new MithraRuntimeConfig(masterReplicationServer.getSyncThreads());
            mithraRuntimeList.add(mithraRuntimeConfig);
            List<Config> configs = new ArrayList<Config>();
            mithraRuntimeConfig.setConfigs(configs);

            MasterCacheService masterCacheService = getMasterCacheServiceInstance(masterReplicationServer);
            MasterCacheUplink uplink = new MasterCacheUplink(masterReplicationServer.getMasterCacheId(), masterCacheService);
            uplink.setSyncThreads(masterReplicationServer.getSyncThreads());
            uplink.setSyncInterval(masterReplicationServer.getSyncIntervalInMilliseconds());
            mithraRuntimeConfig.setMasterCacheUplink(uplink);
            RemoteMithraObjectConfig[] remoteMithraObjectConfigs = masterCacheService.getObjectConfigurations();
            for (int r = 0; r < remoteMithraObjectConfigs.length; r++)
            {
                String className = remoteMithraObjectConfigs[r].getClassName();
                CacheReplicatedObjectConfig config = new CacheReplicatedObjectConfig();
                config.className = className;
                config.postInitializeHook = hook;
                config.masterCacheUplink = uplink;
                config.remoteSerialId = remoteMithraObjectConfigs[r].getSerialVersion();
                config.persisterId = remoteMithraObjectConfigs[r].getPersisterId();
                config.minQueriesToKeep = remoteMithraObjectConfigs[r].getMinQueriesToKeep();
                config.relationshipCacheSize = remoteMithraObjectConfigs[r].getRelationshipCacheSize();
                config.isPure = remoteMithraObjectConfigs[r].isPure();
                config.factoryParameter = remoteMithraObjectConfigs[r].getFactoryParameter();
                config.pureNotificationId = remoteMithraObjectConfigs[r].getPureNotificationId();
                config.fullCache = true;
                config.offHeapFullCache = true;
                if (config.offHeapFullCache)
                {
                    StringPool.getInstance().enableOffHeapSupport();
                }
                config.disableCache = false;
                config.isParticipatingInTx = false;

                config.cacheTimeToLive = remoteMithraObjectConfigs[r].getCacheTimeToLive();
                config.relationshipCacheTimeToLive = remoteMithraObjectConfigs[r].getRelationshipCacheTimeToLive();

                if (config.relationshipCacheTimeToLive != 0 && config.cacheTimeToLive == 0)
                {
                    throw new RuntimeException("relationshipCacheTimeToLive cannot be set without cacheTimeToLive being set for object " + config.className);
                }

                config.useMultiUpdate = remoteMithraObjectConfigs[r].useMultiUpdate();

                config.threeTierExport = false;
                configs.add(config);
                addUnitialized(config, mithraRuntimeType.isDestroyExistingPortal());
            }
        }
    }

    private MithraDatabaseObject instantiateDatabaseObject(String className, List<String> mithraInitializationErrors)
    {
        String databaseObjectClassName = className + "DatabaseObject";
        return (MithraDatabaseObject) instantiateFactory(mithraInitializationErrors, databaseObjectClassName);
    }

    private MithraObjectDeserializer instantiateDeserializer(String className, List<String> mithraInitializationErrors)
    {
        String objectFactoryClassName = className + "ObjectFactory";
        return instantiateFactory(mithraInitializationErrors, objectFactoryClassName);
    }

    private MithraObjectDeserializer instantiateFactory(List<String> mithraInitializationErrors, String objectFactoryClassName)
    {
        try
        {
            Class dboClass = Class.forName(objectFactoryClassName);
            MithraObjectDeserializer deserializer = (MithraObjectDeserializer) dboClass.newInstance();
            initSimulatedSequences(deserializer);
            return deserializer;
        }
        catch (IllegalAccessException e)
        {
            final String msg = "Could not access class or constructor for class " + objectFactoryClassName;
            getLogger().error(msg, e);
            mithraInitializationErrors.add(msg);
        }
        catch (ClassNotFoundException e)
        {
            final String msg = "Class " + objectFactoryClassName + " could not be found";
            getLogger().error(msg, e);
            mithraInitializationErrors.add(msg);
        }
        catch (InstantiationException e)
        {
            final String msg = "Could not instantiate class " + objectFactoryClassName;
            getLogger().error(msg, e);
            mithraInitializationErrors.add(msg);
        }
        return null;
    }

    private void initSimulatedSequences(MithraObjectDeserializer deserializer)
    {
        List initValues = deserializer.getSimulatedSequenceInitValues();
        if (initValues != null)
        {
            for (int i = 0; i < initValues.size(); i++)
            {
                MithraPrimaryKeyGenerator.getInstance().initializeSimulatedSequencePrimaryKeyGenerator((SimulatedSequenceInitValues)initValues.get(i));
            }
        }
    }

    private RemoteMithraService getRemoteServiceInstance(RemoteServerType remoteServerType)
    {
        String factoryClassName = remoteServerType.getClassName();
        List props = remoteServerType.getProperties();
        Properties properties = this.createProperties(props);
        Object instance;
        try
        {
            Class clazz = MithraManager.class.getClassLoader().loadClass(factoryClassName);
            Method method = getMethodByReflection(clazz, "getInstance", GET_INSTANCE_PARAMETER_TYPES);
            if (method == null)
            {
                throw new MithraBusinessException(
                    "The remote factory class " + factoryClassName
                    + " must have a public static getInstance() method. See the error *above*"
                );
            }
            instance = method.invoke(null, new Object[] { properties} );
        }
        catch (ClassNotFoundException e)
        {
            throw new MithraBusinessException("Unable to find remote factory class: " + factoryClassName, e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("Unable to invoke getInstance(Properties) on remote factory class '" +
                    factoryClassName + "' Is it declared public static? See the error *below* for details.", e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Unable to call getInstance() on remote factory class '" +
                    factoryClassName + "' See the error *below* for the cause", e);
        }

        return (RemoteMithraService) instance;
    }

    private MasterCacheService getMasterCacheServiceInstance(MasterCacheReplicationServerType masterReplicationServerType)
    {
        String factoryClassName = masterReplicationServerType.getClassName();
        List props = masterReplicationServerType.getProperties();
        Properties properties = this.createProperties(props);
        Object instance;
        try
        {
            Class clazz = MithraManager.class.getClassLoader().loadClass(factoryClassName);
            Method method = getMethodByReflection(clazz, "getInstance", GET_INSTANCE_PARAMETER_TYPES);
            if (method == null)
            {
                throw new MithraBusinessException(
                    "The master cache factory class " + factoryClassName
                    + " must have a public static getInstance() method. See the error *above*"
                );
            }
            instance = method.invoke(null, new Object[] { properties} );
        }
        catch (ClassNotFoundException e)
        {
            throw new MithraBusinessException("Unable to find master cache factory class: " + factoryClassName, e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("Unable to invoke getInstance(Properties) on master cache factory class '" +
                    factoryClassName + "' Is it declared public static? See the error *below* for details.", e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Unable to call getInstance() on master cache class '" +
                    factoryClassName + "' See the error *below* for the cause", e);
        }

        return (MasterCacheService) instance;
    }

    private Properties createProperties(List props)
    {
        Properties properties = new Properties();
        for (int i = 0; i < props.size(); i++)
        {
            PropertyType propertyType = (PropertyType) props.get(i);
            properties.put(propertyType.getName(), propertyType.getValue());
        }
        return properties;
    }

    private void lazyInitLocalObjects(MithraRuntimeType mithraRuntimeType, List<MithraRuntimeConfig> mithraRuntimeList, PostInitializeHook hook)
    {
        List<ConnectionManagerType> connectionManagers = mithraRuntimeType.getConnectionManagers();

        for (int connManIndex=0; connManIndex<connectionManagers.size(); connManIndex++)
        {
            ConnectionManagerType connectionManagerType = connectionManagers.get(connManIndex);
            Object connectionManager = this.getConnectionManagerInstance(connectionManagerType);
            PersisterId persisterId = this.getConnectionManagerId(connectionManager);

            MithraRuntimeConfig mithraRuntimeConfig = new MithraRuntimeConfig(connectionManagerType.getInitialLoaderThreads());
            mithraRuntimeConfig.setConnectionManager(connectionManager);
            mithraRuntimeList.add(mithraRuntimeConfig);
            List<MithraObjectConfigurationType> mithraObjectConfigurations = connectionManagerType.getMithraObjectConfigurations();

            List<Config> configs = new ArrayList<Config>(mithraObjectConfigurations.size());
            mithraRuntimeConfig.setConfigs(configs);


            lazyInitDatabaseObjects(mithraRuntimeType, connectionManagerType, null, connectionManager, persisterId,
                    mithraObjectConfigurations, configs, hook);
            lazyInitTempObjects(mithraRuntimeType, connectionManagerType, null, connectionManager, persisterId,
                    connectionManagerType.getMithraTemporaryObjectConfigurations(), configs, hook);
            List schemas = connectionManagerType.getSchemas();
            if (schemas != null)
            {
                for (int i = 0; i < schemas.size(); i++)
                {
                    SchemaType schemaType = (SchemaType) schemas.get(i);
                    this.lazyInitDatabaseObjects(mithraRuntimeType, connectionManagerType, schemaType, connectionManager, persisterId,
                            schemaType.getMithraObjectConfigurations(), configs, hook);
                }
            }
        }
    }

    private void lazyInitPureObjects(MithraRuntimeType mithraRuntimeType, List<MithraRuntimeConfig> mithraRuntimeList, PostInitializeHook hook)
    {
        PureObjectsType pureObjectsType = mithraRuntimeType.getPureObjects();

        if (pureObjectsType != null)
        {
            List<Config> configs = new ArrayList<Config>();

            MithraRuntimeConfig mithraRuntimeConfig = new MithraRuntimeConfig(1);
            mithraRuntimeConfig.setConfigs(configs);
            mithraRuntimeList.add(mithraRuntimeConfig);

            List<MithraPureObjectConfigurationType> pureConfigs = pureObjectsType.getMithraObjectConfigurations();
            int finalMinQueriesToKeep = this.defaultMinQueriesToKeep;
            int finalRelationshipCacheSize = this.defaultRelationshipCacheSize;

            if (mithraRuntimeType.isDefaultMinQueriesToKeepSet())
            {
                finalMinQueriesToKeep = mithraRuntimeType.getDefaultMinQueriesToKeep();
            }
            if (mithraRuntimeType.isDefaultRelationshipCacheSizeSet())
            {
                finalRelationshipCacheSize = mithraRuntimeType.getDefaultRelationshipCacheSize();
            }
            for(int i=0;i<pureConfigs.size();i++)
            {
                MithraPureObjectConfigurationType conf = pureConfigs.get(i);
                PureObjectConfig config = new PureObjectConfig();
                config.fullCache = true;
                config.offHeapFullCache = conf.isOffHeapFullCache();
                if (config.offHeapFullCache)
                {
                    StringPool.getInstance().enableOffHeapSupport();
                }
                config.isParticipatingInTx = true; // pure objects can't be read-only
                config.className = conf.getClassName();
                config.postInitializeHook = hook;
                config.pureNotificationId = pureObjectsType.getNotificationIdentifier();

                config.minQueriesToKeep = finalMinQueriesToKeep;
                if (conf.isMinQueriesToKeepSet())
                {
                    config.minQueriesToKeep = conf.getMinQueriesToKeep();
                }

                config.relationshipCacheSize = finalRelationshipCacheSize;
                if (conf.isRelationshipCacheSizeSet())
                {
                    config.relationshipCacheSize = conf.getRelationshipCacheSize();
                }

                config.isPure = true;
                config.factoryParameter = conf.getFactoryParameter();

                config.threeTierExport = conf.isThreeTierExport();
                config.threeTierExport = overrideBoolean(config.threeTierExport, pureObjectsType.isThreeTierExportSet(), pureObjectsType.isThreeTierExport());
                config.threeTierExport = overrideBoolean(config.threeTierExport, conf.isThreeTierExportSet(), conf.isThreeTierExport());
                configs.add(config);
                addUnitialized(config, mithraRuntimeType.isDestroyExistingPortal());
            }
        }
    }

    private void initializeReplicationNotificationPollingObject(List<String> mithraInitializationErrors)
    {
        String className = "com.gs.fw.common.mithra.notification.RunsMasterQueue";
        MithraDatabaseObject databaseObject;
        LocalObjectConfig runsConfig = new LocalObjectConfig();
        runsConfig.relationshipCacheSize = 1;
        runsConfig.disableCache = true;
        runsConfig.className = className;
        ReplicationNotificationConnectionManager connectionManager = ReplicationNotificationConnectionManager.getInstance();
        runsConfig.persisterId = getConnectionManagerId(connectionManager);

        databaseObject = this.instantiateDatabaseObject(className, mithraInitializationErrors);
        if (databaseObject != null)
        {
            databaseObject.setConnectionManager(connectionManager, this.getOrCreateConnectionManagerWrapper(connectionManager));
            databaseObject.setDefaultSchema("");
            databaseObject.setSchemaManager(connectionManager);
            ((MithraObjectDeserializer)databaseObject).instantiatePartialCache(runsConfig);
        }
    }

    public boolean isClassConfigured(String className)
    {
        synchronized (this.initializedClasses)
        {
            if (initializedClasses.contains(className)) return true;
        }
        synchronized (this.uninitialized)
        {
            return this.uninitialized.containsKey(className);
        }
    }

    protected MithraObjectPortal initializeObject(String className, List<String> mithraInitializationErrors)
    {
        Config config = null;
        synchronized (uninitialized)
        {
            config = (Config) this.uninitialized.get(className);
        }
        if (config != null)
        {
            MithraObjectPortal portal = config.initializeObject(mithraInitializationErrors);
            if (portal != null)
            {
                synchronized (this.initializedClasses)
                {
                    this.initializedClasses.add(config.className);
                }
            }
            synchronized (uninitialized)
            {
                this.uninitialized.remove(className);
            }
            return portal;
        }
        return null;
    }

    public void resetAllInitializedClasses()
    {
        synchronized (this.initializedClasses)
        {
            for(Iterator it = this.initializedClasses.iterator(); it.hasNext(); )
            {
                try
                {
                    this.invokeStaticMethod(Class.forName(it.next()+"Finder"), "zResetPortal");
                }
                catch (ClassNotFoundException e)
                {
                    throw new RuntimeException("this should never happen", e);
                }
            }
            this.initializedClasses.clear();
        }
    }

    private void resetAllInitializedClasses(Set<String> classesToCleanUp)
    {
        synchronized (this.initializedClasses)
        {
            for(String className: classesToCleanUp)
            {
                try
                {
                    this.invokeStaticMethod(Class.forName(className+"Finder"), "zResetPortal");
                    initializedClasses.remove(className);
                }
                catch (ClassNotFoundException e)
                {
                    throw new RuntimeException("this should never happen", e);
                }
            }
        }
    }

    private void lazyInitDatabaseObjects(MithraRuntimeType mithraRuntimeType, ConnectionManagerType connectionManagerType,
            SchemaType schemaType, Object connectionManager, PersisterId persisterId, List mithraObjectConfigurations,
            List<Config> configs, PostInitializeHook hook)
    {
        String schemaName = null;
        boolean connectionManagerProvidesSchema = false;
        long replicationPollingInterval = connectionManagerType.getReplicationPollingInterval();
        String replicationSchemaName = connectionManagerType.getReplicationSchemaName();
        if (schemaType != null)
        {
            schemaName = schemaType.getName();
            connectionManagerProvidesSchema = schemaType.isGetFromConnectionManager();
            if (schemaType.isReplicationPollingIntervalSet())
            {
                replicationPollingInterval = schemaType.getReplicationPollingInterval();
            }
            if (schemaType.isReplicationSchemaNameSet())
            {
                replicationSchemaName = schemaType.getReplicationSchemaName();
            }
        }
        for (int i = 0; i < mithraObjectConfigurations.size(); i++)
        {
            MithraObjectConfigurationType mithraObjectConfigurationType = (MithraObjectConfigurationType) mithraObjectConfigurations.get(i);
            LocalObjectConfig config = new LocalObjectConfig();
            config.className = mithraObjectConfigurationType.getClassName();
            config.postInitializeHook = hook;
            config.persisterId = persisterId;
            String loadOperationProviderName = null;
            if (connectionManagerType.isLoadOperationProviderSet())
            {
                loadOperationProviderName = connectionManagerType.getLoadOperationProvider();
            }
            if (mithraObjectConfigurationType.isLoadOperationProviderSet())
            {
                loadOperationProviderName = mithraObjectConfigurationType.getLoadOperationProvider();
            }

            config.connectionManagerProvidesTableName = mithraObjectConfigurationType.isGetTableNameFromConnectionManager();

            config.loadOperationProviderName = loadOperationProviderName;
            boolean useMultiUpdate = true;
            if (!connectionManagerType.isUseMultiUpdate() || !mithraObjectConfigurationType.isUseMultiUpdate())
            {
                useMultiUpdate = false;
            }
            config.useMultiUpdate = useMultiUpdate;
            config.connectionManager = connectionManager;
            config.schemaName = schemaName;
            config.connectionManagerProvidesSchema = connectionManagerProvidesSchema;
            config.isDbReplicated = mithraObjectConfigurationType.isReplicated();
            config.replicationPollingInterval = replicationPollingInterval;
            config.replicationSchemaName = replicationSchemaName;
            config.fullCache = mithraObjectConfigurationType.getCacheType().isFull();
            config.offHeapFullCache = config.fullCache && mithraObjectConfigurationType.isOffHeapFullCache();
            if (config.offHeapFullCache)
            {
                StringPool.getInstance().enableOffHeapSupport();
            }
            config.isParticipatingInTx = mithraObjectConfigurationType.getTxParticipation().isFull();
            config.loadCacheOnStartup = mithraObjectConfigurationType.isLoadCacheOnStartup();
            config.minQueriesToKeep = mithraObjectConfigurationType.getFinalMinQueriesToKeep(mithraRuntimeType, this.defaultMinQueriesToKeep);
            config.relationshipCacheSize = mithraObjectConfigurationType.getFinalRelationshipCacheSize(mithraRuntimeType, this.defaultRelationshipCacheSize);
            config.cacheTimeToLive = mithraObjectConfigurationType.getFinalCacheTimeToLive(mithraRuntimeType);
            config.relationshipCacheTimeToLive = mithraObjectConfigurationType.getFinalRelationshipCacheTimeToLive(mithraRuntimeType);
            if (config.relationshipCacheTimeToLive != 0 && config.cacheTimeToLive == 0)
            {
                throw new RuntimeException("relationshipCacheTimeToLive cannot be set without cacheTimeToLive being set for object " + config.className);
            }
            config.disableCache = mithraObjectConfigurationType.getCacheType().isNone();
            config.threeTierExport = mithraObjectConfigurationType.isThreeTierExport();
            config.threeTierExport = overrideBoolean(config.threeTierExport, connectionManagerType.isThreeTierExportSet(), connectionManagerType.isThreeTierExport());
            config.threeTierExport = overrideBoolean(config.threeTierExport, mithraObjectConfigurationType.isThreeTierExportSet(), mithraObjectConfigurationType.isThreeTierExport());
            configs.add(config);
            addUnitialized(config, mithraRuntimeType.isDestroyExistingPortal());
        }
    }

    private void lazyInitTempObjects(MithraRuntimeType mithraRuntimeType, ConnectionManagerType connectionManagerType,
            SchemaType schemaType, Object connectionManager, PersisterId persisterId, List mithraObjectConfigurations,
            List<Config> configs, PostInitializeHook hook)
    {
        int finalMinQueriesToKeep = this.defaultMinQueriesToKeep;
        int finalRelationshipCacheSize = this.defaultRelationshipCacheSize;
        long finalRelationshipCacheTimeToLive = 0;

        if (mithraRuntimeType.isDefaultMinQueriesToKeepSet())
        {
            finalMinQueriesToKeep = mithraRuntimeType.getDefaultMinQueriesToKeep();
        }
        if (mithraRuntimeType.isDefaultRelationshipCacheSizeSet())
        {
            finalRelationshipCacheSize = mithraRuntimeType.getDefaultRelationshipCacheSize();
        }
        String schemaName = null;
        boolean connectionManagerProvidesSchema = false;
        long replicationPollingInterval = connectionManagerType.getReplicationPollingInterval();
        String replicationSchemaName = connectionManagerType.getReplicationSchemaName();
        if (schemaType != null)
        {
            schemaName = schemaType.getName();
            connectionManagerProvidesSchema = schemaType.isGetFromConnectionManager();
            if (schemaType.isReplicationPollingIntervalSet())
            {
                replicationPollingInterval = schemaType.getReplicationPollingInterval();
            }
            if (schemaType.isReplicationSchemaNameSet())
            {
                replicationSchemaName = schemaType.getReplicationSchemaName();
            }
        }
        for (int i = 0; i < mithraObjectConfigurations.size(); i++)
        {
            MithraTemporaryObjectConfigurationType mithraObjectConfigurationType = (MithraTemporaryObjectConfigurationType) mithraObjectConfigurations.get(i);
            TempObjectConfig config = new TempObjectConfig();
            config.className = mithraObjectConfigurationType.getClassName();
            config.postInitializeHook = hook;
            config.persisterId = persisterId;
            boolean useMultiUpdate = true;
            if (!connectionManagerType.isUseMultiUpdate() || !mithraObjectConfigurationType.isUseMultiUpdate())
            {
                useMultiUpdate = false;
            }
            config.useMultiUpdate = useMultiUpdate;
            config.connectionManager = connectionManager;
            config.schemaName = schemaName;
            config.connectionManagerProvidesSchema = connectionManagerProvidesSchema;
            config.replicationPollingInterval = replicationPollingInterval;
            config.replicationSchemaName = replicationSchemaName;
            config.minQueriesToKeep = finalMinQueriesToKeep;
            if (mithraObjectConfigurationType.isMinQueriesToKeepSet())
            {
                config.minQueriesToKeep = mithraObjectConfigurationType.getMinQueriesToKeep();
            }
            config.relationshipCacheSize = finalRelationshipCacheSize;
            if (mithraObjectConfigurationType.isRelationshipCacheSizeSet())
            {
                config.relationshipCacheSize = mithraObjectConfigurationType.getRelationshipCacheSize();
            }
            config.relationshipCacheTimeToLive = finalRelationshipCacheTimeToLive;
            if (config.relationshipCacheTimeToLive != 0 && config.cacheTimeToLive == 0)
            {
                throw new RuntimeException("relationshipCacheTimeToLive cannot be set without cacheTimeToLive being set for object " + config.className);
            }
            config.threeTierExport = mithraObjectConfigurationType.isThreeTierExport();
            config.threeTierExport = overrideBoolean(config.threeTierExport, connectionManagerType.isThreeTierExportSet(), connectionManagerType.isThreeTierExport());
            config.threeTierExport = overrideBoolean(config.threeTierExport, mithraObjectConfigurationType.isThreeTierExportSet(), mithraObjectConfigurationType.isThreeTierExport());
            configs.add(config);
            addUnitialized(config, mithraRuntimeType.isDestroyExistingPortal());
        }
    }

    private void addUnitialized(Config config, boolean destroyExistingPortal)
    {
        boolean reset = false;
        synchronized (initializedClasses)
        {
            if (initializedClasses.contains(config.className))
            {
                if (!destroyExistingPortal)
                {
                    return; // nothing to do
                }
                initializedClasses.remove(config.className);
                reset = true;
            }
        }
        if (reset)
        {
            try
            {
                Class finderClass = Class.forName(config.className + "Finder");
                this.invokeStaticMethod(finderClass, "zResetPortal");
                synchronized (this)
                {
                    runtimeCacheControllerSet.remove(new MithraRuntimeCacheController(finderClass));
                }
            }
            catch (Exception e)
            {
                //ignore, we were trying to reset the portal
            }
        }
        synchronized (uninitialized)
        {
            this.uninitialized.put(config.className, config);
        }
    }

    private boolean overrideBoolean(boolean current, boolean isSet, boolean value)
    {
        return isSet ? value : current;
    }

    private synchronized RelatedFinder getRelatedFinder(String className, List<String> mithraInitializationErrors)
    {
        String finderClassName = className + "Finder";
        Class finderClass;
        try
        {
            finderClass = Class.forName(finderClassName);
            MithraRuntimeCacheController runtimeCacheController = new MithraRuntimeCacheController(finderClass);
            this.runtimeCacheControllerSet.add(runtimeCacheController);
            return runtimeCacheController.getFinderInstance();
        }
        catch (ClassNotFoundException e)
        {
            getLogger().error("Class "+finderClassName+" could not be found", e);
            mithraInitializationErrors.add("Class "+finderClassName+" could not be found");
        }

        return null;
    }

    /**
     * Parses the configuration file. Should only be called by MithraTestResource. Use readConfiguration to parse
     * and initialize the configuration.
     * @param mithraFileIs input stream containing the runtime configuration.
     * @return the parsed configuration
     */
    public MithraRuntimeType parseConfiguration(InputStream mithraFileIs)
    {
        if(mithraFileIs == null)
        {
            throw new MithraBusinessException("Could not parse Mithra configuration, because the input stream is null.");
        }
        try
        {
            MithraRuntimeUnmarshaller unmarshaller = new MithraRuntimeUnmarshaller();
            unmarshaller.setValidateAttributes(false);
            return unmarshaller.parse(mithraFileIs, "");
        }
        catch (IOException e)
        {
            throw new MithraBusinessException("unable to parse ",e);
        }
        finally
        {
            try
            {
                mithraFileIs.close();
            }
            catch (IOException e)
            {
                getLogger().error("Could not close Mithra XML input stream", e);
            }
        }
    }

    public Set getThreeTierConfigSet()
    {
        this.fullyInitialize();
        return this.threeTierConfigSet;
    }

    protected void checkForErrors(List<String> mithraInitializationErrors)
    {
        if (!mithraInitializationErrors.isEmpty())
        {
            StringBuilder sb = new StringBuilder();
            int size =  mithraInitializationErrors.size();
            for (int i = 0; i < size; i++)
            {
                sb.append("\n").append(i+1).append(". ").append(mithraInitializationErrors.get(i));
            }
            throw new MithraBusinessException("Could not initialize Mithra database objects for the following "+mithraInitializationErrors.size()+" reason(s):"+
                sb.toString());
        }
    }

    protected Object invokeStaticMethod(Class classToInvoke, String methodName)
    {
        try
        {
            Method method = classToInvoke.getMethod(methodName, NO_PARAMS);
            return method.invoke(null, NO_ARGS);
        }
        catch (Exception e)
        {
            String msg = "Could not invoke method " + methodName + " on class " + classToInvoke;
            getLogger().error(msg, e);
            throw new MithraException(msg, e);
        }
    }

    /**
     * Clears all query caches. Note: During a transaction, Mithra allocates a special query cache, which will NOT be
     * cleared by this method.
     */
    public void clearAllQueryCaches()
    {
        MithraRuntimeCacheController[] controllers;
        synchronized (this)
        {
            controllers = new MithraRuntimeCacheController[this.runtimeCacheControllerSet.size()];
            Iterator<MithraRuntimeCacheController> setIterator = this.runtimeCacheControllerSet.iterator();
            for (int i=0; i<this.runtimeCacheControllerSet.size(); i++)
            {
                controllers[i] = setIterator.next();
            }
        }
        for (MithraRuntimeCacheController controller : controllers)
        {
            controller.clearQueryCache();
        }
    }

    public synchronized Set<MithraRuntimeCacheController> getRuntimeCacheControllerSet()
    {
        return new UnifiedSet(this.runtimeCacheControllerSet);
    }

    public void fullyInitialize()
    {
        ArrayList configs = null;
        synchronized (uninitialized)
        {
            if (uninitialized.size() == 0) return;
            configs = new ArrayList(uninitialized.values());
        }
        List<String> mithraInitializationErrors = new ArrayList<String>();
        List<MithraObjectPortal> portals = new ArrayList<MithraObjectPortal>();
        for(int i=0;i<configs.size();i++)
        {
            Config config = (Config) configs.get(i);
            MithraObjectPortal portal = this.initializeObject(config.className, mithraInitializationErrors);
            if (portal != null)
            {
                portals.add(portal);
            }
        }
        checkForErrors(mithraInitializationErrors);
        this.loadMithraCache(portals, 1);
    }

    public MithraReplicationNotificationManager getReplicationNotificationManager()
    {
        return this.replicationNotificationManager;
    }

    private static Logger getLogger()
    {
        return logger;
    }

    public MithraObjectPortal initializePortal(String className)
    {
        List<String> errors = new ArrayList<String>();
        final MithraObjectPortal portal = this.initializeObject(className, errors);
        checkForErrors(errors);
        if (portal != null)
        {
            if (MithraManagerProvider.getMithraManager().isInTransaction())
            {
                ExceptionCatchingThread.executeTask(new ExceptionHandlingTask()
                {
                    @Override
                    public void execute()
                    {
                        portal.loadCache();
                    }
                });
            }
            else
            {
                portal.loadCache();
            }
        }
        return portal;
    }

    public Set<RemoteMithraObjectConfig> getCacheReplicableConfigSet()
    {
        return this.cacheReplicableConfigSet;
    }

    public interface PostInitializeHook
    {
        public void callbackAfterInitialize(String className, MithraObjectPortal portal, List<String> mithraInitializationErrors, boolean isRemote);
        public void callbackAfterDatabaseObjectInitialize(String className, MithraDatabaseObject dbo, List<String> mithraInitializationErrors,
                RelatedFinder finder);
    }

    public static abstract class Config
    {
        protected String className;
        protected int minQueriesToKeep;
        protected int relationshipCacheSize;
        protected String schemaName;
        protected long replicationPollingInterval;
        protected String replicationSchemaName;
        protected boolean useMultiUpdate;
        protected boolean connectionManagerProvidesSchema;
        protected boolean isDbReplicated;
        protected boolean fullCache;
        protected boolean offHeapFullCache;
        protected boolean disableCache;
        protected boolean threeTierExport;
        protected String pureNotificationId;
        protected boolean isPure;
        protected long cacheTimeToLive;
        protected long relationshipCacheTimeToLive;
        protected String factoryParameter;
        protected boolean loadCacheOnStartup = true;
        protected boolean initialized = false;
        protected PostInitializeHook postInitializeHook;
        protected PersisterId persisterId;
        protected String loadOperationProviderName;
        protected boolean isParticipatingInTx = true;

        public abstract MithraObjectPortal initializeObject(List<String> mithraInitializationErrors);

        public abstract void initializePortal(MithraObjectPortal portal);

        protected void addToExportedConfigs(RelatedFinder relatedFinder, MithraConfigurationManager configManager)
        {
            if (threeTierExport)
            {
                synchronized (configManager.threeTierConfigSet)
                {
                    configManager.threeTierConfigSet.add(new RemoteMithraObjectConfig(relationshipCacheSize,
                            minQueriesToKeep, className, relatedFinder.getSerialVersionId(),
                            useMultiUpdate, relatedFinder.getHierarchyDepth(), pureNotificationId,
                            this.cacheTimeToLive, this.relationshipCacheTimeToLive, this.factoryParameter, persisterId));
                }
            }
            if (isOffHeapFullCache() && relatedFinder.getAsOfAttributes() != null)
            {
                synchronized (configManager.cacheReplicableConfigSet)
                {
                    configManager.cacheReplicableConfigSet.add(new RemoteMithraObjectConfig(relationshipCacheSize,
                            minQueriesToKeep, className, relatedFinder.getSerialVersionId(),
                            useMultiUpdate, relatedFinder.getHierarchyDepth(), pureNotificationId,
                            this.cacheTimeToLive, this.relationshipCacheTimeToLive, this.factoryParameter, persisterId));
                }
            }
        }

        public String getClassName()
        {
            return className;
        }

        public long getCacheTimeToLive()
        {
            return cacheTimeToLive;
        }

        public boolean isDisableCache()
        {
            return disableCache;
        }

        public int getMinQueriesToKeep()
        {
            return this.disableCache ? 0 : minQueriesToKeep;
        }

        public int getRelationshipCacheSize()
        {
            return relationshipCacheSize;
        }

        public long getRelationshipCacheTimeToLive()
        {
            return relationshipCacheTimeToLive;
        }

        public RemoteMithraService getRemoteMithraService()
        {
            return null;
        }

        public MasterCacheUplink getMasterCacheUplink()
        {
            return null;
        }

        public boolean isLocal()
        {
            return false;
        }

        public boolean isOffHeapFullCache()
        {
            return offHeapFullCache;
        }

        public boolean isThreeTierClient()
        {
            return false;
        }

        public boolean isParticipatingInTx()
        {
            return isParticipatingInTx;
        }
    }

    public class TempObjectConfig extends Config
    {
        private Object connectionManager;

        public synchronized MithraObjectPortal initializeObject(List<String> mithraInitializationErrors)
        {
            RelatedFinder relatedFinder = getRelatedFinder(this.className, mithraInitializationErrors);
            if (initialized)
            {
                return relatedFinder.getMithraObjectPortal();
            }
            addToExportedConfigs(relatedFinder, MithraConfigurationManager.this);
            String finderClassName = className + "Finder";
            Class finderClass;
            try
            {
                finderClass = Class.forName(finderClassName);
                Method method = getMethodByReflection(finderClass, "setTempConfig", new Class[]{TempObjectConfig.class});
                method.invoke(null, new Object[] { this});
                initialized = true;
                if (postInitializeHook != null)
                {
                    postInitializeHook.callbackAfterInitialize(this.className, null, mithraInitializationErrors, false);
                }
            }
            catch (Exception e)
            {
                throw new MithraBusinessException("could not set temp object config", e);
            }
            return null;
        }

        public boolean getUseMultiUpdate()
        {
            return this.useMultiUpdate;
        }

        public PersisterId getPersisterId()
        {
            return this.persisterId;
        }

        public void initializePortal(MithraObjectPortal portal)
        {
            // nothing to do
        }

        public MithraDatabaseObject createDatabaseObject(List<String> mithraInitializationErrors)
        {
            MithraDatabaseObject databaseObject = instantiateDatabaseObject(this.className, mithraInitializationErrors);
            if (databaseObject != null)
            {
                databaseObject.setConnectionManager(this.connectionManager, getOrCreateConnectionManagerWrapper(this.connectionManager));
                databaseObject.setDefaultSchema(this.schemaName);
                if (this.connectionManagerProvidesSchema)
                {
                    if(this.connectionManager == null)
                    {
                        mithraInitializationErrors.add("The connection manager instance for " + databaseObject.getClass().getName() + " is configured to manage " +
                                "the schema, but has null value" );
                    }
                    if (this.connectionManager instanceof SchemaManager
                            || this.connectionManager instanceof ObjectSourceSchemaManager
                            || this.connectionManager instanceof IntSourceSchemaManager)
                    {
                        databaseObject.setSchemaManager(this.connectionManager);
                    }
                    else
                    {
                        mithraInitializationErrors.add("The connection manager class " + this.connectionManager.getClass().getName() +
                                " configured for " + databaseObject.getClass().getName() + " is configured to manage " +
                                "the schema, but does not implement the correct SchemaManager interface." );
                    }
                }
            }
            return databaseObject;
        }
    }

    public class LocalObjectConfig extends Config
    {
        private Object connectionManager;
        private boolean connectionManagerProvidesTableName;

        public boolean isLocal()
        {
            return true;
        }

        public synchronized MithraObjectPortal initializeObject(List<String> mithraInitializationErrors)
        {
            if (initialized)
            {
                RelatedFinder relatedFinder = getRelatedFinder(this.className, mithraInitializationErrors);
                return relatedFinder.getMithraObjectPortal();
            }
            MithraDatabaseObject databaseObject = instantiateDatabaseObject(this.className, mithraInitializationErrors);
            if (databaseObject != null)
            {
                initializeDbObject(mithraInitializationErrors, databaseObject);

                initializeReplicationNotification(mithraInitializationErrors, databaseObject);

                RelatedFinder relatedFinder = getRelatedFinder(this.className, mithraInitializationErrors);
                if (postInitializeHook != null)
                {
                    postInitializeHook.callbackAfterDatabaseObjectInitialize(this.className, databaseObject, mithraInitializationErrors, relatedFinder);
                }
                if (this.fullCache)
                {
                    ((MithraObjectDeserializer)databaseObject).instantiateFullCache(this);
                }
                else // partial or none
                {
                    ((MithraObjectDeserializer)databaseObject).instantiatePartialCache(this);
                }
                MithraObjectPortal mithraObjectPortal = relatedFinder.getMithraObjectPortal();
                addToExportedConfigs(relatedFinder, MithraConfigurationManager.this);
                initialized = true;
                if (postInitializeHook != null)
                {
                    postInitializeHook.callbackAfterInitialize(this.className, mithraObjectPortal, mithraInitializationErrors, false);
                }
                return mithraObjectPortal;
            }
            return null;
        }

        private void initializeReplicationNotification(List<String> mithraInitializationErrors, MithraDatabaseObject databaseObject)
        {
            if(this.isDbReplicated)
            {
                if(databaseObject.isReplicated())
                {
                    synchronized (MithraConfigurationManager.this)
                    {
                        if(replicationNotificationManager == null)
                        {
                            replicationNotificationManager = new MithraReplicationNotificationManager();
                            initializeReplicationNotificationPollingObject(mithraInitializationErrors);
                        }
                        MithraDatabaseObject replicatedDbObject = databaseObject;
                        if (replicationSchemaName != null)
                        {
                            replicatedDbObject = instantiateDatabaseObject(this.className, mithraInitializationErrors);
                            initializeDbObject(mithraInitializationErrors, replicatedDbObject);
                            replicatedDbObject.setDefaultSchema(this.replicationSchemaName);
                        }
                        replicationNotificationManager.addDatabaseObject(replicatedDbObject, this.replicationSchemaName == null ? this.schemaName : this.replicationSchemaName,
                                                                                this.connectionManagerProvidesSchema, this.replicationPollingInterval);
                    }
                }
                else
                {
                    getLogger().error("The MithraObject class " + this.className +
                            " is configured for replication during runtime but was not generated as a replicated object. " +
                            "Make sure to mark it as replicated during generation.");
                }
            }
        }

        private void initializeDbObject(List<String> mithraInitializationErrors, MithraDatabaseObject databaseObject)
        {
            databaseObject.setConnectionManager(this.connectionManager, getOrCreateConnectionManagerWrapper(this.connectionManager));
            databaseObject.setDefaultSchema(this.schemaName);
            if (this.loadOperationProviderName != null)
            {
                databaseObject.setLoadOperationProvider(instantiateLoadOperationProvider(this.loadOperationProviderName, mithraInitializationErrors));
            }
            if (this.connectionManagerProvidesSchema)
            {
                if(this.connectionManager == null)
                {
                    mithraInitializationErrors.add("The connection manager instance for " + databaseObject.getClass().getName() + " is configured to manage " +
                            "the schema, but has null value" );
                }
                else if (this.connectionManager instanceof SchemaManager
                        || this.connectionManager instanceof ObjectSourceSchemaManager
                        || this.connectionManager instanceof IntSourceSchemaManager)
                {
                    databaseObject.setSchemaManager(this.connectionManager);
                }
                else
                {
                    mithraInitializationErrors.add("The connection manager class " + this.connectionManager.getClass().getName() +
                            " configured for " + databaseObject.getClass().getName() + " is configured to manage " +
                            "the schema, but does not implement the correct SchemaManager interface." );
                }
            }

            if(this.connectionManagerProvidesTableName)
            {
                if(this.connectionManager == null)
                {
                    mithraInitializationErrors.add("The connection manager instance for " + databaseObject.getClass().getName() + " is configured to manage " +
                            "the table name, but has a null value" );
                }
                if (this.connectionManager instanceof TablePartitionManager
                        || this.connectionManager instanceof ObjectSourceTablePartitionManager
                        || this.connectionManager instanceof IntSourceTablePartitionManager )
                {
                    databaseObject.setTablePartitionManager(this.connectionManager);
                }
                else
                {
                    mithraInitializationErrors.add("The connection manager class " + this.connectionManager.getClass().getName() +
                            " configured for " + databaseObject.getClass().getName() + " is configured to manage " +
                            "the table name, but does not implement the correct TablePartitionManager interface." );
                }
            }
        }

        public void initializePortal(MithraObjectPortal portal)
        {
            portal.setPersisterId(this.persisterId);
            if (!this.useMultiUpdate)
            {
                portal.setUseMultiUpdate(this.useMultiUpdate);
            }
            if (this.disableCache)
            {
                portal.setDisableCache(true);
            }
        }
    }


    private LoadOperationProvider instantiateLoadOperationProvider(String loadOperationProviderName, List<String> mithraInitializationErrors)
    {
        try
        {
            Class lopClass = Class.forName(loadOperationProviderName);
            LoadOperationProvider loadOperationProvider = (LoadOperationProvider) lopClass.newInstance();
            return loadOperationProvider;
        }
        catch (IllegalAccessException e)
        {
            final String msg = "Could not access class or constructor for class " + loadOperationProviderName;
            getLogger().error(msg, e);
            mithraInitializationErrors.add(msg);
        }
        catch (ClassNotFoundException e)
        {
            final String msg = "Class " + loadOperationProviderName + " could not be found";
            getLogger().error(msg, e);
            mithraInitializationErrors.add(msg);
        }
        catch (InstantiationException e)
        {
            final String msg = "Could not instantiate class " + loadOperationProviderName;
            getLogger().error(msg, e);
            mithraInitializationErrors.add(msg);
        }
        return null;

    }

    private class PureObjectConfig extends Config
    {
        public synchronized MithraObjectPortal initializeObject(List<String> mithraInitializationErrors)
        {
            RelatedFinder relatedFinder = getRelatedFinder(className, mithraInitializationErrors);
            if (relatedFinder != null)
            {
                if (initialized)
                {
                    return relatedFinder.getMithraObjectPortal();
                }
                MithraObjectDeserializer deserializer = instantiateDeserializer(className, mithraInitializationErrors);
                MithraPureObjectFactory factory = (MithraPureObjectFactory) deserializer;
                factory.setFactoryParameter(factoryParameter);

                deserializer.instantiateFullCache(this);
                MithraObjectPortal mithraObjectPortal = relatedFinder.getMithraObjectPortal();
                addToExportedConfigs(relatedFinder, MithraConfigurationManager.this);
                initialized = true;
                if (postInitializeHook != null)
                {
                    postInitializeHook.callbackAfterInitialize(this.className, mithraObjectPortal, mithraInitializationErrors, false);
                }
                return mithraObjectPortal;
            }
            return null;
        }

        public void initializePortal(MithraObjectPortal portal)
        {
            portal.setPureHome(true, pureNotificationId);
            portal.setPersisterId(PURE_PERSISTER_ID);
        }
    }

    private class RemoteObjectConfig extends Config
    {
        private RemoteMithraService remoteMithraService;
        private int remoteSerialId;

        public synchronized MithraObjectPortal initializeObject(List<String> mithraInitializationErrors)
        {
            RelatedFinder relatedFinder = getRelatedFinder(className, mithraInitializationErrors);
            if (relatedFinder != null)
            {
                if (initialized)
                {
                    return relatedFinder.getMithraObjectPortal();
                }
                if (remoteSerialId != relatedFinder.getSerialVersionId())
                {
                    mithraInitializationErrors.add("serial version between server and client does not match for class " + className
                            +" server version: " + remoteSerialId + " local version: " + relatedFinder.getSerialVersionId());
                }
                else
                {
                    MithraObjectDeserializer deserializer = null;
                    if (isPure)
                    {
                        deserializer = instantiateDeserializer(className, mithraInitializationErrors);
                        MithraPureObjectFactory factory = (MithraPureObjectFactory) deserializer;
                        factory.setFactoryParameter(factoryParameter);
                    }
                    else
                    {
                        MithraDatabaseObject databaseObject = instantiateDatabaseObject(className, mithraInitializationErrors);
                        deserializer = (MithraObjectDeserializer) databaseObject;
                    }
                    if (fullCache)
                    {
                        deserializer.instantiateFullCache(this);
                    }
                    else
                    {
                        deserializer.instantiatePartialCache(this);
                    }
                    MithraObjectPortal portal = relatedFinder.getMithraObjectPortal();
                    addToExportedConfigs(relatedFinder, MithraConfigurationManager.this);
                    initialized = true;
                    if (postInitializeHook != null)
                    {
                        postInitializeHook.callbackAfterInitialize(this.className, portal, mithraInitializationErrors, true);
                    }
                    return portal;
                }
            }
            return null;
        }

        @Override
        public boolean isThreeTierClient()
        {
            return true;
        }

        public void initializePortal(MithraObjectPortal portal)
        {
            portal.setPersisterId(this.persisterId);
            if (isPure)
            {
                portal.setPureHome(false, this.pureNotificationId);
            }
            if (!useMultiUpdate)
            {
                portal.setUseMultiUpdate(useMultiUpdate);
            }
            if (this.disableCache)
            {
                portal.setDisableCache(true);
            }
        }

        public RemoteMithraService getRemoteMithraService()
        {
            return remoteMithraService;
        }
    }

    private class CacheReplicatedObjectConfig extends Config
    {
        private MasterCacheUplink masterCacheUplink;
        private int remoteSerialId;

        public synchronized MithraObjectPortal initializeObject(List<String> mithraInitializationErrors)
        {
            RelatedFinder relatedFinder = getRelatedFinder(className, mithraInitializationErrors);
            if (relatedFinder != null)
            {
                if (initialized)
                {
                    return relatedFinder.getMithraObjectPortal();
                }
                if (remoteSerialId != relatedFinder.getSerialVersionId())
                {
                    mithraInitializationErrors.add("serial version between server and client does not match for class " + className
                            +" server version: " + remoteSerialId + " local version: " + relatedFinder.getSerialVersionId());
                }
                else
                {
                    MithraObjectDeserializer deserializer = null;
                    if (isPure)
                    {
                        deserializer = instantiateDeserializer(className, mithraInitializationErrors);
                        MithraPureObjectFactory factory = (MithraPureObjectFactory) deserializer;
                        factory.setFactoryParameter(factoryParameter);
                    }
                    else
                    {
                        MithraDatabaseObject databaseObject = instantiateDatabaseObject(className, mithraInitializationErrors);
                        deserializer = (MithraObjectDeserializer) databaseObject;
                    }
                    deserializer.instantiateFullCache(this);
                    MithraObjectPortal portal = relatedFinder.getMithraObjectPortal();
                    addToExportedConfigs(relatedFinder, MithraConfigurationManager.this);
                    initialized = true;
                    if (postInitializeHook != null)
                    {
                        postInitializeHook.callbackAfterInitialize(this.className, portal, mithraInitializationErrors, true);
                    }
                    return portal;
                }
            }
            return null;
        }

        public void initializePortal(MithraObjectPortal portal)
        {
            portal.setPersisterId(this.persisterId);
            if (isPure)
            {
                portal.setPureHome(false, this.pureNotificationId);
            }
            if (!useMultiUpdate)
            {
                portal.setUseMultiUpdate(useMultiUpdate);
            }
        }

        @Override
        public MasterCacheUplink getMasterCacheUplink()
        {
            return this.masterCacheUplink;
        }
    }

    private static class PortalLoadCacheRunnable extends ExceptionHandlingTask
    {
        private MithraObjectPortal portal;

        private PortalLoadCacheRunnable(MithraObjectPortal portal)
        {
            this.portal = portal;
        }

        @Override
        public void execute()
        {
            portal.loadCache();
        }
    }
}
