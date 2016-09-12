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

package com.gs.fw.common.mithra.ui.gwt.server;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.ui.gwt.client.CachedClassData;
import com.gs.fw.common.mithra.ui.gwt.client.JvmMemory;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;



public class MithraCacheRemoteServiceImpl implements MithraCacheRemoteService
{
    private static final String MITHRA_TEST_DATA_FILE_PATH = "hsqldb/";

    private transient Map<String, MithraRuntimeCacheController> classToControllerMap;
    private transient long lastRefreshTime;

    public MithraCacheRemoteServiceImpl()
    {
        setUpTestResource();
    }

    protected void setUpTestResource()
    {
    /* uncomment for testing
        com.gs.fw.common.mithra.test.MithraTestResource mithraTestResource = new com.gs.fw.common.mithra.test.MithraTestResource("MithraCacheUiConfig.xml");

        com.gs.fw.common.mithra.test.ConnectionManagerForTests connectionManager = com.gs.fw.common.mithra.test.ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");

        try
        {
            com.gs.fw.common.mithra.test.ConnectionManagerForTests cm = com.gs.fw.common.mithra.test.ConnectionManagerForTests.getInstanceForDbName("TEST");
            mithraTestResource.createDatabaseForStringSourceAttribute(cm, "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceA.txt");
            mithraTestResource.createDatabaseForIntSourceAttribute(cm, 0, MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceA.txt");
            mithraTestResource.createDatabaseForStringSourceAttribute(cm, "B", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceB.txt");
            mithraTestResource.createDatabaseForIntSourceAttribute(cm, 1, MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceB.txt");
            mithraTestResource.createSingleDatabase(cm, MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataDefaultSource.txt");

            mithraTestResource.setUp();
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not initialize mithra", e);
        }
    */
    }

    public JvmMemory getJvmMemory()
    {
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        return new JvmMemory(free, total);
    }

    public JvmMemory forceGc()
    {
        System.gc();
        return this.getJvmMemory();
    }

    public List<CachedClassData> clearAllQueryCaches()
    {
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        return this.getCachedClasses();
    }

    public CachedClassData clearCache( String className )
    {
        clearControllerMap();
        MithraRuntimeCacheController cacheController = this.getControllerMap().get(className);
        if (cacheController == null)
        {
            throw new RuntimeException("no cache controller for "+className);
        }
        cacheController.clearQueryCache();
        return createCachedClassData(cacheController);
    }

    public List<CachedClassData> getCachedClasses()
    {
        Map<String, MithraRuntimeCacheController> controllerMap = this.getControllerMap();
        List<CachedClassData> result = new ArrayList<CachedClassData>(controllerMap.size());
        for(MithraRuntimeCacheController cont: controllerMap.values())
        {
            if (!cont.isTemporaryObject())
            {
                result.add(createCachedClassData(cont));
            }
        }
        Collections.sort(result, NAME_COMPARATOR);
        return result;
    }

    public CachedClassData reloadCache( String className )
    {
        MithraRuntimeCacheController cacheController = this.getControllerMap().get(className);
        if (cacheController == null)
        {
            throw new RuntimeException("no cache controller for "+className);
        }
        cacheController.reloadCache();
        return createCachedClassData(cacheController);
    }

    public CachedClassData turnSqlOff( String className )
    {
        return setSqlLoggerLevel(className, Level.INFO, Level.INFO);
    }

    public CachedClassData turnSqlOn( String className )
    {
        return setSqlLoggerLevel(className, Level.DEBUG, Level.INFO);
    }

    public CachedClassData turnSqlMaxOn( String className )
    {
        return setSqlLoggerLevel(className, Level.DEBUG, Level.DEBUG);
    }

    private void clearControllerMap()
    {
        synchronized (this)
        {
            this.classToControllerMap = null;
        }
    }

    private transient static final Comparator<CachedClassData> NAME_COMPARATOR = new Comparator<CachedClassData>()
    {
        public int compare(CachedClassData o1, CachedClassData o2)
        {
            return o1.getClassName().compareTo(o2.getClassName());
        }
    };

    private Map<String, MithraRuntimeCacheController> getControllerMap()
    {
        synchronized (this)
        {
            if (classToControllerMap == null || lastRefreshTime < System.currentTimeMillis() - 10000) // refresh every 10 seconds
            {
                lastRefreshTime = System.currentTimeMillis();
                classToControllerMap = new HashMap<String, MithraRuntimeCacheController>();
                inflateCacheControllerMap();
            }
            return this.classToControllerMap;
        }
    }

    private void inflateCacheControllerMap()
    {
        Set<MithraRuntimeCacheController> controllerSet = MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet();
        for(MithraRuntimeCacheController cont: controllerSet)
        {
            classToControllerMap.put(cont.getClassName(), cont);
        }
    }

    private CachedClassData createCachedClassData(MithraRuntimeCacheController cacheController)
    {
        String name = cacheController.getClassName();
        Logger logger = getLogger(name);
        Logger batchLogger = getBatchLogger(name);
        byte sqlLevel = CachedClassData.SQL_IS_OFF;
        if (batchLogger.isDebugEnabled()) sqlLevel = CachedClassData.SQL_IS_MAX_ON;
        else if (logger.isDebugEnabled()) sqlLevel = CachedClassData.SQL_IS_ON;
        return new CachedClassData(cacheController.getCacheSize(), name,
                cacheController.isPartialCache(), sqlLevel);
    }

    private Logger getLogger(String name)
    {
        return LogManager.getLogger( "com.gs.fw.common.mithra.sqllogs." + name.substring( name.lastIndexOf( '.' ) + 1 ) );
    }

    private Logger getBatchLogger(String name)
    {
        return LogManager.getLogger("com.gs.fw.common.mithra.batch.sqllogs." + name.substring(name.lastIndexOf('.') + 1));
    }

    private CachedClassData setSqlLoggerLevel(String className, Level normalLevel, Level batchLevel)
    {
        MithraRuntimeCacheController cacheController = this.getControllerMap().get(className);
        if (cacheController == null)
        {
            throw new RuntimeException("no cache controller for "+className);
        }
        this.getLogger(className).setLevel(normalLevel);
        this.getBatchLogger(className).setLevel(batchLevel);
        return createCachedClassData(cacheController);
    }
}
