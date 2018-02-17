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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cacheloader.AdditionalOperationBuilder;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderContext;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderEngine;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderManagerImpl;
import com.gs.fw.common.mithra.cacheloader.ConfigValues;
import com.gs.fw.common.mithra.cacheloader.DateCluster;
import com.gs.fw.common.mithra.cacheloader.DependentKeyIndex;
import com.gs.fw.common.mithra.cacheloader.DependentLoadingTaskSpawner;
import com.gs.fw.common.mithra.cacheloader.LoadOperationBuilder;
import com.gs.fw.common.mithra.cacheloader.LoadingTaskImpl;
import com.gs.fw.common.mithra.cacheloader.LoadingTaskMonitor;
import com.gs.fw.common.mithra.cacheloader.LoadingTaskRunner;
import com.gs.fw.common.mithra.cacheloader.PostLoadFilterBuilder;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.test.glew.LewProductFinder;
import com.gs.fw.common.mithra.test.glew.LewRelationshipFinder;
import com.gs.fw.common.mithra.test.glew.LewTransactionFinder;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.sql.Timestamp;


public class CacheLoaderEngineTest extends TestCase
{
    private static final Timestamp BUSINESS_DATE = Timestamp.valueOf("2010-10-30 23:59:00");

    private MithraTestResource mithraTestResource;
    private static final String NYK_REGION = "NYK";

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        this.mithraTestResource = new MithraTestResource("MithraCacheTestConfig.xml");
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("nystlew");
        this.mithraTestResource.createSingleDatabase(connectionManager, "nystlew", "testdata/cacheloader/CacheLoaderTest_STLEW.txt");

        this.mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, NYK_REGION, "testdata/cacheloader/CacheLoaderTest_NYLEW.txt");
        this.mithraTestResource.setUp();
    }

    @Override
    protected void tearDown() throws Exception
    {
        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;
        super.tearDown();
    }

    private LoadingTaskImpl createLoadingTask(Class finderClass, Operation op)
    {
        final MithraRuntimeCacheController cacheController = new MithraRuntimeCacheController(finderClass);
        LoadOperationBuilder loadOperationBuilder = new LoadOperationBuilder(op, FastList.<AdditionalOperationBuilder>newList(), new DateCluster(BUSINESS_DATE), cacheController.getFinderInstance());
        PostLoadFilterBuilder postLoadFilterBuilder = new PostLoadFilterBuilder(null, null, null, null);
        return new LoadingTaskImpl(cacheController, loadOperationBuilder, postLoadFilterBuilder, null);
    }

    public void testLoadWithPrerequisites()
    {
        CacheLoaderContext context = new CacheLoaderContext(new CacheLoaderManagerImpl(), FastList.newListWith(BUSINESS_DATE));
        CacheLoaderEngine engine = context.getEngine();
        engine.setConfigValues(new ConfigValues(1, 1, true, 2, 45, 1200));

        Operation op1 = LewProductFinder.region().eq(NYK_REGION).and(LewProductFinder.businessDate().eq(BUSINESS_DATE)).and(LewProductFinder.processingDate().equalsEdgePoint());
        LoadingTaskRunner loadingTask1 = new LoadingTaskRunner(engine, "nylew", createLoadingTask(LewProductFinder.class, op1), LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);
        engine.addTaskToLoadAndSetupThreadPool(loadingTask1);

        Operation op2 = LewContractFinder.region().eq(NYK_REGION).and(LewContractFinder.businessDate().eq(BUSINESS_DATE)).and(LewContractFinder.processingDate().equalsEdgePoint());
        LoadingTaskRunner loadingTask2 = new LoadingTaskRunner(engine, "nylew", createLoadingTask(LewContractFinder.class, op2), LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);
        engine.addTaskToLoadAndSetupThreadPool(loadingTask2);
        loadingTask1.addPrerequisite(loadingTask2);

        assertEquals(0, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(0, LewProductFinder.findMany(LewProductFinder.businessDate().equalsEdgePoint().and(LewProductFinder.processingDate().equalsEdgePoint())).size());

        engine.waitUntilAllTasksCompleted();

        assertEquals(3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(7, LewProductFinder.findMany(LewProductFinder.businessDate().equalsEdgePoint().and(LewProductFinder.processingDate().equalsEdgePoint())).size());

        LoadingTaskMonitor productsInfo = engine.getTaskMonitors().get(0);
        LoadingTaskMonitor contractInfo = engine.getTaskMonitors().get(1);

//        assertTrue(contractInfo.getStartTime() + "<=" + contractInfo.getFinishTime(), contractInfo.getStartTime() <= contractInfo.getFinishTime());
//        assertTrue(contractInfo.getFinishTime() + "<=" + productsInfo.getStartTime(), contractInfo.getFinishTime() <= productsInfo.getStartTime());
//        assertTrue(contractInfo.getSql().indexOf("select ") > 0);
    }

    public void testLoadWithPrerequisiteFailure()
    {
        CacheLoaderContext context = new CacheLoaderContext(new CacheLoaderManagerImpl(), FastList.newListWith(BUSINESS_DATE));
        CacheLoaderEngine engine = context.getEngine();
        engine.setConfigValues(new ConfigValues(1, 1, true, 2, 45, 1200));

        Operation op1 = LewProductFinder.region().eq(NYK_REGION).and(LewProductFinder.businessDate().eq(BUSINESS_DATE)).and(LewProductFinder.processingDate().equalsEdgePoint());
        LoadingTaskRunner loadingTask1 = new LoadingTaskRunner(engine, "nylew", createLoadingTask(LewProductFinder.class, op1), LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);
        engine.addTaskToLoadAndSetupThreadPool(loadingTask1);

        Operation operationToThrowException = new All(LewProductFinder.region());

        LoadingTaskRunner loadingTask2 = new LoadingTaskRunner(engine, "nylew", createLoadingTask(LewContractFinder.class, operationToThrowException), LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);
        engine.addTaskToLoadAndSetupThreadPool(loadingTask2);
        loadingTask1.addPrerequisite(loadingTask2);

        try
        {
            engine.waitUntilAllTasksCompleted();
            fail("missed the expceiton");
        }
        catch (Exception e)
        {
            // expected;
        }

        LoadingTaskMonitor task2Info = engine.getTaskMonitors().get(1);

        assertEquals(LoadingTaskRunner.State.FAILED, task2Info.getState());
    }

    public void testCircularDependencyLoad()
    {
        CacheLoaderContext context = new CacheLoaderContext(new CacheLoaderManagerImpl(), FastList.newListWith(BUSINESS_DATE));
        CacheLoaderEngine engine = context.getEngine();
        engine.setConfigValues(new ConfigValues(1, 1, false, 2, 45, 1200));

        Attribute[] productKeyAttribute = {LewProductFinder.instrumentId()};
        Operation productOperation = LewProductFinder.region().eq(NYK_REGION).and(LewProductFinder.businessDate().eq(BUSINESS_DATE)).and(LewProductFinder.processingDate().equalsEdgePoint());
        DependentLoadingTaskSpawner productClassLoader = new DependentLoadingTaskSpawner(
                productKeyAttribute,
                "nystlew", "NYT",
                productOperation,
                new MithraRuntimeCacheController(LewProductFinder.class),
                context,
                null, null);

        Attribute[] relationshipKeyAttribute = {LewRelationshipFinder.instrumentId()};
        Operation relationshipOperation = LewRelationshipFinder.region().eq(NYK_REGION).and(LewRelationshipFinder.businessDate().eq(BUSINESS_DATE)).and(LewRelationshipFinder.processingDate().equalsEdgePoint());
        DependentLoadingTaskSpawner relationshipClassLoader = new DependentLoadingTaskSpawner(
                relationshipKeyAttribute,
                "nystlew", "NYT",
                relationshipOperation,
                new MithraRuntimeCacheController(LewRelationshipFinder.class),
                context,
                null, null);

        Extractor[] instrumentForContract = {LewContractFinder.instrumentId()};
        DependentKeyIndex productForContract =
                productClassLoader.createDependentKeyIndex(engine, instrumentForContract, null);

        Extractor[] instrumentForProduct = {LewProductFinder.instrumentId()};
        DependentKeyIndex relationshipForProdcut =
                relationshipClassLoader.createDependentKeyIndex(engine, instrumentForProduct, null);

        Extractor[] instrumentForRelationship = {LewRelationshipFinder.underlierInstrumentId()};
        DependentKeyIndex productsForRelationship =
                productClassLoader.createDependentKeyIndex(engine, instrumentForRelationship, null);

        productClassLoader.addDependentThread(relationshipForProdcut);
        relationshipClassLoader.addDependentThread(productsForRelationship);

        Operation contractOperation = LewContractFinder.region().eq(NYK_REGION).and(LewContractFinder.businessDate().eq(BUSINESS_DATE)).and(LewContractFinder.processingDate().equalsEdgePoint());

        LoadingTaskImpl loadingTask = createLoadingTask(LewContractFinder.class, contractOperation);

        LoadingTaskRunner dataSetLoader = new LoadingTaskRunner(engine, "nylew", loadingTask, LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);

        loadingTask.setDependentKeyIndices(FastList.newListWith(productForContract));


        engine.addTaskToLoadAndSetupThreadPool(dataSetLoader);

        assertEquals(0, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(0, LewProductFinder.findMany(LewProductFinder.businessDate().equalsEdgePoint().and(LewProductFinder.processingDate().equalsEdgePoint())).size());

        engine.waitUntilAllTasksCompleted();

        assertEquals("contracts", 3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals("dependent products", 6, LewProductFinder.findMany(LewProductFinder.businessDate().equalsEdgePoint().and(LewProductFinder.processingDate().equalsEdgePoint())).size());
    }

    public void testCombinedDependencyLoad()
    {
        CacheLoaderContext context = new CacheLoaderContext(new CacheLoaderManagerImpl(), FastList.newListWith(BUSINESS_DATE));
        CacheLoaderEngine engine = context.getEngine();
        engine.setConfigValues(new ConfigValues(1, 1, false, 2, 45, 1200));

        Attribute[] productKeyAttribute = {LewProductFinder.instrumentId()};
        Operation productOperation = LewProductFinder.region().eq(NYK_REGION).and(LewProductFinder.businessDate().eq(BUSINESS_DATE)).and(LewProductFinder.processingDate().equalsEdgePoint());
        DependentLoadingTaskSpawner productClassLoader = new DependentLoadingTaskSpawner(
                productKeyAttribute,
                "nystlew", "NYT",
                productOperation,
                new MithraRuntimeCacheController(LewProductFinder.class),
                context,
                null, null);

        Attribute[] relationshipKeyAttribute = {LewRelationshipFinder.instrumentId()};
        Operation relationshipOperation = LewRelationshipFinder.region().eq(NYK_REGION).and(LewRelationshipFinder.businessDate().eq(BUSINESS_DATE)).and(LewRelationshipFinder.processingDate().equalsEdgePoint());
        DependentLoadingTaskSpawner relationshipTaskSpawner = new DependentLoadingTaskSpawner(
                relationshipKeyAttribute,
                "nystlew", "NYT",
                relationshipOperation,
                new MithraRuntimeCacheController(LewRelationshipFinder.class),
                context,
                null, null);

        Extractor[] instrumentForContract = {LewContractFinder.instrumentId()};
        DependentKeyIndex productForContract =
                productClassLoader.createDependentKeyIndex(engine, instrumentForContract, null);

        Extractor[] instrumentForProduct = {LewProductFinder.instrumentId()};
        DependentKeyIndex relationshipForProdcut =
                relationshipTaskSpawner.createDependentKeyIndex(engine, instrumentForProduct, null);

        Extractor[] instrumentForRelationship = {LewRelationshipFinder.underlierInstrumentId()};
        DependentKeyIndex productsForRelationship =
                productClassLoader.createDependentKeyIndex(engine, instrumentForRelationship, null);

        productClassLoader.addDependentThread(relationshipForProdcut);
        relationshipTaskSpawner.addDependentThread(productsForRelationship);

        Operation contractOperation = LewContractFinder.region().eq(NYK_REGION).and(LewContractFinder.businessDate().eq(BUSINESS_DATE)).and(LewContractFinder.processingDate().equalsEdgePoint());

        LoadingTaskImpl loadingTask = createLoadingTask(LewContractFinder.class, contractOperation);

        LoadingTaskRunner dataSetLoader = new LoadingTaskRunner(engine, "nylew", loadingTask, LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);

        loadingTask.setDependentKeyIndices(FastList.newListWith(productForContract));


        engine.addTaskToLoadAndSetupThreadPool(dataSetLoader);

        assertEquals(0, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(0, LewProductFinder.findMany(LewProductFinder.businessDate().equalsEdgePoint().and(LewProductFinder.processingDate().equalsEdgePoint())).size());

        engine.waitUntilAllTasksCompleted();

        assertEquals("contracts", 3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals("dependent products", 6, LewProductFinder.findMany(LewProductFinder.businessDate().equalsEdgePoint().and(LewProductFinder.processingDate().equalsEdgePoint())).size());
    }

    public void testDependentTupleLoad()
    {
        CacheLoaderContext context = new CacheLoaderContext(new CacheLoaderManagerImpl(), FastList.newListWith(BUSINESS_DATE));
        CacheLoaderEngine engine = context.getEngine();
        engine.setConfigValues(new ConfigValues(1, 1, false, 2, 45, 1200));

        Attribute[] keyAttribute = {LewTransactionFinder.instrumentId(), LewTransactionFinder.acctId()};
        Operation operation = LewTransactionFinder.region().eq(NYK_REGION).and(LewTransactionFinder.businessDate().eq(BUSINESS_DATE)).and(LewTransactionFinder.processingDate().equalsEdgePoint());
        DependentLoadingTaskSpawner taskSpawner = new DependentLoadingTaskSpawner(
                keyAttribute,
                "nystlew", "NYT",
                operation,
                new MithraRuntimeCacheController(LewTransactionFinder.class),
                context,
                null, null);

        Extractor[] keyExtractors = {LewContractFinder.instrumentId(), LewContractFinder.acctId()};
        DependentKeyIndex productForContract =
                taskSpawner.createDependentKeyIndex(engine, keyExtractors, null);

        Operation contractOperation = LewContractFinder.region().eq(NYK_REGION).and(LewContractFinder.businessDate().eq(BUSINESS_DATE)).and(LewContractFinder.processingDate().equalsEdgePoint());

        LoadingTaskImpl loadingTask = createLoadingTask(LewContractFinder.class, contractOperation);

        LoadingTaskRunner dataSetLoader = new LoadingTaskRunner(engine, "nylew", loadingTask, LoadingTaskRunner.State.WAITING_FOR_PREREQUISITES);

        loadingTask.setDependentKeyIndices(FastList.newListWith(productForContract));

        engine.addTaskToLoadAndSetupThreadPool(dataSetLoader);

        assertEquals(0, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(0, LewTransactionFinder.findMany(LewTransactionFinder.businessDate().equalsEdgePoint().and(LewTransactionFinder.processingDate().equalsEdgePoint())).size());

        engine.waitUntilAllTasksCompleted();

        assertEquals(3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(5, LewTransactionFinder.findMany(LewTransactionFinder.businessDate().equalsEdgePoint().and(LewTransactionFinder.processingDate().equalsEdgePoint())).size());
    }
}
