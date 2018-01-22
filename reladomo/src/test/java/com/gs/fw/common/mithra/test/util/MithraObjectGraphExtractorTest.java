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

package com.gs.fw.common.mithra.test.util;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.gs.collections.impl.factory.primitive.*;
import com.gs.collections.impl.set.mutable.*;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.test.*;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.dbextractor.*;
import junit.framework.*;



public class MithraObjectGraphExtractorTest extends TestCase implements ExtractorConfig, MilestoneStrategy, OutputStrategy
{
    private static final Timestamp FIXED_BUSINESS_DATE = Timestamp.valueOf("2010-01-01 23:59:00");
    private static final Timestamp FIXED_PROCESSING_DATE = Timestamp.valueOf("2010-01-02 12:00:00");
    private MithraTestResource mithraTestResource;
    private int extractThreshold = 10;
    private boolean isFixedBusinessDate = true;
    private boolean isFixedProcessingDate = true;
    static final String SOURCE_DIR = "reladomo" + File.separator + "src" + File.separator + "test" + File.separator+ "resources" + File.separator+ "testdata" + File.separator+ "objectgraph" + File.separator;
    static final String TARGET_DIR = "reladomo" + File.separator + "target" + File.separator + "tmp" + File.separator;
    private File extractFile;
    private Set<RelatedFinder> nullOutputFiles = UnifiedSet.newSet();
    private boolean overwriteOutputFile = true;

    protected void setUp() throws Exception
    {
        super.setUp();

        mithraTestResource = new MithraTestResource(System.getProperty("mithra.xml.config"));

        ConnectionManagerForTests lewConnectionManager = ConnectionManagerForTests.getInstance("lew");
        ConnectionManagerForTests paraConnectionManager  = ConnectionManagerForTests.getInstance("para");
        mithraTestResource.createSingleDatabase(lewConnectionManager, "testdata/lewDataSource.txt");
        mithraTestResource.createSingleDatabase(paraConnectionManager, "testdata/paraDataSource.txt");

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("extract");
        connectionManager.setDatabaseTimeZone(TimeZone.getDefault());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createSingleDatabase(connectionManager, "extract", "testdata/objectgraph/MithraObjectGraphExtractorTest_data.txt");
        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "extract", "extract", "testdata/objectgraph/MithraObjectGraphExtractorTest_dataStringSourceA.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        super.tearDown();
        DbExtractorTest.removeTmpFiles(MithraObjectGraphExtractor.class.getSimpleName());
    }

    public void testSingleObject() throws Exception
	{
        testScenario(new MithraObjectGraphExtractor(this, this, this), 6, orderId(1));
    }

    public void testOneToOne() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 10, orderId(2));
    }

    public void testOneToMany() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 14, orderId(3));
    }

    public void testUnConfiguredRelatedObject() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 7, orderId(4));
    }

    public void testMultipleOperationsWithDeepGraph() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 25, orderId(5), orderId(6));
    }

    public void testMaxObjectThresholdExceeded() throws Exception
    {
        try
        {
            this.extractThreshold = 1;
            MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
            testScenario(extractor, 14, orderId(3), orderId(5));
            fail("maxObjectThreshold should have been exceeded by order items");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Too many objects (BitemporalOrderItemList/2) extracted via BitemporalOrder.items()*", e.getMessage());
        }
    }

    public void testWarnIfMaxObjectThresholdExceeded() throws Exception
    {
        this.extractThreshold = 1;
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        extractor.setWarnIfMaxObjectThresholdExceeded(true);
        testScenario(extractor, 15, orderId(3), orderId(5));
    }

    public void testAddFilterToStayWithinThreshold() throws Exception
    {
        this.extractThreshold = 1;
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        extractor.addRelationshipFilter(BitemporalOrderFinder.items(), BitemporalOrderItemFinder.id().in(IntSets.immutable.of(1, 3)));
        testScenario(extractor, 28, orderId(3), orderId(5));
    }

    public void testIgnoreRelationship() throws Exception
    {
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        extractor.ignoreRelationship(BitemporalOrderItemFinder.productInfo());
        testScenario(extractor, 14, orderId(5));
    }

    public void testNoEdgePoints() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 15, orderId(7));
    }

    public void testProcessingDateEdgePoints() throws Exception
    {
        isFixedProcessingDate = false;
        testScenario(new MithraObjectGraphExtractor(this, this, this), 15, orderId(7));
    }

    public void testProcessingDateAndBusinessDateEdgePoints() throws Exception
    {
        this.isFixedBusinessDate = false;
        this.isFixedProcessingDate = false;
        testScenario(new MithraObjectGraphExtractor(this, this, this), 15, orderId(7));
    }

    public void testInClauseOperationWithDeepGraph() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 16, orderId(5, 6));
    }

    public void testSourceAttribute() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 4, accountId(1));
    }

    public void testSourceAttributeWithEdgePoints() throws Exception
    {
        isFixedProcessingDate = false;
        testScenario(new MithraObjectGraphExtractor(this, this, this), 4, accountId(1));
    }

    public void testUnConfiguredJoinObject() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 7, orderId(8));
    }

    // see orderWithSameIdAsProduct which links back to different order with other items
    // Sale relationship was added to fix bug where explosion logic was not per-branch
    public void testPreventToManyExplosion() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 23, orderId(9));
    }

    public void testRelationshipWithFilterObject() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 10, orderId(10));
    }

    //
    public void testEdgePointOperationPropagatesCorrectlyThroughGraph() throws Exception
    {
        isFixedProcessingDate = false;
        testScenario(new MithraObjectGraphExtractor(this, this, this), 13, orderId(11));
    }

    public void testToManyExplosionLogicIsPerExtractedOperation() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 31, orderId(9), orderId(90));
    }

    public void testRelationshipWithJoinObject() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 10, orderId(12));
    }

    public void testRelationshipWithJoinObjectAndFilter() throws Exception
    {
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        extractor.ignoreRelationship(BitemporalOrderFinder.orderStatus());
        extractor.ignoreRelationship(BitemporalOrderFinder.orderStatusViaJoin());

        // todo: whitba: add this filter from the filtered mapper
        extractor.addRelationshipFilter(BitemporalOrderToOrderStatusFinder.getFinderInstance(), BitemporalOrderToOrderStatusFinder.getFinderInstance().statusId().eq(130));

        testScenario(extractor, 11, orderId(13));
    }

    public void testJoinObjectExceedsExtractThreshold() throws Exception
    {
        this.extractThreshold = 1;
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        extractor.ignoreRelationship(BitemporalOrderFinder.orderStatus());
        extractor.ignoreRelationship(BitemporalOrderFinder.orderStatusViaJoin());
        try
        {
            testScenario(extractor, 9, orderId(13));
            fail("maxObjectThreshold should have been exceeded by join object");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Too many objects (BitemporalOrderToOrderStatusList/2) extracted via BitemporalOrder.orderStatusViaJoinWithFilter()", e.getMessage());
        }
    }

    public void testAddFilterUsingFinderInstance() throws Exception
    {
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        extractor.addRelationshipFilter(BitemporalOrderItemFinder.getFinderInstance(), BitemporalOrderItemFinder.id().in(IntSets.immutable.of(1, 3)));
        testScenario(extractor, 28, orderId(3), orderId(5));
    }

    public void testAddedFiltersAreCombined() throws Exception
    {
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        extractor.addRelationshipFilter(BitemporalOrderItemFinder.getFinderInstance(), BitemporalOrderItemFinder.id().eq(1));
        extractor.addRelationshipFilter(BitemporalOrderItemFinder.getFinderInstance(), BitemporalOrderItemFinder.id().eq(3));
        testScenario(extractor, 28, orderId(3), orderId(5));
    }

    public void testTimestampBusinessDateWithFixedProcessingDate() throws Exception
    {
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        testScenario(extractor, 3, ParaPositionFinder.productIdentifier().eq("EXTCT:2000000001"));
    }

    public void testTimestampBusinessDateWithProcessingDateEdgePoint() throws Exception
    {
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        isFixedProcessingDate = false;
        testScenario(extractor, 3, ParaPositionFinder.productIdentifier().eq("EXTCT:2000000001"));
    }

    public void testExtractAll() throws IOException
    {
        this.isFixedBusinessDate = false;
        this.isFixedProcessingDate = false;
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        this.testScenario(extractor, 42, BitemporalOrderFinder.all(), DatedAccountFinder.deskId().eq("extract"), ParaPositionFinder.all());
    }

    public void testJoinWithNull() throws Exception
    {
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        testScenario(extractor, 1, ParaPositionFinder.productIdentifier().eq("EXTCT:2000000002"));
    }

    public void testExtractThresholdWithFullMilestoning() throws Exception
    {
        this.extractThreshold = 2;
        testScenario(new MithraObjectGraphExtractor(this, this, this), 14, orderId(14));
    }

    public void testExtractThresholdWithProcessingEdgePoint() throws Exception
    {
        this.extractThreshold = 2;
        this.isFixedProcessingDate = false;
        testScenario(new MithraObjectGraphExtractor(this, this, this), 14, orderId(14));
    }

    public void testExtractThresholdWithBusinessAndProcessingEdgePoint() throws Exception
    {
        this.extractThreshold = 2;
        this.isFixedProcessingDate = false;
        this.isFixedBusinessDate = false;
        testScenario(new MithraObjectGraphExtractor(this, this, this), 14, orderId(14));
    }

    public void testNullOutputFile() throws Exception
    {
        this.nullOutputFiles.add(BitemporalOrderFinder.getFinderInstance());
        testScenario(new MithraObjectGraphExtractor(this, this, this), 10, orderId(2));
    }

    public void testOverwriteOutputFile() throws Exception
    {
        this.overwriteOutputFile = true;
        this.extractFile = targetFile(".actual.txt");
        MithraObjectGraphExtractor extractor1 = new MithraObjectGraphExtractor(this, this, this);
        extractor1.addRootOperation(orderId(1));
        extractor1.extract();
        MithraObjectGraphExtractor extractor2 = new MithraObjectGraphExtractor(this, this, this);
        extractor2.addRootOperation(orderId(2));
        extractor2.extract();
        DbExtractorTest.diffFiles(sourceFile(".txt"), this.extractFile);
    }

    public void testDoNotOverwriteOutputFile() throws Exception
    {
        this.overwriteOutputFile = false;
        this.extractFile = targetFile(".actual.txt");
        MithraObjectGraphExtractor extractor1 = new MithraObjectGraphExtractor(this, this, this);
        extractor1.addRootOperation(orderId(1));
        extractor1.extract();
        MithraObjectGraphExtractor extractor2 = new MithraObjectGraphExtractor(this, this, this);
        extractor2.addRootOperation(orderId(2));
        extractor2.extract();
        DbExtractorTest.diffFiles(sourceFile(".txt"), this.extractFile);
    }

    // Sale 16 must be traversed via saleWithUserId relationship first to ensure SaleLineItems are extracted. Depth
    // first traversal causes Sale 16 to be traversed via SaleLineItem.saleWithManufacturerId and to-many explosion
    // logic causes SaleLineItems not to be traversed.
    public void testBreadthFirstTraversalForToManyExplosion() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 16, orderId(15));
    }

    public void testOperationWithNoResult() throws Exception
    {
        final int nonExistingOrderId = -1;
        // adding order 1 to ensure actual results are created
        testScenario(new MithraObjectGraphExtractor(this, this, this), 7, orderId(nonExistingOrderId), orderId(1));
    }

    public void testNonMilestonedObject() throws Exception
    {
        testScenario(new MithraObjectGraphExtractor(this, this, this), 3, ProductFinder.productId().eq(100));
    }

    public void testEvaluateRelationship() throws Exception
    {
        this.isFixedProcessingDate = false;
        BitemporalOrderList orders = BitemporalOrderFinder.findMany(this.addAsOfOperations(orderId(3, 5, 7)));
        MithraObjectGraphExtractor extractor = new MithraObjectGraphExtractor(this, this, this);
        BitemporalOrderItemList items = extractor.evaluateDeepRelationship(orders, BitemporalOrderFinder.items());
        assertEquals(7, items.size());
        ProductList products = extractor.evaluateDeepRelationship(orders, BitemporalOrderFinder.items().productInfo());
        assertEquals(2, products.size());
        BitemporalOrderItemStatusList statuses = extractor.evaluateDeepRelationship(orders, BitemporalOrderFinder.items().orderItemStatus());
        assertEquals(0, statuses.size());
    }

    private void testScenario(MithraObjectGraphExtractor extractor, int expectedDbRetrieveCount, Operation... operation) throws IOException
    {
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        int initialDbCount = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        for (Operation op : operation)
        {
            extractor.addRootOperation(op);
        }
        extractor.ignoreRelationship(LewAccountFinder.contracts());
        this.extractFile = targetFile(".actual.txt");
        extractor.extract();
        int dbRetrieveCount = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() - initialDbCount;
        DbExtractorTest.diffFiles(sourceFile(".txt"), this.extractFile);
        if (BitemporalOrderFinder.getMithraObjectPortal().isPartiallyCached())
        {
            assertTrue(expectedDbRetrieveCount >= dbRetrieveCount);
        }
    }

    private File sourceFile(String extension)
    {
        return new File(SOURCE_DIR + MithraObjectGraphExtractorTest.class.getSimpleName() + "_" + this.getName() + extension);
    }

    private File targetFile(String extension)
    {
        return new File(TARGET_DIR + MithraObjectGraphExtractorTest.class.getSimpleName() + "_" + this.getName() + extension);
    }

    private Operation orderId(int... ids)
    {
        return BitemporalOrderFinder.orderId().in(IntSets.immutable.of(ids));
    }

    private Operation accountId(int... ids)
    {
        Operation operation = DatedAccountFinder.deskId().eq("extract");
        return operation.and(DatedAccountFinder.id().in(IntSets.immutable.of(ids)));
    }

    @Override
    public int getExtractThresholdRatio()
    {
        return this.extractThreshold;
    }

    @Override
    public int getThreadPoolSize()
    {
        return 1;
    }

    @Override
    public int getTimeoutSeconds()
    {
        return 1000;
    }

    @Override
    public Operation addAsOfOperations(Operation operation)
    {
        Operation milestoned = operation;
        RelatedFinder finder = operation.getResultObjectPortal().getFinder();
        Attribute businessDate = finder.getAttributeByName("businessDate");
        if(businessDate != null )
        {
            if(businessDate instanceof AsOfAttribute)
            {
                AsOfAttribute asOfAttribute = (AsOfAttribute) businessDate;
                milestoned = milestoned.and(this.isFixedBusinessDate ? asOfAttribute.eq(FIXED_BUSINESS_DATE) : asOfAttribute.equalsEdgePoint());
            }
            else
            {
                milestoned = milestoned.and(businessDate.nonPrimitiveEq(FIXED_BUSINESS_DATE));
            }
        }
        Attribute processingDate = finder.getAttributeByName("processingDate");
        if(processingDate != null )
        {
            AsOfAttribute asOfAttribute = (AsOfAttribute) processingDate;
            milestoned = milestoned.and(this.isFixedProcessingDate ? asOfAttribute.eq(FIXED_PROCESSING_DATE) : asOfAttribute.equalsEdgePoint());
        }
        return milestoned;
    }

    @Override
    public File getOutputFile(RelatedFinder relatedFinder, Object source)
    {
        return this.nullOutputFiles.contains(relatedFinder) ? null : this.extractFile;
    }

    @Override
    public String getOutputFileHeader()
    {
        return "";
    }

    @Override
    public boolean overwriteOutputFile()
    {
        return this.overwriteOutputFile;
    }
}
