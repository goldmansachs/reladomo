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
import java.util.*;

import com.gs.fw.common.mithra.test.*;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.dbextractor.*;
import junit.framework.*;



public class DbExtractorMergeTest extends TestCase
{
    private MithraTestResource mithraTestResource;
    private File otherFile;
    private File mergedFile;
    private File baselineFile;

    protected void setUp() throws Exception
    {
        super.setUp();

        this.otherFile = new File(DbExtractorTest.COMPARE_PATH + "DbExtractorMergeTest_other.txt");
        this.mergedFile = new File(DbExtractorTest.OUTPUT_PATH + "DbExtractorMergeTest_target.txt");
        this.baselineFile = new File(DbExtractorTest.COMPARE_PATH + "DbExtractorMergeTest_expected.txt");
        MithraTestAbstract.copyFile(otherFile, mergedFile);

        String xmlFile = System.getProperty("mithra.xml.config");

        mithraTestResource = new MithraTestResource(xmlFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("M");
        connectionManager.setDatabaseTimeZone(TimeZone.getDefault());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createSingleDatabase(connectionManager, "M", "testdata/DbExtractorMergeTest_source.txt");
        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "M", "M", "testdata/mithraTestDataStringSourceA.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    public void testMerge() throws Exception
	{
        DbExtractor extractor = new DbExtractor(this.mergedFile.getPath(), false, null);
        extractor.addClassToFile(OrderFinder.getFinderInstance(),
                OrderFinder.all());
        extractor.addClassToFile(AuditedOrderFinder.getFinderInstance(),
                AuditedOrderFinder.processingDate().equalsEdgePoint());
        extractor.addClassToFile(BitemporalOrderFinder.getFinderInstance(),
                BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint()));
        extractor.addClassToFile(AccountTransactionFinder.getFinderInstance(),
                AccountTransactionFinder.deskId().eq("M"));
        DbExtractorTest.diffFiles(this.baselineFile, this.mergedFile);
    }

    public void testMergeMultiple() throws Exception
    {
        DbExtractor extractor = new DbExtractor(this.mergedFile.getPath(), false, null);
        extractor.addClassToFile(OrderFinder.getFinderInstance(), OrderFinder.all());
        extractor.addClassToFile(AuditedOrderFinder.getFinderInstance(), AuditedOrderFinder.processingDate().equalsEdgePoint());
        extractor.addClassToFile(BitemporalOrderFinder.getFinderInstance(), BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint()));
        extractor.addClassToFile(AccountTransactionFinder.getFinderInstance(), AccountTransactionFinder.deskId().eq("M"));
        DbExtractorTest.diffFiles(this.baselineFile, this.mergedFile);
    }

    public void testMergeFile() throws Exception
    {
        DbExtractor extractor = new DbExtractor(this.mergedFile.getPath(), false, null);
        extractor.addDataFrom(DbExtractorTest.COMPARE_PATH + "DbExtractorMergeTest_source.txt");
        DbExtractorTest.diffFiles(this.baselineFile, this.mergedFile);
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        super.tearDown();
    }
}