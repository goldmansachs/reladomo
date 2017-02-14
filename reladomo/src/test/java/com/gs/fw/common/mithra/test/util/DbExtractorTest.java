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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.gs.fw.common.mithra.test.MithraTestDataParser;
import com.gs.fw.common.mithra.test.TestMithraTestDataParser;
import com.gs.fw.common.mithra.util.fileparser.BinaryCompressor;
import com.gs.fw.common.mithra.util.fileparser.MithraParsedData;
import junit.framework.TestCase;

import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.TestBinaryArrayFinder;
import com.gs.fw.common.mithra.util.dbextractor.DbExtractor;
import com.gs.fw.common.mithra.util.dbextractor.FitnesseHeaderFormatter;
import com.gs.fw.common.mithra.util.dbextractor.FitnesseRowFormatter;



public class DbExtractorTest extends TestCase
{
    private MithraTestResource mithraTestResource;
    static final String COMPARE_PATH = "reladomo" + File.separator + "src" + File.separator + "test" + File.separator+ "resources" + File.separator+ "testdata" + File.separator;
    static final String OUTPUT_PATH = "reladomo" + File.separator + "target" + File.separator + "tmp" + File.separator;

    protected void setUp() throws Exception
    {
        super.setUp();
        String xmlFile = System.getProperty("mithra.xml.config");

        mithraTestResource = new MithraTestResource(xmlFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseTimeZone(TimeZone.getDefault());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createSingleDatabase(connectionManager, "A", "testdata/DbExtractorTest_source.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    public void testMithraTestDataExtractor() throws Exception
	{
        DbExtractor extractor = new DbExtractor(OUTPUT_PATH + "DbExtractorTest_target.txt", false);
        this.executeTest(extractor, "DbExtractorTest_target.txt", "DbExtractorTest_source.txt");
        deleteFile("DbExtractorTest_target.txt");
    }

    public void testMithraTestDataExtractorCompressed() throws Exception
	{
        DbExtractor extractor = new DbExtractor(OUTPUT_PATH + "DbExtractorTest_target.txt.ccbf", false);
        deleteFile("DbExtractorTest_target.txt.ccbf");

        extractStuff(extractor);
        List<MithraParsedData> compressedData = new BinaryCompressor().decompress(OUTPUT_PATH + "DbExtractorTest_target.txt.ccbf");
        List<MithraParsedData> uncompressedData = new MithraTestDataParser(COMPARE_PATH + "DbExtractorTest_source.txt").getResults();
        TestMithraTestDataParser.compareData(uncompressedData, compressedData);
        deleteFile("DbExtractorTest_target.txt.ccbf");
    }

    public void testInMemoryMithraTestDataExtractorCompressed() throws Exception
    {
        DbExtractor extractor = new DbExtractor(OUTPUT_PATH + "DbExtractorTest_target.txt.ccbf", false);
        extractor.saveMergedDataInMemory();
        deleteFile("DbExtractorTest_target.txt.ccbf");
        extractStuff(extractor);
        extractor.writeMergedDataToColumnarFile();
        List<MithraParsedData> compressedData = new BinaryCompressor().decompress(OUTPUT_PATH + "DbExtractorTest_target.txt.ccbf");
        List<MithraParsedData> uncompressedData = new MithraTestDataParser(COMPARE_PATH + "DbExtractorTest_source.txt").getResults();
        TestMithraTestDataParser.compareData(uncompressedData, compressedData);
        deleteFile("DbExtractorTest_target.txt.ccbf");
    }

    public void deleteFile(String fname)
    {
        File extracted = new File(COMPARE_PATH + fname);
        extracted.delete();
    }

    protected void extractStuff(DbExtractor extractor) throws IOException
    {
        extractor.addClassToFile(ParaDeskFinder.getFinderInstance(), ParaDeskFinder.all());

        extractor.addClassToFile(TestBinaryArrayFinder.getFinderInstance(), TestBinaryArrayFinder.all());

        List<DeepRelationshipAttribute> deepFetchAttributes = new ArrayList<DeepRelationshipAttribute>();
        deepFetchAttributes.add(OrderFinder.items());
        deepFetchAttributes.add(OrderFinder.orderStatus());
        deepFetchAttributes.add(OrderFinder.items().orderItemStatus());
        extractor.addClassAndRelatedToFile(OrderFinder.getFinderInstance(), OrderFinder.all(), deepFetchAttributes);
    }

    public void testFitnesseExtractor() throws Exception
	{
        DbExtractor extractor = new DbExtractor(OUTPUT_PATH + "DbExtractorTest_targetFitnesse.txt", new FitnesseRowFormatter(), new FitnesseHeaderFormatter(), "|", true);
        extractor.setEndPointsInclusive(true);
        this.executeTest(extractor, "DbExtractorTest_targetFitnesse.txt", "DbExtractorTest_expectedFitnesse.txt");
    }

    private void executeTest(DbExtractor extractor, String targetFile, String expectedFile) throws IOException
    {
        File extracted = new File(OUTPUT_PATH + targetFile);
        extracted.delete();

        extractStuff(extractor);

        diffFiles(new File(COMPARE_PATH + expectedFile), extracted);
    }

    static void diffFiles(File baseline, File extracted) throws IOException
    {
        BufferedReader inBaseline = null;
        BufferedReader inExtracted = null;
        try
        {
            inBaseline = new BufferedReader(new FileReader(baseline));
            inExtracted = new BufferedReader(new FileReader(extracted));
            
            skipHeader(inBaseline);

            boolean stop = false;

            String baselineLine = null;
            String extractedLine = null;
            while (!stop)
            {
                baselineLine = inBaseline.readLine();
                extractedLine = inExtracted.readLine();

                assertEquals(baselineLine, extractedLine);

                if (baselineLine == null || extractedLine == null)
                {
                    stop = true;
                }
            }
            assertNull(baselineLine);
            assertNull(extractedLine);
        }
        finally
        {
            if (inBaseline != null)
            {
                inBaseline.close();
            }
            if (inExtracted != null)
            {
                inExtracted.close();
            }
        }
    }

    static void skipHeader(BufferedReader in) throws IOException
    {
        boolean done = false;
        boolean inComment = false;
        while(!done)
        {
            in.mark(10000);
            String line = in.readLine().trim();
            if (inComment)
            {
                if (line.endsWith("*/"))
                {
                    inComment = false;
                }
            }
            else if (line.startsWith("/*"))
            {
                inComment = true;
            }
            else if (!line.isEmpty())
            {
                in.reset();
                done = true;
            }
        }
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        super.tearDown();
        removeTmpFiles("DbExtractor");
    }

    public static void removeTmpFiles(String prefix)
    {
        File outDir = new File(OUTPUT_PATH);
        File[] files = outDir.listFiles();
        for(File f: files)
        {
            if (f.isFile() && f.getName().startsWith(prefix))
            {
                f.delete();
            }
        }
    }
}
