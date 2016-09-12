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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import junit.framework.TestCase;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityFinder;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityList;
import com.gs.fw.common.mithra.finder.Operation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;


public class TestDatabaseReadPerformance extends TestCase
{
    public static final String SOURCE_X = "X";
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private MithraTestResource mithraTestResource;

    protected void setUp()
    throws Exception
    {
        String xmlFile = System.getProperty("mithra.xml.config");

        mithraTestResource = new MithraTestResource(xmlFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("X");
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "X", "X", "testdata/mithraPerformanceTestData.txt");
        mithraTestResource.createSingleDatabase(connectionManager, "X", "testdata/mithraTestDataDefaultSource.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    public void testLoadTimeForAll()
    {
        for(int i=0; i < 2; i++)
        {
            long now = System.currentTimeMillis();
            Operation op = PositionQuantityFinder.businessDate().equalsEdgePoint();
            op = op.and(PositionQuantityFinder.processingDate().equalsEdgePoint());
            op = op.and(PositionQuantityFinder.acmapCode().eq(SOURCE_X));

            PositionQuantityList positionQuantityList = new PositionQuantityList(op);
            positionQuantityList.setBypassCache(true);
            positionQuantityList.forceResolve();
            long totalTime = (System.currentTimeMillis() - now);
            System.out.println("took "+totalTime +" ms "+" per object: "+((double)totalTime*1000)/positionQuantityList.size()+ " microseconds");
        }
    }

    public void testArchiveReadTime() throws IOException, ClassNotFoundException
    {
        Operation op = PositionQuantityFinder.businessDate().equalsEdgePoint();
        op = op.and(PositionQuantityFinder.processingDate().equalsEdgePoint());
        op = op.and(PositionQuantityFinder.acmapCode().eq(SOURCE_X));

        PositionQuantityList positionQuantityList = new PositionQuantityList(op);
        positionQuantityList.setBypassCache(true);
        MithraRuntimeCacheController cacheController = new MithraRuntimeCacheController(PositionQuantityFinder.class);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        cacheController.archiveObjects(baos, positionQuantityList);
        ByteArrayInputStream bain = new ByteArrayInputStream(baos.toByteArray());
        for(int i=0; i < 6; i++)
        {
            long now = System.currentTimeMillis();
            cacheController.readCacheFromArchive(bain);
            long totalTime = (System.currentTimeMillis() - now);
            System.out.println("took "+totalTime +" ms "+" per object: "+((double)totalTime*1000)/positionQuantityList.size()+ " microseconds");
            bain.reset();
        }
    }

    public void testLoadTimeForTwoDates() throws Exception
    {
        for(int i=0; i < 2; i++)
        {
            long now = System.currentTimeMillis();
            Operation op = PositionQuantityFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2006-08-03 00:00:00").getTime()));
            op = op.and(PositionQuantityFinder.acmapCode().eq(SOURCE_X));

            PositionQuantityList positionQuantityList = new PositionQuantityList(op);
            positionQuantityList.setBypassCache(true);
            positionQuantityList.forceResolve();

            op = PositionQuantityFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2006-07-28 00:00:00").getTime()));
            op = op.and(PositionQuantityFinder.acmapCode().eq(SOURCE_X));

            positionQuantityList = new PositionQuantityList(op);
            positionQuantityList.setBypassCache(true);
            positionQuantityList.forceResolve();

            long totalTime = (System.currentTimeMillis() - now);
            System.out.println("took "+totalTime +" ms "+" per object: "+((double)totalTime*1000)/positionQuantityList.size()+ " microseconds");
        }
    }

}
