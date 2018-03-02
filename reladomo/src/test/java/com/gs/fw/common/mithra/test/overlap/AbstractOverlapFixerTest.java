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

package com.gs.fw.common.mithra.test.overlap;


import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.database.MithraAbstractDatabaseObject;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.mithraruntime.CacheType;
import com.gs.fw.common.mithra.mithraruntime.ConnectionManagerType;
import com.gs.fw.common.mithra.mithraruntime.MithraObjectConfigurationType;
import com.gs.fw.common.mithra.mithraruntime.MithraRuntimeType;
import com.gs.fw.common.mithra.overlap.OverlapFixer;
import com.gs.fw.common.mithra.overlap.OverlapHandler;
import com.gs.fw.common.mithra.overlap.OverlapProcessor;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestDataParser;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.TestOverlapBusinessDateMilestoned;
import com.gs.fw.common.mithra.test.domain.TestOverlapFullyMilestoned;
import com.gs.fw.common.mithra.test.domain.TestOverlapProcessingDateMilestoned;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;
import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import com.gs.fw.common.mithra.util.fileparser.MithraParsedData;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;

public abstract class AbstractOverlapFixerTest extends TestCase
{
    private final Class mithraClass;
    private MithraTestResource mithraTestResource;
    private Operation operation;

    protected AbstractOverlapFixerTest(Class mithraClass, Operation op)
    {
        this.mithraClass = mithraClass;
        this.operation = op;
    }

    @Override
    protected void setUp() throws Exception
    {
        MithraRuntimeType mithraRuntimeType = new MithraRuntimeType();
        ConnectionManagerType connectionManagerType = new ConnectionManagerType();
        connectionManagerType.setClassName(ConnectionManagerForTests.class.getName());
        connectionManagerType.setMithraObjectConfigurations(FastList.newListWith(
                getMithraObjectConfigurationType(TestOverlapFullyMilestoned.class),
                getMithraObjectConfigurationType(TestOverlapBusinessDateMilestoned.class),
                getMithraObjectConfigurationType(TestOverlapProcessingDateMilestoned.class)));
        mithraRuntimeType.setConnectionManagers(FastList.newListWith(connectionManagerType));

        this.mithraTestResource = new MithraTestResource(mithraRuntimeType);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseTimeZone(TimeZone.getDefault());
        connectionManager.setDatabaseType(this.mithraTestResource.getDatabaseType());

        this.mithraTestResource.createSingleDatabase(connectionManager, "A", "testdata/overlapTestDataBroken.txt");
        this.mithraTestResource.setTestConnectionsOnTearDown(true);

        executeSql("DROP INDEX IF EXISTS I_TEST_OVERLAP_FULLY_MILESTONED_F");
        executeSql("DROP INDEX IF EXISTS I_TEST_OVERLAP_FULLY_MILESTONED_T");
        executeSql("DROP INDEX IF EXISTS I_TEST_OVERLAP_BUSINESS_DATE_MILESTONED_F");
        executeSql("DROP INDEX IF EXISTS I_TEST_OVERLAP_BUSINESS_DATE_MILESTONED_T");
        executeSql("DROP INDEX IF EXISTS I_TEST_OVERLAP_PROCESSING_DATE_MILESTONED_F");
        executeSql("DROP INDEX IF EXISTS I_TEST_OVERLAP_PROCESSING_DATE_MILESTONED_T");

        this.mithraTestResource.setUp();

    }

    private MithraObjectConfigurationType getMithraObjectConfigurationType(Class mithraClass)
    {
        MithraObjectConfigurationType mithraObjectConfigurationType = new MithraObjectConfigurationType();
        mithraObjectConfigurationType.setClassName(mithraClass.getName());
        mithraObjectConfigurationType.setCacheType(CacheType.NONE);
        return mithraObjectConfigurationType;
    }

    private void executeSql(String sql) throws SQLException
    {
        Object connectionManager = getMithraObjectPortal().getDatabaseObject().getConnectionManager();
        Connection connection = ((SourcelessConnectionManager) connectionManager).getConnection();
        Statement statement = connection.createStatement();
        statement.execute(sql);
        ((MithraAbstractDatabaseObject) getMithraObjectPortal().getDatabaseObject()).closeDatabaseObjects(connection, statement, null);
    }

    private MithraObjectPortal getMithraObjectPortal()
    {
        try
        {
            Class<?> finderClass = Class.forName(this.mithraClass.getName() + "Finder");
            return (MithraObjectPortal) finderClass.getMethod("getMithraObjectPortal").invoke(finderClass);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void tearDown() throws Exception
    {
        if (this.mithraTestResource != null)
        {
            this.mithraTestResource.tearDown();
        }
        super.tearDown();
    }

    public void testOverlapFixerWithDefaultBatchSize()
    {
        fixOverlapsAndAssertResults(new OverlapFixer());
    }

    public void testOverlapFixerWithBatchSizeOne()
    {
        fixOverlapsAndAssertResults(new OverlapFixer(1));
    }

    public void testOverlapFixerWithOperation()
    {
        fixOverlapsWithOperationAndAssertResults(new OverlapFixer());
    }

    private void fixOverlapsWithOperationAndAssertResults(OverlapFixer overlapFixer)
    {
        new OverlapProcessor(getMithraObjectPortal(), null, overlapFixer, this.operation).process();
        MithraList fixed = this.getFixedRowsInExpectedOrderWithOperation();

        //printActual(fixed);

        String slash = File.separator;
        MithraTestDataParser parser = new MithraTestDataParser("reladomo" + slash + "src" + slash + "test" + slash+ "resources" + slash+ "testdata" + slash + "overlapTestDataFixedWithOperation.txt");
        List<MithraParsedData> results = parser.getResults();
        List<MithraDataObject> dataObjects = null;
        for (MithraParsedData parsedData : results)
        {
            if (this.mithraClass.getName().equals(parsedData.getParsedClassName()))
            {
                dataObjects = parsedData.getDataObjects();
                break;
            }
        }
        assertEquals(dataObjects.size(), fixed.size());

        this.verifyAttributes(fixed, dataObjects);
    }

    private void fixOverlapsAndAssertResults(OverlapFixer overlapFixer)
    {
        new OverlapProcessor(getMithraObjectPortal(), null, overlapFixer).process();

        MithraList fixed = this.getFixedRowsInExpectedOrder();

        //printActual(fixed);

        String slash = File.separator;
        MithraTestDataParser parser = new MithraTestDataParser("reladomo" + slash + "src" + slash + "test" + slash+ "resources" + slash+ "testdata" + slash + "overlapTestDataFixed.txt");
        List<MithraParsedData> results = parser.getResults();
        List<MithraDataObject> dataObjects = null;
        for (MithraParsedData parsedData : results)
        {
            if (this.mithraClass.getName().equals(parsedData.getParsedClassName()))
            {
                dataObjects = parsedData.getDataObjects();
                break;
            }
        }
        assertEquals(dataObjects.size(), fixed.size());

        this.verifyAttributes(fixed, dataObjects);
        new OverlapProcessor(getMithraObjectPortal(), null, new ExplodingOverlapHandler()).process();
    }

    private static void verifyAttributes(MithraList fixed, List<MithraDataObject> dataObjects)
    {
        for (int i = 0; i < dataObjects.size(); i++)
        {
            int rowNum = i + 1;
            MithraDataObject expected = dataObjects.get(i);
            Object actual = fixed.get(i);
            Attribute[] attributes = expected.zGetMithraObjectPortal().getFinder().getPersistentAttributes();
            for (Attribute attr : attributes)
            {
                if (attr instanceof TimestampAttribute)
                {
                    assertTimestampsEqual(rowNum, expected, actual, (TimestampAttribute) attr);
                }
                else
                {
                    assertAttributesEqual(rowNum, expected, actual, attr);
                }
            }
        }
    }

    private static void assertAttributesEqual(int rowNum, Object expected, Object actual, Attribute attr)
    {
        assertEquals("Row " + rowNum + ' ' + attr.getAttributeName(), attr.valueOf(expected), attr.valueOf(actual));
    }

    private static void assertTimestampsEqual(int rowNum, Object expected, Object actual, TimestampAttribute attr)
    {
        long actualTime = attr.valueOf(actual).getTime();
        Timestamp actualTimestamp = DefaultInfinityTimestamp.getDefaultInfinity();
        if (actualTime < DefaultInfinityTimestamp.getDefaultInfinity().getTime())
        {
            actualTimestamp = new ImmutableTimestamp(actualTime - TimeZone.getDefault().getOffset(actualTime));
        }
        assertEquals("Row " + rowNum + ' ' + attr.getAttributeName(), attr.valueOf(expected), actualTimestamp);
    }

    private static void printActual(MithraList fixedList)
    {
        MithraObject mithraObject = (MithraObject) fixedList.get(0);
        Attribute[] attributes = mithraObject.zGetCurrentData().zGetMithraObjectPortal().getFinder().getPersistentAttributes();
        for (Object actual : fixedList)
        {
            String delim = "";
            for (Attribute attr : attributes)
            {
                System.out.print(delim + attr.valueOf(actual));
                delim = ",";
            }
            System.out.println();
        }
    }

    protected abstract MithraList getFixedRowsInExpectedOrder();

    protected abstract MithraList getFixedRowsInExpectedOrderWithOperation();

    private static class ExplodingOverlapHandler implements OverlapHandler
    {
        @Override
        public void overlapProcessingStarted(Object connectionManager, String mithraClassName)
        {
        }

        @Override
        public void overlapProcessingFinished(Object connectionManager, String mithraClassName)
        {
        }

        @Override
        public void overlapsDetected(Object connectionManager, List<MithraDataObject> overlaps, String mithraClassName)
        {
            fail("No overlaps should have been detected following fix");
        }
    }
}