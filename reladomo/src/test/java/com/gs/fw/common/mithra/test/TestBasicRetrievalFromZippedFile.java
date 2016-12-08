

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

import com.gs.fw.common.mithra.test.domain.criters.PetType;
import com.gs.fw.common.mithra.test.domain.criters.PetTypeFinder;
import com.gs.fw.common.mithra.test.domain.criters.PetTypeList;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

public class TestBasicRetrievalFromZippedFile
        extends MithraTestAbstract
{
    static private Logger logger = LoggerFactory.getLogger(MithraTestAbstract.class.getName());
    private MithraTestResource mithraTestResource;

    public TestBasicRetrievalFromZippedFile()
    {
        super("Mithra Object Tests");
    }

    protected void setUp() throws Exception
    {
        mithraTestResource = new MithraTestResource("MithraConfigPartialCache.xml");
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());
        connectionManager.setConnectionManagerIdentifier("");

        String testFile = MITHRA_TEST_DATA_FILE_PATH + "mithraZippedDataSource.txt.gz";
        URL testFileLocation = this.getClass().getClassLoader().getResource(testFile).toURI().toURL();
        GZIPInputStream zipInputStream = new GZIPInputStream(this.getClass().getClassLoader().getResourceAsStream(testFile));

        mithraTestResource.createSingleDatabase(connectionManager, testFileLocation, zipInputStream);
        mithraTestResource.setUp();
    }

    public void testRetrieveOneRow() throws SQLException
    {
        int id = 100;

        PetType petType = PetTypeFinder.findOne(PetTypeFinder.id().eq(id));
        Assert.assertNotNull(petType);
    }

    public void testRetrieveAllRows() throws SQLException
    {
        PetTypeList petTypeList = new PetTypeList(PetTypeFinder.all());

        Assert.assertEquals(2, petTypeList.size());
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
    }
}
