package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;

public class GraphqlTestBase
{

    public MithraTestResource initReladomo(String configFilename, String dataFile)
    {
        MithraTestResource mithraTestResource = new MithraTestResource(configFilename);

        ConnectionManagerForTests connectionManagerForTestDb = ConnectionManagerForTests.getInstance("testdb");
        mithraTestResource.createSingleDatabase(connectionManagerForTestDb, dataFile);

        ConnectionManagerForTests connectionManagerForTestDbStringSourceA = ConnectionManagerForTests.getInstance("A");

        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManagerForTestDbStringSourceA, "A", "test-data-A.txt");

        mithraTestResource.setUp();

        return mithraTestResource;
    }
}
