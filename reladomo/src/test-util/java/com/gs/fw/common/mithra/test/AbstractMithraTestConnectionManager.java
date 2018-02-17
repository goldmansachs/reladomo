
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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraDatabaseObject;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public abstract class AbstractMithraTestConnectionManager implements MithraTestConnectionManager
{
    private String defaultSource;
    private Set<TestDatabaseConfiguration> testDbConfigurations = new UnifiedSet(3);
    private String connectionManagerIdentifier;
    private boolean shutdown = false;
    private Properties credentials = new Properties();

    protected void readCredentials()
    {
        try
        {
            credentials.load(this.getClass().getClassLoader().getResourceAsStream("credentials.properties"));
        }
        catch (IOException e)
        {
            throw new RuntimeException("No credentials found");
        }
    }

    protected String getCredential(String key)
    {
        String property = credentials.getProperty(key);
        if (property == null)
        {
            throw new RuntimeException("Missing property: "+key);
        }
        return property;
    }

    public boolean addConnectionManagerForSource(String schemaName)
    {
        // Implemented in relevant subclasses
        return false;
    }

    public boolean addConnectionManagerForSource(Object sourceId, String schemaName)
    {
        // Implemented in relevant subclasses
        return false;
    }

    public void setDefaultSource(String defaultSource)
    {
        if(connectionManagerIdentifier != null)
        {
            //logger.warn("Changed default source");
        }
        this.defaultSource = defaultSource;
    }

    public String getDefaultSource()
    {
        return this.defaultSource;
    }

    public String getConnectionManagerIdentifier()
    {
        return this.connectionManagerIdentifier;
    }

    public void setConnectionManagerIdentifier(String connectionManagerIdentifier)
    {
        this.connectionManagerIdentifier = connectionManagerIdentifier;
    }

    public void createSchema(String databaseName, String schemaName, Class sourceAttributeType)
    {

    }

    public void addTestDbConfiguration(TestDatabaseConfiguration testDbConfiguration)
    {
        this.testDbConfigurations.add(testDbConfiguration);
    }

    public void setUpDatabases(MithraTestResource mtr, boolean createTables)
    {
        // We may have accumulated configs from prior tests for which the connection manager has been removed by fullyShutdown().
        // We need to re-register the connection managers and recreate missing tables.
        this.ensureAllDatabasesRegisteredAndTablesExist(mtr, this.testDbConfigurations);

        if (createTables)
        {
            Iterator<TestDatabaseConfiguration> it = testDbConfigurations.iterator();
            for (int i = 0; i < testDbConfigurations.size(); i++)
            {
                TestDatabaseConfiguration testDbConfiguration = it.next();
                testDbConfiguration.createTables(this, mtr);
            }
        }
        this.cleanUpAllData(mtr);

        // Repeat this step as cleanUpAllData() may have called fullyShutdown() if something went wrong (e.g. H2 timeout is a common problem).
        // This is a recoverable error provided we re-register all the connection managers and recreate the missing tables.
        this.ensureAllDatabasesRegisteredAndTablesExist(mtr, this.testDbConfigurations);

        this.insertAllData(mtr);
    }

    protected void ensureAllDatabasesRegisteredAndTablesExist(MithraTestResource mtr, Set<TestDatabaseConfiguration> testDbConfigs)
    {
        // Implemented in relevant subclasses
    }

    public Set<String> getCreatedTables()
    {
        UnifiedSet<String> tables = UnifiedSet.newSet();
        for(TestDatabaseConfiguration configuration : this.testDbConfigurations)
        {
            tables.addAll(configuration.getCreatedTableNames());
        }

        return tables;
    }

    public void tearDownDatabases(MithraTestResource mtr)
    {
        Iterator<TestDatabaseConfiguration> it = testDbConfigurations.iterator();
        for (int i = 0; i < testDbConfigurations.size(); i++)
        {
            if (shutdown) break;
            TestDatabaseConfiguration testDbConfiguration = it.next();
            testDbConfiguration.tearDownDatabase(this, mtr);
        }
        if (shutdown)
        {
            it = testDbConfigurations.iterator();
            for (int i = 0; i < testDbConfigurations.size(); i++)
            {
                TestDatabaseConfiguration testDbConfiguration = it.next();
                testDbConfiguration.setShutdown();
            }
        }
        testDbConfigurations.clear();
        shutdown = false; // we're ready to start up again
    }

    public void addDatabaseObjectsToTestDatabases(List<MithraDatabaseObject> databaseObjects, MithraTestResource mtr)
    {
        int size = testDbConfigurations.size();
        Iterator<TestDatabaseConfiguration> it = testDbConfigurations.iterator();
        for (int i = 0; i < size; i++)
        {
            TestDatabaseConfiguration testDbConfig = it.next();
            testDbConfig.addMoreDatabaseObjects(mtr, databaseObjects);
        }
    }

    public boolean hasConnectionManagerForSource(String source)
    {
        return false;
    }

    private void cleanUpAllData(MithraTestResource mtr)
    {
        Iterator<TestDatabaseConfiguration> it = testDbConfigurations.iterator();
        for (int i = 0; i < testDbConfigurations.size(); i++)
        {
            TestDatabaseConfiguration testDbConfiguration = it.next();
            if (!mtr.getAdditionalConnectionManagersInUse().contains(this) && mtr.isDeleteOnCreate())
            {
                testDbConfiguration.tearDownDatabase(this, mtr);
            }
        }
    }

    private void insertAllData(MithraTestResource mtr)
    {
        Iterator<TestDatabaseConfiguration> it = testDbConfigurations.iterator();
        for (int i = 0; i < testDbConfigurations.size(); i++)
        {
            TestDatabaseConfiguration testDbConfiguration = it.next();
            testDbConfiguration.insertData(this, mtr);
            testDbConfiguration.setConfigured(true);
        }
    }

    public void fullyShutdown()
    {
        shutdown = true;
    }

    public boolean ensureAllConnectionsReturnedToPool()
    {
        for (XAConnectionManager connectionManager : getAllConnectionManagers())
        {
            if (connectionManager.getNumberOfActiveConnections() != 0)
            {
                return false;
            }
        }
        return true;
    }

    protected abstract Collection<XAConnectionManager> getAllConnectionManagers();
}
