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

package com.gs.fw.common.mithra.test.mithraTestResource;

import com.gs.fw.common.mithra.connectionmanager.IntSourceTablePartitionManager;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceTablePartitionManager;
import com.gs.fw.common.mithra.connectionmanager.TablePartitionManager;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;

public class ConnectionManagerForTestsWithTableManager extends ConnectionManagerForTests
    implements TablePartitionManager, IntSourceTablePartitionManager, ObjectSourceTablePartitionManager
{
    public static ConnectionManagerForTestsWithTableManager instanceWithTableManager = new ConnectionManagerForTestsWithTableManager();

    private ConnectionManagerForTestsWithTableManager()
    {
        super();
        this.setConnectionManagerIdentifier("tablePartitionManager");
    }

    public static ConnectionManagerForTestsWithTableManager getInstance()
    {
        return ConnectionManagerForTestsWithTableManager.instanceWithTableManager;
    }

    @Override
    public String getTableName( String defaultTable, int source )
    {
        return defaultTable + "_" + source;
    }

    @Override
    public String getTableName( String defaultTable, Object source )
    {
       return defaultTable + "_" + source;
    }

    @Override
    public String getTableName( String defaultTable )
    {
        return defaultTable + "_default";
    }
}
