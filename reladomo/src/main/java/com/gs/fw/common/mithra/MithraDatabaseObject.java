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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.connectionmanager.ConnectionManagerWrapper;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.portal.MithraObjectReader;



public interface MithraDatabaseObject extends MithraObjectReader
{

    public String getTableNameForQuery(SqlQuery query, MapperStackImpl mapperStack, int queryNumber);

    public String getFullyQualifiedTableNameGenericSource(Object source);

    public Object getConnectionManager();

    public void setConnectionManager(Object connectionManager, ConnectionManagerWrapper wrapper);

    public void setDefaultSchema(String schema);

    public void setSchemaManager(Object schemaManager);

    public void setTablePartitionManager(Object tablePartitionManager);

    public boolean isReplicated();

    /**
     * @deprecated Use {@link MithraDatabaseObject#getDefaultTableName()} instead which does not throw if the object have a source attribute.
     */
    public String getTableName();
    
    public String getDefaultTableName();
    
    public boolean hasIdentity();

    public String getNotificationEventIdentifier();

    public MithraObjectPortal getMithraObjectPortal();

    public RelatedFinder getFinder();

    public String getDefaultSchema();

    public void setLoadOperationProvider(LoadOperationProvider loadOperationProvider);
}
