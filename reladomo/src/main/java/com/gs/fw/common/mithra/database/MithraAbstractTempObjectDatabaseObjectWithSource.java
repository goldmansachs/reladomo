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

package com.gs.fw.common.mithra.database;

import com.gs.fw.common.mithra.MithraBusinessException;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;


public abstract class MithraAbstractTempObjectDatabaseObjectWithSource extends MithraAbstractTempObjectDatabaseObject
{

    private UnifiedMap<Object, String> sourceToTableName = new UnifiedMap<Object, String>();

    protected MithraAbstractTempObjectDatabaseObjectWithSource(String loggerClassName, String fullyQualifiedFinderClassName,
            int totalColumnsInResultSet, int totalColumnsInInsert, String columnListWithoutPK, String columnListWithoutPkWithAlias,
            boolean hasOptimisticLocking, boolean hasNullablePrimaryKeys, boolean hasSourceAttribute,
            String primaryKeyWhereSqlWithDefaultAlias, String primaryKeyIndexColumns)
    {
        super(loggerClassName, fullyQualifiedFinderClassName, totalColumnsInResultSet, totalColumnsInInsert, columnListWithoutPK,
                columnListWithoutPkWithAlias, hasOptimisticLocking, hasNullablePrimaryKeys, hasSourceAttribute,
                primaryKeyWhereSqlWithDefaultAlias, primaryKeyIndexColumns);
    }

    public String getFullyQualifiedTableNameGenericSource(Object source)
    {
        String tempTable = sourceToTableName.get(source);
        if (tempTable == null)
        {
            throw new MithraBusinessException("No temporary table has been created for source "+source+" for "+this.getClass().getName());
        }
        return tempTable;
    }

    protected void setTempTableName(Object source, String tempTableName)
    {
        sourceToTableName.put(source, tempTableName);
    }

    public void dropTempTable(Object source)
    {
        super.dropTempTable(source);
        sourceToTableName.remove(source);
    }
}
