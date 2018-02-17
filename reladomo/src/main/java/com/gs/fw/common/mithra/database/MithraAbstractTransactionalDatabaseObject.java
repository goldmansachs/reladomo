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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraTransactionalDatabaseObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;

import java.util.List;



public abstract class MithraAbstractTransactionalDatabaseObject
        extends MithraAbstractDatabaseObject
        implements MithraCodeGeneratedTransactionalDatabaseObject,
        MithraTransactionalDatabaseObject

{
    protected MithraAbstractTransactionalDatabaseObject(String loggerClassName, String fullyQualifiedFinderClassName,
                int totalColumnsInResultSet, int totalColumnsInInsert, String columnListWithoutPK,
                String columnListWithoutPkWithAlias, boolean hasOptimisticLocking, boolean hasNullablePrimaryKeys,
                boolean hasSourceAttribute, String primaryKeyWhereSqlWithDefaultAlias, String primaryKeyIndexColumns)
    {
        super(loggerClassName, fullyQualifiedFinderClassName, totalColumnsInResultSet, totalColumnsInInsert,
              columnListWithoutPK, columnListWithoutPkWithAlias, hasOptimisticLocking, hasNullablePrimaryKeys,
              hasSourceAttribute, primaryKeyWhereSqlWithDefaultAlias, primaryKeyIndexColumns);
    }

    public void batchDelete(List mithraObjects) throws MithraDatabaseException
    {
        this.zBatchDelete(mithraObjects, true);
    }

    @Override
    public void batchDeleteQuietly(List mithraObjects) throws MithraDatabaseException
    {
        this.zBatchDelete(mithraObjects, false);
    }

    public void insert(MithraDataObject dataToInsert) throws MithraDatabaseException
    {
        this.zInsert(dataToInsert);
    }

    public void delete(MithraDataObject dataToDelete) throws MithraDatabaseException
    {
        this.zDelete(dataToDelete);
    }

    public void batchUpdate(BatchUpdateOperation batchUpdateOperation)
    {
        this.zBatchUpdate(batchUpdateOperation);
    }

    public void batchInsert(List mithraObjects, int bulkInsertThreshold) throws MithraDatabaseException
    {
        this.zBatchInsert(mithraObjects, bulkInsertThreshold);
    }

    public void update(MithraTransactionalObject mithraObject, List updateWrappers)
    throws MithraDatabaseException
    {
        this.zUpdate(mithraObject, updateWrappers);
    }

    public void update(MithraTransactionalObject mithraObject, AttributeUpdateWrapper wrapper)
    throws MithraDatabaseException
    {
        this.zUpdate(mithraObject, wrapper);
    }

    public void purge(MithraDataObject dataToDelete)
    throws MithraDatabaseException
    {
        throw new RuntimeException("purge not meaningful for non-dated objects");
    }

    public void batchPurge(List mithraObjects)
    throws MithraDatabaseException
    {
        throw new RuntimeException("purge not meaningful for non-dated objects");
    }

    public List findForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        return this.zFindForMassDelete(op, forceImplicitJoin);
    }

    public void deleteUsingOperation(Operation op)
    throws MithraDatabaseException
    {
        this.zDeleteUsingOperation(op);
    }

    public int deleteBatchUsingOperation(Operation op, int rowCount)
    throws MithraDatabaseException
    {
        return this.zDeleteUsingOperation(op, rowCount);
    }

    public void multiUpdate(MultiUpdateOperation multiUpdateOperation)
    {
        this.zMultiUpdate(multiUpdateOperation);
    }

    public void analyzeChangeForReload(PrimaryKeyIndex fullUniqueIndex, MithraDataObject data, List newDataList, List updatedDataList)
    {
        MithraTransactionalObject object = fullUniqueIndex == null ? null : (MithraTransactionalObject) fullUniqueIndex.removeUsingUnderlying(data);
        if (object == null)
        {
            newDataList.add(data);
        }
        else
        {
            if (object.zUnsynchronizedGetData().changed(data))
            {
                updatedDataList.add(data);
            }
        }
    }

}
