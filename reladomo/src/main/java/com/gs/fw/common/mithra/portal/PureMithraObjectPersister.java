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

package com.gs.fw.common.mithra.portal;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.ResultSetParser;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.MithraTuplePersister;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.MithraDatedObjectPersister;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.RenewedCacheStats;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class PureMithraObjectPersister implements MithraObjectPersister, MithraDatedObjectPersister, MithraTuplePersister
{
    private RelatedFinder finder;

    public PureMithraObjectPersister(RelatedFinder finder)
    {
        this.finder = finder;
    }

    public void update(MithraTransactionalObject mithraObject, AttributeUpdateWrapper wrapper) throws MithraDatabaseException
    {
    }

    public void update(MithraTransactionalObject mithraObject, List updateWrappers) throws MithraDatabaseException
    {
    }

    public void insert(MithraDataObject mithraDataObject) throws MithraDatabaseException
    {
    }

    public void delete(MithraDataObject mithraDataObject) throws MithraDatabaseException
    {
    }

    @Override
    public void batchDeleteQuietly(List mithraObjects) throws MithraDatabaseException
    {
    }

    public void purge(MithraDataObject mithraDataObject) throws MithraDatabaseException
    {
    }

    public void batchInsert(List mithraObjects, int bulkInsertThreshold) throws MithraDatabaseException
    {
    }

    public void batchDelete(List mithraObjects) throws MithraDatabaseException
    {
    }

    public void batchPurge(List mithraObjects) throws MithraDatabaseException
    {
    }

    public List findForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        throw new RuntimeException("should never get here");
    }

    public int deleteBatchUsingOperation(Operation op, int batchSize)
    {
        throw new RuntimeException("should never get here");
    }

    public void deleteUsingOperation(Operation op)
    {
    }

    public void batchUpdate(BatchUpdateOperation batchUpdateOperation)
    {
    }

    public void multiUpdate(MultiUpdateOperation multiUpdateOperation)
    {
    }

    public MithraDataObject enrollDatedObject(MithraDatedTransactionalObject mithraObject)
    {
        return mithraObject.zGetTxDataForRead();
    }

    public List getForDateRange(MithraDataObject mithraDataObject, Timestamp start, Timestamp end)
    {
        throw new RuntimeException("not implemented");
    }

    public void insertTuples(TupleTempContext context, List list, int bulkInsertThreshold)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void insertTuplesForSameSource(TupleTempContext context, List list, int bulkInsertThreshold, Object source)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void destroyTempContext(String fullyQualifiedTableName, Object source, boolean isForQuery)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin)
    {
    }

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin)
    {
    }

    @Override
    public void prepareForMassPurge(List mithraObjects)
    {
    }

    public void setTxParticipationMode(TxParticipationMode mode, MithraTransaction tx)
    {
    }

    public CachedQuery find(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, int rowcount, int numberOfThreads, boolean bypassCache, boolean forceImplicitJoin)
    {
        throw new RuntimeException("not implemented");
    }

    public Cursor findCursor(AnalyzedOperation analyzedOperation, Filter postLoadFilter, OrderBy orderby, int rowcount, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        throw new RuntimeException("not implemented");
    }

    public int count(Operation op)
    {
        throw new RuntimeException("not implemented");
    }

    public List computeFunction(Operation op, OrderBy orderby, String sqlExpression, ResultSetParser resultSetParser)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject refresh(MithraDataObject data, boolean lockInDatabase) throws MithraDatabaseException
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject refreshDatedObject(MithraDatedObject mithraDatedObject, boolean lockInDatabase) throws MithraDatabaseException
    {
        throw new RuntimeException("not implemented");
    }

    public List findAggregatedData(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap, Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, boolean bypassCache, Class bean)
    {
        throw new RuntimeException("not implemented");
    }

    public void loadFullCache()
    {
        ((MithraPureObjectFactory) finder.getMithraObjectPortal().getMithraObjectDeserializer()).loadFullCache();
    }

    public void reloadFullCache()
    {
        ((MithraPureObjectFactory) finder.getMithraObjectPortal().getMithraObjectDeserializer()).reloadFullCache();
    }

    @Override
    public RenewedCacheStats renewCacheForOperation(Operation op)
    {
        return RenewedCacheStats.EMPTY_STATS;
    }

    public Map extractDatabaseIdentifiers(Operation op)
    {
        MithraDatabaseIdentifierExtractor extractor = new MithraDatabaseIdentifierExtractor();
        return extractor.extractDatabaseIdentifierMap(op);
    }

    public Map extractDatabaseIdentifiers(Set sourceAttributeValueSet)
    {
        MithraDatabaseIdentifierExtractor extractor = new MithraDatabaseIdentifierExtractor();
        return extractor.extractDatabaseIdentifierMap(this.finder, sourceAttributeValueSet);
    }
}