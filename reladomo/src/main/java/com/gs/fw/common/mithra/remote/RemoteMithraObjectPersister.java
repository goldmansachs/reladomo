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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.ResultSetParser;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.portal.DatedUpdateDataChooser;
import com.gs.fw.common.mithra.portal.MithraAbstractObjectPortal;
import com.gs.fw.common.mithra.portal.NonDatedUpdateDataChooser;
import com.gs.fw.common.mithra.portal.UpdateDataChooser;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.MithraTuplePersister;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.MithraDatedObjectPersister;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.RenewedCacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RemoteMithraObjectPersister implements MithraDatedObjectPersister, MithraTuplePersister
{
    private RemoteMithraService remoteMithraService;
    private transient RelatedFinder finder;
    private UpdateDataChooser updateDataChooser;
    private Logger logger;
    private static final ClientTransactionContext NULL_CLIENT_TRANSACTION_CONTEXT = new ClientTransactionContext(null, null);

    public RemoteMithraObjectPersister(RemoteMithraService remoteMithraService, RelatedFinder finder,
                                       boolean isDated)
    {
        this.remoteMithraService = remoteMithraService;
        this.finder = finder;
        if (isDated)
        {
            this.updateDataChooser = DatedUpdateDataChooser.getInstance();
        }
        else
        {
            this.updateDataChooser = NonDatedUpdateDataChooser.getInstance();
        }
        String finderClassName = this.getFinderClassName();
        String loggerName = "com.gs.fw.common.mithra.remotelogs." + finderClassName.substring(finderClassName.lastIndexOf('.') + 1,
                finderClassName.length() - "Finder".length());
        this.logger = LoggerFactory.getLogger(loggerName);
    }

    public MithraObjectPortal getPortal()
    {
        return finder.getMithraObjectPortal();
    }

    public String getFinderClassName()
    {
        return finder.getFinderClassName();
    }

    public RelatedFinder getFinder()
    {
        return finder;
    }

    public void loadFullCache()
    {
        RemoteReloadResult result = this.remoteMithraService.reload(this.getPortal().getMithraObjectDeserializer().getOperationsForFullCacheLoad());
        result.registerForNotification();
    }

    public void reloadFullCache()
    {
        loadFullCache();
    }

    @Override
    public RenewedCacheStats renewCacheForOperation(Operation op)
    {
        throw new UnsupportedOperationException("Don't allow remote renew operation currently");
    }

    public Logger getLogger()
    {
        return logger;
    }

    public Cursor findCursor(AnalyzedOperation analyzedOperation, Filter postLoadFilter, OrderBy orderby, int rowcount, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        if (this.logger.isDebugEnabled())
        {
            logger.debug("remote find cursor: " + analyzedOperation.getOriginalOperation().toString());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);

        RemoteCursorResult queryResult = null;
        try
        {
            queryResult = this.remoteMithraService.findRemoteCursorResult(
                    context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    analyzedOperation.getOriginalOperation(), postLoadFilter,
                    orderby, bypassCache, rowcount, maxParallelDegree, forceImplicitJoin);
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }

        MithraManagerProvider.getMithraManager().incrementRemoteRetrieveCount();

        queryResult.registerForNotification();
        context.setRemoteTransactionId(queryResult.getRemoteTransactionId());

        RemoteCursor cursor = queryResult.getCursor(this);
        cursor.setMaxParallelDegree(maxParallelDegree);
        cursor.setTransactional(currentTransaction != null);
        return cursor;
    }

    private ClientTransactionContext getOrCreateContext(MithraTransaction currentTransaction)
    {
        if (currentTransaction == null) return NULL_CLIENT_TRANSACTION_CONTEXT;
        ClientTransactionContext context = ClientTransactionContextManager.getInstance().getClientTransactionContext(this.remoteMithraService, currentTransaction);
        if (context == null)
        {
            context = new ClientTransactionContext(this.remoteMithraService, currentTransaction);
            ClientTransactionContextManager.getInstance().setClientTransactionContext(this.remoteMithraService, context, currentTransaction);
        }
        return context;
    }

    public CachedQuery find(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, int rowcount, int numberOfThreads, boolean bypassCache, boolean forceImplicitJoin)
    {
        long startTime = System.currentTimeMillis();
        if (this.logger.isDebugEnabled())
        {
            this.logger.debug("remote find: " + analyzedOperation.getOriginalOperation().toString());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        RemoteQueryResult queryResult = null;
        try
        {
            queryResult = this.remoteMithraService.find(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    analyzedOperation.getOriginalOperation(),
                    orderby, bypassCache, forRelationship, rowcount, forceImplicitJoin);
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }

        CachedQuery cachedQuery = cacheRemoteAnalyzedOperation(analyzedOperation, orderby, queryResult, forRelationship);
        MithraManagerProvider.getMithraManager().incrementRemoteRetrieveCount();

        queryResult.registerForNotification();
        context.setRemoteTransactionId(queryResult.getRemoteTransactionId());

        this.getPortal().getPerformanceData().recordTimeForFind(queryResult.getDeserializedResult().size(), startTime);
        if (this.logger.isDebugEnabled())
        {
            long duration = System.currentTimeMillis() - startTime;
            int size = cachedQuery.getResult().size();
            this.logger.debug("retrieved " + size + " objects in " + duration + " ms. " + (size > 0 ? duration / (double) size + " ms per object" : ""));
        }
        return cachedQuery;
    }

    private CachedQuery cacheRemoteAnalyzedOperation(AnalyzedOperation analyzedOperation, OrderBy orderby, RemoteQueryResult queryResult, boolean forRelationship)
    {
        CachedQuery cachedQuery = new CachedQuery(analyzedOperation.getOriginalOperation(), orderby);
        cachedQuery.setReachedMaxRetrieveCount(queryResult.isReachedMaxRetrieveCount());
        cachedQuery.setResult(queryResult.getDeserializedResult());
        cachedQuery.cacheQuery(forRelationship);
        if (analyzedOperation.isAnalyzedOperationDifferent())
        {
            cachedQuery.setWasDefaulted();
            CachedQuery cachedQuery2 = new CachedQuery(analyzedOperation.getAnalyzedOperation(), orderby);
            cachedQuery2.setReachedMaxRetrieveCount(queryResult.isReachedMaxRetrieveCount());
            cachedQuery2.setResult(queryResult.getDeserializedResult());
            cachedQuery2.cacheQuery(forRelationship);
        }
        return cachedQuery;
    }

    public MithraDataObject refresh(MithraDataObject data, boolean lockInDatabase) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        if (this.logger.isDebugEnabled())
        {
            this.logger.debug("remote refresh " + data.zGetPrintablePrimaryKey());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        RemoteRefreshResult result = remoteMithraService.refresh(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                new ExternalizablePrimaryKey(data), lockInDatabase);
        context.setRemoteTransactionId(result.getRemoteTransactionId());
        this.getPortal().getPerformanceData().recordTimeForRefresh(startTime);
        return result.getMithraDataObject();
    }

    public MithraDataObject refreshDatedObject(MithraDatedObject mithraDatedObject, boolean lockInDatabase) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        if (this.logger.isDebugEnabled())
        {
            this.logger.debug("remote refresh " + mithraDatedObject.zGetCurrentOrTransactionalData().zGetPrintablePrimaryKey());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        RemoteRefreshDatedObjectResult result = remoteMithraService.refreshDatedObject(context.getRemoteTransactionId(),
                context.getTransactionTimeoutWithoutException(), context.getXid(), new ExternalizableDatedPrimaryKey(mithraDatedObject), lockInDatabase);
        context.setRemoteTransactionId(result.getRemoteTransactionId());
        this.getPortal().getPerformanceData().recordTimeForRefresh(startTime);
        return result.getResultData();
    }

    public int count(Operation op)
    {
        if (this.logger.isDebugEnabled())
        {
            this.logger.debug("remote count: " + op.toString());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteCountResult result = this.remoteMithraService.count(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(), op);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            return result.getCount();
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
            throw e; // already thrown from above, just make the compiler happy
        }
    }

    public List computeFunction(Operation op, OrderBy orderby, String columnOrFunctions, ResultSetParser resultSetParser)
    {
        if (this.logger.isDebugEnabled())
        {
            this.logger.debug("remote computeFunction: " + op.toString());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteComputeFunctionResult result = this.remoteMithraService.computeFuntcion(context.getRemoteTransactionId(),
                    context.getTransactionTimeoutWithoutException(), context.getXid(), op, orderby, columnOrFunctions, resultSetParser);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            return result.getResultList();
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
            throw e; // already thrown from above, just make the compiler happy
        }
    }

    public List findAggregatedData(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                   Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, boolean bypassCache, Class bean)
    {
        if (this.logger.isDebugEnabled())
        {
            this.logger.debug("remote findAggregateData: " + op.toString());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        RemoteAggregateResult aggregateQueryResult = null;
        List returnList = null;
        try
        {
            if (bean == AggregateData.class)
            {
                aggregateQueryResult = this.remoteMithraService.findAggregatedData(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                        op, nameToAggregateAttributeMap, nameToGroupByAttributeMap, havingOperation, bypassCache);
                returnList = aggregateQueryResult.getAggregateList();
            }
            else
            {
                Map<String, MithraAggregateAttribute> nameToOriginalAggregateAttributeMap = MithraAbstractObjectPortal.getAggregateMapForAggregateData(nameToAggregateAttributeMap);
                Map<String, MithraGroupByAttribute> nameToOriginalGroupByAttributeMap = MithraAbstractObjectPortal.getGroupByMapForAggregateData(nameToGroupByAttributeMap);
                aggregateQueryResult = this.remoteMithraService.findAggregatedData(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                        op, nameToOriginalAggregateAttributeMap, nameToOriginalGroupByAttributeMap, havingOperation, bypassCache);

                returnList = MithraAbstractObjectPortal.convertAggregateDataToBean(nameToAggregateAttributeMap, nameToGroupByAttributeMap, bean, aggregateQueryResult.getAggregateList());
            }
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
        MithraManagerProvider.getMithraManager().incrementRemoteRetrieveCount();
        aggregateQueryResult.registerForNotification();
        context.setRemoteTransactionId(aggregateQueryResult.getRemoteTransactionId());

        return returnList;
    }

    public Map extractDatabaseIdentifiers(Operation op)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteExtractOperationDatabaseIdentifiersResult result = this.remoteMithraService.extractDatabaseIdentifiers(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(), op);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            return result.getDatabaseIdentifierMap();
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
            throw e; // already thrown from above, just make the compiler happy
        }
    }

    public Map extractDatabaseIdentifiers(Set sourceAttributeValueSet)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteExtractListDatabaseIdentifiersResult result = this.remoteMithraService.extractDatabaseIdentifiers(context.getRemoteTransactionId(),
                    context.getTransactionTimeoutWithoutException(), context.getXid(), this.getFinder().getClass().getName(), new ExternalizableSourceAttributeValueSet(sourceAttributeValueSet));
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            return result.getDatabaseIdentifierMap();
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
            throw e; // already thrown from above, just make the compiler happy
        }
    }

    public void insertTuples(TupleTempContext tempContext, List list, int bulkInsertThreshold)
    {
        if (this.logger.isDebugEnabled())
        {
            this.logger.debug("remote insert tuples: " + list.size());
        }
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = NULL_CLIENT_TRANSACTION_CONTEXT;
        if (currentTransaction != null)
        {
            context = ClientTransactionContextManager.getInstance().getClientTransactionContext(this.remoteMithraService, currentTransaction);
            if (context == null)
            {
                context = NULL_CLIENT_TRANSACTION_CONTEXT;
            }
        }

        RemoteTupleInsertResult tupleInsertResult = this.remoteMithraService.insertTuples(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                new ExternalizableTupleList(list, tempContext.getTupleAttributeCount()), tempContext,
                this.getFinderClassName(), bulkInsertThreshold);
        tempContext.updateTempTableNames(tupleInsertResult.getTempContext(), this.getPortal());
    }

    public void insertTuplesForSameSource(TupleTempContext context, List list, int bulkInsertThreshold, Object source)
    {
        throw new RuntimeException("should never get here");
    }

    public void destroyTempContext(String fullyQualifiedTableName, Object source, boolean isForQuery)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = NULL_CLIENT_TRANSACTION_CONTEXT;

        if (currentTransaction != null)
        {
            context = ClientTransactionContextManager.getInstance().getClientTransactionContext(this.remoteMithraService, currentTransaction);
            if (context == null)
            {
                context = NULL_CLIENT_TRANSACTION_CONTEXT;
            }
        }
        this.remoteMithraService.destroyTempContext(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(), fullyQualifiedTableName,
                source, this.getFinderClassName(), isForQuery);
    }

    public void update(MithraTransactionalObject mithraObject, AttributeUpdateWrapper wrapper) throws MithraDatabaseException
    {
        this.update(mithraObject, ListFactory.create(wrapper));
    }

    public void update(MithraTransactionalObject mithraObject, List updateWrappers) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteMultiUpdateResult result = this.remoteMithraService.update(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizablePrimaryKey(this.updateDataChooser.chooseDataForMultiUpdate(mithraObject)), updateWrappers);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            this.getPortal().getPerformanceData().recordTimeForUpdate(1, startTime);
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    private void setRemoteServiceOnException(ClientTransactionContext context, MithraTransactionException e)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (e.mustWaitForRemoteTransaction())
        {
            e.setRemoteMithraService(this.remoteMithraService);
        }
        context.setRemoteTransactionId(e.getRemoteTransactionId());
        if (e instanceof MithraOptimisticLockException)
        {
            e.setRetriable(tx.retryOnOptimisticLockFailure());
        }
        if (e instanceof MithraRemoteOptimisticLockException)
        {
            MithraRemoteOptimisticLockException optimException = (MithraRemoteOptimisticLockException) e;
            List data = optimException.getDirtyData();
            for (int i = 0; i < data.size(); i++)
            {
                MithraDataObject dirtyData = (MithraDataObject) data.get(i);
                this.getPortal().getCache().markDirtyForReload(dirtyData, tx);
            }
        }
        throw e;
    }

    public void insert(MithraDataObject mithraDataObject) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        RemoteInsertResult result;
        try
        {
            result = this.remoteMithraService.insert(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizableFullData(mithraDataObject), this.getPortal().getHierarchyDepth());
            if (mithraDataObject.zHasIdentity())
            {
                Number identityValue = result.getIdentityValue();
                mithraDataObject.zSetIdentity(identityValue);
            }

            Map databaseIdentifierMap = result.getDatabaseIdentifierMap();

            registerIdentifierMapForNotification(databaseIdentifierMap);

            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }

        this.getPortal().getPerformanceData().recordTimeForInsert(1, startTime);
    }

    private void registerIdentifierMapForNotification(Map databaseIdentifierMap)
    {
        Set keySet = databaseIdentifierMap.keySet();
        Iterator it = keySet.iterator();
        RelatedFinder finder;
        for (int i = 0; i < keySet.size(); i++)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    (MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey) it.next();

            finder = key.getFinder();
            MithraObjectPortal portal = finder.getMithraObjectPortal();
            portal.registerForNotification((String) databaseIdentifierMap.get(key));
        }
    }

    public void delete(MithraDataObject mithraDataObject) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteDeleteResult result = this.remoteMithraService.delete(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizablePrimaryKey(mithraDataObject), this.getPortal().getHierarchyDepth());
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            this.getPortal().getPerformanceData().recordTimeForDelete(1, startTime);
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    public void purge(MithraDataObject mithraDataObject) throws MithraDatabaseException
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemotePurgeResult result = this.remoteMithraService.purge(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizablePrimaryKey(mithraDataObject), this.getPortal().getHierarchyDepth());
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    public void batchInsert(List mithraObjects, int bulkInsertThreshold) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        RemoteBatchInsertResult result;
        try
        {
            result = this.remoteMithraService.batchInsert(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizableFullDataList(mithraObjects), this.getPortal().getHierarchyDepth(), bulkInsertThreshold);
            Map databaseIdentifierMap = result.getDatabaseIdentifierMap();

            registerIdentifierMapForNotification(databaseIdentifierMap);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            this.getPortal().getPerformanceData().recordTimeForInsert(mithraObjects.size(), startTime);
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    public void batchDelete(List mithraObjects) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteBatchDeleteResult result = this.remoteMithraService.batchDelete(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizablePrimaryKeyList(mithraObjects), this.getPortal().getHierarchyDepth());
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
        this.getPortal().getPerformanceData().recordTimeForDelete(mithraObjects.size(), startTime);
    }

    @Override
    public void batchDeleteQuietly(List mithraObjects) throws MithraDatabaseException
    {
        // todo: whitba: test and implement
        throw new UnsupportedOperationException("not implemented");
    }

    public void batchPurge(List mithraObjects) throws MithraDatabaseException
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteBatchPurgeResult result = this.remoteMithraService.batchPurge(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizablePrimaryKeyList(mithraObjects), this.getPortal().getHierarchyDepth());
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    public List findForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void deleteUsingOperation(Operation op)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteDeleteUsingOperationResult result = this.remoteMithraService.deleteUsingOperation(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    op);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    public int deleteBatchUsingOperation(Operation op, int batchSize)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        RemoteDeleteBatchUsingOperationResult result = null;

        try
        {
            result = this.remoteMithraService.deleteBatchUsingOperation(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    op, batchSize);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
        return result.getDeletedRowsCount();
    }

    public void batchUpdate(BatchUpdateOperation batchUpdateOperation)
    {
        long startTime = System.currentTimeMillis();
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteBatchUpdateResult result = this.remoteMithraService.batchUpdate(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizableBatchUpdateOperation(batchUpdateOperation, this.updateDataChooser));
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
        batchUpdateOperation.setUpdated();
        this.getPortal().getPerformanceData().recordTimeForUpdate(batchUpdateOperation.getUpdateOperations().size(), startTime);
    }

    public void multiUpdate(MultiUpdateOperation multiUpdateOperation)
    {
        long startTime = System.currentTimeMillis();
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteMultiBatchUpdateResult result = this.remoteMithraService.multiUpdate(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    new ExternalizableMultiUpdateOperation(multiUpdateOperation, this.updateDataChooser));
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
        this.getPortal().getPerformanceData().recordTimeForUpdate(multiUpdateOperation.getMithraObjects().size(), startTime);
    }

    public List getForDateRange(MithraDataObject data, Timestamp start, Timestamp end)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        AsOfAttribute businessAttribute = this.getFinder().getAsOfAttributes()[0];
        try
        {
            RemoteGetForDateRangeResult result = this.remoteMithraService.getForDateRange(context.getRemoteTransactionId(),
                    context.getTransactionTimeoutWithoutException(), context.getXid(), new ExternalizablePrimaryKey(data),
                    new ExternalizableDateRange(businessAttribute, start, end));
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            return result.getResultList();
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
            throw e; // already thrown from above, just make the compiler happy
        }
    }

    public MithraDataObject enrollDatedObject(MithraDatedTransactionalObject mithraDatedObject)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemoteEnrollDatedObjectResult result = this.remoteMithraService.enrollDatedObject(context.getRemoteTransactionId(),
                    context.getTransactionTimeoutWithoutException(), context.getXid(), new ExternalizableDatedPrimaryKey(mithraDatedObject));
            context.setRemoteTransactionId(result.getRemoteTransactionId());
            return result.getResultData();
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
            throw e; // already thrown from above, just make the compiler happy
        }
    }

    public void setTxParticipationMode(TxParticipationMode mode, MithraTransaction tx)
    {
        ClientTransactionContext context = getOrCreateContext(tx);
        try
        {
            RemoteTxParticipationResult result = this.remoteMithraService.setTxParticipationMode(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    this.getFinder().getClass().getName(), mode);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
            throw e; // already thrown from above, just make the compiler happy
        }
    }

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemotePrepareForMassDeleteResult result = this.remoteMithraService.prepareForMassDelete(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    op, forceImplicitJoin);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        ClientTransactionContext context = getOrCreateContext(currentTransaction);
        try
        {
            RemotePrepareForMassPurgeResult result = this.remoteMithraService.prepareForMassPurge(context.getRemoteTransactionId(), context.getTransactionTimeoutWithoutException(), context.getXid(),
                    op, forceImplicitJoin);
            context.setRemoteTransactionId(result.getRemoteTransactionId());
        }
        catch (MithraTransactionException e)
        {
            setRemoteServiceOnException(context, e);
        }
    }

    @Override
    public void prepareForMassPurge(List mithraObjects)
    {


    }

    public RemoteContinuedCursorResult continueCursor(RemoteCursor remoteCursor)
    {
        return this.remoteMithraService.continueCursor(remoteCursor.getRemoteCursorId());
    }

    public void closeCursor(RemoteTransactionId remoteCursorId)
    {
        this.remoteMithraService.closeCursor(remoteCursorId);
    }
}
