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
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.ResultSetParser;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.AutoShutdownThreadExecutor;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.gs.fw.common.mithra.util.ReflectionMethodCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;



public class RemoteMithraServiceImpl implements RemoteMithraService
{
    static private Logger logger = LoggerFactory.getLogger(RemoteMithraServiceImpl.class.getName());

    private final HashMap transactionIdToWorkerThreadMap = new HashMap();
    private static int VM_ID;
    private static AtomicInteger transactionNumber = new AtomicInteger((int) (Math.random() * Integer.MAX_VALUE / 2));

    private static AutoShutdownThreadExecutor executor = new AutoShutdownThreadExecutor(5000, "Mithra Reusable")
    {
        protected void cleanUpAfterTask()
        {
            MithraManagerProvider.getMithraManager().zDealWithHungTx();
        }
    };

    static
    {
        executor.setTimeoutInMilliseconds(5 * 60 * 1000); // 5 minutes
    }

    static
    {
        VM_ID = (int) System.currentTimeMillis();
    }

    protected ServerContext prepareServerContext(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid)
    {
        if (transactionTimeout != RemoteMithraService.NO_TRANSACTION)
        {
            ServerTransactionWorkerTask workerTask;
            if (remoteTransactionId == null)
            {
                remoteTransactionId = new RemoteTransactionId(VM_ID, transactionNumber.incrementAndGet());
                workerTask = new ServerTransactionWorkerTask(remoteTransactionId, transactionTimeout, this);
                synchronized (transactionIdToWorkerThreadMap)
                {
                    transactionIdToWorkerThreadMap.put(remoteTransactionId, workerTask);
                }
                executor.submit(workerTask);
                workerTask.startTransaction(xid);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Created server side context " + workerTask + " for id " + remoteTransactionId);
                }
                return workerTask;
            }
            else
            {
                synchronized (transactionIdToWorkerThreadMap)
                {
                    workerTask = (ServerTransactionWorkerTask) transactionIdToWorkerThreadMap.get(remoteTransactionId);
                }
                if (workerTask == null || workerTask.isTimedOut())
                {
                    throw new MithraTransactionException("server side transaction context is timed out or non-existant");
                }
            }
            return workerTask;
        }
        return ServerNonTransactionalContext.getInstance();
    }

    public RemoteQueryResult find(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                  Operation op, OrderBy orderBy, boolean bypassCache,
                                  boolean forRelationship, int maxObjectsToRetrieve, boolean forceImplicitJoin) throws MithraException
    {
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        // we purposely ignore the forRelationship flag, in case the server is set to cacheType="none"
        RemoteQueryResult result = new RemoteQueryResult(op, orderBy, bypassCache, false, maxObjectsToRetrieve, serverContext, forceImplicitJoin);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side context " + serverContext.getClass().getName());
        }
        serverContext.execute(result);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side found " + result.getServerSideSize() + " for " + op.getResultObjectPortal().getFinder().getClass().getName());
        }
        return result;
    }

    public RemoteMithraObjectConfig[] getObjectConfigurations()
    {
        Set set = MithraManagerProvider.getMithraManager().getThreeTierConfigSet();
        RemoteMithraObjectConfig[] result = new RemoteMithraObjectConfig[set.size()];
        set.toArray(result);
        Arrays.sort(result);
        return result;
    }

    public RemoteRefreshResult refresh(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                       ExternalizablePrimaryKey externalizablePrimaryKey, boolean lockInDatabase) throws MithraException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("refreshing " + externalizablePrimaryKey.getMithraDataObject().zGetPrintablePrimaryKey());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteRefreshResult result = new RemoteRefreshResult(externalizablePrimaryKey.getMithraDataObject(), lockInDatabase);
        serverContext.execute(result);
        return result;
    }

    public void commit(RemoteTransactionId remoteTransactionId, boolean onePhase)
    {
        ServerTransactionWorkerTask workerTask;
        synchronized (transactionIdToWorkerThreadMap)
        {
            workerTask = (ServerTransactionWorkerTask) transactionIdToWorkerThreadMap.get(remoteTransactionId);
        }
        if (workerTask == null || workerTask.isTimedOut())
        {
            throw new MithraTransactionException("server side transaction context is timed out or non-existant");
        }
        workerTask.getRemoteTransactionId().setRequestorVmId(remoteTransactionId.getRequestorVmId());
        workerTask.commit(onePhase);
        logger.debug("server side commit finished");
    }

    public void rollback(RemoteTransactionId remoteTransactionId)
    {
        ServerTransactionWorkerTask workerTask;
        synchronized (transactionIdToWorkerThreadMap)
        {
            workerTask = (ServerTransactionWorkerTask) transactionIdToWorkerThreadMap.get(remoteTransactionId);
        }
        if (workerTask == null || workerTask.isTimedOut())
        {
            throw new MithraTransactionException("server side transaction context is timed out or non-existant");
        }
        workerTask.rollback();
        logger.debug("server side rollback finished");
    }

    public RemoteMultiUpdateResult update(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                          ExternalizablePrimaryKey externalizablePrimaryKey, List updateWrappers)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("updating " + externalizablePrimaryKey.getMithraDataObject().zGetPrintablePrimaryKey());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteMultiUpdateResult result = new RemoteMultiUpdateResult(externalizablePrimaryKey.getMithraDataObject(), updateWrappers);
        serverContext.execute(result);
        return result;
    }

    public RemoteInsertResult insert(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                     ExternalizableFullData fullData, int hierarchyDepth)
    {
        MithraDataObject mithraDataObject = fullData.getMithraDataObject();
        if (logger.isDebugEnabled())
        {
            logger.debug("inserting " + mithraDataObject.zGetPrintablePrimaryKey());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteInsertResult result = new RemoteInsertResult(mithraDataObject, hierarchyDepth);
        serverContext.execute(result);
        return result;

    }

    protected MithraObjectPortal getPortalFromFinder(String finderClassName)
    {
        try
        {
            Class finderClass = Class.forName(finderClassName);
            Method method = ReflectionMethodCache.getZeroArgMethod(finderClass, "getMithraObjectPortal");
            return (MithraObjectPortal) method.invoke(null, (Object[]) null);
        }
        catch (Exception e)
        {
            RuntimeException ioe = new RuntimeException("could not invoke getMithraObjectPortal on " + finderClassName);
            ioe.initCause(e);
            throw ioe;
        }
    }

    public RemoteTupleInsertResult insertTuples(RemoteTransactionId remoteTransactionId,
                                                int transactionTimeout, Xid xid, ExternalizableTupleList tupleList, TupleTempContext context,
                                                String destinationFinderClassName, int bulkInsertThreshold)
    {
        List toInsert = tupleList.getTupleList();
        if (logger.isDebugEnabled())
        {
            logger.debug("inserting tuples " + toInsert.size());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteTupleInsertResult result = new RemoteTupleInsertResult(getPortalFromFinder(destinationFinderClassName),
                context, toInsert, bulkInsertThreshold);
        serverContext.execute(result);
        return result;
    }

    public RemoteBatchInsertResult batchInsert(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                               ExternalizableFullDataList fullDataList, int hierarchyDepth, int bulkInsertThreshold)
    {
        List mithraDataObjects = fullDataList.getMithraDataObjects();
        if (logger.isDebugEnabled())
        {
            logger.debug("inserting " + mithraDataObjects.size());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteBatchInsertResult result = new RemoteBatchInsertResult(mithraDataObjects, hierarchyDepth, bulkInsertThreshold);
        serverContext.execute(result);
        return result;
    }

    public RemoteDeleteResult delete(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                     ExternalizablePrimaryKey externalizablePrimaryKey, int hierarchyDepth)
    {
        MithraDataObject mithraDataObject = externalizablePrimaryKey.getMithraDataObject();
        if (logger.isDebugEnabled())
        {
            logger.debug("deleting " + mithraDataObject.zGetPrintablePrimaryKey());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteDeleteResult result = new RemoteDeleteResult(mithraDataObject, hierarchyDepth);
        serverContext.execute(result);
        return result;
    }

    public RemotePurgeResult purge(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                   ExternalizablePrimaryKey externalizablePrimaryKey, int hierarchyDepth)
    {
        MithraDataObject mithraDataObject = externalizablePrimaryKey.getMithraDataObject();
        if (logger.isDebugEnabled())
        {
            logger.debug("purging " + mithraDataObject.zGetPrintablePrimaryKey());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemotePurgeResult result = new RemotePurgeResult(mithraDataObject, hierarchyDepth);
        serverContext.execute(result);
        return result;
    }

    public RemoteBatchPurgeResult batchPurge(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                             Xid xid, ExternalizablePrimaryKeyList externalizablePrimaryKeyList, int hierarchyDepth)
    {
        List mithraDataObjects = externalizablePrimaryKeyList.getMithraDataObjects();
        if (logger.isDebugEnabled())
        {
            logger.debug("deleting " + mithraDataObjects.size());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteBatchPurgeResult result = new RemoteBatchPurgeResult(mithraDataObjects, hierarchyDepth);
        serverContext.execute(result);
        return result;
    }

    public RemoteBatchDeleteResult batchDelete(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                               Xid xid, ExternalizablePrimaryKeyList externalizablePrimaryKeyList, int hierarchyDepth)
    {
        List mithraDataObjects = externalizablePrimaryKeyList.getMithraDataObjects();
        if (logger.isDebugEnabled())
        {
            logger.debug("deleting " + mithraDataObjects.size());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteBatchDeleteResult result = new RemoteBatchDeleteResult(mithraDataObjects, hierarchyDepth);
        serverContext.execute(result);
        return result;
    }

    public RemoteBatchUpdateResult batchUpdate(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                               Xid xid, ExternalizableBatchUpdateOperation externalizableBatchUpdateOperation)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("batch updating " + externalizableBatchUpdateOperation.getMithraDataObjects().length);
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteBatchUpdateResult result = new RemoteBatchUpdateResult(externalizableBatchUpdateOperation.getMithraDataObjects(),
                externalizableBatchUpdateOperation.getUpdateWrappers());
        serverContext.execute(result);
        return result;
    }

    public RemoteMultiBatchUpdateResult multiUpdate(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, ExternalizableMultiUpdateOperation externalizableMultiUpdateOperation)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("multi updating " + externalizableMultiUpdateOperation.getMithraDataObjects().length);
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteMultiBatchUpdateResult result = new RemoteMultiBatchUpdateResult(externalizableMultiUpdateOperation.getMithraDataObjects(),
                externalizableMultiUpdateOperation.getUpdateWrappers());
        serverContext.execute(result);
        return result;
    }

    public RemoteCountResult count(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                   Xid xid, Operation op)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("counting");
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteCountResult result = new RemoteCountResult(op);
        serverContext.execute(result);
        return result;
    }

    public RemoteComputeFunctionResult computeFuntcion(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                       Xid xid, Operation op, OrderBy orderby, String columnOrFunctions, ResultSetParser resultSetParser)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("computing function " + columnOrFunctions);
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteComputeFunctionResult result = new RemoteComputeFunctionResult(op, orderby, columnOrFunctions, resultSetParser);
        serverContext.execute(result);
        return result;
    }

    public RemoteGetForDateRangeResult getForDateRange(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                       Xid xid, ExternalizablePrimaryKey externalizablePrimaryKey, ExternalizableDateRange dateRange)
    {
        MithraDataObject mithraDataObject = externalizablePrimaryKey.getMithraDataObject();
        Timestamp start = dateRange.getStart();
        Timestamp end = dateRange.getEnd();
        if (logger.isDebugEnabled())
        {
            logger.debug("getForDateRange " + mithraDataObject.zGetPrintablePrimaryKey() + " " + start + " " + end);
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteGetForDateRangeResult result = new RemoteGetForDateRangeResult(mithraDataObject, start, end);
        serverContext.execute(result);
        return result;
    }

    public RemoteRefreshDatedObjectResult refreshDatedObject(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                                             ExternalizableDatedPrimaryKey externalizableDatedPrimaryKey, boolean lockInDatabase)
    {
        MithraDatedObject mithraDatedObject = externalizableDatedPrimaryKey.getMithraDatedObject();
        if (logger.isDebugEnabled())
        {
            logger.debug("refreshDatedObject " + mithraDatedObject.zGetCurrentOrTransactionalData().zGetPrintablePrimaryKey());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteRefreshDatedObjectResult result = new RemoteRefreshDatedObjectResult(mithraDatedObject, lockInDatabase);
        serverContext.execute(result);
        return result;
    }

    public RemoteDeleteUsingOperationResult deleteUsingOperation(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, Operation op)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("deleteUsingOperation");
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteDeleteUsingOperationResult result = new RemoteDeleteUsingOperationResult(op);
        serverContext.execute(result);
        return result;
    }

    public RemoteDeleteBatchUsingOperationResult deleteBatchUsingOperation(RemoteTransactionId remoteTransactionId,
                                                                           int transactionTimeout, Xid xid, Operation op, int batchSize)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("deleteBatchUsingOperation");
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteDeleteBatchUsingOperationResult result = new RemoteDeleteBatchUsingOperationResult(op, batchSize);
        serverContext.execute(result);
        return result;
    }

    public RemotePrepareForMassDeleteResult prepareForMassDelete(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, Operation op, boolean forceImplicitJoin)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("prepareForMassDelete");
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemotePrepareForMassDeleteResult result = new RemotePrepareForMassDeleteResult(op, forceImplicitJoin);
        serverContext.execute(result);
        return result;
    }

    public RemotePrepareForMassPurgeResult prepareForMassPurge(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, Operation op, boolean forceImplicitJoin)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("prepareForMassPurge");
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemotePrepareForMassPurgeResult result = new RemotePrepareForMassPurgeResult(op, forceImplicitJoin);
        serverContext.execute(result);
        return result;
    }

    public void waitForRemoteTransaction(RemoteTransactionId remoteTransactionId)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("waiting for transaction");
        }
        ServerTransactionWorkerTask workerTask;
        synchronized (transactionIdToWorkerThreadMap)
        {
            workerTask = (ServerTransactionWorkerTask) transactionIdToWorkerThreadMap.get(remoteTransactionId);
        }
        if (workerTask == null || workerTask.isTimedOut())
        {
            throw new MithraTransactionException("server side transaction context is timed out or non-existant");
        }
        workerTask.waitForOtherTransactionToFinish();
        logger.debug("server side rollback finished");
    }

    public RemoteEnrollDatedObjectResult enrollDatedObject(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, ExternalizableDatedPrimaryKey externalizableDatedPrimaryKey)
    {
        MithraDatedTransactionalObject mithraDatedObject = (MithraDatedTransactionalObject) externalizableDatedPrimaryKey.getMithraDatedObject();
        if (logger.isDebugEnabled())
        {
            logger.debug("enrollDatedObject " + mithraDatedObject.zGetCurrentOrTransactionalData().zGetPrintablePrimaryKey());
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteEnrollDatedObjectResult result = new RemoteEnrollDatedObjectResult(mithraDatedObject);
        serverContext.execute(result);
        return result;
    }

    public RemoteExtractOperationDatabaseIdentifiersResult extractDatabaseIdentifiers(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, Operation op)
    {
        logger.debug("server side finding");
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        // we purposely ignore the forRelationship flag, in case the server is set to cacheType="none"
        RemoteExtractOperationDatabaseIdentifiersResult result = new RemoteExtractOperationDatabaseIdentifiersResult(op);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side context " + serverContext.getClass().getName());
        }
        serverContext.execute(result);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side found " + result.getDatabaseIdentifierMap().keySet().size());
        }
        return result;
    }

    public RemoteExtractListDatabaseIdentifiersResult extractDatabaseIdentifiers(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, String finderClassname, ExternalizableSourceAttributeValueSet externalizableSourceAttributeValueSet)
    {
        logger.debug("server side finding");
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        // we purposely ignore the forRelationship flag, in case the server is set to cacheType="none"
        RemoteExtractListDatabaseIdentifiersResult result = new RemoteExtractListDatabaseIdentifiersResult(finderClassname, externalizableSourceAttributeValueSet.getSourceAttributeValueSet());
        if (logger.isDebugEnabled())
        {
            logger.debug("server side context " + serverContext.getClass().getName());
        }
        serverContext.execute(result);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side found " + result.getDatabaseIdentifierMap().keySet().size());
        }
        return result;
    }

    public RemoteTxParticipationResult setTxParticipationMode(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid, String finderClassName, TxParticipationMode mode)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("server side setting participation mode " + finderClassName + " mode " + mode);
        }
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        RemoteTxParticipationResult result = new RemoteTxParticipationResult(finderClassName, mode);
        serverContext.execute(result);
        return result;
    }

    public void clearCacheOrReload(List classNames)
    {
        Set controllerSet = MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet();
        Map controllersByClassname = new HashMap();
        for (Iterator it = controllerSet.iterator(); it.hasNext();)
        {
            MithraRuntimeCacheController controller = (MithraRuntimeCacheController) it.next();
            controllersByClassname.put(controller.getClassName(), controller);
        }
        for (int i = 0; i < classNames.size(); i++)
        {
            MithraRuntimeCacheController controller = (MithraRuntimeCacheController) controllersByClassname.get(classNames.get(i));
            if (controller == null)
            {
                logger.warn("could not find class " + classNames.get(i));
            }
            else
            {
                logger.info("clearing or reloading " + classNames.get(i));
                controller.clearPartialCacheOrReloadFullCache();
            }
        }
    }

    public void clearOrReloadAll()
    {
        logger.info("Clearing or reloading all classes");
        Set controllerSet = MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet();
        for (Iterator it = controllerSet.iterator(); it.hasNext();)
        {
            MithraRuntimeCacheController controller = (MithraRuntimeCacheController) it.next();
            controller.clearPartialCacheOrReloadFullCache();
        }
    }

    public RemoteAggregateResult findAggregatedData(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                    Xid xid, Operation op, Map<String, MithraAggregateAttribute> aggregateAttributes,
                                                    Map<String, MithraGroupByAttribute> groupByAttributes, HavingOperation havingOperation, boolean bypassCache)
    {
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        // we purposely ignore the forRelationship flag, in case the server is set to cacheType="none"
        RemoteAggregateResult result = new RemoteAggregateResult(op, aggregateAttributes, groupByAttributes, havingOperation, bypassCache);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side context " + serverContext.getClass().getName());
        }
        serverContext.execute(result);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side found " + result.getServerSideSize() + " for " + op.getResultObjectPortal().getFinder().getClass().getName());
        }
        return result;
    }

    public RemoteReloadResult reload(List operations)
    {
        ServerContext serverContext = ServerNonTransactionalContext.getInstance();
        // we purposely ignore the forRelationship flag, in case the server is set to cacheType="none"
        RemoteReloadResult result = new RemoteReloadResult(operations, serverContext);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side context " + serverContext.getClass().getName());
        }
        serverContext.execute(result);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side reload found " + result.getServerSideSize() + " for " +
                    ((Operation) operations.get(0)).getResultObjectPortal().getFinder().getClass().getName());
        }
        return result;
    }

    public void destroyTempContext(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
            final String fullyQualifiedTableName, final Object source, final String finderClassName, final boolean isForQuery)
    {
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        // we purposely ignore the forRelationship flag, in case the server is set to cacheType="none"
        if (logger.isDebugEnabled())
        {
            logger.debug("server side context " + serverContext.getClass().getName());
        }
        serverContext.execute(new MithraRemoteResult()
        {
            public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
            {
            }

            public void writeExternal(ObjectOutput out) throws IOException
            {
            }

            public void run()
            {
                getPortalFromFinder(finderClassName).getMithraTuplePersister().destroyTempContext(fullyQualifiedTableName, source, isForQuery);
            }
        });
    }

    public RemoteCursorResult findRemoteCursorResult(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                                     Operation op, Filter postLoadFilter, OrderBy orderBy, boolean bypassCache, int maxObjectsToRetrieve, int maxParallelDegree, boolean forceImplicitJoin)
    {
        ServerContext serverContext = this.prepareServerContext(remoteTransactionId, transactionTimeout, xid);
        // we purposely ignore the forRelationship flag, in case the server is set to cacheType="none"
        RemoteCursorResult result = new RemoteCursorResult(op, postLoadFilter, orderBy, bypassCache, maxObjectsToRetrieve, maxParallelDegree,
                serverContext, VM_ID, forceImplicitJoin);
        if (logger.isDebugEnabled())
        {
            logger.debug("server side context " + serverContext.getClass().getName());
        }
        serverContext.execute(result);
        return result;
    }

    public RemoteContinuedCursorResult continueCursor(RemoteTransactionId remoteCursorId)
    {
        if (remoteCursorId.getServerVmId() != VM_ID)
        {
            throw new RuntimeException("Server restarted while remote cursor active!");
        }
        RemoteCursorResult original = RemoteCursorResult.getExisting(remoteCursorId);
        if (original == null)
        {
            return null;
        }
        return original.getContinuedResult();
    }

    public void closeCursor(RemoteTransactionId remoteCursorId)
    {
        RemoteCursorResult original = RemoteCursorResult.getExisting(remoteCursorId);
        if (original == null)
        {
            return;
        }
        original.closeCursor();
    }

    protected void removeServerSideTask(ServerTransactionWorkerTask task)
    {
        synchronized (transactionIdToWorkerThreadMap)
        {
            transactionIdToWorkerThreadMap.remove(task.getRemoteTransactionId());
        }
    }
}
