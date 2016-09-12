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

import com.gs.fw.common.mithra.HavingOperation;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.MithraGroupByAttribute;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.ResultSetParser;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.Filter;

import javax.transaction.xa.Xid;
import java.util.List;
import java.util.Map;



public interface RemoteMithraService
{

    public static final int NO_TRANSACTION = -222;

    public RemoteQueryResult find(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                  Operation op, OrderBy orderBy, boolean bypassCache,
                                  boolean forRelationship, int maxObjectsToRetrieve, boolean forceImplicitJoin) throws MithraException;

    public RemoteMithraObjectConfig[] getObjectConfigurations();

    public RemoteRefreshResult refresh(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                       ExternalizablePrimaryKey externalizablePrimaryKey, boolean lockInDatabase) throws MithraException;

    public void commit(RemoteTransactionId remoteTransactionId, boolean onePhase);

    public void rollback(RemoteTransactionId remoteTransactionId);

    public RemoteMultiUpdateResult update(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                          ExternalizablePrimaryKey externalizablePrimaryKey, List updateWrappers);

    public RemoteInsertResult insert(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                     ExternalizableFullData fullData, int hierarchyDepth);

    public RemoteTupleInsertResult insertTuples(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                                ExternalizableTupleList tupleList, TupleTempContext context, String destinationFinderClassName, int bulkInsertThreshold);

    public RemoteBatchInsertResult batchInsert(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                               ExternalizableFullDataList externalizableFullDataList, int hierarchyDepth, int bulkInsertThreshold);

    public RemoteDeleteResult delete(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                     ExternalizablePrimaryKey externalizablePrimaryKey, int hierarchyDepth);

    public RemotePurgeResult purge(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                   ExternalizablePrimaryKey externalizablePrimaryKey, int hierarchyDepth);

    public RemoteBatchPurgeResult batchPurge(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                             Xid xid, ExternalizablePrimaryKeyList externalizablePrimaryKeyList, int hierarchyDepth);

    public RemoteBatchDeleteResult batchDelete(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                               Xid xid, ExternalizablePrimaryKeyList externalizablePrimaryKeyList, int hierarchyDepth);

    public RemoteBatchUpdateResult batchUpdate(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                               Xid xid, ExternalizableBatchUpdateOperation externalizableBatchUpdateOperation);

    public RemoteMultiBatchUpdateResult multiUpdate(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                    Xid xid, ExternalizableMultiUpdateOperation externalizableMultiUpdateOperation);

    public RemoteCountResult count(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                   Xid xid, Operation op);

    public RemoteComputeFunctionResult computeFuntcion(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                       Xid xid, Operation op, OrderBy orderby, String columnOrFunctions, ResultSetParser resultSetParser);

    public RemoteGetForDateRangeResult getForDateRange(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                       Xid xid, ExternalizablePrimaryKey externalizablePrimaryKey, ExternalizableDateRange dateRange);

    public RemoteRefreshDatedObjectResult refreshDatedObject(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                             Xid xid, ExternalizableDatedPrimaryKey externalizableDatedPrimaryKey, boolean lockInDatabase);

    public RemoteDeleteUsingOperationResult deleteUsingOperation(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                                 Xid xid, Operation op);

    public RemoteDeleteBatchUsingOperationResult deleteBatchUsingOperation(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                                           Xid xid, Operation op, int batchSize);

    public RemotePrepareForMassDeleteResult prepareForMassDelete(RemoteTransactionId remoteTransactionId,
                                                                 int transactionTimeout, Xid xid, Operation op, boolean forceImplicitJoin);

    public RemotePrepareForMassPurgeResult prepareForMassPurge(RemoteTransactionId remoteTransactionId,
                                                               int transactionTimeout, Xid xid, Operation op, boolean forceImplicitJoin);

    public void waitForRemoteTransaction(RemoteTransactionId remoteTransactionId);

    public RemoteEnrollDatedObjectResult enrollDatedObject(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                           Xid xid, ExternalizableDatedPrimaryKey externalizableDatedPrimaryKey);

    public RemoteExtractOperationDatabaseIdentifiersResult extractDatabaseIdentifiers(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                                                      Xid xid, Operation op);

    public RemoteExtractListDatabaseIdentifiersResult extractDatabaseIdentifiers(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                                                 Xid xid, String finderClassname, ExternalizableSourceAttributeValueSet externalizableSourceAttributeValueSet);

    public RemoteTxParticipationResult setTxParticipationMode(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                              Xid xid, String finderClassName, TxParticipationMode mode);

    public void clearCacheOrReload(List classNames);

    public void clearOrReloadAll();

    public RemoteAggregateResult findAggregatedData(RemoteTransactionId remoteTransactionId, int transactionTimeout,
                                                    Xid xid, Operation op, Map<String, MithraAggregateAttribute> aggregateAttributes,
                                                    Map<String, MithraGroupByAttribute> groupByAttributes, HavingOperation havingOperation, boolean bypassCache);

    public RemoteReloadResult reload(List operations);

    public void destroyTempContext(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
            String fullyQualifiedTableName, Object source, String finderClassName, boolean isForQuery);

    public RemoteCursorResult findRemoteCursorResult(RemoteTransactionId remoteTransactionId, int transactionTimeout, Xid xid,
                                                     Operation originalOperation, Filter postLoadFilter, OrderBy orderby,
                                                     boolean bypassCache, int rowcount, int maxParallelDegree, boolean forceImplicitJoin);

    public RemoteContinuedCursorResult continueCursor(RemoteTransactionId remoteCursorId);

    public void closeCursor(RemoteTransactionId remoteCursorId);
}
