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

package com.gs.fw.common.mithra.behavior.state;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TemporalContainer;



public abstract class DatedPersistenceState
{

    public abstract DatedTransactionalBehavior getForNoTransaction();

    public abstract DatedTransactionalBehavior getForSameTransaction();

    public abstract DatedTransactionalBehavior getForDifferentTransaction();

    public abstract DatedTransactionalBehavior getForEnrollTransaction();

    public abstract DatedTransactionalBehavior getForThreadNoObjectYesTransaction();

    public static final int IN_MEMORY = PersistenceState.IN_MEMORY;
    public static final int PERSISTED = PersistenceState.PERSISTED;
    public static final int DELETED = PersistenceState.DELETED;
    public static final int DETACHED = PersistenceState.DETACHED;
    public static final int DETACHED_DELETED = PersistenceState.DETACHED_DELETED;
    public static final int IN_MEMORY_NON_TRANSACTIONAL = PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
    public static final int PERSISTED_NON_TRANSACTIONAL = PersistenceState.PERSISTED_NON_TRANSACTIONAL;

    private static final DatedPersistenceState[] allStates = new DatedPersistenceState[7];

    static
    {
        allStates[IN_MEMORY] = new DatedInMemoryState();
        allStates[PERSISTED] = new DatedPersistedState();
        allStates[DELETED] = new DatedDeletedState();
        allStates[DETACHED] = new DatedDetachedState();
        allStates[DETACHED_DELETED] = new DatedDetachedDeletedState();
        allStates[IN_MEMORY_NON_TRANSACTIONAL] = new DatedInMemoryNonTransactionalState();
    }

    public static final DatedPersistenceState getPersistenceState(int state)
    {
        return allStates[state];
    }

    public static DatedTransactionalBehavior getTransactionalBehaviorForTransaction(MithraTransaction threadTx,
                                                                                    MithraDatedTransactionalObject mto,
                                                                                    DatedTransactionalState transactionalState, int persistenceState)
    {
        DatedTransactionalBehavior result = null;
        if (transactionalState == null || transactionalState.hasNoTransactions()) // we're not in a transaction
        {
            if (threadTx.zIsInOperationEvaluationMode())
            {
                result = allStates[persistenceState].getForNoTransaction();
            }
            else
            {
                result = allStates[persistenceState].getForEnrollTransaction();
                result = result.enrollInTransaction(mto, threadTx, transactionalState, persistenceState);
            }
        }
        else
        {
            if (transactionalState.isParticipatingInReadOrWrite(threadTx))
            {
                result = allStates[transactionalState.getPersistenceState()].getForSameTransaction();
            }
            else
            {
                if (threadTx.zIsInOperationEvaluationMode() || !mto.zGetPortal().getTxParticipationMode(threadTx).mustParticipateInTxOnRead())
                {
                    result = allStates[persistenceState].getForNoTransaction();
                }
                else if (transactionalState.isEnrolledForWriteByOther(threadTx))
                {
                    transactionalState.waitForTransactions(threadTx);
                }
                else
                {
                    if (mto.zEnrollInTransactionForRead(transactionalState, threadTx, persistenceState))
                    {
                        result = allStates[persistenceState].getForSameTransaction();
                    }
                }
            }
        }
        return result;
    }

    public static DatedTransactionalBehavior getTransactionalBehaviorForNoTransaction(MithraDatedTransactionalObject mto,
                                                                                      DatedTransactionalState transactionalState, int persistenceState)
    {
        DatedTransactionalBehavior result = null;
        if (transactionalState == null) // we're not in a transaction
        {
            result = allStates[persistenceState].getForNoTransaction();
        }
        else if (transactionalState.hasNoTransactions())
        {
            mto.zClearUnusedTransactionalState(transactionalState);
            result = allStates[persistenceState].getForNoTransaction();
        }
        else
        {
            result = allStates[persistenceState].getForThreadNoObjectYesTransaction();
        }
        return result;
    }

    public static DatedTransactionalBehavior getTransactionalBehaviorForWrite(
            MithraDatedTransactionalObject mto, DatedTransactionalState transactionalState, int persistenceState)
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (threadTx == null)
        {
            if (persistenceState == PERSISTED)
            {
                throw new MithraBusinessException("Dated objects can only be modified in a transaction");
            }
            return getTransactionalBehaviorForNoTransaction(mto, transactionalState, persistenceState);
        }
        DatedTransactionalBehavior result = null;
        if (transactionalState == null || transactionalState.hasNoTransactions()) // object is not in a transaction
        {
            DatedTransactionalBehavior enrollBehavior = allStates[persistenceState].getForEnrollTransaction();
            result = enrollBehavior.enrollInTransactionForWrite(mto, threadTx, transactionalState, persistenceState);
        }
        else
        {
            if (transactionalState.isEnrolledForWrite(threadTx))
            {
                result = allStates[transactionalState.getPersistenceState()].getForSameTransaction();
            }
            else if (transactionalState.hasNoTransactionsExcept(threadTx))
            {
                // upgrading from read to write
                MithraDataObject data = mto.zGetTxDataForRead();
                MithraDataObject txData = null;
                TemporalContainer container = null;
                if (persistenceState == PERSISTED)
                {
                    container = mto.zGetCache().getOrCreateContainer(data);
                }
                else
                {
                    txData = data.copy();
                }
                if (mto.zEnrollInTransactionForWrite(transactionalState, container, txData, threadTx))
                {
                    result = allStates[persistenceState].getForSameTransaction();
                }
            }
            else
            {
                transactionalState.waitForTransactions(threadTx);
            }
        }
        return result;
    }

    public static DatedTransactionalBehavior getTransactionalBehaviorForDelete(
            MithraDatedTransactionalObject mto, DatedTransactionalState transactionalState, int persistenceState)
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (threadTx == null)
        {
            throw new MithraBusinessException("Dated objects can only be modified in a transaction");
        }
        DatedTransactionalBehavior result = null;
        if (transactionalState == null || transactionalState.hasNoTransactions()) // object is not in a transaction
        {
            DatedTransactionalBehavior enrollBehavior = allStates[persistenceState].getForEnrollTransaction();
            result = enrollBehavior.enrollInTransactionForDelete(mto, threadTx, transactionalState, persistenceState);
        }
        else
        {
            if (transactionalState.isEnrolledForWrite(threadTx))
            {
                result = allStates[transactionalState.getPersistenceState()].getForSameTransaction();
            }
            else if (transactionalState.hasNoTransactionsExcept(threadTx))
            {
                // upgrading from read to write
                MithraDataObject data = mto.zGetTxDataForRead();
                if (mto.zEnrollInTransactionForWrite(transactionalState, mto.zGetCache().getOrCreateContainer(data), data.copy(), threadTx))
                {
                    result = allStates[persistenceState].getForSameTransaction();
                }
            }
            else
            {
                transactionalState.waitForTransactions(threadTx);
            }
        }
        return result;
    }


}
