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

import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalState;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.transaction.MithraTempTransaction;



public abstract class PersistenceState
{

    public abstract TransactionalBehavior getForNoTransaction();

    public abstract TransactionalBehavior getForSameTransaction();

    public abstract TransactionalBehavior getForDifferentTransaction();

    public abstract TransactionalBehavior getForEnrollTransaction();

    public abstract TransactionalBehavior getForThreadNoObjectYesTransaction();

    public static final int IN_MEMORY = 1;
    public static final int PERSISTED = 2;
    public static final int DELETED = 3;
    public static final int DETACHED = 4;
    public static final int DETACHED_DELETED = 5;
    public static final int IN_MEMORY_NON_TRANSACTIONAL = 6;
    public static final int PERSISTED_NON_TRANSACTIONAL = 7;

    private static final PersistenceState[] allStates = new PersistenceState[7];

    static
    {
        allStates[IN_MEMORY] = new InMemoryState();
        allStates[PERSISTED] = new PersistedState();
        allStates[DELETED] = new DeletedState();
        allStates[DETACHED] = new DetachedState();
        allStates[DETACHED_DELETED] = new DetachedDeletedState();
        allStates[IN_MEMORY_NON_TRANSACTIONAL] = new InMemoryNonTransactionalState();
    }

    public static TransactionalBehavior getTransactionalBehaviorForNoTransactionWithWaitIfNecessary(TransactionalState transactionalState, int persistenceState, MithraTransactionalObject mithraTransactionalObject)
    {
        TransactionalBehavior result = null;
        if (transactionalState == null) // we're not in a transaction
        {
            result = allStates[persistenceState].getForNoTransaction();
        }
        else if (transactionalState.hasNoTransactions())
        {
            result = allStates[persistenceState].getForNoTransaction();
            mithraTransactionalObject.zClearUnusedTransactionalState(transactionalState);
        }
        else
        {
            result = allStates[persistenceState].getForThreadNoObjectYesTransaction();
        }
        return result;
    }

    public static TransactionalBehavior getTransactionalBehaviorForNoTransactionWriteWithWaitIfNecessary(TransactionalState transactionalState,
                                                                                                         int persistenceState, MithraTransactionalObject transactionalObject)
    {
        TransactionalBehavior result = null;
        if (transactionalState == null || transactionalState.hasNoTransactions())
        {
            result = allStates[persistenceState].getForNoTransaction();
            if (result.isPersisted())
            {
                MithraTempTransaction tempTx = new MithraTempTransaction(transactionalObject);
                if (!transactionalObject.zEnrollInTransactionForWrite(transactionalState, new TransactionalState(tempTx, PersistenceState.PERSISTED)))
                {
                    result = null;
                }
            }
            return result;
        }
        else
        {
            // we'll have to wait
            transactionalState.waitForTransactions();
        }
        return result;
    }

    public static TransactionalBehavior getTransactionalBehaviorForTransactionForWriteWithWaitIfNecessary(MithraTransaction threadTx,
                                                                                                          MithraTransactionalObject mto, TransactionalState transactionalState, int persistenceState)
    {
        TransactionalBehavior result = null;
        if (transactionalState == null || transactionalState.hasNoTransactions()) // object is not in a transaction
        {
            TransactionalBehavior enrollBehavior = allStates[persistenceState].getForEnrollTransaction();
            result = enrollBehavior.enrollInTransactionForWrite(mto, threadTx, transactionalState);
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
                if (mto.zEnrollInTransactionForWrite(transactionalState, new TransactionalState(threadTx, persistenceState)))
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

    public static TransactionalBehavior getTransactionalBehaviorForTransactionForReadWithWaitIfNecessary(MithraTransaction threadTx,
                                                                                                         MithraTransactionalObject mto, TransactionalState transactionalState, int persistenceState)
    {
        TransactionalBehavior result = null;
        if (transactionalState == null || transactionalState.hasNoTransactions()) // object is not in a transaction
        {
            if (threadTx.zIsInOperationEvaluationMode() || !mto.zGetPortal().getTxParticipationMode(threadTx).mustParticipateInTxOnRead())
            {
                result = allStates[persistenceState].getForNoTransaction();
            }
            else
            {
                TransactionalBehavior enrollBehavior = allStates[persistenceState].getForEnrollTransaction();
                result = enrollBehavior.enrollInTransactionForRead(mto, threadTx, transactionalState);
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

    public static TransactionalBehavior getTransactionalBehaviorForTransactionForDeleteWithWaitIfNecessary(MithraTransaction threadTx,
                                                                                                           MithraTransactionalObject mto, TransactionalState transactionalState, int persistenceState)
    {
        TransactionalBehavior result = null;
        if (transactionalState == null || transactionalState.hasNoTransactions()) // object is not in a transaction
        {
            TransactionalBehavior enrollBehavior = allStates[persistenceState].getForEnrollTransaction();
            result = enrollBehavior.enrollInTransactionForDelete(mto, threadTx, transactionalState);
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
                if (mto.zEnrollInTransactionForDelete(transactionalState, new TransactionalState(threadTx, persistenceState)))
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
