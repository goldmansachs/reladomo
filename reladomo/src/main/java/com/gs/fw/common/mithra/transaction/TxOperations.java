
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

package com.gs.fw.common.mithra.transaction;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.UpdateCountHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class TxOperations extends AbstractTxOperations
{

    private static final Logger logger = LoggerFactory.getLogger(MithraRootTransaction.class.getName());

    private static final Object NO_DELETE_OP = new Object();
    private static final Object DELETE_OP = new Object();
    private static final Procedure commitProc = new CommitUpdateCountProc();
    private static final Procedure rollbackProc = new RollbackUpdateCountProc();
    private final UnifiedSet updateCountHolders = new UnifiedSet(20);

    private boolean isFailed = false;

    private Map portalsWithOperations = new UnifiedMap(8);

    public void synchronizedHandleCacheCommit() throws MithraTransactionException
    {
        updateCountHolders.forEach(commitProc);
    }

    public void synchronizedHandleCacheRollback() throws MithraTransactionException
    {
        updateCountHolders.forEach(rollbackProc);
    }

    public boolean addUpdate(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraTransactionException
    {
        MithraObjectPortal mithraObjectPortal = obj.zGetPortal();
        updateCountHolders.add(attributeUpdateWrapper.getAttribute());

        if (! this.portalsWithOperations.containsKey(mithraObjectPortal))
        {
            this.portalsWithOperations.put(mithraObjectPortal, NO_DELETE_OP);
        }

        MithraObjectPortal ownerPortal = attributeUpdateWrapper.getAttribute().getOwnerPortal();
        return addUpdateWithConsolidation(obj, attributeUpdateWrapper, ownerPortal);
    }

    public void addInsert(MithraTransactionalObject obj, MithraObjectPortal portal, int depthCheck) throws MithraTransactionException
    {
        updateCountHolders.add(portal.getPerClassUpdateCountHolder());
        TransactionOperation consolidatedOperation = null;
        if (! this.portalsWithOperations.containsKey(portal))
        {
            this.portalsWithOperations.put(portal, NO_DELETE_OP);
        }

        if (this.operations.size() > depthCheck)
        {
            TransactionOperation lastOperation = (TransactionOperation) this.operations.get(operations.size() - 1 - depthCheck);
            consolidatedOperation = lastOperation.combineInsert(obj, portal);
        }

        if (consolidatedOperation != null)
        {
            this.operations.set(this.operations.size() - 1 - depthCheck, consolidatedOperation);
        }
        else
        {
            this.operations.add(new InsertOperation(obj, portal));
        }
    }

    public void addDelete(MithraTransactionalObject obj, MithraObjectPortal portal) throws MithraTransactionException
    {
        updateCountHolders.add(portal.getPerClassUpdateCountHolder());
        TransactionOperation consolidatedOperation = null;
        this.portalsWithOperations.put(portal, DELETE_OP);

        if (this.operations.size() > 0)
        {
            TransactionOperation lastOperation = (TransactionOperation) this.operations.get(operations.size()-1);
            consolidatedOperation = lastOperation.combineDelete(obj, portal);
        }

        if (consolidatedOperation != null)
        {
            if (consolidatedOperation == DoNothingTransactionOperation.getInstance())
            {
                this.operations.remove(operations.size() - 1);
            }
            else
            {
                this.operations.set(operations.size()-1, consolidatedOperation);
            }
        }
        else
        {
            this.operations.add(new DeleteOperation(obj, portal));
        }
    }

    public void addPurge(MithraTransactionalObject obj, MithraObjectPortal portal) throws MithraTransactionException
    {
        TransactionOperation consolidatedOperation = null;
        updateCountHolders.add(portal.getPerClassUpdateCountHolder());
        this.portalsWithOperations.put(portal, DELETE_OP);

        if (this.operations.size() > 0)
        {
            TransactionOperation lastOperation = (TransactionOperation) this.operations.get(operations.size()-1);
            consolidatedOperation = lastOperation.combinePurge(obj, portal);
        }

        if (consolidatedOperation != null)
        {
            if (consolidatedOperation == DoNothingTransactionOperation.getInstance())
            {
                this.operations.remove(operations.size() - 1);
            }
            else
            {
                this.operations.set(operations.size()-1, consolidatedOperation);
            }
        }
        else
        {
            this.operations.add(new PurgeOperation(obj, portal));
        }
    }

    public void deleteUsingOperation(Operation op) throws MithraDatabaseException
    {
        this.operations.add(new DeleteUsingOperationOperation(op));
        MithraObjectPortal portal = op.getResultObjectPortal();
        updateCountHolders.add(portal.getPerClassUpdateCountHolder());
        this.portalsWithOperations.put(portal, DELETE_OP);
    }

    public int deleteBatchUsingOperation(Operation op, int batchSize)
    {
        this.executeBufferedOperations();
        MithraObjectPortal portal = op.getResultObjectPortal();
        updateCountHolders.add(portal.getPerClassUpdateCountHolder());
        return op.getResultObjectPortal().getMithraObjectPersister().deleteBatchUsingOperation(op, batchSize);
    }

    public void purgeUsingOperation(Operation op) throws MithraDatabaseException
    {
        this.operations.add(new PurgeUsingOperationOperation(op));
        MithraObjectPortal portal = op.getResultObjectPortal();
        updateCountHolders.add(portal.getPerClassUpdateCountHolder());
        this.portalsWithOperations.put(portal, DELETE_OP);
    }

    public void addLogicalDeleteForPortal(MithraObjectPortal portal)
    {
        updateCountHolders.add(portal.getPerClassUpdateCountHolder());
        this.portalsWithOperations.put(portal, DELETE_OP);
    }

    public boolean executeBufferedOperations(MithraObjectPortal mithraObjectPortal, boolean lookForDelete)
    {
        if (isFailed && operations.size() > 0)
        {
            logger.error("Transaction already failed, no further actions will be executed");
            this.clear();
            return false;
        }
        if (this.operations.size() > 0)
        {
            Object operationType = this.portalsWithOperations.get(mithraObjectPortal);
            boolean todo = operationType != null;
            if (todo && lookForDelete)
            {
                todo = operationType == DELETE_OP;
            }
            if (todo)
            {
                this.executeBufferedOperations();
            }
        }
        return this.operations.size() == 0;
    }

    public void executeBufferedOperations() throws MithraDatabaseException
    {
        if (isFailed && operations.size() > 0)
        {
            logger.error("Transaction already failed, no further actions will be executed");
            this.clear();
            return;
        }
        if (operations.size() > 0)
        {
            combineAll();
            for (int i = 0; i < this.operations.size(); i++)
            {
                TransactionOperation op = (TransactionOperation) this.operations.get(i);
                op.execute();
            }
            this.operations.clear();
            this.portalsWithOperations.clear();
        }
    }

    public boolean hasPendingOperation(MithraObjectPortal portal)
    {
        return this.portalsWithOperations.containsKey(portal);
    }

    public Logger getLogger()
    {
        return logger;
    }

    public void clear()
    {
        this.operations.clear();
        this.portalsWithOperations.clear();
        this.updateCountHolders.clear();
    }

    public void handledFailedBufferedOperation()
    {
        this.isFailed = true;
        this.clear();
    }

    private static class CommitUpdateCountProc implements Procedure
    {
        public void value(Object object)
        {
            ((UpdateCountHolder)object).commitUpdateCount();
        }
    }

    private static class RollbackUpdateCountProc implements Procedure
    {
        public void value(Object object)
        {
            ((UpdateCountHolder)object).rollbackUpdateCount();
        }
    }
}
