
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
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.UpdateCountHolder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;


public class TxOperationsForIndependentClass extends AbstractTxOperations
{

    private static final Logger logger = LoggerFactory.getLogger(MithraRootTransaction.class.getName());

    private static final Procedure commitProc = new CommitUpdateCountProc();
    private static final Procedure rollbackProc = new RollbackUpdateCountProc();

    private MithraObjectPortal portal;
    private UnifiedSet updateCountHolders = null;
    private boolean updatePerClassCount;
    private boolean hasDelete;
    private boolean isFailed = false;

    public TxOperationsForIndependentClass(MithraObjectPortal portal)
    {
        this.portal = portal;
    }

    public void synchronizedHandleCacheCommit() throws MithraTransactionException
    {
        if (updateCountHolders != null)
        {
            updateCountHolders.forEach(commitProc);
        }
        if (updatePerClassCount)
        {
            this.portal.getPerClassUpdateCountHolder().commitUpdateCount();
        }
    }

    public void synchronizedHandleCacheRollback() throws MithraTransactionException
    {
        if (updateCountHolders != null)
        {
            updateCountHolders.forEach(rollbackProc);
        }
        if (updatePerClassCount)
        {
            this.portal.getPerClassUpdateCountHolder().rollbackUpdateCount();
        }
    }

    public boolean addUpdate(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraTransactionException
    {
        if (!updatePerClassCount)
        {
            if (updateCountHolders == null)
            {
                updateCountHolders = new UnifiedSet(11);
            }
            updateCountHolders.add(attributeUpdateWrapper.getAttribute());
        }
        return addUpdateWithConsolidation(obj, attributeUpdateWrapper, this.portal);
    }

    public void addInsert(MithraTransactionalObject obj, MithraObjectPortal portal, int depthCheck) throws MithraTransactionException
    {
        updatePerClassCount = true;
        updateCountHolders = null;
        TransactionOperation consolidatedOperation = null;

        if (this.operations.size() > 0)
        {
            TransactionOperation lastOperation = (TransactionOperation) this.operations.get(operations.size() - 1);
            consolidatedOperation = lastOperation.combineInsert(obj, portal);
        }

        if (consolidatedOperation != null)
        {
            this.operations.set(this.operations.size() - 1, consolidatedOperation);
        }
        else
        {
            this.operations.add(new InsertOperation(obj, portal));
        }
    }

    public void addDelete(MithraTransactionalObject obj, MithraObjectPortal portal) throws MithraTransactionException
    {
        updatePerClassCount = true;
        updateCountHolders = null;
        TransactionOperation consolidatedOperation = null;
        this.hasDelete = true;

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
        updatePerClassCount = true;
        updateCountHolders = null;
        this.hasDelete = true;

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
        updatePerClassCount = true;
        updateCountHolders = null;
        this.hasDelete = true;
    }

    public int deleteBatchUsingOperation(Operation op, int batchSize)
    {
        this.executeBufferedOperations();
        updatePerClassCount = true;
        updateCountHolders = null;
        this.hasDelete = true;
        return op.getResultObjectPortal().getMithraObjectPersister().deleteBatchUsingOperation(op, batchSize);
    }

    public void purgeUsingOperation(Operation op) throws MithraDatabaseException
    {
        this.operations.add(new PurgeUsingOperationOperation(op));
        updatePerClassCount = true;
        updateCountHolders = null;
        this.hasDelete = true;
    }

    public void addLogicalDeleteForPortal(MithraObjectPortal portal)
    {
        updatePerClassCount = true;
        updateCountHolders = null;
        this.hasDelete = true;
    }

    public void executeBufferedOperations() throws MithraDatabaseException
    {
        if (isFailed && operations.size() > 0)
        {
            logger.error("Transaction already failed, no further actions will be executed");
            clear();
            return;
        }
        if (this.operations.size() > 0)
        {
            combineAll();
            for (int i = 0; i < this.operations.size(); i++)
            {
                TransactionOperation op = (TransactionOperation) this.operations.get(i);
                op.execute();
            }
            this.operations.clear();
            this.hasDelete = false;
        }
    }

    private void clear()
    {
        this.operations.clear();
        this.hasDelete = false;
        updatePerClassCount = false;
        updateCountHolders = null;
    }

    public Logger getLogger()
    {
        return logger;
    }

    public boolean hasPendingOperation()
    {
        return this.operations.size() > 0;
    }

    public void handleFailedOperations()
    {
        this.isFailed = true;
        this.clear();
    }

    public static class PortalExtractor implements Extractor
    {
        public void setValue(Object o, Object newValue)
        {
            throw new RuntimeException("not implemented");
        }

        public void setValueNull(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
        {
            throw new RuntimeException("not implemented");
        }

        public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
        {
            throw new RuntimeException("not implemented");
        }

        public boolean isAttributeNull(Object o)
        {
            return false;
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            return this.valueOf(first) == secondExtractor.valueOf(second);
        }

        public OrderBy ascendingOrderBy()
        {
            throw new RuntimeException("not implemented");
        }

        public OrderBy descendingOrderBy()
        {
            throw new RuntimeException("not implemented");
        }

        public int valueHashCode(Object o)
        {
            return System.identityHashCode(((TxOperationsForIndependentClass)o).portal);
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.valueOf(first) == this.valueOf(second);
        }

        public Object valueOf(Object anObject)
        {
            return ((TxOperationsForIndependentClass)anObject).portal;
        }

    }

    public boolean hasDeleteOperation()
    {
        return hasDelete;
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
