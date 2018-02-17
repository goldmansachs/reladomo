
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

package com.gs.fw.common.mithra.list;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.BooleanAttribute;
import com.gs.fw.common.mithra.attribute.ByteArrayAttribute;
import com.gs.fw.common.mithra.attribute.ByteAttribute;
import com.gs.fw.common.mithra.attribute.CharAttribute;
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.FloatAttribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.LongAttribute;
import com.gs.fw.common.mithra.attribute.ShortAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.attribute.TimeAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.extractor.EmbeddedValueExtractor;
import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelationshipMultiEqualityOperation;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.transaction.MithraTransactionalResource;
import com.gs.fw.common.mithra.util.Time;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;


public class AbstractTransactionalOperationBasedList<E> extends AbstractOperationBasedList<E> implements MithraDelegatedTransactionalList<E>
{
    public static final AbstractTransactionalOperationBasedList DEFAULT = new AbstractTransactionalOperationBasedList(true);
    private static final long BATCH_COMMAND_RETRY_SLEEP_TIME = 2000;
    private static final int BATCH_COMMAND_DEFAULT_RETRY_COUNT = 5;

    private DependentRelationshipRemoveHandler removeHandler = DefaultRemoveHandler.getInstance();
    private DependentRelationshipAddHandler addHandler = DefaultAddHandler.getInstance();

    public AbstractTransactionalOperationBasedList()
    {
    }

    protected AbstractTransactionalOperationBasedList(boolean forDefaultUnused)
    {
        super(true);
    }

    @Override
    protected AbstractTransactionalOperationBasedList newCopy()
    {
        return new AbstractTransactionalOperationBasedList();
    }

    @Override
    protected AbstractTransactionalOperationBasedList copyIfDefault()
    {
        return (AbstractTransactionalOperationBasedList) super.copyIfDefault();
    }

    public MithraDelegatedTransactionalList<E> zSetRemoveHandler(DelegatingList<E> delegatingList, DependentRelationshipRemoveHandler removeHandler)
    {
        if (removeHandler != this.removeHandler)
        {
            AbstractTransactionalOperationBasedList result = copyIfDefault();
            result.removeHandler = removeHandler;
            return result;
        }
        return this;
    }

    public MithraDelegatedTransactionalList<E> zSetAddHandler(DelegatingList<E> delegatingList, DependentRelationshipAddHandler addHandler)
    {
        if (addHandler != this.addHandler)
        {
            AbstractTransactionalOperationBasedList result = copyIfDefault();
            result.addHandler = addHandler;
            return result;
        }
        return this;
    }

    protected boolean isOperationResolved(final DelegatingList<E> delegatingList)
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (delegatingList.getOperation().getResultObjectPortal().getTxParticipationMode(threadTx).mustLockOnRead())
        {
            if (threadTx == null)
            {
                if (delegatingList.zGetCurrentTransaction() != null)
                {
                    throw new MithraBusinessException("Lists are currently only supported in one transaction at a time!");
                }
            }
            else if (!threadTx.equals(delegatingList.zGetCurrentTransaction()))
            {
                if (delegatingList.zGetCurrentTransaction() == null)
                {
                    synchronized (delegatingList)
                    {
                        if (delegatingList.zGetCurrentTransaction() == null) // recheck under lock
                        {
                            this.clearResolved(delegatingList);
                            delegatingList.zSetCurrentTransaction(threadTx);
                            threadTx.enrollResource(new MithraTransactionalResource()
                            {
                                @Override
                                public void zHandleCommit()
                                {
                                    commit(delegatingList);
                                }

                                @Override
                                public void zHandleRollback(MithraTransaction tx)
                                {
                                    rollback(delegatingList);
                                }
                            });
                        }
                    }
                }
                else
                {
                    throw new MithraBusinessException("Lists are currently only supported in one transaction at a time!");
                }
            }
        }
        return super.isOperationResolved(delegatingList);
    }

    public void commit(DelegatingList<E> delegatingList)
    {
        delegatingList.zSetCurrentTransaction(null);
//        this.clearResolved();
    }

    public void rollback(DelegatingList<E> delegatingList)
    {
        delegatingList.zSetCurrentTransaction(null);
        this.clearResolved(delegatingList);
    }

    public void insertAll(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("An operation based list cannot be inserted!");
    }

    public void bulkInsertAll(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("An operation based list cannot be bulk-inserted!");
    }

    public void cascadeInsertAll(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("An operation based list cannot be inserted!");
    }

    public void cascadeInsertAllUntil(DelegatingList<E> delegatingList, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("An operation based list cannot be inserted!");
    }

    private boolean isComplexOperation(DelegatingList<E> delegatingList)
    {
        Operation op = delegatingList.getOperation();
        boolean oneByOne = (op instanceof MappedOperation);
        if (!oneByOne)
        {
            UnifiedSet portals = new UnifiedSet(3);
            op.addDependentPortalsToSet(portals);
            MithraObjectPortal portal = op.getResultObjectPortal();
            oneByOne = portals.size() > 1 || portal.getSuperClassPortals() != null || portal.getJoinedSubClassPortals() != null;
        }
        return oneByOne;
    }

    private void verifyNonDatedList(Operation op)
    {
        if (op.getResultObjectPortal().getClassMetaData().isDated())
        {
            throw new MithraBusinessException("Must not call deleteAll() on a Mithra list containing Dated objects. The terminate method will chain out an existing object.");
        }
    }

    private void verifyNotInTransaction()
    {
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            throw new MithraTransactionException("deleteAllInBatches cannot be called from another transaction!");
        }
    }

    private static void executeCommandInBatches(String commandName, int[] batchSize,TransactionalCommand command)
    {
        int affectedRows;
        long sleepTime= BATCH_COMMAND_RETRY_SLEEP_TIME;
        int retryCount = BATCH_COMMAND_DEFAULT_RETRY_COUNT;
        do
        {
            try
            {
                do
                {
                   affectedRows = (Integer) MithraManagerProvider.getMithraManager().executeTransactionalCommand(command, 0);
                }
                while(affectedRows >= batchSize[0]);
                retryCount = 0;
            }
            catch (Throwable throwable)
            {
                retryCount = handleRetryException(commandName, throwable, retryCount);
                try
                {
                    Thread.sleep(sleepTime);
                    sleepTime *= 2;
                }
                catch(InterruptedException e)
                {
                    //log something here
                }
            }
        }
        while(retryCount > 0);
    }

    private static int handleRetryException(String commandName, Throwable t, int retryCount)
    {
        --retryCount;
        if(retryCount == 0)
        {
            throw new MithraBusinessException(commandName+" rolled back tx. All the retry attempts failed; will not retry.", t);
        }
        return retryCount;
    }

    public void deleteAll(DelegatingList<E> delegatingList)
    {
        Operation op = delegatingList.getOperation();
        verifyNonDatedList(op);
        boolean oneByOne = (op instanceof MappedOperation);
        if (!oneByOne)
        {
            UnifiedSet portals = new UnifiedSet(3);
            op.addDependentPortalsToSet(portals);
            MithraObjectPortal portal = op.getResultObjectPortal();
            oneByOne = portals.size() > 1 || portal.getSuperClassPortals() != null || portal.getJoinedSubClassPortals() != null;
        }
        TransactionalCommand command = null;
        if (oneByOne)
        {
            command = new DeleteAllOneByOneCommand(delegatingList);
        }
        else
        {
            command = new DeleteAllTransactionalCommand(delegatingList, this);
        }
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(command);
    }

    public void deleteAllInBatches(DelegatingList<E> delegatingList, int batchSize)
    {
        verifyNonDatedList(delegatingList.getOperation());
        verifyNotInTransaction();
        final int[] bSize = new int[1];
        bSize[0] = batchSize;
        TransactionalCommand command;

        if (isComplexOperation(delegatingList))
        {
            command = new DeleteAllOneByOneInBatchesCommand(delegatingList, bSize);
        }
        else
        {
            command = new DeleteAllInBatchesTransactionalCommand(delegatingList, this, bSize);
        }
        executeCommandInBatches("DeleteAllInBatches"+batchSize,bSize, command);
    }

    public void purgeAllInBatches(DelegatingList<E> delegatingList, int batchSize)
    {
        verifyNotInTransaction();
        final int[] bSize = new int[1];
        bSize[0] = batchSize;
        TransactionalCommand command;

        if (isComplexOperation(delegatingList))
        {
            throw new RuntimeException("Multiple portal purgeAll not implemented");
        }
        else
        {
            command = new PurgeAllInBatchesTransactionalCommand(delegatingList, this, bSize);
        }
        executeCommandInBatches("PurgeAllInBatches"+batchSize, bSize,command);
    }

    public void cascadeDeleteAll(DelegatingList<E> delegatingList)
    {
        Operation op = delegatingList.getOperation();
        verifyNonDatedList(op);
        if (op instanceof MappedOperation)
        {
            List result = this.resolveOperation(delegatingList);
            if (result.size() > 0)
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeDeleteAllTransactionalCommand(delegatingList));
            }
        }
        else
        {
            fixOperation(delegatingList);
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeDeleteAllTransactionalCommand(delegatingList));
        }
    }

    private void fixOperation(DelegatingList<E> delegatingList)
    {
        Operation op = delegatingList.getOperation();
        if (op instanceof RelationshipMultiEqualityOperation)
        {
            delegatingList.zSetOperation(((RelationshipMultiEqualityOperation)op).getOrCreateMultiEqualityOperation());
        }
    }

    public void purgeAll(DelegatingList<E> delegatingList)
    {
        Operation op = delegatingList.getOperation();
        UnifiedSet portals = new UnifiedSet(3);
        op.addDependentPortalsToSet(portals);
        TransactionalCommand command;
        if (portals.size() > 1)
        {
            throw new RuntimeException("Multiple portal purgeAll not implemented");
        }
        else
        {
            command = new PurgeAllTransactionalCommand(delegatingList, this);
        }
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(command);
    }

    public void terminateAll(DelegatingList<E> delegatingList)
    {
        fixOperation(delegatingList);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TerminateAllTransactionalCommand(delegatingList));
    }

    public void cascadeTerminateAll(DelegatingList<E> delegatingList)
    {
        fixOperation(delegatingList);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeTerminateAllTransactionalCommand(delegatingList));
    }

    public void cascadeTerminateAllUntil(DelegatingList<E> delegatingList, Timestamp exclusiveUntil)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeTerminateAllUntilTransactionalCommand(delegatingList, exclusiveUntil));
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("An operation based list cannot be updated");
    }

    public void zCopyDetachedValuesDeleteIfRemovedOnly(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("An operation based list cannot be updated");
    }

    public void cascadeUpdateInPlaceBeforeTerminate(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("An operation based list cannot be updated");
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("An operation based list cannot be updated");
    }

    public void copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(DelegatingList<E> delegatingList, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("An operation based list cannot be updated");
    }

    protected class CascadeDeleteAllTransactionalCommand implements TransactionalCommand
    {
        private DelegatingList delegatingList;

        public CascadeDeleteAllTransactionalCommand(DelegatingList<E> delegatingList)
        {
            this.delegatingList = delegatingList;
        }

        public Object executeTransaction(MithraTransaction tx) throws Throwable
        {
            delegatingList.zCascadeDeleteRelationships();
            deleteAll(delegatingList);
            return null;
        }
    }

    public void setBoolean(DelegatingList<E> delegatingList, BooleanAttribute attr, boolean newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setBooleanValue(resolved.get(i), newValue);
        }
    }

    public void setByte(DelegatingList<E> delegatingList, ByteAttribute attr, byte newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setByteValue(resolved.get(i), newValue);
        }
    }

    public void setShort(DelegatingList<E> delegatingList, ShortAttribute attr, short newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setShortValue(resolved.get(i), newValue);
        }
    }

    public void setChar(DelegatingList<E> delegatingList, CharAttribute attr, char newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setCharValue(resolved.get(i), newValue);
        }
    }

    public void setInteger(DelegatingList<E> delegatingList, IntegerAttribute attr, int newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setIntValue(resolved.get(i), newValue);
        }
    }

    public void setLong(DelegatingList<E> delegatingList, LongAttribute attr, long newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setLongValue(resolved.get(i), newValue);
        }
    }

    public void setFloat(DelegatingList<E> delegatingList, FloatAttribute attr, float newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setFloatValue(resolved.get(i), newValue);
        }
    }

    public void setDouble(DelegatingList<E> delegatingList, DoubleAttribute attr, double newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setDoubleValue(resolved.get(i), newValue);
        }
    }

    public void setString(DelegatingList<E> delegatingList, StringAttribute attr, String newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setStringValue(resolved.get(i), newValue);
        }
    }

    public void setTimestamp(DelegatingList<E> delegatingList, TimestampAttribute attr, Timestamp newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setTimestampValue(resolved.get(i), newValue);
        }
    }

    public void setDate(DelegatingList<E> delegatingList, DateAttribute attr, Date newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setDateValue(resolved.get(i), newValue);
        }
    }

    public void setTime(DelegatingList<E> delegatingList, TimeAttribute attr, Time newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setTimeValue(resolved.get(i), newValue);
        }
    }

    public void setByteArray(DelegatingList<E> delegatingList, ByteArrayAttribute attr, byte[] newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setByteArrayValue(resolved.get(i), newValue);
        }
    }

    public void setBigDecimal(DelegatingList<E> delegatingList, BigDecimalAttribute attr, BigDecimal newValue)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setBigDecimalValue(resolved.get(i), newValue);
        }
    }

    public void setAttributeNull(DelegatingList<E> delegatingList, Attribute attr)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setValueNull(resolved.get(i));
        }
    }

    public void setEvoValue(DelegatingList<E> delegatingList, EmbeddedValueExtractor attr, Object evo)
    {
        List resolved = this.resolveOperation(delegatingList);
        for(int i=0;i<resolved.size();i++)
        {
            attr.setValue(resolved.get(i), evo);
        }
    }

    public DelegatingList zCloneForRelationship(DelegatingList delegatingList)
    {
        DelegatingList result = super.zCloneForRelationship(delegatingList);
        result.zSetAddHandler(this.addHandler);
        result.zSetRemoveHandler(this.removeHandler);
        return result;
    }

    // List methods
    private List resolveOperationAndCopy(DelegatingList delegatingList)
    {
        synchronized(delegatingList)
        {
            CachedQuery resolved = getResolved(delegatingList);
            if (resolved != null && resolved.isModifiable())
            {
                return resolved.getResult();
            }
            resolveOperation(delegatingList);
            resolved = getResolved(delegatingList).getModifiableClone();
            delegatingList.zSetFastListOrCachedQuery(resolved);
            return resolved.getResult();
        }
    }

    public E remove(DelegatingList<E> delegatingList, int index)
    {
        List list = resolveOperationAndCopy(delegatingList);
        MithraObject toBeRemoved = (MithraObject) list.get(index);
        this.removeHandler.removeRelatedObject(toBeRemoved);
        list.remove(index);
        return (E) toBeRemoved;
    }

    public void add(DelegatingList<E> delegatingList, int index, E o)
    {
        this.addHandler.addRelatedObject((MithraTransactionalObject) o);
    }

    @Override
    public MithraDelegatedList<E> getNonPersistentDelegate()
    {
        return AbstractTransactionalNonOperationBasedList.DEFAULT;
    }

    public boolean add(DelegatingList<E> delegatingList, E o)
    {
        this.addHandler.addRelatedObject((MithraTransactionalObject) o);
        return true;
    }

    public boolean addAll(DelegatingList<E> delegatingList, int index, Collection<? extends E> c)
    {
        return this.addAll(delegatingList, c);
    }

    public boolean addAll(DelegatingList<E> delegatingList, Collection<? extends E> c)
    {
        for(Iterator it = c.iterator(); it.hasNext(); )
        {
            Object o = it.next();
            this.addHandler.addRelatedObject((MithraTransactionalObject) o);
        }
        return true;
    }

    public void clear(DelegatingList<E> delegatingList)
    {
        for(int i=this.size(delegatingList) - 1;i >=0 ;i--)
        {
            this.remove(delegatingList, i);
        }
    }

    public boolean remove(DelegatingList<E> delegatingList, Object o)
    {
        List list = resolveOperation(delegatingList);
        for(int i=0;i<list.size();i++)
        {
            if (o == list.get(i))
            {
                this.remove(delegatingList, i);
                return true;
            }
        }
        return false;
    }

    public boolean removeAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        boolean removed = false;
        for(Iterator it = c.iterator(); it.hasNext(); )
        {
            removed |= this.remove(delegatingList, it.next());
        }
        return removed;
    }

    @Override
    public boolean retainAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        List list = resolveOperationAndCopy(delegatingList);
        FastList newList = FastList.newList(list.size());
        for(int i=0;i<list.size();i++)
        {
            Object item = list.get(i);
            if (c.contains(item))
            {
                newList.add(item);
            }
            else
            {
                this.removeHandler.removeRelatedObject((MithraObject) item);
            }
        }
        if (newList.size() < list.size())
        {
            getResolved(delegatingList).setResult(newList);
            return true;
        }
        return false;
    }

//    @Override
//    public ListIterator listIterator(DelegatingList<E> es)
//    {
//        asdf: must override remove/set -- will break collections.sort
//    }
//
//    @Override
//    public ListIterator listIterator(DelegatingList<E> es, int index)
//    {
//        asdf: must override remove/set
//    }

    @Override
    public Iterator<E> iterator(DelegatingList<E> delegatingList)
    {
        return new IteratorWithRemoveHandler(resolveOperation(delegatingList).iterator());
    }

    private class IteratorWithRemoveHandler<E> implements Iterator<E>
    {
        private Iterator<E> delegate;
        private E last;

        private IteratorWithRemoveHandler(Iterator<E> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public E next()
        {
            last = delegate.next();
            return last;
        }

        @Override
        public void remove()
        {
            removeHandler.removeRelatedObject((MithraObject) last);
            delegate.remove();
        }
    }
}
