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

package com.gs.fw.common.mithra;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.ExceptionCatchingThread;
import com.gs.fw.common.mithra.util.ExceptionHandlingTask;
import com.gs.fw.common.mithra.util.ListFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;


public class SimulatedSequencePrimaryKeyGenerator
{

    private static Logger logger = LoggerFactory.getLogger(SimulatedSequencePrimaryKeyGenerator.class.getName());

    private String sequenceName;
    private Set sourceAttributeSet = new UnifiedSet();
    private int initialValue;
    private int batchSize;
    private int incrementSize;
    private int unfulfilledCapacityRequest = 0;
    private MithraSequence simulatedSequence;
    private MithraSequenceObjectFactory sequenceObjectFactory;
    private transient long nextIdToGive = Long.MAX_VALUE;
    private long nextLimit;
    private List primaryKeyAttributes;
    private boolean sequenceHasSourceAttribute;

    public SimulatedSequencePrimaryKeyGenerator(SimulatedSequenceInitValues initValues)
    {
         this.sequenceName = initValues.getSequenceName();
         this.initialValue = initValues.getInitialValue();
         this.batchSize = initValues.getBatchSize();
         this.incrementSize = initValues.getIncrementSize();
         this.sequenceObjectFactory = (MithraSequenceObjectFactory) initValues.getSequenceObjectFactory();
         this.primaryKeyAttributes = initValues.getPrimaryKeyAttributes();
         this.sequenceHasSourceAttribute = initValues.getHasSourceAttribute();
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    private class NextBatchTask extends ExceptionHandlingTask
    {
        private int size;
        private long localNextLimit;
        private long localNextIdToGive;

        public NextBatchTask(int size)
        {
            this.size = Math.max(batchSize, Math.max(size, unfulfilledCapacityRequest));
            this.localNextIdToGive = nextIdToGive;
        }

        public long getLocalNextLimit()
        {
            return localNextLimit;
        }

        public long getLocalNextIdToGive()
        {
            return localNextIdToGive;
        }

        public void execute()
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                {
                    borrow(size);
                    return null;
                }
            });
        }

        public void borrow(int numberToBorrow)
        {
            long simulatedSequenceNextId = simulatedSequence.getNextId();

            localNextIdToGive = simulatedSequenceNextId;
            localNextLimit = simulatedSequenceNextId + (numberToBorrow * incrementSize);
            simulatedSequence.setNextId(localNextLimit);
        }
    }

    private NextBatchTask getNextBatchTask(int size)
    {
        return new NextBatchTask(size);
    }

    public synchronized void ensureCapacityForBatch(int size, Object sourceAttribute)
    {
        initializeSimulatedSequence(size, sourceAttribute);
        ensureCapacity(size);
    }

    private void ensureCapacity(int size)
    {
        int unused = getNumberOfUnusedIds();
        if (unused == 0)
        {
            getNextBatch(size);
        }
        else if (unused < size)
        {
            unfulfilledCapacityRequest += (size - unused);
        }
    }

    private void getNextBatch(int minSize)
    {
        NextBatchTask task = this.getNextBatchTask(minSize);
        ExceptionCatchingThread.executeTask(task);
        nextIdToGive = task.getLocalNextIdToGive();
        nextLimit = task.getLocalNextLimit();
        unfulfilledCapacityRequest = 0;
    }

    private class InitializeTask extends ExceptionHandlingTask
    {
        private int size;
        private Object sourceAttribute;
        private long validId;
        private NextBatchTask task;
        private Throwable batchInitalizationException;

        public InitializeTask(int size, Object sourceAttribute, long validId)
        {
            this.size = size;
            this.sourceAttribute = sourceAttribute;
            this.validId = validId;
        }

        public Throwable getException()
        {
            return this.batchInitalizationException;
        }

        public void execute()
        {
            try
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                    {
                        if(sequenceHasSourceAttribute)
                            simulatedSequence = sequenceObjectFactory.getMithraSequenceObject(sequenceName, sourceAttribute, initialValue);
                        else
                            simulatedSequence = sequenceObjectFactory.getMithraSequenceObject(sequenceName, null, initialValue);

                        if(checkIfWeNeedToReset(simulatedSequence.getNextId(), validId))
                        {
                            resetSequenceValue(validId, simulatedSequence, nextIdToGive);
                        }
                        task = getNextBatchTask(size);
                        task.execute();
                        return null;
                    }
                });
                nextIdToGive = task.getLocalNextIdToGive();
                nextLimit = task.getLocalNextLimit();
                unfulfilledCapacityRequest = 0;
            }
            catch (Throwable t)
            {
                this.batchInitalizationException = t;
                logger.error("Unable to initialize simulated sequence batch, "+t.getMessage());
            }
        }
    }

    private InitializeTask getInitializeTask(int size, Object sourceAttribute, long validId)
    {
         return new InitializeTask(size, sourceAttribute, validId);
    }

    private void initializeSimulatedSequence(int size, Object sourceAttribute)
    {
        if(simulatedSequence == null)
        {
            long validPrimaryKeyValue = this.getValidPrimaryKeyValue(sourceAttribute);
            InitializeTask initializer = this.getInitializeTask(size, sourceAttribute, validPrimaryKeyValue);
            ExceptionCatchingThread.executeTask(initializer);

            if(initializer.getException() != null)
            {
                throw new MithraBusinessException("Exception during simulated sequence initialization: ", initializer.getException());
            }
            if (sourceAttribute != null) sourceAttributeSet.add(sourceAttribute);
        }
        else if (sourceAttribute != null && !sourceAttributeSet.contains(sourceAttribute))
        {
            this.validateSequenceNextValue(sourceAttribute);
        }
    }

    private long getValidPrimaryKeyValue(Object sourceAttribute)
    {
        long validPrimaryKeyValue;
        if(this.incrementSize > 0)
        {
            validPrimaryKeyValue = this.getMax(sourceAttribute);
        }
        else
        {
            validPrimaryKeyValue = this.getMin(sourceAttribute);
        }
        return validPrimaryKeyValue;

    }
    private boolean checkIfWeNeedToReset(long nextIdToGive, long validPrimaryKeyValue)
    {
        boolean mustCheck;
        if(nextIdToGive == Long.MAX_VALUE)
        {
            return true;
        }

        if(this.incrementSize > 0)
        {
            mustCheck = nextIdToGive <= validPrimaryKeyValue;
        }
        else
        {
            mustCheck = nextIdToGive >= validPrimaryKeyValue;
        }
        return mustCheck;
    }


    private void validateSequenceNextValue(Object sourceAttribute)
    {
        final long validPrimaryKeyValue = this.getValidPrimaryKeyValue(sourceAttribute);
        boolean mustCheck = this.checkIfWeNeedToReset(nextIdToGive, validPrimaryKeyValue);

        if (mustCheck)
        {
            Long next = (Long) MithraManagerProvider.getMithraManager().executeTransactionalCommandInSeparateThread(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    return Long.valueOf(resetSequenceValue(validPrimaryKeyValue, simulatedSequence, nextIdToGive));
                }
            });
            nextIdToGive = next.longValue();
            nextLimit = nextIdToGive;
        }
        sourceAttributeSet.add(sourceAttribute);
    }

    public synchronized long getNextId(Object sourceAttribute) throws MithraException
    {
        initializeSimulatedSequence(batchSize, sourceAttribute);
        ensureNotEmpty();
        long nextId = nextIdToGive;
        nextIdToGive += incrementSize;
        return nextId;
    }

    private void ensureNotEmpty()
    {
        if (this.getNumberOfUnusedIds() == 0)
        {
            getNextBatch(1);
        }
    }

    public synchronized List<BulkSequence> getNextIdsInBulk(Object sourceAttribute, int numberToGet)
    {
        initializeSimulatedSequence(batchSize, sourceAttribute);
        int unused = getNumberOfUnusedIds();
        if (unused >= numberToGet)
        {
            long start = nextIdToGive;
            long end = nextIdToGive + numberToGet*incrementSize;
            nextIdToGive = end;
            return ListFactory.create(new BulkSequence(start, end, incrementSize));
        }
        else
        {
            FastList<BulkSequence> result = new FastList<BulkSequence>(2);
            result.add(new BulkSequence(nextIdToGive, nextLimit, incrementSize));
            nextIdToGive = nextLimit;
            numberToGet -= unused;
            getNextBatch(numberToGet);
            long start = nextIdToGive;
            long end = nextIdToGive + numberToGet*incrementSize;
            nextIdToGive = end;
            result.add(new BulkSequence(start, end, incrementSize));
            return result;
        }
    }

    private int getNumberOfUnusedIds()
    {
        if (nextIdToGive == Long.MAX_VALUE) return 0;
        return (int) ((nextLimit - nextIdToGive)/incrementSize);
    }

    /*
     * Validates that the sequence next id to give is greater than the value of the primary key from all the tables
     * this sequence is used. If not, the value for the next id to give in the sequence is set to a value higher than
     * the max from all tables.
     */
    private long getMax(Object sourceAttribute)
    {
        int size = primaryKeyAttributes.size();
        long maxFromAllTables = Long.MIN_VALUE;
        Long maxFromTable;
        Attribute primaryKeyAttribute;

        for(int i = 0; i < size; i++)
        {
            primaryKeyAttribute = (Attribute)primaryKeyAttributes.get(i);
            maxFromTable = this.getValidIdForPrimaryKey(primaryKeyAttribute, sourceAttribute, true);
            if((maxFromTable != null) && (maxFromTable > maxFromAllTables))
            {
                maxFromAllTables = maxFromTable;
            }
        }
        return maxFromAllTables;
    }

    private long getMin(Object sourceAttribute)
    {
        int size = primaryKeyAttributes.size();
        long minFromAllTables = Long.MAX_VALUE;
        Long minFromTable;
        Attribute primaryKeyAttribute;

        for(int i = 0; i < size; i++)
        {
            primaryKeyAttribute = (Attribute)primaryKeyAttributes.get(i);
            minFromTable = this.getValidIdForPrimaryKey(primaryKeyAttribute, sourceAttribute, false);
            if((minFromTable != null) && (minFromTable < minFromAllTables))
            {
                minFromAllTables = minFromTable;
            }
        }
        return minFromAllTables;

    }

    private long resetSequenceValue(long primaryKeyValue, MithraSequence simulatedSequence, long nextIdToGive)
    {
        long tempId = simulatedSequence.getNextId();
        if (this.incrementSize > 0 && tempId > primaryKeyValue) return tempId;
        if (this.incrementSize < 0 && tempId < primaryKeyValue) return tempId;

        tempId = tempId + (((primaryKeyValue + incrementSize) - tempId)/incrementSize)*incrementSize;
        simulatedSequence.setNextId(tempId);

        return tempId;
    }

    public Long getValidIdForPrimaryKey(Attribute primaryKeyAttribute, Object sourceAttribute, boolean max)
    {
        SourceAttributeType sourceAttributeType = primaryKeyAttribute.getSourceAttributeType();
        MithraObjectPortal ownerPortal = primaryKeyAttribute.getOwnerPortal();
        RelatedFinder finder = ownerPortal.getFinder();
        Operation op = finder.all();

        if ((sourceAttributeType != null ? sourceAttributeType.isStringSourceAttribute() : false) && (sourceAttribute != null ? sourceAttribute.getClass() == String.class : false))
        {
            op = op.and(((StringAttribute)primaryKeyAttribute.getSourceAttribute()).eq(sourceAttribute.toString()));
        }
        else if ((sourceAttributeType != null ? sourceAttributeType.isIntSourceAttribute() : false) && (sourceAttribute != null ? sourceAttribute.getClass() == Integer.class : false))
        {
            op = op.and(((IntegerAttribute)primaryKeyAttribute.getSourceAttribute()).eq(((Integer)sourceAttribute).intValue()));
        }
        AsOfAttribute[] asOfAttributes = primaryKeyAttribute.getAsOfAttributes();
        if (asOfAttributes != null)
        {
            for(int i=0;i<asOfAttributes.length;i++)
            {
                op = op.and(asOfAttributes[i].equalsEdgePoint());
            }
        }

        AggregateList aggregateList = new AggregateList(op);
        if(max)
        {
           aggregateList.addAggregateAttribute("maxId", primaryKeyAttribute.max());
        }
        else
        {
           aggregateList.addAggregateAttribute("maxId", primaryKeyAttribute.min());
        }

        AggregateData data = aggregateList.get(0);

        return data.isAttributeNull("maxId")? null : data.getAttributeAsLong("maxId");
    }
}
