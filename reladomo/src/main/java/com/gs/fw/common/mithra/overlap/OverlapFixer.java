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

package com.gs.fw.common.mithra.overlap;


import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.transaction.BatchDeleteOperation;
import com.gs.fw.common.mithra.transaction.BatchInsertOperation;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.dbextractor.MilestoneRectangle;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class OverlapFixer implements OverlapHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OverlapFixer.class.getName());

    private final int batchSize;
    private BatchInsertOperation insertBatch;
    private BatchDeleteOperation deleteBatch;
    private long startTime;
    private int totalInsertCount;
    private int totalDeleteCount;

    public OverlapFixer()
    {
        this(1000);
    }

    public OverlapFixer(int batchSize)
    {
        this.batchSize = batchSize;
    }

    @Override
    public void overlapProcessingStarted(Object connectionManager, String mithraClassName)
    {
        if (this.getPendingOperationCount() > 0)
        {
            throw new IllegalStateException("There are pending batch operations!");
        }
        LOGGER.info("Fixing overlaps for " + mithraClassName);
        this.startTime = System.currentTimeMillis();
    }

    private int getPendingOperationCount()
    {
        return this.getInsertBatchSize() + this.getDeleteBatchSize();
    }

    private int getInsertBatchSize()
    {
        return this.insertBatch == null ? 0 : this.insertBatch.getTotalOperationsSize();
    }

    private int getDeleteBatchSize()
    {
        return this.deleteBatch == null ? 0 : this.deleteBatch.getTotalOperationsSize();
    }

    @Override
    public void overlapProcessingFinished(Object connectionManager, String mithraClassName)
    {
        if (this.getPendingOperationCount() > 0)
        {
            this.executePendingOperations();
        }
        long time = System.currentTimeMillis() - this.startTime;
        LOGGER.info("Fixed overlaps for " + mithraClassName + " in " + time + "ms (deletes=" + this.totalDeleteCount + ", inserts=" + this.totalInsertCount + ')');
    }

    private void executePendingOperations()
    {
        int deleteCount = this.getDeleteBatchSize();
        int insertCount = this.getInsertBatchSize();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                LOGGER.info("Processing batch (deletes=" + getDeleteBatchSize() + ", inserts=" + getInsertBatchSize() + ')');
                if (deleteBatch != null)
                {
                    deleteBatch.execute();
                }
                if (insertBatch != null)
                {
                    insertBatch.execute();
                }
                return null;
            }
        });
        this.totalDeleteCount += deleteCount;
        this.totalInsertCount += insertCount;
        this.deleteBatch = null;
        this.insertBatch = null;
    }

    @Override
    public void overlapsDetected(Object connectionManager, List<MithraDataObject> overlaps, String mithraClassName)
    {
        RelatedFinder finder = overlaps.get(0).zGetMithraObjectPortal().getFinder();
        for (MithraDataObject toDelete : overlaps)
        {
            this.addDelete(finder, toDelete);
        }

        Collections.sort(overlaps, getOrderOfPrecedence(finder));
        List<MilestoneRectangle> mergedData = MilestoneRectangle.merge(MilestoneRectangle.fromMithraData(finder, overlaps));
        for (MilestoneRectangle rect : mergedData)
        {
            this.addInsert(finder, rect.getMithraDataCopyWithNewMilestones(finder));
        }

        if (this.getPendingOperationCount() >= this.batchSize)
        {
            this.executePendingOperations();
        }
    }

    private void addDelete(RelatedFinder finder, MithraDataObject mithraDatedObject)
    {
        MithraObjectPortal portal = finder.getMithraObjectPortal();
        MithraTransactionalObject transactionalObject = new InTransactionDatedTransactionalObject(portal, null, mithraDatedObject, InTransactionDatedTransactionalObject.DELETED_STATE);
        if (this.deleteBatch == null)
        {
            this.deleteBatch = new BatchDeleteOperation(transactionalObject, Collections.EMPTY_LIST, portal, true);
        }
        else
        {
            this.deleteBatch.combineDelete(transactionalObject, portal);
        }
    }

    private void addInsert(RelatedFinder finder, MithraDataObject newData)
    {
        MithraObjectPortal portal = finder.getMithraObjectPortal();
        MithraTransactionalObject transactionalObject = new InTransactionDatedTransactionalObject(portal, null, newData, InTransactionDatedTransactionalObject.INSERTED_STATE);
        if (this.insertBatch == null)
        {
            this.insertBatch = new BatchInsertOperation(transactionalObject, Collections.EMPTY_LIST, portal);
        }
        else
        {
            this.insertBatch.combineInsert(transactionalObject, portal);
        }
    }

    private static OrderBy getOrderOfPrecedence(RelatedFinder finder)
    {
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        OrderBy orderBy = null;
        Set<String> asOfFromToAttributes = UnifiedSet.newSet(asOfAttributes.length * 2);
        for (int i = asOfAttributes.length - 1; i >= 0; i--)
        {
            OrderBy descendingFrom = asOfAttributes[i].getFromAttribute().descendingOrderBy();
            orderBy = orderBy == null ? descendingFrom : orderBy.and(descendingFrom);
            orderBy = orderBy.and(asOfAttributes[i].getToAttribute().ascendingOrderBy());

            asOfFromToAttributes.add(asOfAttributes[i].getFromAttribute().getAttributeName());
            asOfFromToAttributes.add(asOfAttributes[i].getToAttribute().getAttributeName());
        }

        for (Attribute attr : finder.getPersistentAttributes())
        {
            if (!asOfFromToAttributes.contains(attr.getAttributeName()))
            {
                OrderBy asc = attr.ascendingOrderBy();
                orderBy = orderBy == null ? asc : orderBy.and(asc);
            }
        }

        return orderBy;
    }
}
