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

import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.impl.block.factory.Predicates;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.BigDecimalIncrementUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.DoubleIncrementUpdateWrapper;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.MithraFastList;

import java.util.List;



public class BatchUpdateOperation extends TransactionOperation
{

    private final MithraFastList<UpdateOperation> updateOperations;
    private final List mithraObjects;
    private transient FullUniqueIndex index;

    public BatchUpdateOperation(UpdateOperation first, UpdateOperation second)
    {
        super(first.getMithraObject(), first.getPortal());
        updateOperations = new MithraFastList<UpdateOperation>();
        mithraObjects = new FastList();
        addOperation(first);
        addOperation(second);
    }

    public BatchUpdateOperation(List<UpdateOperation> updateOperations)
    {
        super((updateOperations.get(0)).getMithraObject(), (updateOperations.get(0)).getPortal());
        this.updateOperations = new MithraFastList(updateOperations);
        this.mithraObjects = new FastList();

        for (int i = 0; i < updateOperations.size(); i++)
        {
            this.mithraObjects.add(updateOperations.get(i));
        }
    }

    private void addOperation(UpdateOperation updateOperation)
    {
        updateOperations.add(updateOperation);
        mithraObjects.add(updateOperation.getMithraObject());
        if (index != null)
        {
            index.put(updateOperation.getMithraObject());
        }
    }

    @Override
    protected FullUniqueIndex getIndexedObjects()
    {
        if (index == null)
        {
            index = createFullUniqueIndex(mithraObjects);
        }
        return index;
    }

    @Override
    public int getTotalOperationsSize()
    {
        return this.updateOperations.size();
    }

    @Override
    public boolean isBatchUpdate()
    {
        return true;
    }

    @Override
    protected boolean isAsOfAttributeToOnlyUpdate()
    {
        for(int i=0;i<updateOperations.size();i++)
        {
            if (!updateOperations.get(i).isAsOfAttributeToOnlyUpdate())
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void execute() throws MithraDatabaseException
    {
        this.getPortal().getMithraObjectPersister().batchUpdate(this);
    }

    public void setUpdated()
    {
        for(int i=0;i < updateOperations.size(); i++)
        {
            UpdateOperation updateOperation = this.updateOperations.get(i);
            updateOperation.setUpdated();
        }
    }

    public List<UpdateOperation> getUpdateOperations()
    {
        if (isIncrement())
        {
            return this.updateOperations;
        }
        MithraFastList<Attribute> allAttribute = computeKeyAttributes();
        Extractor[] keyExtractors = new Extractor[allAttribute.size()];
        allAttribute.toArray(keyExtractors);
        FullUniqueIndex updatedObjects = new FullUniqueIndex(keyExtractors, this.updateOperations.size());
        for(int i=updateOperations.size() - 1; i >= 0; i--)
        {
            UpdateOperation updateOperation = updateOperations.get(i);
            if (updatedObjects.put(getDataWithKey(updateOperation)) != null)
            {
                updateOperations.set(i, null);
            }
        }
        if (updatedObjects.size() != updateOperations.size())
        {
            updateOperations.removeNullItems();
        }
        return updateOperations;
    }

    public boolean isIncrement()
    {
        List<AttributeUpdateWrapper> updates = updateOperations.get(0).getUpdates();
        for(int i=0;i<updates.size();i++)
        {
            AttributeUpdateWrapper attributeUpdateWrapper = updates.get(i);
            if (attributeUpdateWrapper instanceof DoubleIncrementUpdateWrapper || attributeUpdateWrapper instanceof BigDecimalIncrementUpdateWrapper)
            {
                return true;
            }
        }
        return false;
    }

    public MithraDataObject getDataWithKey(UpdateOperation updateOperation)
    {
        MithraTransactionalObject mithraObject = updateOperation.getMithraObject();
        MithraDataObject keyHolder = mithraObject.zGetNonTxData();
        if (keyHolder == null)
        {
            keyHolder = mithraObject.zGetTxDataForRead();
        }

        return keyHolder;
    }

    public MithraFastList<Attribute> computeKeyAttributes()
    {
        RelatedFinder finder = this.getPortal().getFinder();
        Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        MithraFastList<Attribute> allAttribute = new MithraFastList<Attribute>(primaryKeyAttributes.length + 3);
        for(Attribute a: primaryKeyAttributes)
        {
            allAttribute.add(a.zGetShadowAttribute());
        }
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        if (asOfAttributes != null)
        {
            for (AsOfAttribute a : asOfAttributes) allAttribute.add(a.getToAttribute());
        }
        if (this.getPortal().getTxParticipationMode().isOptimisticLocking())
        {
            Attribute versionAttribute = (Attribute) finder.getVersionAttribute();
            if (versionAttribute != null)
            {
                allAttribute.add(versionAttribute.zGetShadowAttribute());
            }
            if (asOfAttributes != null)
            {
                Attribute optimisticProcessing = this.getOptimisticKey(asOfAttributes);
                if (optimisticProcessing != null)
                {
                    allAttribute.add(optimisticProcessing);
                }
            }
        }
        return allAttribute;
    }

    private Attribute getOptimisticKey(AsOfAttribute[] asOfAttributes)
    {
        AsOfAttribute businessDate = null;
        AsOfAttribute processingDate = null;
        if (asOfAttributes.length == 2)
        {
            businessDate = asOfAttributes[0];
            processingDate = asOfAttributes[1];
        }
        else if (asOfAttributes[0].isProcessingDate())
        {
            processingDate = asOfAttributes[0];
        }
        if (processingDate != null)
        {
            return processingDate.getFromAttribute();
        }
        return null;
    }

    @Override
    public List getAllObjects()
    {
        return this.mithraObjects;
    }

    @Override
    public TransactionOperation combineUpdate(TransactionOperation op)
    {
        if (op instanceof UpdateOperation)
        {
            UpdateOperation first = this.updateOperations.get(0);
            UpdateOperation incoming = (UpdateOperation) op;
            if (first.canBeBatched(incoming) )
            {
                this.addOperation(incoming);
                return this;
            }
        }
        return null;
    }

    @Override
    public TransactionOperation combineBatchUpdate(TransactionOperation op)
    {
        if (op.getPortal() == this.getPortal())
        {
            UpdateOperation first = this.updateOperations.get(0);
            BatchUpdateOperation incoming = (BatchUpdateOperation) op;
            UpdateOperation firstIncoming = incoming.updateOperations.get(0);
            if (first.canBeBatched(firstIncoming))
            {
                this.updateOperations.addAll(incoming.updateOperations);
                this.mithraObjects.addAll(incoming.mithraObjects);
                if (index != null) index.addAll(incoming.mithraObjects);
                return this;
            }
        }
        return null;
    }

    @Override
    protected int getCombineDirectionForParent()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    @Override
    protected int getCombineDirectionForChild()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    public boolean isEligibleForUpdateViaJoin()
    {
        return this.getAllObjects().size() == this.getIndexedObjects().size();
    }
}
