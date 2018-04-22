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

import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.TransactionalUnderlyingObjectGetter;
import com.gs.fw.common.mithra.cache.UnderlyingObjectGetter;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IdentityExtractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.reladomo.metadata.ReladomoClassMetaData;

import java.util.List;



public abstract class TransactionOperation
{

    private MithraTransactionalObject mithraObject;
    private MithraObjectPortal portal;

    public static final int COMBINE_DIRECTION_NONE = 0;
    public static final int COMBINE_DIRECTION_FORWARD = 1;
    public static final int COMBINE_DIRECTION_BACKWARD = 2;

    public TransactionOperation(MithraTransactionalObject mithraObject, MithraObjectPortal portal)
    {
        this.mithraObject = mithraObject;
        this.portal = portal;
    }

    public MithraTransactionalObject getMithraObject()
    {
        return mithraObject;
    }

    public abstract void execute() throws MithraDatabaseException;

    public TransactionOperation combinePurge(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        return null;
    }

    public TransactionOperation combineInsert(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        return null;
    }

    public TransactionOperation combineDelete(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        return null;
    }

    public TransactionOperation combineUpdate(TransactionOperation op)
    {
        return null;
    }

    public boolean isInsert()
    {
        return false;
    }

    public boolean isDelete()
    {
        return false;
    }

    public boolean isUpdate()
    {
        return false;
    }

    public boolean isMultiUpdate()
    {
        return false;
    }

    public boolean isBatchUpdate()
    {
        return false;
    }

    public TransactionOperation combine(TransactionOperation op)
    {
        if (op.isInsert())
        {
            return combineInsertOperation(op);
        }
        else if (op.isDelete())
        {
            return combineDeleteOperation(op);
        }
        else if (op.isUpdate())
        {
            return combineUpdate(op);
        }
        else if (op.isMultiUpdate())
        {
            return combineMultiUpdate(op);
        }
        else if (op.isBatchUpdate())
        {
            return combineBatchUpdate(op);
        }
        return null;
    }

    public TransactionOperation combineBatchUpdate(TransactionOperation op)
    {
        return null;
    }

    public TransactionOperation combineMultiUpdate(TransactionOperation op)
    {
        return null;
    }

    public TransactionOperation combineInsertOperation(TransactionOperation op)
    {
        return null;
    }

    public TransactionOperation combineDeleteOperation(TransactionOperation op)
    {
        return null;
    }

    public boolean isCombinableWithInsertOperation()
    {
        return true;
    }

    public List getAllObjects()
    {
        return ListFactory.create(this.mithraObject);
    }

    protected MithraObjectPortal getPortal()
    {
        return this.portal;
    }

    public int getPassThroughDirection(TransactionOperation next)
    {
        if (next == DoNothingTransactionOperation.getInstance())
        {
            return COMBINE_DIRECTION_BACKWARD | COMBINE_DIRECTION_FORWARD;
        }
        if (!next.isCombinableWithInsertOperation())
        {
            return COMBINE_DIRECTION_NONE;
        }
        if (this.getPortal() == next.getPortal())
        {
            if (this.touchesSameObject(next))
            {
                if (this.getPortal().getFinder().getAsOfAttributes() != null)
                {
                    return getPassThroughForSameDated(next);
                }
                return COMBINE_DIRECTION_NONE;
            }
            else return getCombineDirectionWithDependencyCheck(next);
        }
        else
        {
            return getCombineDirectionWithDependencyCheck(next);
        }
    }

    protected int getPassThroughForSameDated(TransactionOperation next)
    {
        if (this.isInsert() && next.isAnyUpdate() && next.isAsOfAttributeToOnlyUpdate())
        {
            return COMBINE_DIRECTION_FORWARD;
        }
        if (next.isInsert() && this.isAnyUpdate() && this.isAsOfAttributeToOnlyUpdate())
        {
            return COMBINE_DIRECTION_BACKWARD;
        }
        return COMBINE_DIRECTION_NONE;
    }

    protected boolean isAsOfAttributeToOnlyUpdate()
    {
        return false;
    }

    protected boolean isAnyUpdate()
    {
        return this.isUpdate() || this.isMultiUpdate() || this.isBatchUpdate();
    }

    private int getCombineDirectionWithDependencyCheck(TransactionOperation next)
    {
        MithraObjectPortal thisPortal = this.getPortal();
        MithraObjectPortal nextPortal = next.getPortal();
        if (thisPortal.isParentFinder(nextPortal.getFinder()))
        {
            return getCombineDirectionForParent();
        }
        else if (nextPortal.isParentFinder(thisPortal.getFinder()))
        {
            return getCombineDirectionForChild();
        }
        return COMBINE_DIRECTION_FORWARD | COMBINE_DIRECTION_BACKWARD;
    }

    protected FullUniqueIndex getIndexedObjects()
    {
        return null;
    }

    protected abstract int getCombineDirectionForParent();
    protected abstract int getCombineDirectionForChild();

    private boolean touchesSameObject(TransactionOperation otherOp)
    {
        int otherSize = otherOp.getAllObjects().size();
        int localSize = this.getAllObjects().size();
        if (otherSize == 1 && localSize == 1)
        {
            return this.getMithraObject().zIsSameObjectWithoutAsOfAttributes(otherOp.getMithraObject());
        }
        if (localSize <= 8 || otherSize <= 8)
        {
            return loopCompare(this.getAllObjects(), otherOp.getAllObjects());
        }
        return touchesSameObjectUsingIndex(otherOp);
    }

    private boolean touchesSameObjectUsingIndex(TransactionOperation otherOp)
    {
        int otherSize = otherOp.getAllObjects().size();
        int localSize = this.getAllObjects().size();
        TransactionOperation biggerOp;
        TransactionOperation smallerOp;
        if(localSize > otherSize)
        {
            biggerOp = this;
            smallerOp = otherOp;
        }
        else
        {
            biggerOp = otherOp;
            smallerOp = this;
        }
        RelatedFinder finder = this.getPortal().getFinder();
        if (finder.getAsOfAttributes() == null)
        {
            return indexCompare(smallerOp.getIndexedObjects(), biggerOp.getAllObjects());
        }
        //for temporal objects can't use the unique index with as-of attributes
        Extractor[] extractors = finder.getPrimaryKeyAttributes();
        List objectsFromSmallerOp = smallerOp.getAllObjects();
        FullUniqueIndex index = new FullUniqueIndex(extractors, objectsFromSmallerOp.size());
        index.setUnderlyingObjectGetter(new TransactionalUnderlyingObjectGetter());
        for (int i =0 ; i < objectsFromSmallerOp.size(); i++)
        {
            index.put(objectsFromSmallerOp.get(i));
        }
        return indexCompare(index, biggerOp.getAllObjects());
    }

    public int getTotalOperationsSize()
    {
        return 1;
    }

    protected FullUniqueIndex createFullUniqueIndex(List objects)
    {
        RelatedFinder finder = this.getPortal().getFinder();
        Extractor[] extractors = IdentityExtractor.getArrayInstance();
        UnderlyingObjectGetter getter = null;
        if (finder.getAsOfAttributes() != null)
        {
            extractors = getObjectSamenessKeyAttributes(finder);
            getter = new TransactionalUnderlyingObjectGetter();
        }
        FullUniqueIndex index = new FullUniqueIndex(extractors, objects.size());
        if (getter != null)
        {
            index.setUnderlyingObjectGetter(getter);
        }
        for(int i=0;i<objects.size();i++)
        {
            index.put(objects.get(i));
        }
        return index;
    }

    private Extractor[] getObjectSamenessKeyAttributes(RelatedFinder finder) {
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        Extractor nonProcessingDateAttribute = null;
        if(asOfAttributes != null)
        {
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                if(!asOfAttributes[i].isProcessingDate())
                {
                    nonProcessingDateAttribute = asOfAttributes[i].getFromAttribute();
                    break;
                }
            }
        }
        Extractor[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        if(nonProcessingDateAttribute == null)
        {
            return primaryKeyAttributes;
        }
        else
        {
            Extractor[] fullKey = new Extractor[primaryKeyAttributes.length + 1];
            System.arraycopy(primaryKeyAttributes, 0, fullKey, 0, primaryKeyAttributes.length);
            fullKey[primaryKeyAttributes.length] = nonProcessingDateAttribute;
            return fullKey;
        }
    }


    private boolean indexCompare(FullUniqueIndex index, List large)
    {
        for(int i=0;i<large.size();i++)
        {
            if (index.contains(large.get(i))) return true;
        }
        return false;
    }

    private boolean loopCompare(List allObjects, List localObjects)
    {
        List small = allObjects;
        List large = localObjects;
        if (allObjects.size() > localObjects.size())
        {
            small = localObjects;
            large = allObjects;
        }
        for(int i=0;i<large.size();i++)
        {
            MithraTransactionalObject localObject = (MithraTransactionalObject) large.get(i);
            for(int j=0;j<small.size();j++)
            {
                MithraTransactionalObject other = (MithraTransactionalObject) small.get(j);
                if (localObject.zIsSameObjectWithoutAsOfAttributes(other)) return true;
            }
        }
        return false;
    }
}
