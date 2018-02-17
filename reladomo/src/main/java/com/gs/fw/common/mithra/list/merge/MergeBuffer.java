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

package com.gs.fw.common.mithra.list.merge;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.finder.OrderBy;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class MergeBuffer<E> // todo: implements QueueExecutor or create a super interface for both
{
    private static final Comparator CANONICAL_COMPARATOR = new CanonicalComparator();

    private MergeOptions<E> mergeOptions;
    private boolean topLevel;
    private List<MergeBuffer> children;
    private FullUniqueIndex<E> incomingIndex;
    private FullUniqueIndex<E> dbIndex;
    private Comparator<E> comparator;
    private List<E> toTerminate;
    private List<E> toInsert;
    private List<E> toUpdateDbSide;
    private List<E> toUpdateIncomngSide;
    private Attribute[] toUpdate;
    private boolean detached;
    private Extractor[] toMatchOn;

    public MergeBuffer(TopLevelMergeOptions<E> mergeOptions)
    {
        this(mergeOptions, false);
    }

    public MergeBuffer(TopLevelMergeOptions<E> mergeOptions, boolean detached)
    {
        this.detached = detached;
        this.mergeOptions = mergeOptions;
        topLevel = true;
        MergeOptionNode topNode = mergeOptions.getTopNode();
        List<MergeOptionNode> children = topNode.getChildren();
        wireUpChildren(children);
    }

    public MergeBuffer(MergeOptionNode node, boolean detached)
    {
        this.mergeOptions = node.getNodeMergeOption();
        this.detached = detached;
        wireUpChildren(node.getChildren());
    }

    private void wireUpChildren(List<MergeOptionNode> children)
    {
        if (children != null)
        {
            this.children = FastList.newList(children.size());
            for(int i=0;i<children.size();i++)
            {
                MergeOptionNode mergeOptionNode = children.get(i);
                this.children.add(new MergeBuffer(mergeOptionNode, this.detached));
            }
        }
    }

    public FullUniqueIndex<E> getIncomingIndex(int size)
    {
        if (incomingIndex == null)
        {
            incomingIndex = this.createFullUniqueIndex(size);
        }
        else
        {
            incomingIndex.clear();
        }
        return incomingIndex;
    }

    public FullUniqueIndex<E> getDbIndex(int size)
    {
        if (dbIndex == null)
        {
            dbIndex = this.createFullUniqueIndex(size);
        }
        else
        {
            dbIndex.clear();
        }
        return dbIndex;
    }

    public Comparator<E> getCompartor()
    {
        if (comparator == null)
        {
            Extractor[] toMatchOn = this.resolveToMatchOn();
            Attribute[] doNotCompare = mergeOptions.getDoNotCompare();
            Attribute[] persistentAttributes = mergeOptions.getMetaData().getPersistentAttributes();
            Attribute[] primaryKeyAttributes = mergeOptions.getMetaData().getPrimaryKeyAttributes();

            if (Arrays.equals(toMatchOn ,primaryKeyAttributes) && doNotCompare == null && this.topLevel)
            {
                comparator = CANONICAL_COMPARATOR;
            }
            else
            {
                UnifiedSet<Attribute> attributesToCheck = new UnifiedSet<Attribute>(persistentAttributes.length);
                for(Attribute a: persistentAttributes)
                {
                    attributesToCheck.add(a);
                }
                for (Extractor e : toMatchOn)
                {
                    attributesToCheck.remove(e);
                }
                if (doNotCompare != null)
                {
                    for(Attribute a: doNotCompare)
                    {
                        attributesToCheck.remove(a);
                    }
                }
                for(Attribute a: this.getForeignKeys())
                {
                    attributesToCheck.remove(a);
                }
                comparator = new ChainedAttributeComparator(FastList.newList(attributesToCheck));
            }
        }

        return comparator;
    }
    
    public void mergeLists(List<E> dbList, List<E> incoming)
    {
        mergeLists(dbList, incoming, null);
    }

    public void mergeLists(List<E> dbList, List<E> incoming, Object owner)
    {
        FullUniqueIndex<E> incomingIndex = this.getIncomingIndex(incoming.size());
        incomingIndex.addAll(incoming);
        if (mergeOptions.getInputDuplicateHandling() == MergeOptions.DuplicateHandling.THROW_ON_DUPLICATE && incomingIndex.size() < incoming.size())
        {
            throw new MithraBusinessException("the incoming list of "+incoming.get(0).getClass().getName()+" had duplicates based on the provided merge keys!");
        }
        if (mergeOptions.getDbDuplicateHandling() == MergeOptions.DuplicateHandling.THROW_ON_DUPLICATE)
        {
            FullUniqueIndex<E> index = this.getDbIndex(dbList.size());
            index.addAll(dbList);
            if (index.size() != dbList.size())
            {
                throw new MithraBusinessException("the database list of "+dbList.get(0).getClass().getName()+" had duplicates based on the provided merge keys!");
            }
        }
        Comparator<E> comparator = this.getCompartor();
        for(int i=0;i<dbList.size();i++)
        {
            E e = dbList.get(i);
            E match = incomingIndex.remove(e);
            if (match != null)
            {
                considerUpdate(comparator, e, match, dbList, null);
            }
            else
            {
                considerTermination(e, dbList, null);
            }
        }
        if (!incomingIndex.isEmpty())
        {
            List<E> toInsert = incomingIndex.getAll();
            if (owner != null)
            {
                MithraObject nonPersistentCopy = ((MithraObject) owner).getNonPersistentCopy();
                MithraList mithraList = this.mergeOptions.getMetaData().getFinderInstance().constructEmptyList();
                mithraList.addAll(toInsert);
                setValueOnRelationship(nonPersistentCopy, mithraList); // this fixes these dependent's foreign keys
            }

            for(int i=0;i<toInsert.size();i++)
            {
                E e = toInsert.get(i);
                considerInsert(e, dbList, null, false);
            }
        }

    }

    private void considerUpdate(Comparator<E> comparator, E e, E match, List<E> ownerList, Object ownerObject)
    {
        boolean terminateInsertInstead = false;
        if (comparator.compare(e, match) == 0)
        {
            MergeHook.UpdateInstruction updateInstruction = mergeOptions.getMergeHook().matchedNoDifference(e, match);
            if (updateInstruction == MergeHook.UpdateInstruction.UPDATE)
            {
                this.addForUpdate(e, match);
            }
            else if (updateInstruction == MergeHook.UpdateInstruction.TERMINATE_AND_INSERT_INSTEAD)
            {
                this.addForTermination(e, ownerList, ownerObject);
                this.addForInsert(match, ownerList, ownerObject);
                terminateInsertInstead = true;
            }
        }
        else
        {
            MergeHook.UpdateInstruction updateInstruction = mergeOptions.getMergeHook().matchedWithDifferenceBeforeAttributeCopy(e, match);
            if (updateInstruction == MergeHook.UpdateInstruction.UPDATE)
            {
                this.addForUpdate(e, match);
            }
            else if (updateInstruction == MergeHook.UpdateInstruction.TERMINATE_AND_INSERT_INSTEAD)
            {
                this.addForTermination(e, ownerList, ownerObject);
                this.addForInsert(match, ownerList, ownerObject);
                terminateInsertInstead = true;
            }
        }
        if (terminateInsertInstead)
        {
            navigateChildrenForTerminate(e);
            navigateChildrenForInsert(match);
        }
        else
        {
            if (this.children != null)
            {
                for (int j = 0; j < children.size(); j++)
                {
                    MergeBuffer childMergeBuffer = this.children.get(j);
                    childMergeBuffer.navigateThenMerge(e, match);
                }
            }
        }
    }

    private void navigateThenMerge(Object db, Object incoming)
    {
        AbstractRelatedFinder abstractRelatedFinder = ((NavigatedMergeOption<E>) mergeOptions).getRelatedFinder().zWithoutParentSelector();
        Object dbNav = abstractRelatedFinder.valueOf(db);
        Object incomingNav = abstractRelatedFinder.valueOf(incoming);
        if (abstractRelatedFinder.isToOne())
        {
            if (dbNav != null)
            {
                if (incomingNav == null)
                {
                    considerTermination((E) dbNav, null, db);
                }
                else
                {
                    considerUpdate(this.getCompartor(), (E) dbNav, (E) incomingNav, null, db);
                }
            }
            else if (incomingNav != null)
            {
                considerInsert((E) incomingNav, null, db, true);
            }
        }
        else
        {
            mergeLists((List<E>)dbNav, (List<E>) incomingNav, db);
        }
    }

    private void considerInsert(E e, List<E> ownerList, Object ownerObject, boolean fixForeignKeys)
    {
        if (mergeOptions.getMergeHook().beforeInsertOfNew(e) == MergeHook.InsertInstruction.INSERT)
        {
            if (fixForeignKeys)
            {
                MithraObject nonPersistentCopy = ((MithraObject) ownerObject).getNonPersistentCopy();
                setValueOnRelationship(nonPersistentCopy, e); // fix the FK
            }
            this.addForInsert(e, ownerList, ownerObject);
            if (!detached)
            {
                navigateChildrenForInsert(e);
            }
        }
    }

    private void navigateChildrenForInsert(E e)
    {
        if (this.children != null)
        {
            for(int j=0;j<children.size();j++)
            {
                MergeBuffer childMergeBuffer = this.children.get(j);
                childMergeBuffer.navigateThenInsert(e);
            }
        }
    }

    private void navigateThenInsert(Object e)
    {
        AbstractRelatedFinder abstractRelatedFinder = ((NavigatedMergeOption<E>) mergeOptions).getRelatedFinder().zWithoutParentSelector();
        Object o = abstractRelatedFinder.valueOf(e);
        if (abstractRelatedFinder.isToOne())
        {
            if (o != null)
            {
                considerInsert((E) o, null, e, false);
            }
        }
        else
        {
            List<E> list = (List<E>) o;
            for(int i=0;i<list.size();i++)
            {
                considerInsert(list.get(i), list, null, false);
            }
        }
    }

    private void considerTermination(E e, List<E> ownerList, Object ownerObject)
    {
        if (mergeOptions.getMergeHook().beforeDeleteOrTerminate(e, this) == MergeHook.DeleteOrTerminateInstruction.DELETE_OR_TERMINATE)
        {
            this.addForTermination(e, ownerList, ownerObject);
            if (!detached)
            {
                navigateChildrenForTerminate(e);
            }
        }
    }

    private void navigateChildrenForTerminate(Object e)
    {
        if (this.children != null)
        {
            for(int j=0;j<children.size();j++)
            {
                MergeBuffer childMergeBuffer = this.children.get(j);
                childMergeBuffer.navigateThenTerminate(e);
            }
        }
    }

    private void navigateThenTerminate(Object e)
    {
        AbstractRelatedFinder abstractRelatedFinder = ((NavigatedMergeOption<E>) mergeOptions).getRelatedFinder().zWithoutParentSelector();
        Object o = abstractRelatedFinder.valueOf(e);
        if (abstractRelatedFinder.isToOne())
        {
            if (o != null)
            {
                considerTermination((E) o, null, e);
            }
        }
        else
        {
            List<E> list = (List<E>) o;
            for(int i=0;i<list.size();i++)
            {
                considerTermination(list.get(i), list, null);
            }
        }
    }

    public void addForTermination(E o, List<E> ownerList, Object ownerObject)
    {
        if (this.detached)
        {
            if (ownerList != null)
            {
                ownerList.remove(o);
            }
            else
            {
                setValueOnRelationship(ownerObject, null);
            }
        }
        else
        {
            this.toTerminate = addToList(o, this.toTerminate);
        }
    }

    private List<E> addToList(E o, List<E> list)
    {
        if (list == null)
        {
            list = FastList.newList();
        }
        list.add(o);
        return list;
    }

    public void addForInsert(E o, List<E> ownerList, Object ownerObject)
    {
        if (this.detached)
        {
            if (ownerList != null)
            {
                ownerList.add(o);
            }
            else
            {
                setValueOnRelationship(ownerObject, o);
            }
        }
        else
        {
            this.toInsert = addToList(o, this.toInsert);
        }
    }

    private void setValueOnRelationship(Object ownerObject, Object o)
    {
        AbstractRelatedFinder relatedFinder = ((NavigatedMergeOption) this.mergeOptions).getRelatedFinder();
        Method relationshipSetter = ReladomoClassMetaData.fromObjectInstance((MithraObject)ownerObject).getRelationshipSetter(relatedFinder.getRelationshipName());

        try
        {
            relationshipSetter.invoke(ownerObject, o);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not set relationship "+relatedFinder.getRelationshipName()+" on object "+((MithraObject)ownerObject).zGetCurrentData().zGetPrintablePrimaryKey(), e);
        }
    }

    public void addForUpdate(E dbObject, E incomingObject)
    {
        if (this.detached)
        {
            copyAttributes(dbObject, incomingObject);
        }
        else
        {
            this.toUpdateDbSide = addToList(dbObject, this.toUpdateDbSide);
            this.toUpdateIncomngSide = addToList(incomingObject, this.toUpdateIncomngSide);
        }
    }

    public void executeBufferForPersistence()
    {
        executeTerminationBottomUp();
        executeUpdateTopDown();
        executeInsertTopDown();
    }

    private void executeInsertTopDown()
    {
        if (this.toInsert != null)
        {
            for(int i=0;i<this.toInsert.size();i++)
            {
                E e = toInsert.get(i);
                ((MithraTransactionalObject)e).insert();
            }
        }
        if (children != null)
        {
            for (int i=0;i<children.size();i++)
            {
                children.get(i).executeInsertTopDown();
            }
        }
    }

    private void executeUpdateTopDown()
    {
        if (this.toUpdateDbSide != null)
        {
            for(int i=0;i<this.toUpdateDbSide.size();i++)
            {
                E db = toUpdateDbSide.get(i);
                E incoming = toUpdateIncomngSide.get(i);
                copyAttributes(db, incoming);
            }
        }
        if (children != null)
        {
            for (int i=0;i<children.size();i++)
            {
                children.get(i).executeUpdateTopDown();
            }
        }
    }

    private void copyAttributes(E db, E incoming)
    {
        Attribute[] toUpdate = this.getAttributesToUpdate();
        for(Attribute a: toUpdate)
        {
            a.copyValueFrom(db, incoming);
        }
    }

    private Attribute[] getAttributesToUpdate()
    {
        if (this.toUpdate == null)
        {
            Extractor[] toMatchOn = this.resolveToMatchOn();
            Attribute[] doNotUpdate = mergeOptions.getDoNotUpdate();
            Attribute[] persistentAttributes = mergeOptions.getMetaData().getPersistentAttributes();
            Attribute[] primaryKeyAttributes = mergeOptions.getMetaData().getPrimaryKeyAttributes();
            AsOfAttribute[] asOfAttributes = mergeOptions.getMetaData().getAsOfAttributes();

            Set<Attribute> foreignKeys = getForeignKeys();

            UnifiedSet<Attribute> attributesToUpdate = new UnifiedSet<Attribute>(persistentAttributes.length);
            for(Attribute a: persistentAttributes)
            {
                attributesToUpdate.add(a);
            }
            for(Attribute a: primaryKeyAttributes)
            {
                if (!a.hasShadowAttriute())
                {
                    attributesToUpdate.remove(a);
                }
            }
            for(Extractor e: toMatchOn)
            {
                attributesToUpdate.remove(e);
            }
            if (doNotUpdate != null)
            {
                for(Attribute a: doNotUpdate)
                {
                    attributesToUpdate.remove(a);
                }
            }
            for(Attribute a: foreignKeys)
            {
                attributesToUpdate.remove(a);
            }
            if (asOfAttributes != null)
            {
                for(AsOfAttribute asOfAttribute: asOfAttributes)
                {
                    attributesToUpdate.remove(asOfAttribute.getFromAttribute());
                    attributesToUpdate.remove(asOfAttribute.getToAttribute());
                }
            }
            this.toUpdate = new Attribute[attributesToUpdate.size()];
            attributesToUpdate.toArray(this.toUpdate);

        }
        return this.toUpdate;
    }

    private void executeTerminationBottomUp()
    {
        if (children != null)
        {
            for (int i=0;i<children.size();i++)
            {
                children.get(i).executeTerminationBottomUp();
            }
        }
        if (this.toTerminate != null)
        {
            for(int i=0;i<this.toTerminate.size();i++)
            {
                E e = toTerminate.get(i);
                if (mergeOptions.getMetaData().isDated())
                {
                    ((MithraDatedTransactionalObject) e).terminate();
                }
                else
                {
                    ((MithraTransactionalObject)e).delete();
                }
            }
        }
    }

    public FullUniqueIndex<E> createFullUniqueIndex(int capacity)
    {
        Extractor[] toMatchOn = resolveToMatchOn();
        return new FullUniqueIndex<E>(toMatchOn, capacity);
    }

    protected Extractor[] resolveToMatchOn()
    {
        if (this.toMatchOn == null)
        {
            Set matchAttr = UnifiedSet.newSet();
            if (this.mergeOptions.getToMatchOn() != null)
            {
                for(Extractor a: this.mergeOptions.getToMatchOn())
                {
                    matchAttr.add(a);
                }
            }
            else
            {
                for(Extractor a: this.mergeOptions.getMetaData().getPrimaryKeyAttributes())
                {
                    matchAttr.add(a);
                }
            }
            Set<Attribute> foreignKeys = getForeignKeys();
            for(Attribute a: foreignKeys)
            {
                matchAttr.remove(a);
            }

            Extractor[] x = new Extractor[matchAttr.size()];
            matchAttr.toArray(x);
            this.toMatchOn = x;
        }
        return this.toMatchOn;
    }

    private Set<Attribute> getForeignKeys()
    {
        Set<Attribute> foreignKeys = UnifiedSet.newSet();
        if (!topLevel)
        {
            ((NavigatedMergeOption)mergeOptions).getRelatedFinder().zWithoutParentSelector().zGetMapper().addDepenedentAttributesToSet(foreignKeys);
        }
        return foreignKeys;
    }

    private static class CanonicalComparator<X extends MithraTransactionalObject> implements Comparator<X>
    {
        @Override
        public int compare(X o1, X o2)
        {
            if (o1.nonPrimaryKeyAttributesChanged(o2))
            {
                return 1;
            }
            return 0;
        }
    }

    private static class ChainedAttributeComparator<X extends MithraTransactionalObject> implements Comparator<X>
    {
        private OrderBy[] toCompare;

        public ChainedAttributeComparator(List<Attribute> attributes)
        {
            toCompare = new OrderBy[attributes.size()];
            for(int i=0;i<attributes.size();i++)
            {
                toCompare[i] = attributes.get(i).ascendingOrderBy();
            }
        }

        @Override
        public int compare(X o1, X o2)
        {
            for(OrderBy a: toCompare)
            {
                int result = a.compare(o1, o2);
                if (result != 0) return result;
            }
            return 0;
        }
    }
}

