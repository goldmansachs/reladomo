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

package com.gs.fw.common.mithra.list.merge;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.finder.Navigation;
import com.gs.reladomo.metadata.ReladomoClassMetaData;

import java.util.List;

public class TopLevelMergeOptions<E> extends MergeOptions<E>
{
    private MergeOptionNode topNode;

    public TopLevelMergeOptions(RelatedFinder finder)
    {
        super(ReladomoClassMetaData.fromFinder(finder));
        this.topNode = new MergeOptionNode(this);
    }

    @Override
    public TopLevelMergeOptions<E> doNotCompare(Attribute... attributes)
    {
        return (TopLevelMergeOptions<E>) super.doNotCompare(attributes);
    }

    @Override
    public TopLevelMergeOptions<E> doNotUpdate(Attribute... attributes)
    {
        return (TopLevelMergeOptions<E>) super.doNotUpdate(attributes);
    }

    @Override
    public TopLevelMergeOptions<E> doNotCompareOrUpdate(Attribute... attributes)
    {
        return (TopLevelMergeOptions<E>) super.doNotCompareOrUpdate(attributes);
    }

    @Override
    public TopLevelMergeOptions<E> withMergeHook(MergeHook<E> mergeHook)
    {
        return (TopLevelMergeOptions<E>) super.withMergeHook(mergeHook);
    }

    @Override
    public TopLevelMergeOptions<E> matchOn(Extractor... extractors)
    {
        return (TopLevelMergeOptions<E>) super.matchOn(extractors);
    }

    @Override
    public TopLevelMergeOptions<E> withDatabaseSideDuplicateHandling(DuplicateHandling duplicateHandling)
    {
        return (TopLevelMergeOptions<E>) super.withDatabaseSideDuplicateHandling(duplicateHandling);
    }

    @Override
    public TopLevelMergeOptions<E> withInputSideDuplicateHandling(DuplicateHandling duplicateHandling)
    {
        return (TopLevelMergeOptions<E>) super.withInputSideDuplicateHandling(duplicateHandling);
    }

    public NavigatedMergeOption navigateTo(Navigation navigation)
    {
        AbstractRelatedFinder arf = (AbstractRelatedFinder) navigation;
        ensureDependent(arf);
        DeepRelationshipAttribute parentAttribute = arf.getParentDeepRelationshipAttribute();
        InternalList fullList = new InternalList(parentAttribute == null ? 1 : 6);
        fullList.add(arf);
        while(parentAttribute != null)
        {
            fullList.add(parentAttribute);
            parentAttribute = parentAttribute.getParentDeepRelationshipAttribute();
        }
        return this.addAll(fullList);
    }

    private void ensureDependent(AbstractRelatedFinder arf)
    {
        DeepRelationshipAttribute parentAttribute = arf.getParentDeepRelationshipAttribute();
        RelatedFinder finderInstance = this.getMetaData().getFinderInstance();
        if (parentAttribute == null)
        {
            if (!finderInstance.getDependentRelationshipFinders().contains(arf))
            {
                throw new MithraBusinessException("Can only navigate to dependent relationships! "+arf.getRelationshipName()+" is not a dependent");
            }
        }
        FastList<AbstractRelatedFinder> list = FastList.newList(4);
        list.add(arf);
        while(parentAttribute != null)
        {
            list.add((AbstractRelatedFinder) parentAttribute);
            parentAttribute = parentAttribute.getParentDeepRelationshipAttribute();
        }

        for(int i=list.size() - 1; i >=0; i--)
        {
            AbstractRelatedFinder child = list.get(i);
            if (!finderInstance.getDependentRelationshipFinders().contains(child))
            {
                throw new MithraBusinessException("Can only navigate to dependent relationships! "+ child.getRelationshipName()+" is not a dependent");
            }
            finderInstance = child;
        }
    }

    public void navigateToAllDeepDependents()
    {
        List dependentRelationshipFinders = this.getMetaData().getFinderInstance().getDependentRelationshipFinders();
        recursiveNavigate(dependentRelationshipFinders);
    }

    private void recursiveNavigate(List dependentRelationshipFinders)
    {
        for(int i=0;i<dependentRelationshipFinders.size();i++)
        {
            AbstractRelatedFinder dependentFinder = (AbstractRelatedFinder) dependentRelationshipFinders.get(i);
            navigateTo((Navigation) dependentFinder);
            List secondLevel = dependentFinder.getDependentRelationshipFinders();
            if (!secondLevel.isEmpty())
            {
                recursiveNavigate(secondLevel);
            }
        }
    }

    private NavigatedMergeOption addAll(InternalList fullList)
    {
        int end = fullList.size() - 1;
        MergeOptionNode cur = this.topNode;
        MergeOptionNode found = null;
        while(end >= 0)
        {
            found = null;
            AbstractRelatedFinder toAdd = (AbstractRelatedFinder) fullList.get(end);
            if (cur.getChildren() != null)
            {
                for(int i=0;i<cur.getChildren().size();i++)
                {
                    MergeOptionNode o = cur.getChildren().get(i);
                    if (o.equalsRelatedFinder(toAdd))
                    {
                        found = o;
                        break;
                    }
                }
            }
            if (found == null)
            {
                found = new MergeOptionNode(toAdd);
                cur.addChild(found);
            }
            end--;
            cur = found;
        }
        return found.getNodeMergeOption();
    }

    public MergeOptionNode getTopNode()
    {
        return topNode;
    }
}
