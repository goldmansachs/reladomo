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

import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;

class MergeOptionNode
{
    private NavigatedMergeOption nodeMergeOption;
    private List<MergeOptionNode> children = FastList.newList(2);

    public MergeOptionNode(TopLevelMergeOptions mergeOptions)
    {
        // do nothing.
    }

    public MergeOptionNode(AbstractRelatedFinder relatedFinder)
    {
        this.nodeMergeOption = new NavigatedMergeOption(relatedFinder);
    }

    public boolean equalsRelatedFinder(AbstractRelatedFinder toAdd)
    {
        return nodeMergeOption.equalsRelatedFinder(toAdd);
    }

    public void addChild(MergeOptionNode node)
    {
        children.add(node);
    }

    public NavigatedMergeOption getNodeMergeOption()
    {
        return nodeMergeOption;
    }

    public List<MergeOptionNode> getChildren()
    {
        return children;
    }
}
