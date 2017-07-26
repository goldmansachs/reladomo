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

import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.reladomo.metadata.ReladomoClassMetaData;

public class NavigatedMergeOption<E> extends MergeOptions<E>
{
    private AbstractRelatedFinder relatedFinder;

    public NavigatedMergeOption(AbstractRelatedFinder relatedFinder)
    {
        super(ReladomoClassMetaData.fromFinder(relatedFinder));
        this.relatedFinder = relatedFinder;
    }

    public AbstractRelatedFinder getRelatedFinder()
    {
        return relatedFinder;
    }

    public boolean equalsRelatedFinder(AbstractRelatedFinder toAdd)
    {
        return this.relatedFinder == toAdd;
    }
}
