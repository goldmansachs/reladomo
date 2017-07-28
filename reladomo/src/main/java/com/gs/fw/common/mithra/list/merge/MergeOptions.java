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
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.reladomo.metadata.ReladomoClassMetaData;

import java.util.List;

public class MergeOptions<E>
{
    private ReladomoClassMetaData metaData;
    private MergeHook<E> mergeHook = MergeHook.DEFAULT;
    private Extractor[] toMatchOn;
    private Attribute[] doNotCompare;
    private Attribute[] doNotUpdate;

    public MergeHook<E> getMergeHook()
    {
        return mergeHook;
    }

    public enum DuplicateHandling
    {
        THROW_ON_DUPLICATE, TAKE_LAST_DUPLICATE;
    }

    private DuplicateHandling dbDuplicateHandling = DuplicateHandling.THROW_ON_DUPLICATE;
    private DuplicateHandling inputDuplicateHandling = DuplicateHandling.THROW_ON_DUPLICATE;

    public MergeOptions(ReladomoClassMetaData metaData)
    {
        this.metaData = metaData;
    }

    public DuplicateHandling getDbDuplicateHandling()
    {
        return dbDuplicateHandling;
    }

    public DuplicateHandling getInputDuplicateHandling()
    {
        return inputDuplicateHandling;
    }

    public ReladomoClassMetaData getMetaData()
    {
        return metaData;
    }

    public Extractor[] getToMatchOn()
    {
        return toMatchOn;
    }

    public Attribute[] getDoNotCompare()
    {
        return doNotCompare;
    }

    public Attribute[] getDoNotUpdate()
    {
        return doNotUpdate;
    }

    /**
     * when considering if an object has changed, do not compare these attributes.
     * If something else in the object has changed, the new values for these attributes will be updated
     * (unless also added to doNotUpdate list).
     * @param attributes
     * @return
     */
    public MergeOptions<E> doNotCompare(Attribute... attributes)
    {
        if (this.doNotCompare == null)
        {
            this.doNotCompare = attributes;
        }
        else
        {
            List<Attribute> all = FastList.newListWith(this.doNotCompare);
            for(Attribute a: attributes) all.add(a);
            this.doNotCompare = new Attribute[all.size()];
            all.toArray(this.doNotCompare);
        }
        return this;
    }

    /**
     * Do not update these attributes
     * @param attributes
     * @return
     */
    public MergeOptions<E> doNotUpdate(Attribute... attributes)
    {
        if (this.doNotUpdate == null)
        {
            this.doNotUpdate = attributes;
        }
        else
        {
            List<Attribute> all = FastList.newListWith(this.doNotUpdate);
            for(Attribute a: attributes) all.add(a);
            this.doNotUpdate = new Attribute[all.size()];
            all.toArray(this.doNotUpdate);
        }
        return this;
    }

    public MergeOptions<E> doNotCompareOrUpdate(Attribute... attributes)
    {
        return this.doNotCompare(attributes).doNotUpdate(attributes);
    }

    public MergeOptions<E> withMergeHook(MergeHook<E> mergeHook)
    {
        if (this.mergeHook != MergeHook.DEFAULT)
        {
            throw new IllegalStateException("Must not call withMergeHook multiple times");
        }
        this.mergeHook = mergeHook;
        return this;
    }

    public MergeOptions<E> matchOn(Extractor... extractors)
    {
        if (this.toMatchOn != null)
        {
            throw new IllegalStateException("Must not call matchOn multiple times");
        }
        this.toMatchOn = extractors;
        return this;
    }

    public MergeOptions<E> withDatabaseSideDuplicateHandling(DuplicateHandling duplicateHandling)
    {
        this.dbDuplicateHandling = duplicateHandling;
        return this;
    }

    public MergeOptions<E> withInputSideDuplicateHandling(DuplicateHandling duplicateHandling)
    {
        this.inputDuplicateHandling = duplicateHandling;
        return this;
    }

}
