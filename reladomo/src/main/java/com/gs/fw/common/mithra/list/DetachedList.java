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

package com.gs.fw.common.mithra.list;

import com.gs.fw.common.mithra.MithraTransactionalList;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.list.merge.MergeBuffer;
import com.gs.fw.common.mithra.list.merge.TopLevelMergeOptions;

import java.util.Collection;


public class DetachedList<E> extends AbstractTransactionalNonOperationBasedList<E>
{
    public static final DetachedList DEFAULT = new DetachedList();

    public DetachedList()
    {
        // for externalizable
    }

    @Override
    protected AdhocDetachedList getFastList(DelegatingList delegatingList)
    {
        return (AdhocDetachedList) super.getFastList(delegatingList);
    }

    @Override
    public void init(DelegatingList delegatingList)
    {
        throw new RuntimeException("shouldn't get here");
    }

    @Override
    public void init(DelegatingList delegatingList, Collection c)
    {
        throw new RuntimeException("shouldn't get here");
    }

    @Override
    public MithraDelegatedList getNonPersistentDelegate()
    {
        return AbstractTransactionalNonOperationBasedList.DEFAULT;
    }

    @Override
    public void init(DelegatingList delegatingList, int initialSize)
    {
        delegatingList.zSetFastListOrCachedQuery(new AdhocDetachedList(delegatingList, initialSize));
    }

    public void setOperation(DelegatingList<E> delegatingList, Operation op)
    {
        getFastList(delegatingList).setOperation(op);
    }

    public void addRemovedItem(DelegatingList delegatingList, MithraTransactionalObject detachedCopy)
    {
        getFastList(delegatingList).addRemovedItem(detachedCopy);
    }

    @Override
    public void merge(DelegatingList<E> dbList, MithraTransactionalList<E> incoming, TopLevelMergeOptions<E> mergeOptions)
    {
        MergeBuffer mergeBuffer = new MergeBuffer(mergeOptions, true);
        mergeBuffer.mergeLists(dbList, incoming);
    }
}
