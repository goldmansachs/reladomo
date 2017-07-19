
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

import com.gs.fw.common.mithra.list.merge.TopLevelMergeOptions;
import com.gs.fw.finder.TransactionalDomainList;

public interface MithraTransactionalList<E> extends MithraList<E>, TransactionalDomainList<E>
{
    public MithraTransactionalList<E> getNonPersistentCopy();

    public MithraTransactionalList getNonPersistentGenericCopy();

    public MithraTransactionalList getDetachedCopy();

    public void copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();

    public void insertAll();

    public void bulkInsertAll();

    public void cascadeInsertAll();

    /**
     * Incorporate the changes in the incoming list, according to the mergeOptions, into
     * this list.
     * Operation based lists first copy this list to an adhoc list (see asAdhoc) and then merge, returning the result
     *
     * @param incoming list with changes.
     * @param mergeOptions options for merging.
     * @return The resulting list, which can be "this" if the list was not operation based.
     */
    public MithraTransactionalList<E> merge(MithraTransactionalList<E> incoming, TopLevelMergeOptions<E> mergeOptions);
}
