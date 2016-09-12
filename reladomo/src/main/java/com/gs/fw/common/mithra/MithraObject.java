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

import com.gs.fw.common.mithra.cache.Cache;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.Serializable;



public interface MithraObject extends Serializable
{

    public void zReindexAndSetDataIfChanged(MithraDataObject data, Cache cache);

    public void zSerializeFullData(ObjectOutput out) throws IOException;

    public void zWriteDataClassName(ObjectOutput out) throws IOException;

    public void zSerializeFullTxData(ObjectOutput out) throws IOException;

    /* the optional object is the TransactionalBehavior in case of transactional objects */
    public void zSetData(MithraDataObject data, Object optional);

    public MithraDataObject zGetCurrentData();

    public void zMarkDirty();

    public void zSetNonTxPersistenceState(int state);

    /**
     * This method is used to check if the Mithra object has been
     * deleted (or mark for deletion for detached objects).
     * Read only objects are only marked as deleted in the event of
     * a full cache reload.
     * @return true if the object has been deleted.
     * For detached objects return true it it has been mark for deletion.
     */
    public boolean isDeletedOrMarkForDeletion();

    /**
     * creates a copy of the object. Modifying the copy has no relation to the original persistent object or
     * the underlying database.
     * @return a copy of the original.
     */
    public MithraObject getNonPersistentCopy();
}
