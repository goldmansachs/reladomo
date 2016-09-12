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

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.MithraTransaction;



public interface TransactionalIndex
{

    public Object removeUsingUnderlying(Object underlyingObject);

    public Object putIgnoringTransaction(Object object, Object newData, boolean weak);

    public Object preparePut(Object object);

    public void commitPreparedForIndex(Object index);

    public Object removeIgnoringTransaction(Object object);

    public Object getFromPreparedUsingData(Object data);

    public void prepareForCommit(MithraTransaction tx);

    public void commit(MithraTransaction tx);

    public void rollback(MithraTransaction tx);

    public boolean prepareForReindex(Object businessObject, MithraTransaction tx);

    public void finishForReindex(Object businessObject, MithraTransaction tx);

    public void prepareForReindexInTransaction(Object businessObject, MithraTransaction tx);
}
