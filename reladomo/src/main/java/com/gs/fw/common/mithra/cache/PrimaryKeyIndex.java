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

import java.util.List;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.util.Filter;



public interface PrimaryKeyIndex extends Index, IterableIndex
{
    public Object getFromData(Object data);

    public List getAll();

    public HashStrategy getHashStrategy();

    public PrimaryKeyIndex copy();

    public int size();

    public Object markDirty(MithraDataObject object);

    public Object getFromDataEvenIfDirty(Object data, NonNullMutableBoolean isDirty);

    public Object putWeak(Object object);

    public Object putWeakUsingUnderlying(Object businessObject, Object underlying);

    public List removeAll(Filter filter);

    public boolean sizeRequiresWriteLock();

    public void ensureCapacity(int capacity);
}
