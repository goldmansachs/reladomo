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

package com.gs.fw.common.mithra.cache.offheap;


import org.slf4j.Logger;

public interface OffHeapIntArrayStorage
{
    public int allocate(int size);

    public void free(int ref);

    public boolean isFragmented();

    public int getLength(int arrayRef);

    public int getInt(int arrayRef, int pos);

    public void setInt(int arrayRef, int pos, int value);

    public int incrementAndGet(int arrayRef, int pos, int value);

    public int reallocate(int arrayRef, int newSize);

    public void clear(int arrayRef);

    public void destroy();

    public void reportSpaceUsage(Logger logger, String msg);

    public void ensureCapacity(long sizeInBytes);

    public long getAllocatedSize();

    public long getUsedSize();
}
