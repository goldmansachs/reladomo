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


import com.gs.fw.common.mithra.cache.offheap.MasterRetrieveStringResult;

public interface StringIndex extends Evictable
{
    public static final int NULL_STRING = 0;
    public static final int UNKNOWN_STRING = 1;

    public String getIfAbsentPut(String data, boolean hard);

    public int getIfAbsentPutOffHeap(String data);

    public int getOffHeapReference(String data);

    public String getStringFromOffHeapAddress(int address);

    public MasterRetrieveStringResult retrieveStrings(int startAddress);

    public int size();

    public void ensureCapacity(int capacity);
}
