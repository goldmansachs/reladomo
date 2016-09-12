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


import com.gs.fw.common.mithra.cache.ParallelProcedure;
import com.gs.fw.common.mithra.cache.ReadWriteLock;
import com.gs.fw.common.mithra.cache.ReferenceListener;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import org.slf4j.Logger;

public interface OffHeapDataStorage extends ReferenceListener
{
    public int getInt(int dataOffset, int fieldOffset);
    public boolean getBoolean(int dataOffset, int fieldOffset);
    public short getShort(int dataOffset, int fieldOffset);
    public char getChar(int dataOffset, int fieldOffset);
    public byte getByte(int dataOffset, int fieldOffset);
    public long getLong(int dataOffset, int fieldOffset);
    public float getFloat(int dataOffset, int fieldOffset);
    public double getDouble(int dataOffset, int fieldOffset);

    public void setInt(int dataOffset, int fieldOffset, int value);
    public void setBoolean(int dataOffset, int fieldOffset, boolean value);
    public void setShort(int dataOffset, int fieldOffset, short value);
    public void setChar(int dataOffset, int fieldOffset, char value);
    public void setByte(int dataOffset, int fieldOffset, byte value);
    public void setLong(int dataOffset, int fieldOffset, long value);
    public void setFloat(int dataOffset, int fieldOffset, float value);
    public void setDouble(int dataOffset, int fieldOffset, double value);

    public int allocate(MithraOffHeapDataObject data);
    public void free(int dataOffset);

    public Object getDataAsObject(int dataOffset);

    public MithraOffHeapDataObject getData(int dataOffset);

    public boolean isBooleanNull(int dataOffset, int fieldOffset);
    public void setBooleanNull(int dataOffset, int fieldOffset);

    public void destroy();

    public boolean forAll(DoUntilProcedure procedure);

    public void forAllInParallel(ParallelProcedure procedure);

    public void clear();

    public void ensureExtraCapacity(int size);

    public void reportSpaceUsage(Logger logger, String className);

    public long getAllocatedSize();

    public long getUsedSize();

    public void setReadWriteLock(ReadWriteLock cacheLock);

    public boolean syncWithMasterCache(MasterCacheUplink uplink, OffHeapSyncableCache cache);

    public void markDataDirty(int dataOffset);

    public MasterSyncResult sendSyncResult(long maxReplicatedPageVersion);
}
