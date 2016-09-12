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


import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.util.StringPool;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.TimestampPool;

import java.sql.Timestamp;

public abstract class MithraOffHeapDataObject implements MithraDataObject
{
    private int _offset;

    public abstract OffHeapDataStorage zGetStorage();

    protected MithraOffHeapDataObject()
    {
        _offset = this.zGetStorage().allocate(this);
    }

    protected MithraOffHeapDataObject(int offset)
    {
        _offset = offset;
    }

    public int zGetOffset()
    {
        return _offset;
    }

    public void zSetOffset(int offset)
    {
        this._offset = offset;
    }

    protected boolean zGetBoolean(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getBoolean(_offset, fieldOffset);
    }

    protected char zGetChar(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getChar(_offset, fieldOffset);
    }

    protected byte zGetByte(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getByte(_offset, fieldOffset);
    }

    protected short zGetShort(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getShort(_offset, fieldOffset);
    }

    protected int zGetInteger(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getInt(_offset, fieldOffset);
    }

    protected long zGetLong(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getLong(_offset, fieldOffset);
    }

    protected float zGetFloat(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getFloat(_offset, fieldOffset);
    }

    protected double zGetDouble(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getDouble(_offset, fieldOffset);
    }

    protected String zGetString(OffHeapDataStorage storage, int fieldOffset)
    {
        return StringPool.getInstance().getStringFromOffHeapAddress(storage.getInt(_offset, fieldOffset));
    }

    protected Timestamp zGetTimestamp(OffHeapDataStorage storage, int fieldOffset)
    {
        return TimestampPool.getInstance().getTimestampFromOffHeapTime(storage.getLong(_offset, fieldOffset));
    }

    protected java.sql.Date zGetDate(OffHeapDataStorage storage, int fieldOffset)
    {
        long date = storage.getLong(_offset, fieldOffset);
        if (date == TimestampPool.OFF_HEAP_NULL)
        {
            return null;
        }
        return new java.sql.Date(date);
    }

    protected Time zGetTime(OffHeapDataStorage storage, int fieldOffset)
    {
        long time = storage.getLong(_offset, fieldOffset);
        if (time == TimestampPool.OFF_HEAP_NULL)
        {
            return null;
        }
        return Time.offHeap(time);
    }

    protected boolean zIsNull(OffHeapDataStorage storage, int nullBitsOffset, int nullBitsPosition)
    {
        return (storage.getInt(_offset, nullBitsOffset) & (1 << nullBitsPosition)) != 0;
    }

    protected boolean zIsStringNull(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getInt(_offset, fieldOffset) == 0;
    }

    protected boolean zIsTimestampNull(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getLong(_offset, fieldOffset) == TimestampPool.OFF_HEAP_NULL;
    }

    protected boolean zIsDateNull(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getLong(_offset, fieldOffset) == TimestampPool.OFF_HEAP_NULL;
    }

    protected boolean zIsTimeNull(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.getLong(_offset, fieldOffset) == TimestampPool.OFF_HEAP_NULL;
    }

    protected boolean zIsBooleanNull(OffHeapDataStorage storage, int fieldOffset)
    {
        return storage.isBooleanNull(_offset, fieldOffset);
    }

    protected void zSetBooleanNull(OffHeapDataStorage storage, int fieldOffset)
    {
        storage.setBooleanNull(_offset, fieldOffset);
    }

    protected void zSetByte(OffHeapDataStorage storage, byte value, int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setByte(_offset, fieldOffset, value);
        zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
    }

    protected void zSetShort(OffHeapDataStorage storage, short value, int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setShort(_offset, fieldOffset, value);
        zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
    }

    protected void zSetInteger(OffHeapDataStorage storage, int value, int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setInt(_offset, fieldOffset, value);
        zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
    }

    protected void zSetLong(OffHeapDataStorage storage, long value, int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setLong(_offset, fieldOffset, value);
        zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
    }

    protected void zSetBoolean(OffHeapDataStorage storage, boolean value, int fieldOffset)
    {
        storage.setBoolean(_offset, fieldOffset, value);
    }

    protected void zSetChar(OffHeapDataStorage storage, char value, int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setChar(_offset, fieldOffset, value);
        zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
    }

    protected void zSetFloat(OffHeapDataStorage storage, float value, int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setFloat(_offset, fieldOffset, value);
        zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
    }

    protected void zSetDouble(OffHeapDataStorage storage, double value, int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setDouble(_offset, fieldOffset, value);
        zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
    }

    protected void zSetString(OffHeapDataStorage storage, String value, int fieldOffset)
    {
        storage.setInt(_offset, fieldOffset, StringPool.getInstance().getOffHeapAddress(value));
    }

    protected void zSetTimestamp(OffHeapDataStorage storage, Timestamp value, int fieldOffset)
    {
        if (value == null)
        {
            storage.setLong(_offset, fieldOffset, TimestampPool.OFF_HEAP_NULL);
        }
        else
        {
            TimestampPool.getInstance().getOrAddToCacheForOffHeap(value);
            storage.setLong(_offset, fieldOffset, value.getTime());
        }
    }

    protected void zSetDate(OffHeapDataStorage storage, java.util.Date value, int fieldOffset)
    {
        if (value == null)
        {
            storage.setLong(_offset, fieldOffset, TimestampPool.OFF_HEAP_NULL);
        }
        else
        {
            storage.setLong(_offset, fieldOffset, value.getTime());
        }
    }

    protected void zSetTime(OffHeapDataStorage storage, Time value, int fieldOffset)
    {
        if (value == null)
        {
            storage.setLong(_offset, fieldOffset, TimestampPool.OFF_HEAP_NULL);
        }
        else
        {
            storage.setLong(_offset, fieldOffset, value.getOffHeapTime());
        }
    }

    protected void zSetByte(OffHeapDataStorage storage, byte value, int fieldOffset, int nullBitsOffset, int nullBitsPosition, boolean isNull)
    {
        if (isNull)
        {
            zSetNull(storage, nullBitsOffset, nullBitsPosition);
        }
        else
        {
            storage.setByte(_offset, fieldOffset, value);
            zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
        }
    }

    protected void zSetShort(OffHeapDataStorage storage, short value, int fieldOffset, int nullBitsOffset, int nullBitsPosition, boolean isNull)
    {
        if (isNull)
        {
            zSetNull(storage, nullBitsOffset, nullBitsPosition);
        }
        else
        {
            storage.setShort(_offset, fieldOffset, value);
            zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
        }
    }

    protected void zSetInteger(OffHeapDataStorage storage, int value, int fieldOffset, int nullBitsOffset, int nullBitsPosition, boolean isNull)
    {
        if (isNull)
        {
            zSetNull(storage, nullBitsOffset, nullBitsPosition);
        }
        else
        {
            storage.setInt(_offset, fieldOffset, value);
            zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
        }
    }

    protected void zSetLong(OffHeapDataStorage storage, long value, int fieldOffset, int nullBitsOffset, int nullBitsPosition, boolean isNull)
    {
        if (isNull)
        {
            zSetNull(storage, nullBitsOffset, nullBitsPosition);
        }
        else
        {
            storage.setLong(_offset, fieldOffset, value);
            zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
        }
    }

    protected void zSetBoolean(OffHeapDataStorage storage, boolean value, int fieldOffset, boolean isNull)
    {
        if (isNull)
        {
            storage.setBooleanNull(_offset, fieldOffset);
        }
        else
        {
            storage.setBoolean(_offset, fieldOffset, value);
        }
    }

    protected void zSetChar(OffHeapDataStorage storage, char value, int fieldOffset, int nullBitsOffset, int nullBitsPosition, boolean isNull)
    {
        if (isNull)
        {
            zSetNull(storage, nullBitsOffset, nullBitsPosition);
        }
        else
        {
            storage.setChar(_offset, fieldOffset, value);
            zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
        }
    }

    protected void zSetFloat(OffHeapDataStorage storage, float value, int fieldOffset, int nullBitsOffset, int nullBitsPosition, boolean isNull)
    {
        if (isNull)
        {
            zSetNull(storage, nullBitsOffset, nullBitsPosition);
        }
        else
        {
            storage.setFloat(_offset, fieldOffset, value);
            zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
        }
    }

    protected void zSetDouble(OffHeapDataStorage storage, double value, int fieldOffset, int nullBitsOffset, int nullBitsPosition, boolean isNull)
    {
        if (isNull)
        {
            zSetNull(storage, nullBitsOffset, nullBitsPosition);
        }
        else
        {
            storage.setDouble(_offset, fieldOffset, value);
            zSetNotNull(storage, nullBitsOffset, nullBitsPosition);
        }
    }

    protected void zSetNull(OffHeapDataStorage storage, int nullBitsOffset, int nullBitsPosition)
    {
        storage.setInt(_offset, nullBitsOffset, storage.getInt(_offset, nullBitsOffset) | (1 << nullBitsPosition));
    }

    private void zSetNotNull(OffHeapDataStorage storage, int nullBitsOffset, int nullBitsPosition)
    {
        if (nullBitsOffset >= 0)
        {
            storage.setInt(_offset, nullBitsOffset, storage.getInt(_offset, nullBitsOffset) & ~(1 << nullBitsPosition));
        }
    }

}
