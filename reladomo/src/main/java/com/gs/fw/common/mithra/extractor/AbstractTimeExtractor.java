
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

package com.gs.fw.common.mithra.extractor;

import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.TimestampPool;

public abstract class AbstractTimeExtractor<T> extends NonPrimitiveExtractor<T, Time> implements TimeExtractor<T>
{
    public Time valueOf(T o)
    {
        return this.timeValueOf(o);
    }

    @Override
    public long offHeapTimeValueOfAsLong(T valueHolder)
    {
        Time time = timeValueOf(valueHolder);
        return time == null ? TimestampPool.OFF_HEAP_NULL : time.getOffHeapTime();
    }

    public void setValue(T o, Time newValue)
    {
        this.setTimeValue(o, newValue);
    }
}
