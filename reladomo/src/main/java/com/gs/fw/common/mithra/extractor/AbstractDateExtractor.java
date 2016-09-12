
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

import java.util.Date;

public abstract class AbstractDateExtractor<T> extends NonPrimitiveExtractor<T, Date> implements DateExtractor<T>
{

    public Date valueOf(T o)
    {
        return this.dateValueOf(o);
    }

    @Override
    public long dateValueOfAsLong(T valueHolder)
    {
        return dateValueOf(valueHolder).getTime();
    }

    public void setValue(T o, Date newValue)
    {
        this.setDateValue(o, newValue);
    }
}
