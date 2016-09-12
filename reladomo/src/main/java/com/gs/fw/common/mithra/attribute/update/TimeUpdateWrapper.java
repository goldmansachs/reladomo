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

package com.gs.fw.common.mithra.attribute.update;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimeAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.TimeExtractor;
import com.gs.fw.common.mithra.util.Time;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;

public class TimeUpdateWrapper extends ObjectUpdateWrapper implements TimeExtractor
{
    public TimeUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, Object newValue)
    {
        super(attribute, dataToUpdate, newValue);
    }

    public TimeUpdateWrapper()
    {
    }

    @Override
    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        ((TimeAttribute)this.getAttribute()).setSqlParameter(index, ps, this.getValue(), timeZone, databaseType);
        return 1;
    }

    @Override
    public Time timeValueOf(Object o)
    {
        return (Time) this.getValue();
    }

    @Override
    public void setTimeValue(Object o, Time newValue)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long offHeapTimeValueOfAsLong(Object valueHolder)
    {
        return timeValueOf(valueHolder).getOffHeapTime();
    }
}
