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
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.DateExtractor;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;



public class DateUpdateWrapper extends ObjectUpdateWrapper implements DateExtractor
{

    public DateUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, Object newValue)
    {
        super(attribute, dataToUpdate, newValue);
    }

    public DateUpdateWrapper()
    {
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        ((DateAttribute)this.getAttribute()).setSqlParameter(index, ps, this.getValue(), timeZone, databaseType);
        return 1;
    }

    public java.util.Date dateValueOf(Object o)
    {
        return (Date) this.getValue();
    }

    public void setDateValue(Object o, java.util.Date newValue)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long dateValueOfAsLong(Object valueHolder)
    {
        return dateValueOf(valueHolder).getTime();
    }
}
