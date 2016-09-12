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
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.util.StringPool;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.TimeZone;



public class StringUpdateWrapper extends ObjectUpdateWrapper implements StringExtractor
{

    public StringUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, Object newValue)
    {
        super(attribute, dataToUpdate, newValue);
    }

    public StringUpdateWrapper()
    {
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        String value = (String) this.getValue();
        if (value == null)
        {
            ps.setNull(index, Types.VARCHAR);
        }
        else
        {
            ps.setString(index, value);
        }
        return 1;
    }

    @Override
    public int offHeapValueOf(Object o)
    {
        return StringPool.getInstance().getOffHeapAddressWithoutAdding(stringValueOf(o));
    }

    public String stringValueOf(Object o)
    {
        return (String) this.getValue();
    }

    public void setStringValue(Object o, String newValue)
    {
        throw new RuntimeException("not implemented");
    }
}
