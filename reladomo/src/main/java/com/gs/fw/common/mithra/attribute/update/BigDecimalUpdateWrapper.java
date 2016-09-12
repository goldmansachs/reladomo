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

import com.gs.fw.common.mithra.extractor.BigDecimalExtractor;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.attribute.Attribute;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.math.BigDecimal;



public class BigDecimalUpdateWrapper extends ObjectUpdateWrapper implements BigDecimalExtractor
{

    public BigDecimalUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, BigDecimal newValue)
    {
        super(attribute, dataToUpdate, newValue);
    }

    public BigDecimalUpdateWrapper()
    {        
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone) throws SQLException
    {
        BigDecimal value = (BigDecimal) this.getValue();
        if (value == null)
        {
            ps.setNull(index, Types.DECIMAL);
        }
        else
        {
            ps.setBigDecimal(index, value);
        }
        return 1;
    }


    public BigDecimal bigDecimalValueOf(Object o)
    {
        return (BigDecimal) this.getValue();
    }

    public void setBigDecimalValue(Object o, BigDecimal newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        BigDecimal value = (BigDecimal) this.getValue();
        if (value == null)
        {
            ps.setNull(index, Types.DECIMAL);
        }
        else
        {
            ps.setBigDecimal(index, value);
        }
        return 1;
    }

    public void increment(Object o, BigDecimal d)
    {
        throw new RuntimeException("not implemented");
    }

    public void incrementUntil(Object o, BigDecimal d, Timestamp t)
    {
        throw new RuntimeException("not implemented");
    }

}
