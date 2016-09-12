
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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.test.domain.AllTypesIq;
import com.gs.fw.common.mithra.test.domain.AllTypesIqData;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class AllTypesIqResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object mithraObject) throws SQLException
    {
        return Integer.valueOf(((AllTypesIq) mithraObject).getId());
    }

    public Object getPrimaryKeyFrom(ResultSet rs) throws SQLException
    {
        return Integer.valueOf(rs.getInt("ID"));
    }

    public Object createObjectFrom(ResultSet rs) throws SQLException
    {
        AllTypesIqData data = new AllTypesIqData();
        data.setId(rs.getInt(1));
        data.setBooleanValue(rs.getInt(2) != 0);
        data.setByteValue(rs.getByte(3));
        data.setShortValue(rs.getShort(4));
        data.setCharValue(rs.getString(5).charAt(0));
        data.setIntValue(rs.getInt(6));
        data.setLongValue(rs.getLong(7));
        data.setFloatValue(rs.getFloat(8));
        data.setDoubleValue(rs.getDouble(9));
        data.setDateValue(rs.getDate(10));
        data.setTimestampValue(rs.getTimestamp(11));
        data.setStringValue(rs.getString(12));

        byte b = rs.getByte(13);
        if(rs.wasNull())
        {
            data.setNullableByteValueNull();
        }
        else
        {
            data.setNullableByteValue(b);
        }

        short st = rs.getShort(14);
        if(rs.wasNull())
        {
            data.setNullableShortValueNull();
        }
        else
        {
            data.setNullableShortValue(st);
        }

        String cs = rs.getString(15);
        if(rs.wasNull())
        {
            data.setNullableCharValueNull();
        }
        else
        {
            data.setNullableCharValue(cs.charAt(0));
        }

        int i = rs.getInt(16);
        if(rs.wasNull())
        {
            data.setNullableIntValueNull();
        }
        else
        {
            data.setNullableIntValue(i);
        }

        long l = rs.getLong(17);
        if(rs.wasNull())
        {
            data.setNullableLongValueNull();
        }
        else
        {
            data.setNullableLongValue(l);
        }

        float f = rs.getFloat(18);
        if(rs.wasNull())
        {
            data.setNullableFloatValueNull();
        }
        else
        {
            data.setNullableFloatValue(f);
        }

        double d = rs.getDouble(19);
        if(rs.wasNull())
        {
            data.setNullableDoubleValueNull();
        }
        else
        {
            data.setNullableDoubleValue(d);
        }

        Date date = rs.getDate(20);
        if(rs.wasNull())
        {
            data.setNullableDateValueNull();
        }
        else
        {
            data.setNullableDateValue(date);
        }

        Timestamp timestamp = rs.getTimestamp(21);
        if(rs.wasNull())
        {
            data.setNullableTimestampValueNull();
        }
        else
        {
            data.setNullableTimestampValue(timestamp);
        }

        String s = rs.getString(22);
        if(rs.wasNull())
        {
            data.setNullableStringValueNull();
        }
        else
        {
            data.setNullableStringValue(s);
        }

        AllTypesIq allTypesIq = new AllTypesIq();
        allTypesIq.zSetNonTxData(data);
        return allTypesIq;
    }
}
