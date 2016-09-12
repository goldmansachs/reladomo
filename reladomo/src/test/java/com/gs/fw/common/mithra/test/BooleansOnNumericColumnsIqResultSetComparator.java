
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

import com.gs.fw.common.mithra.test.domain.BooleansOnNumericColumnsIq;
import com.gs.fw.common.mithra.test.domain.BooleansOnNumericColumnsIqData;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BooleansOnNumericColumnsIqResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object mithraObject) throws SQLException
    {
        return Integer.valueOf(((BooleansOnNumericColumnsIq) mithraObject).getId());
    }

    public Object getPrimaryKeyFrom(ResultSet rs) throws SQLException
    {
        return Integer.valueOf(rs.getInt("ID"));
    }

    public Object createObjectFrom(ResultSet rs) throws SQLException
    {
        BooleansOnNumericColumnsIqData data = new BooleansOnNumericColumnsIqData();
        data.setId(rs.getInt(1));
        data.setBooleanValue(rs.getInt(2) == 1);
        data.setBooleanOnByteValue(rs.getByte(3) == 1);
        data.setBooleanOnShortValue(rs.getShort(4) == 1);
        data.setBooleanOnIntValue(rs.getInt(5) == 1);
        data.setBooleanOnLongValue(rs.getLong(6) == 1);

        int ib = rs.getInt(7);
        if(rs.wasNull())
        {
            data.setNullableBooleanValueNull();
        }
        else
        {
            data.setNullableBooleanValue(ib == 1);
        }

        byte b = rs.getByte(8);
        if(rs.wasNull())
        {
            data.setNullableBooleanOnByteValueNull();
        }
        else
        {
            data.setNullableBooleanOnByteValue(b == 1);
        }

        short st = rs.getShort(9);
        if(rs.wasNull())
        {
            data.setNullableBooleanOnShortValueNull();
        }
        else
        {
            data.setNullableBooleanOnShortValue(st == 1);
        }

        int it = rs.getInt(10);
        if(rs.wasNull())
        {
            data.setNullableBooleanOnIntValueNull();
        }
        else
        {
            data.setNullableBooleanOnIntValue(it == 1);
        }

        long lt = rs.getLong(11);
        if(rs.wasNull())
        {
            data.setNullableBooleanOnLongValueNull();
        }
        else
        {
            data.setNullableBooleanOnLongValue(lt == 1);
        }

        BooleansOnNumericColumnsIq booleansOnNumericColumnsIq = new BooleansOnNumericColumnsIq();
        booleansOnNumericColumnsIq.zSetNonTxData(data);
        return booleansOnNumericColumnsIq;
    }
}
