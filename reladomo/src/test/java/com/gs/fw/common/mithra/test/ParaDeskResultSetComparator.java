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

import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskData;

import java.sql.ResultSet;
import java.sql.SQLException;



public class ParaDeskResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object object)
            throws SQLException
    {
        return ((ParaDesk) object).getDeskIdString();
    }

    public Object getPrimaryKeyFrom(ResultSet rs)
            throws SQLException
    {
        return rs.getString(1);
    }

    public Object createObjectFrom(ResultSet rs)
            throws SQLException
    {
        ParaDeskData data = new ParaDeskData();
        data.setDeskIdString(rs.getString(1));
        data.setActiveBoolean(rs.getBoolean(2));
        data.setSizeDouble(rs.getDouble(3));
        data.setConnectionLong(rs.getLong(4));
        data.setTagInt(rs.getInt(5));
        data.setStatusChar(rs.getString(6).charAt(0));
        data.setCreateTimestamp(rs.getTimestamp(7));
        data.setLocationByte(rs.getByte(8));
        data.setClosedDate(rs.getDate(9));
        data.setMaxFloat(rs.getFloat(10));
        data.setMinShort(rs.getShort(11));
        data.setBigDouble(rs.getBigDecimal(12));
        ParaDesk desk = new ParaDesk();
        desk.setFromParaDeskData(data);
        return desk;
    }

}
