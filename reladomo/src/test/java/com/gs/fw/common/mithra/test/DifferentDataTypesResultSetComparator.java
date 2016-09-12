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

import com.gs.fw.common.mithra.test.domain.DifferentDataTypes;
import com.gs.fw.common.mithra.test.domain.DifferentDataTypesData;

import java.sql.ResultSet;
import java.sql.SQLException;


public class DifferentDataTypesResultSetComparator implements MithraTestObjectToResultSetComparator
{
    
    public Object getPrimaryKeyFrom(Object object)
            throws SQLException
    {
        return Integer.valueOf(((DifferentDataTypes) object).getId());
    }

    public Object getPrimaryKeyFrom(ResultSet rs)
            throws SQLException
    {
        return Integer.valueOf(rs.getInt(1));
    }

    public Object createObjectFrom(ResultSet rs)
            throws SQLException
    {
        DifferentDataTypesData data = new DifferentDataTypesData();

        data.setId(rs.getInt("ID"));
        data.setBooleanColumn(rs.getBoolean("BOOLEAN_COLUMN"));
        data.setByteColumn(rs.getByte("BYTE_COLUMN"));
        data.setFloatColumn(rs.getFloat("FLOAT_COLUMN"));
        data.setShortColumn(rs.getShort("SHORT_COLUMN"));
        data.setLongColumn(rs.getLong("LONG_COLUMN"));
        data.setDoubleColumn(rs.getDouble("DOUBLE_COLUMN"));
        data.setCharColumn(rs.getString("CHAR_COLUMN").charAt(0));
        data.setIntColumn(rs.getInt("INT_COLUMN"));

        DifferentDataTypes differentDataTypes = new DifferentDataTypes();
        differentDataTypes.setFromDifferentDataTypesData(data);
        return differentDataTypes;
    }
}
