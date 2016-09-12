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

import com.gs.fw.common.mithra.test.domain.MappedDifferentDataTypesData;
import com.gs.fw.common.mithra.test.domain.MappedDifferentDataTypes;

import java.sql.SQLException;
import java.sql.ResultSet;


public class MappedDifferentDataTypesResultSetComparator implements MithraTestObjectToResultSetComparator
{
    
    public Object getPrimaryKeyFrom(Object object)
            throws SQLException
    {
        return Integer.valueOf(((MappedDifferentDataTypes) object).getId());
    }

    public Object getPrimaryKeyFrom(ResultSet rs)
            throws SQLException
    {
        return Integer.valueOf(rs.getInt(1));
    }

    public Object createObjectFrom(ResultSet rs)
            throws SQLException
    {
        MappedDifferentDataTypesData data = new MappedDifferentDataTypesData();

        data.setId(rs.getInt("ID"));
        data.setName(rs.getString("NAME"));

        MappedDifferentDataTypes mappedDifferentDataTypes = new MappedDifferentDataTypes();
        mappedDifferentDataTypes.setFromMappedDifferentDataTypesData(data);
        return mappedDifferentDataTypes;
    }
}
