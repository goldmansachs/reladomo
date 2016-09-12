
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

import com.gs.fw.common.mithra.test.domain.dated.DatedTable;
import com.gs.fw.common.mithra.test.domain.dated.DatedTableData;
import com.gs.fw.common.mithra.test.domain.dated.DatedTableDatabaseObject;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DatedTableResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object object)
            throws SQLException
    {
        return Integer.valueOf(((DatedTable) object).getId());
    }


    public Object getPrimaryKeyFrom(ResultSet rs)
            throws SQLException
    {
        return Integer.valueOf(rs.getInt(1));
    }

    public Object createObjectFrom(ResultSet rs)
            throws SQLException
    {
        DatedTableData data = DatedTableDatabaseObject.allocateOnHeapData();
        data.setId(rs.getInt("ID"));
        data.setBusinessDateFrom(rs.getTimestamp("FROM_Z"));
        data.setBusinessDateTo(rs.getTimestamp("THRU_Z"));
        data.setProcessingDateFrom(rs.getTimestamp("IN_Z"));
        data.setProcessingDateTo(rs.getTimestamp("OUT_Z"));
        return data;

    }

}

