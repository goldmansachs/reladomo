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

import com.gs.fw.common.mithra.databasetype.OracleDatabaseType;
import com.gs.fw.common.mithra.test.domain.AllTypesData;
import com.gs.fw.common.mithra.util.Time;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;

public class AllTypesOracleResultSetComparator extends AllTypesResultSetComparator
{
    @Override
    protected void readTime(ResultSet rs, AllTypesData data) throws SQLException
    {
        try
        {
            data.setTimeValue(OracleDatabaseType.parseStringAndSet(rs.getString(11).substring(1).trim()));
        } catch (ParseException e1)
        {
            throw new SQLException("couldn't read time " + e1.getMessage(), e1);
        }
    }

    protected Time getTime(ResultSet rs) throws SQLException
    {
        Time time = null;
        try
        {
            String string = rs.getString(23);
            if (string != null)
                time = OracleDatabaseType.parseStringAndSet(string.substring(1).trim());
        }
        catch (ParseException e1)
        {
            e1.printStackTrace();
        }
        return time;
    }
}
