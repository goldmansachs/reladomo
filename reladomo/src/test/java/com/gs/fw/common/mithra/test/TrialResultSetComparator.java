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

import com.gs.fw.common.mithra.test.domain.Trial;
import com.gs.fw.common.mithra.test.domain.TrialData;

import java.sql.ResultSet;
import java.sql.SQLException;



public class TrialResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object object)
            throws SQLException
    {
        return ((Trial) object).getTrialId();
    }

    public Object getPrimaryKeyFrom(ResultSet rs)
            throws SQLException
    {
        return rs.getString(1);
    }

    public Object createObjectFrom(ResultSet rs)
            throws SQLException
    {
        TrialData data = new TrialData();
        data.setTrialId(rs.getString(1));
        data.setDescription(rs.getString(2));

        Trial trial = new Trial();
        trial.setFromTrialData(data);
        return trial;
    }

}
