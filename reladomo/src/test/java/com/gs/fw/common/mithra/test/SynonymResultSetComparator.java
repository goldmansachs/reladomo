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

import com.gs.fw.common.mithra.test.domain.Synonym;
import com.gs.fw.common.mithra.test.domain.SynonymData;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


public class SynonymResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object object) throws SQLException
    {
        Synonym s = (Synonym)object;
        return String.valueOf(s.getProductId()) + s.getSynonymTypeCode() + String.valueOf(s.getSynonymEndDate());
    }

    public Object getPrimaryKeyFrom(ResultSet rs) throws SQLException
    {
        int productId = rs.getInt(1);
        String typeCode = rs.getString(2);
        Timestamp expiryDate = rs.getTimestamp(3);
        return String.valueOf(productId) + typeCode + String.valueOf(expiryDate);
    }

    public Object createObjectFrom(ResultSet rs) throws SQLException
    {
        SynonymData data = new SynonymData();
        data.setProductId(rs.getInt(1));
        data.setSynonymTypeCode(rs.getString(2));
        Timestamp expiryDate = rs.getTimestamp(3);
        if (expiryDate != null)
            data.setSynonymEndDate(new Timestamp(expiryDate.getTime()));
        else
            data.setSynonymEndDateNull();
        data.setSynonym(rs.getString(4));

        Synonym s = new Synonym();
        s.setFromSynonymData(data);
        return s;
    }

}
