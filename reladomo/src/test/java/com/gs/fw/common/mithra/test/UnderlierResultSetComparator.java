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

import com.gs.fw.common.mithra.test.domain.Underlier;
import com.gs.fw.common.mithra.test.domain.UnderlierData;

import java.sql.ResultSet;
import java.sql.SQLException;



public class UnderlierResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object object)
    throws SQLException
    {
        Underlier underlier = (Underlier) object;
        return underlier.getContractType() + underlier.getContractId() + underlier.getAccountId() + underlier.getProductId() + underlier.getUnderlierId();
    }

    public Object getPrimaryKeyFrom(ResultSet rs)
    throws SQLException
    {
        return rs.getString(1) + rs.getInt(2) + rs.getString(3) + rs.getInt(4) + rs.getLong(5);
    }

    public Object createObjectFrom(ResultSet rs)
    throws SQLException
    {
        UnderlierData data = new UnderlierData();
        data.setContractType(rs.getString(1));
        data.setContractId(rs.getInt(2));
        data.setAccountId(rs.getString(3));
        data.setProductId(rs.getInt(4));
        data.setUnderlierId(rs.getLong(5));
        data.setDescription(rs.getString(6));
        data.setCurrency(rs.getString(7));

        Underlier underlier = new Underlier();
        underlier.setFromUnderlierData(data);

        return underlier;
    }

}
