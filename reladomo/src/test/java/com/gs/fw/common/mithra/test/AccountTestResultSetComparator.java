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

import com.gs.fw.common.mithra.test.domain.Account;
import com.gs.fw.common.mithra.test.domain.AccountData;

import java.sql.SQLException;
import java.sql.ResultSet;


public class AccountTestResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object mithraObject) throws SQLException
    {
        return ((Account)mithraObject).getAccountNumber();
    }

    public Object getPrimaryKeyFrom(ResultSet rs) throws SQLException
    {
        return rs.getString("ACCOUNT_NUMBER");
    }

    public Object createObjectFrom(ResultSet rs) throws SQLException
    {
        AccountData data = new AccountData();
        data.setAccountNumber(rs.getString(1));
        data.setCode(rs.getString(2));
        data.setTrialId(rs.getString(3));
        data.setPnlGroupId(rs.getString(4));

        Account account = new Account();
        account.zSetData(data, null);
        return account;
    }
}
