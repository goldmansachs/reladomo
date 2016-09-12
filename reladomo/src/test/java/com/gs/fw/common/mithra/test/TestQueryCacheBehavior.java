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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrial;

import java.sql.SQLException;
import java.sql.Timestamp;



public class TestQueryCacheBehavior extends MithraTestAbstract
{
    public static final String SOURCE = "SOURCE";
    public static final String ACCOUNT_ID = "71234567";
    public static final String FED_REG_STR_PNL = "FED_REG_STR_PNL";

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        TamsMithraAccount.class,
                        TestTamsMithraTrial.class,
                        TamsMithraAccountArbsClassification.class,
                        TamsMithraArbsClassification.class
                };
    }

    public void testQueryCacheCanFindSimpleRelationship() throws SQLException
    {
        TamsMithraAccountList tamsMithraAccounts = TamsMithraAccountFinder.findMany(TamsMithraAccountFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(TamsMithraAccountFinder.accountNumber().eq(ACCOUNT_ID)));
        tamsMithraAccounts.deepFetch(TamsMithraAccountFinder.trial());

        TamsMithraAccount firstTamsAccount = tamsMithraAccounts.get(0);

        int databaseRetrieveCountBeforeGetterCalled = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        assertEquals("1", firstTamsAccount.getTrial().getTrialId());

        int databaseRetrieveCountAfterGetterCalled = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        assertEquals(0, databaseRetrieveCountAfterGetterCalled - databaseRetrieveCountBeforeGetterCalled);
    }

    public void testQueryCacheCanFindComplexParamaterizedRelationships() throws SQLException
    {
        TamsMithraAccountList tamsMithraAccounts = TamsMithraAccountFinder.findMany(TamsMithraAccountFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(TamsMithraAccountFinder.accountNumber().eq(ACCOUNT_ID)));

        tamsMithraAccounts.deepFetch(TamsMithraAccountFinder.arbsClassificationByType(SOURCE));
        tamsMithraAccounts.deepFetch(TamsMithraAccountFinder.arbsClassificationByType(FED_REG_STR_PNL));

        TamsMithraAccount firstTamsAccount = tamsMithraAccounts.get(0);

        int databaseRetrieveCountBeforeGetterCalled = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        assertEquals("PARA", firstTamsAccount.getArbsClassificationByType(SOURCE).getValue());

        int databaseRetrieveCountAfterGetterCalled = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        //please uncomment to see the current bug with complex paramaterized relationships
        assertEquals(0, databaseRetrieveCountAfterGetterCalled - databaseRetrieveCountBeforeGetterCalled);
    }
}
