/*
 Copyright 2019 Goldman Sachs.
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

package com.gs.reladomo.graphql;


import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sample.domain.*;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import java.sql.Timestamp;

@WebServlet(urlPatterns = "/h2-connection", loadOnStartup = 6)
public class SampleLoadServlet extends HttpServlet
{
    private static final Logger LOGGER = LoggerFactory.getLogger (SampleLoadServlet.class);

    // !!! lazy mithra portals
    public SampleLoadServlet ()
    {
        SampleLoadServlet.initReladomo ("TestReladomoRuntimeConfig.xml", "test_data.txt");
        this.inflatePureBalances ();
        GraphQLReladomoServlet.setSchemaResourceName ("test_schema.graphqls");
    }

    static protected MithraTestResource initReladomo (String configFilename, String datasetFilename)
    {
        MithraTestResource mithraTestResource = new MithraTestResource (configFilename);

        final ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstanceForDbName ("testdb");
        mithraTestResource.createSingleDatabase (connectionManager);
        for (String filename : new String[]{datasetFilename})
        {
            mithraTestResource.addTestDataToDatabase (filename, connectionManager);
        }

        mithraTestResource.setUp ();

        return mithraTestResource;
    }

    /**
     * query {
     * pureBalance_aggregate (filter: {
     * processingDate: {eq: "2020-10-13T04:04:39+00:00"} and: {
     * businessDate: {eq: "2019-10-12T23:59:00+00:00"}}})
     * {
     * accountNum
     * sumOfValue
     * }
     * }
     */
    private void inflatePureBalances ()
    {
        int BALANCE_COUNT = 200_000;
        int TRAN_SIZE = 100000;
        final int ACCOUNT_COUNT = 1000;
        final Timestamp businessDate = Timestamp.valueOf ("2019-10-12 23:59:00");

        long time = System.currentTimeMillis ();
        for (int i = 0; i < BALANCE_COUNT; i += TRAN_SIZE)
        {
            PureBalanceList list = new PureBalanceList ();
            final int finalI = i;
            MithraManagerProvider.getMithraManager ().executeTransactionalCommand (tx ->
            {
                for (int j = 0; j < TRAN_SIZE; j++)
                {
                    int currI = finalI + j;
                    PureBalance pureBalance = new PureBalance (businessDate);
                    pureBalance.setId (currI);
                    long accountId = currI % ACCOUNT_COUNT;
                    pureBalance.setAccountNum (generateAccountNum (accountId));
                    pureBalance.setAccountId (accountId);
                    pureBalance.setValue (currI);
                    pureBalance.setInterest (currI / 100);
                    pureBalance.setCount (1);
                    list.add (pureBalance);
                }
                list.insertAll ();
                return null;
            });
            System.out.println ("i=" + i);
        }
        time = System.currentTimeMillis () - time;

        int size = PureBalanceFinder.findMany (PureBalanceFinder.processingDate ().equalsEdgePoint ().and (PureBalanceFinder.businessDate ().eq (businessDate))).size ();
        LOGGER.info ("PureBalance X " + size + " inflated in " + time);

        PureAccountList list = new PureAccountList ();
        MithraManagerProvider.getMithraManager ().executeTransactionalCommand (tx ->
        {
            for (int j = 0; j < ACCOUNT_COUNT; j++)
            {
                PureAccount pureAccount = new PureAccount ();
                pureAccount.setAccountId (j);
                pureAccount.setAccountNum (generateAccountNum(j));
                pureAccount.setAccountId (j);
                pureAccount.setCountry (j < 200 ? "Belgium" : "Australia");
                list.add (pureAccount);
            }
            list.insertAll ();
            return null;
        });
    }

    private String generateAccountNum (long accountId)
    {
        return "100" + accountId;
    }
}
