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

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.test.domain.AccountTransaction;
import com.gs.fw.common.mithra.test.domain.AccountTransactionFinder;
import com.gs.fw.common.mithra.test.domain.AccountTransactionIntSrcAttr;
import com.gs.fw.common.mithra.test.domain.AccountTransactionIntSrcAttrFinder;
import com.gs.fw.common.mithra.test.domain.AccountTransactionIntSrcAttrList;
import com.gs.fw.common.mithra.test.domain.AccountTransactionList;
import com.gs.fw.common.mithra.test.mithraTestResource.ConnectionManagerForTestsWithTableManager;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;

import java.sql.Timestamp;
import java.util.Set;

public class TestTablePartitionManager extends MithraTestAbstract
{
    private MithraTestResource mithraTestResource;

    protected void setUp()
    throws Exception
    {
        String xmlFile = System.getProperty("mithra.xml.config");//String xmlFile = "MithraTestTableManagerConfig.xml";

        mithraTestResource = new MithraTestResource(xmlFile);
        mithraTestResource.setRestrictedClassList(getRestrictedClassList());

        ConnectionManagerForTests connectionManager = ConnectionManagerForTestsWithTableManager.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "A");
        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "B");
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 0);
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 1);
        mithraTestResource.createSingleDatabase(connectionManager);
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
    }

    public void testStringSourceTablePartitionManager() throws Exception
    {
        // insert one
        createAccountTransaction("transaction 1", "A").insert();
        // insert list with mixed sources
        AccountTransactionList list = new AccountTransactionList();
        list.add(createAccountTransaction("transaction 2", "A"));
        list.add(createAccountTransaction("transaction 3", "B"));
        list.add(createAccountTransaction("transaction 4", "A"));
        list.add(createAccountTransaction("transaction 5", "B"));
        list.insertAll();

        // select
        AccountTransactionList listA = AccountTransactionFinder.findMany(AccountTransactionFinder.deskId().eq("A"));
        listA.addOrderBy(AccountTransactionFinder.transactionId().ascendingOrderBy());

        assertEquals(3, listA.size());
        assertEquals(1, listA.get(0).getTransactionId());
        assertEquals(2, listA.get(1).getTransactionId());
        assertEquals(3, listA.get(2).getTransactionId());

        AccountTransactionList listB = AccountTransactionFinder.findMany(AccountTransactionFinder.deskId().eq("B"));
        listB.addOrderBy(AccountTransactionFinder.transactionId().ascendingOrderBy());

        assertEquals(2, listB.size());
        assertEquals(4, listB.get(0).getTransactionId());
        assertEquals(5, listB.get(1).getTransactionId());

        // delete on one source
        AccountTransactionFinder.findMany(AccountTransactionFinder.deskId().eq("A").and(AccountTransactionFinder.transactionId().lessThanEquals(2))).deleteAll();

        // refresh after the delete, one item left
        listA.forceRefresh();
        assertEquals(1, listA.size());

        AccountTransaction transaction = listA.get(0);
        // update
        transaction.setTransactionDescription("new description");

        // select from multiple sources
        AccountTransactionList listAB = AccountTransactionFinder.findMany(AccountTransactionFinder.deskId().in(UnifiedSet.newSetWith("A", "B")));
        assertEquals(3, listAB.size());
        listAB.deleteAll();

        // delete with multiple sources
        listAB.forceRefresh();
        assertEquals(0, listAB.size());
    }

    private AccountTransaction createAccountTransaction(String description, String deskId)
    {
        AccountTransaction accountTransaction = new AccountTransaction();
        accountTransaction.setDeskId(deskId);
        accountTransaction.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        accountTransaction.setTransactionDescription(description);
        return accountTransaction;
    }

    private AccountTransactionIntSrcAttr createAccountTransactionIntSrc(String description, int deskId)
    {
        AccountTransactionIntSrcAttr accountTransaction = new AccountTransactionIntSrcAttr();
        accountTransaction.setDeskId(deskId);
        accountTransaction.setTransactionDate(new Timestamp(System.currentTimeMillis()));
        accountTransaction.setTransactionDescription(description);
        return accountTransaction;
    }

    public void testIntSourceTablePartitionManager() throws Exception
    {
        // insert one
        createAccountTransactionIntSrc( "transaction 1", 0 ).insert();
        // insert list with mixed sources
        AccountTransactionIntSrcAttrList list = new AccountTransactionIntSrcAttrList();
        list.add(createAccountTransactionIntSrc( "transaction 2", 0 ));
        list.add(createAccountTransactionIntSrc( "transaction 3", 1 ));
        list.add(createAccountTransactionIntSrc( "transaction 4", 0 ));
        list.add(createAccountTransactionIntSrc( "transaction 5", 1 ));
        list.insertAll();

        // select
        AccountTransactionIntSrcAttrList listA = AccountTransactionIntSrcAttrFinder.findMany( AccountTransactionIntSrcAttrFinder.deskId().eq(0) );
        listA.addOrderBy(AccountTransactionIntSrcAttrFinder.transactionId().ascendingOrderBy());

        assertEquals(3, listA.size());
        assertEquals(1, listA.get(0).getTransactionId());
        assertEquals(2, listA.get(1).getTransactionId());
        assertEquals(3, listA.get(2).getTransactionId());

        AccountTransactionIntSrcAttrList listB = AccountTransactionIntSrcAttrFinder.findMany(AccountTransactionIntSrcAttrFinder.deskId().eq(1));
        listB.addOrderBy(AccountTransactionIntSrcAttrFinder.transactionId().ascendingOrderBy());

        assertEquals(2, listB.size());
        assertEquals(4, listB.get(0).getTransactionId());
        assertEquals(5, listB.get(1).getTransactionId());

        // delete on one source
        AccountTransactionIntSrcAttrFinder.findMany(AccountTransactionIntSrcAttrFinder.deskId().eq(0).and(AccountTransactionIntSrcAttrFinder.transactionId().lessThanEquals(2))).deleteAll();

        // refresh after the delete, one item left
        listA.forceRefresh();
        assertEquals(1, listA.size());

        AccountTransactionIntSrcAttr transaction = listA.get(0);
        // update
        transaction.setTransactionDescription( "new description" );

        // select from multiple sources
        AccountTransactionIntSrcAttrList listAB = AccountTransactionIntSrcAttrFinder.findMany(AccountTransactionIntSrcAttrFinder.deskId().in(IntHashSet.newSetWith(new int[]{0,1})));
        assertEquals(3, listAB.size());
        listAB.deleteAll();

        // delete with multiple sources
        listAB.forceRefresh();
        assertEquals(0, listAB.size());
    }

    public void testExpectedTablesAreCreated() throws Exception
    {
        Set<String> createdTables = ConnectionManagerForTestsWithTableManager.getInstance().getCreatedTables();

        UnifiedSet<String> expectedTables = UnifiedSet.newSet();

        for(MithraRuntimeCacheController controller : MithraManager.getInstance().getConfigManager().getRuntimeCacheControllerSet())
        {
            String table = controller.getFinderInstanceFromFinderClass().getMithraObjectPortal().getDatabaseObject().getDefaultTableName();

            if(controller.getFinderInstanceFromFinderClass().getSourceAttributeType() == null )
            {
                expectedTables.add(ConnectionManagerForTestsWithTableManager.getInstance().getTableName(table));
            }
            else if(controller.getFinderInstanceFromFinderClass().getSourceAttributeType().isIntSourceAttribute())
            {
                expectedTables.add(ConnectionManagerForTestsWithTableManager.getInstance().getTableName(table, 0));
                expectedTables.add(ConnectionManagerForTestsWithTableManager.getInstance().getTableName(table, 1));
            }
            else
            {
                expectedTables.add(ConnectionManagerForTestsWithTableManager.getInstance().getTableName(table, "A"));
                expectedTables.add(ConnectionManagerForTestsWithTableManager.getInstance().getTableName(table, "B"));
            }
        }

        assertEquals( expectedTables, createdTables );
    }
}
