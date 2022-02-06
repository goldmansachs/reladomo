

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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Account;
import com.gs.fw.common.mithra.test.domain.AccountFinder;
import com.gs.fw.common.mithra.test.domain.AccountList;
import com.gs.fw.common.mithra.test.domain.Book;
import com.gs.fw.common.mithra.test.domain.BookFinder;
import com.gs.fw.common.mithra.test.domain.BookList;
import com.gs.fw.common.mithra.test.domain.Group;
import com.gs.fw.common.mithra.test.domain.Location;
import com.gs.fw.common.mithra.test.domain.LocationFinder;
import com.gs.fw.common.mithra.test.domain.Manufacturer;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductFinder;
import com.gs.fw.common.mithra.test.domain.ProductSynonym;
import com.gs.fw.common.mithra.test.domain.Profile;
import com.gs.fw.common.mithra.test.domain.Trial;
import com.gs.fw.common.mithra.test.domain.TrialFinder;
import com.gs.fw.common.mithra.test.domain.TrialList;
import com.gs.fw.common.mithra.test.domain.User;
import com.gs.fw.common.mithra.test.domain.UserFinder;
import com.gs.fw.common.mithra.test.domain.UserList;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestBasicRetrieval
        extends MithraTestAbstract
{
    static private Logger logger = LoggerFactory.getLogger(MithraTestAbstract.class.getName());

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Trial.class,
            Location.class,
            User.class,
            Order.class,
            OrderItem.class,
            Book.class,
            Manufacturer.class,
            Account.class,
            Group.class,
            Profile.class,
            Product.class,
            ProductSynonym.class,
            Account.class
        };
    }

    public TestBasicRetrieval()
    {
        super("Mithra Object Tests");
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new TrialResultSetComparator());
    }

    public void testRetrievingFromMultipleDesksUsingInOperation()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new AccountTestResultSetComparator());
        String sql = "select * from ACCOUNT where PNLGROUP_ID = '999A'";
        Set deskIdSet = new HashSet();
        deskIdSet.add("A");
        deskIdSet.add("B");
        Operation op = AccountFinder.deskId().in(deskIdSet);
        op = op.and(AccountFinder.pnlGroupId().eq("999A"));

        AccountList mithraList = new AccountList(op);
        AccountList directSqlMithraList = new AccountList();

        Connection conA = this.getConnection("A");
        Connection conB = this.getConnection("B");
        try
        {
            PreparedStatement psA = conA.prepareStatement(sql);
            PreparedStatement psB = conB.prepareStatement(sql);
            ResultSet rsA = psA.executeQuery();
            ResultSet rsB = psB.executeQuery();

            directSqlMithraList.addAll(this.getAccountListFromResultSet(rsA));
            directSqlMithraList.addAll(this.getAccountListFromResultSet(rsB));
            this.genericRetrievalTest(directSqlMithraList, mithraList);
        }
        finally
        {
            conA.close();
            conB.close();
        }
    }

    public void testRetrievingFromMultipleDesksUsingInOperationInParallel()
              throws SQLException
      {
          this.setMithraTestObjectToResultSetComparator(new AccountTestResultSetComparator());
          String sql = "select * from ACCOUNT where PNLGROUP_ID in ('999A', '999B', '888A', '888B')";
          Set pnlGroup = new HashSet();
          pnlGroup.add("999A");
          pnlGroup.add("999B");
          pnlGroup.add("888A");
          pnlGroup.add("888B");
          for (int i = 0; i < 1000; i++)
          {
              pnlGroup.add(String.valueOf(i));
          }
          Set deskIdSet = new HashSet();
          deskIdSet.add("A");

          Operation op = AccountFinder.deskId().in(deskIdSet);
          op = op.and(AccountFinder.pnlGroupId().in(pnlGroup));

          AccountList mithraList = new AccountList(op);
          mithraList.setNumberOfParallelThreads(10);
          AccountList directSqlMithraList = new AccountList();

          Connection conA = this.getConnection("A");
          try
          {
              PreparedStatement psA = conA.prepareStatement(sql);
              ResultSet rsA = psA.executeQuery();

              directSqlMithraList.addAll(this.getAccountListFromResultSet(rsA));
              this.genericRetrievalTest(directSqlMithraList, mithraList);
          }
          finally
          {
              conA.close();
          }
      }



    private List getAccountListFromResultSet(ResultSet rs)
            throws SQLException
    {
        AccountList list = new AccountList();

        while(rs.next())
        {
            Account account = (Account)this.getMithraTestObjectToResultSetComparator().createObjectFrom(rs);
            list.add(account);
        }
        return list;
    }

    public void testRetrieveOneRow()
            throws SQLException
    {
        String trialId = "001A";
        String sql = "select * from TRIAL where TRIAL_ID = '" + trialId + "'";

        Trial trial = TrialFinder.findOne(TrialFinder.trialId().eq(trialId));
        TrialList trialList = new TrialList();
        trialList.add(trial);
        this.genericRetrievalTest(sql, trialList);
    }

    public void testRetrieveOneRowWithTrim()
            throws SQLException
    {
        String trialId = "001A";
        String sql = "select * from TRIAL where TRIAL_ID = '" + trialId + "'";

        Trial trial = TrialFinder.findOne(TrialFinder.trialId().eqWithTrim(trialId+"   "));
        TrialList trialList = new TrialList();
        trialList.add(trial);
        this.genericRetrievalTest(sql, trialList);
    }

    public void testEmptyInOperation()
            throws SQLException
    {
        TrialList trialList = new TrialList(TrialFinder.trialId().in(new HashSet()));
        trialList.forceResolve();
        assertTrue(trialList.size() == 0);
    }

    public void testNoOperation()
    {
        String trialId = "001A";
        TrialList trialList1 = new TrialList(TrialFinder.trialId().eq(trialId));
        TrialList trialList2 = new TrialList(TrialFinder.trialId().eq(trialId).and(NoOperation.instance()));
        assertTrue(trialList1.size() == trialList2.size());
    }

    public void testTrimString()
            throws SQLException
    {
        Location location = LocationFinder.findOne(LocationFinder.id().eq(1).and(LocationFinder.sourceId().eq(0)));
        assertTrue(location.getCity().equals("Bangalore"));
        location = LocationFinder.findOne(LocationFinder.id().eq(1).and(LocationFinder.sourceId().eq(0)));
        assertTrue(location.getGeographicLocation().equals("Asia"));
    }

    public void testRetrieveMultipleRowsOneAtATime()
            throws SQLException
    {
        String[] trialId = {"001A", "001B", "999Z"};

        for (int i = 0; i < trialId.length; i++)
        {
            String sql = "select * from TRIAL where TRIAL_ID = '" + trialId[i] + "'";

            Trial trial = TrialFinder.findOne(TrialFinder.trialId().eq(trialId[i]));
            TrialList trialList = new TrialList();
            trialList.add(trial);

            this.genericRetrievalTest(sql, trialList);
        }
    }

    public void testRetrieveOneRowsUsingAnd()
            throws SQLException
    {
        String directSql = "select * from TRIAL where TRIAL_ID = '001N' and DESCRIPTION = 'N-Trial'";

        TrialList trialList = new TrialList(TrialFinder.description().eq("N-Trial").and(TrialFinder.trialId().eq("001N")));
        this.genericRetrievalTest(directSql, trialList);

        trialList = new TrialList(TrialFinder.trialId().eq("001N").and(TrialFinder.description().eq("N-Trial")));
        this.genericRetrievalTest(directSql, trialList);
    }

    public void testRetrieveMultipleRowsAtOnce()
            throws Exception
    {
        String directSql = "select * from TRIAL where DESCRIPTION = 'N-Trial'";
        TrialList trialList = new TrialList(TrialFinder.description().eq("N-Trial"));

        this.genericRetrievalTest(directSql, trialList);
    }


    public void testRetrieveMultipleRowsUsingIn()
            throws SQLException
    {
        String directSql = "select * from TRIAL where TRIAL_ID in ('001A', '001B')";

        HashSet trialSet = new HashSet();
        trialSet.add("001A");
        trialSet.add("001B");

        TrialList trialList = new TrialList(TrialFinder.trialId().in(trialSet));

        this.genericRetrievalTest(directSql, trialList);
    }

    public void testRetrieveAllRows()
            throws SQLException
    {
        String directSql = "select * from TRIAL";
        TrialList trialList = new TrialList(TrialFinder.all());

        this.genericRetrievalTest(directSql, trialList);
    }

    public void testNullablePrimitiveAttributes()
    {
        User user = UserFinder.findOne(UserFinder.id().eq(1).and(UserFinder.sourceId().eq(0)));
        if (!user.isActiveNull())
        {
            fail("improper inflation of nullable primitive attribute when default is provided");
        }
        if (!user.isActive())
        {
            fail("improper inflation of nullable primitive attribute when default is provided. Default value is not available");
        }
        try
        {
            user.getDefaultGroupId();
            fail("null primitive attribute accessor must throw exception if a default is not set");
        }
        catch (MithraBusinessException e)
        {
            //the whole point is to make sure this exception is thrown
        }
        assertTrue(user.isDefaultGroupIdNull());
    }

    public void testListSize() throws SQLException
    {
        Connection conn = this.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from user_tbl where active = TRUE");
        rs.next();
        int expectedSize = rs.getInt(1);
        conn.close();

        UserList list = new UserList(UserFinder.active().eq(true).and(UserFinder.sourceId().eq(0)));

        assertEquals(expectedSize, list.size());
        //resolve the list
        User[] users = list.elements();
        assertEquals(expectedSize, users.length);
    }

    public void testListCount() throws SQLException
    {
        Connection conn = this.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from user_tbl where active = TRUE");
        rs.next();
        int expectedSize = rs.getInt(1);
        conn.close();

        UserList list = new UserList(UserFinder.active().eq(true).and(UserFinder.sourceId().eq(0)));

        assertTrue(list.count() == expectedSize);
        //resolve the list
        User[] users = list.elements();
        assertTrue(users.length == expectedSize);
    }

    public void testGetAttributeByName()
    {
        User user = UserFinder.findOne(UserFinder.id().eq(1).and(UserFinder.sourceId().eq(0)));
        assertNotNull(user);
        assertNotNull(user.getProfile());
        Function nameSelector = UserFinder.getAttributeByName("name");
        assertNotNull(nameSelector);
        assertEquals(user.getName(), nameSelector.valueOf(user));
        Function profileTypeSelector = UserFinder.getAttributeByName("profile.type");
        assertNotNull(profileTypeSelector);
        assertEquals(user.getProfile().getType(), profileTypeSelector.valueOf(user));
        assertNull(UserFinder.getAttributeByName("xyz"));
        assertNull(UserFinder.getAttributeByName("abc.name"));
        assertNull(UserFinder.getAttributeByName("profile.xyx"));

        Function relationshipOnly = UserFinder.getAttributeOrRelationshipSelector("profile");
        assertNotNull(relationshipOnly);
        assertEquals(user.getProfile(), relationshipOnly.valueOf(user));

        OrderFinder.items().productId();
        assertNull(OrderFinder.getAttributeOrRelationshipSelector("items.productId"));
        assertNotNull(OrderFinder.getAttributeOrRelationshipSelector("items"));

        assertNotNull(BookFinder.manufacturer().location());
        assertNotNull(BookFinder.manufacturer().location().city());
    }

    public void testBypassCacheOnList()
            throws SQLException
    {
        String directSql = "select * from TRIAL where TRIAL_ID in ('001A', '001B')";

        HashSet trialSet = new HashSet();
        trialSet.add("001A");
        trialSet.add("001B");

        TrialList trialList = new TrialList(TrialFinder.trialId().in(trialSet));

        this.genericRetrievalTest(directSql, trialList, false);

        TrialList trialList2 = new TrialList(TrialFinder.trialId().in(trialSet));
        trialList2.setBypassCache(true);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        trialList2.forceResolve();
        assertEquals(count+1, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        this.genericRetrievalTest(directSql, trialList2, false);

        TrialList trialList3 = new TrialList(TrialFinder.trialId().in(trialSet));
        count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        trialList3.forceResolve();
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testCacheOnFindAll()
            throws SQLException
    {
        TrialList trials1 = new TrialList(TrialFinder.all());
        trials1.forceResolve();

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        TrialList trials2 = new TrialList(TrialFinder.all());
        trials2.forceResolve();

        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testBypassCacheOnFindOne()
            throws SQLException
    {
        Trial trial = TrialFinder.findOne(TrialFinder.trialId().eq("001A"));

        assertNotNull(trial);

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Trial trial2 = TrialFinder.findOneBypassCache(TrialFinder.trialId().eq("001A"));
        assertNotNull(trial2);
        assertEquals(count+1, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Trial trial3 = TrialFinder.findOne(TrialFinder.trialId().eq("001A"));
        assertNotNull(trial3);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testDeepFetchBypassCache() throws SQLException
    {
        String directSql = "select * from TRIAL where TRIAL_ID in ('001A', '001B')";

        HashSet trialSet = new HashSet();
        trialSet.add("001A");
        trialSet.add("001B");

        TrialList trialList = new TrialList(TrialFinder.trialId().in(trialSet));
        trialList.deepFetch(TrialFinder.accountsFromA());
        this.genericRetrievalTest(directSql, trialList, false);

        TrialList trialList2 = new TrialList(TrialFinder.trialId().in(trialSet));
        trialList2.setBypassCache(true);
        trialList2.deepFetch(TrialFinder.accountsFromA());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        trialList2.forceResolve();
        assertEquals(count+2, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        this.genericRetrievalTest(directSql, trialList2, false);

        TrialList trialList3 = new TrialList(TrialFinder.trialId().in(trialSet));
        trialList3.deepFetch(TrialFinder.accountsFromA());
        count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        trialList3.forceResolve();
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testDeepFetchOneToManyWithoutSourceId()
    {
        H2DatabaseType.getInstance().setQuoteTableName(true);
        if (OrderFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            OrderList order = new OrderList(OrderFinder.orderId().eq(1));
            order.deepFetch(OrderFinder.items());
            order.forceResolve();
            assertTrue(MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() - count > 1);
        }
        H2DatabaseType.getInstance().setQuoteTableName(false);
    }

    public void testDeepFetchManyToOneWithoutSourceId()
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            BookList books = new BookList(BookFinder.inventoryId().eq(1));
            books.deepFetch(BookFinder.manufacturer());
            books.forceResolve();
            assertTrue(MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() - count > 1);
        }
    }

    public void testClearReadOnlyCache()
    {
        if (TrialFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            String trialId = "001A";

            Trial trial = TrialFinder.findOne(TrialFinder.trialId().eq(trialId));
            assertNotNull(trial);
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            assertSame(trial, TrialFinder.findOne(TrialFinder.trialId().eq(trialId)));
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            TrialFinder.clearQueryCache();
            assertSame(trial, TrialFinder.findOne(TrialFinder.trialId().eq(trialId)));
            assertTrue(count < MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testMaxObjectsToRetrieve()
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            OrderList firstList = OrderFinder.findMany(OrderFinder.orderId().greaterThan(0));
            firstList.setMaxObjectsToRetrieve(1);
            assertEquals(1, firstList.size());
            assertTrue(firstList.reachedMaxObjectsToRetrieve());
            // make sure we don't cache the partial result
            OrderList secondList = OrderFinder.findMany(OrderFinder.orderId().greaterThan(0));
            assertTrue(secondList.size() > 1);
        }
        else
        {
            OrderList firstList = OrderFinder.findMany(OrderFinder.orderId().greaterThan(0));
            firstList.setMaxObjectsToRetrieve(1);
            assertTrue(firstList.size() > 1);
        }
    }

    public void testFindOneThrowsException()
    {
        try
        {
            TrialFinder.findOne(TrialFinder.trialId().startsWith("0"));
            fail("find one did not throw an exception");
        }
        catch(MithraBusinessException e)
        {
            logger.info("find one correctly threw an exception", e);
        }
    }

    public void testToOneFilteredRetrieval()
    {
        Product p = ProductFinder.findOne(ProductFinder.synonymByType("CUS").synonymValue().eq("X"));
        assertEquals(1, p.getProductId());
    }

    public void testToOneFilteredMapperNotExistsOr()
    {
        Operation op = ProductFinder.synonymByType("PRM").notExists().or(ProductFinder.synonymByType("PRM").synonymValue().notEq("1234"));
        op = op.and(ProductFinder.productId().lessThan(3));
        assertEquals(2, ProductFinder.findOne(op).getProductId());
    }

    public void testToManyFilteredMapper()
    {
        UnifiedSet<String> set = UnifiedSet.newSetWith("CUS", "GSN");
        Product p = ProductFinder.findOne(ProductFinder.synonymByTypes(set).synonymValue().eq("X"));
        assertEquals(1, p.getProductId());
    }

    public void testSamePkAndIndex()
    {
        Account account = AccountFinder.findByPrimaryKey("7410161001", "A");
        assertEquals("7410161001", account.getAccountNumber());

        Account account2 = AccountFinder.findByAccountNumber("7410161001", "A");
        assertEquals("7410161001", account2.getAccountNumber());

        Account account3 = AccountFinder.findByAccountNumber2("7410161001", "A");
        assertEquals("7410161001", account3.getAccountNumber());
    }

    public void testSameIndexDifferentOrder()
    {
        Account account = AccountFinder.findByAccountNumberAndCode("7410161001", "74101610", "A");
        assertEquals("7410161001", account.getAccountNumber());

        Account account2 = AccountFinder.findByAccountNumberAndCode2("74101610", "7410161001", "A");
        assertEquals("7410161001", account2.getAccountNumber());
    }
}
