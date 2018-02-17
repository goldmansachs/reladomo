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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.cache.CacheClock;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.domain.Account;
import com.gs.fw.common.mithra.test.domain.AccountFinder;
import com.gs.fw.common.mithra.test.domain.AccountList;
import com.gs.fw.common.mithra.test.domain.AuditedTree;
import com.gs.fw.common.mithra.test.domain.AuditedTreeFinder;
import com.gs.fw.common.mithra.test.domain.AuditedTreeList;
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItem;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemList;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderList;
import com.gs.fw.common.mithra.test.domain.BitemporalProductCategory;
import com.gs.fw.common.mithra.test.domain.BitemporalProductCategoryList;
import com.gs.fw.common.mithra.test.domain.Book;
import com.gs.fw.common.mithra.test.domain.BookFinder;
import com.gs.fw.common.mithra.test.domain.BookList;
import com.gs.fw.common.mithra.test.domain.ExchangeRate;
import com.gs.fw.common.mithra.test.domain.ExchangeRateChild;
import com.gs.fw.common.mithra.test.domain.ExchangeRateFinder;
import com.gs.fw.common.mithra.test.domain.ExchangeRateList;
import com.gs.fw.common.mithra.test.domain.Group;
import com.gs.fw.common.mithra.test.domain.GroupFinder;
import com.gs.fw.common.mithra.test.domain.GroupList;
import com.gs.fw.common.mithra.test.domain.GsDesk;
import com.gs.fw.common.mithra.test.domain.GsDeskFinder;
import com.gs.fw.common.mithra.test.domain.Location;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderItemStatus;
import com.gs.fw.common.mithra.test.domain.OrderItemStatusFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemWi;
import com.gs.fw.common.mithra.test.domain.OrderItemWiFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.OrderParentToChildren;
import com.gs.fw.common.mithra.test.domain.OrderStatus;
import com.gs.fw.common.mithra.test.domain.OrderStatusFinder;
import com.gs.fw.common.mithra.test.domain.OrderStatusList;
import com.gs.fw.common.mithra.test.domain.OrderStatusWi;
import com.gs.fw.common.mithra.test.domain.OrderWi;
import com.gs.fw.common.mithra.test.domain.OrderWiFinder;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductFinder;
import com.gs.fw.common.mithra.test.domain.ProductList;
import com.gs.fw.common.mithra.test.domain.ProductSynonym;
import com.gs.fw.common.mithra.test.domain.Profile;
import com.gs.fw.common.mithra.test.domain.ProfileFinder;
import com.gs.fw.common.mithra.test.domain.ProfileList;
import com.gs.fw.common.mithra.test.domain.SelfJoinConstant;
import com.gs.fw.common.mithra.test.domain.SelfJoinConstantFinder;
import com.gs.fw.common.mithra.test.domain.SelfJoinConstantList;
import com.gs.fw.common.mithra.test.domain.Supplier;
import com.gs.fw.common.mithra.test.domain.SupplierInventoryItem;
import com.gs.fw.common.mithra.test.domain.SupplierInventoryItemList;
import com.gs.fw.common.mithra.test.domain.SupplierList;
import com.gs.fw.common.mithra.test.domain.TestAsOfToTimestampJoinObjectA;
import com.gs.fw.common.mithra.test.domain.TestAsOfToTimestampJoinObjectAFinder;
import com.gs.fw.common.mithra.test.domain.TestAsOfToTimestampJoinObjectB;
import com.gs.fw.common.mithra.test.domain.TestAsOfToTimestampJoinObjectBFinder;
import com.gs.fw.common.mithra.test.domain.TestBankingDealDetailImpl;
import com.gs.fw.common.mithra.test.domain.TestCheckGsDesk;
import com.gs.fw.common.mithra.test.domain.TestConflictCheckImpl;
import com.gs.fw.common.mithra.test.domain.TestConflictCheckImplFinder;
import com.gs.fw.common.mithra.test.domain.TestConflictCheckImplList;
import com.gs.fw.common.mithra.test.domain.TestNonAcquisitionDealDetail;
import com.gs.fw.common.mithra.test.domain.TestSecuritiesDealDetailImpl;
import com.gs.fw.common.mithra.test.domain.TestSecuritiesDealDetailImplFinder;
import com.gs.fw.common.mithra.test.domain.TestSecuritiesDealDetailImplList;
import com.gs.fw.common.mithra.test.domain.TestSellSideDealDetail;
import com.gs.fw.common.mithra.test.domain.Trial;
import com.gs.fw.common.mithra.test.domain.TrialFinder;
import com.gs.fw.common.mithra.test.domain.User;
import com.gs.fw.common.mithra.test.domain.UserFinder;
import com.gs.fw.common.mithra.test.domain.UserGroup;
import com.gs.fw.common.mithra.test.domain.UserList;
import com.gs.fw.common.mithra.test.glew.LewContract;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.test.glew.LewContractList;
import com.gs.fw.common.mithra.test.glew.LewTransaction;
import com.gs.fw.common.mithra.test.glew.LewTransactionFinder;
import com.gs.fw.common.mithra.test.util.Log4JRecordingAppender;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import org.apache.log4j.spi.LoggingEvent;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TestRelationships extends MithraTestAbstract
{

    public TestRelationships(String s)
    {
        super(s);
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AuditedTree.class,
            Trial.class,
            Location.class,
            User.class,
            Order.class,
            BitemporalOrder.class,
            OrderParentToChildren.class,
            OrderItem.class,
            BitemporalOrderItem.class,
            BitemporalProductCategory.class,
            Product.class,
            ProductSynonym.class,
            OrderStatus.class,
            OrderItemStatus.class,
            OrderWi.class,
            OrderItemWi.class,
            OrderStatusWi.class,
            SelfJoinConstant.class,
            Book.class,
            Supplier.class,
            Group.class,
            Profile.class,
            Location.class,
            SupplierInventoryItem.class,
            Account.class,
            UserGroup.class,
            TestConflictCheckImpl.class,
            TestBankingDealDetailImpl.class,
            TestSecuritiesDealDetailImpl.class,
            TestNonAcquisitionDealDetail.class,
            TestSellSideDealDetail.class,
            TestCheckGsDesk.class,
            GsDesk.class,
            ExchangeRate.class,
            ExchangeRateChild.class,
            LewContract.class,
            LewTransaction.class,
            TestAsOfToTimestampJoinObjectA.class,
            TestAsOfToTimestampJoinObjectB.class
        };
    }

    public void testNone()
    {
        OrderList list = new OrderList(new None(OrderFinder.orderId()));
        assertEquals(0, list.size());
        assertEquals(0, list.getItems().size());

        AuditedTreeList atl = new AuditedTreeList(new None(AuditedTreeFinder.name()));
        assertEquals(0, atl.size());
        assertEquals(0, atl.getChildren().size());

        LewContractList lct = new LewContractList(new None(LewContractFinder.acctId()));
        assertEquals(0, lct.size());
        assertEquals(0, lct.getLewTransactions().size());

        LewContractList lct2 = LewContractFinder.findMany(LewContractFinder.acctId().in(new IntHashSet()));
        assertEquals(0, lct2.size());
        assertEquals(0, lct2.getLewTransactions().size());

        BitemporalOrderList bol = new BitemporalOrderList(new None(BitemporalOrderFinder.orderId()));
        assertEquals(0, bol.size());
        assertEquals(0, bol.getItems().size());
    }

    public void testRecursiveNotExists()
    {
        OrderItem item = new OrderItem();
        item.setOrderId(55);
        item.setId(55);
        item.insert();

        Operation op = OrderFinder.items().orderItemStatus().recursiveNotExists();

        Operation existsOp = OrderFinder.items().orderItemStatus().exists();

        OrderList notExistsList = OrderFinder.findMany(op);
        OrderList existsList = OrderFinder.findMany(existsOp);

        assertTrue(notExistsList.size() > 0);
        assertTrue(existsList.size() > 0);

        OrderList all = OrderFinder.findMany(OrderFinder.all());
        assertEquals(all.size(), notExistsList.size() + existsList.size());

        for(int i=0;i<notExistsList.size();i++)
        {
            assertFalse(existsList.contains(notExistsList.get(i)));
        }
    }

    public void testNotExistsWithOr()
    {
        OrderItem item = new OrderItem();
        item.setOrderId(55);
        item.setId(55);
        item.insert();

        OrderList list = OrderFinder.findMany(OrderFinder.items().notExists().or(OrderFinder.items().orderItemStatus().notExists()));
        assertEquals(6, list.size());
    }

    public void testManyToOne()
    {
        User user = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        assertTrue(user.getProfile().getId() == 1);
    }

    public void testDeepValueOf()
    {
        ProductList list = (ProductList) ((Function)OrderFinder.items().productInfo()).valueOf(OrderFinder.findOne(OrderFinder.orderId().eq(1)));
        assertTrue(list.size() > 0);
    }

    public void testCyclicDependency()
    {
        User user = UserFinder.findOne(UserFinder.userId().eq("rezeam").and(UserFinder.sourceId().eq(0)));
        assertEquals("John Fathers", user.getDefaultGroup().getManager().getName());
    }

    public void testSelfJoinThroughRelationshipTable()
    {
        User user = UserFinder.findOne(UserFinder.userId().eq("rezeam").and(UserFinder.sourceId().eq(0)));
        assertEquals("John Fathers", user.getDefaultGroupManager().getName());
    }

    public void testDependentToOneRelationshipInMemory()
    {
        Order order = new Order();
        order.setOrderId(1);
        assertNull(order.getOrderStatus());
    }

    public void testQueryCache()
    {
        User user1 = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        User user2 = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        MithraPerformanceData data = UserFinder.getMithraObjectPortal().getPerformanceData();
        assertTrue(data.getQueryCacheHits() > 0);
        assertTrue(user1 == user2);
        user2 = UserFinder.findOne(UserFinder.id().eq(1).and(UserFinder.sourceId().eq(0)));
        assertTrue(user1 == user2);
        assertTrue(data.getObjectCacheHits() > 0);
    }

    public void testManyToMany() throws SQLException
    {
        User user = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        GroupList groups = user.getGroups();
        assertTrue(groups.notEmpty());
        compareGroups("select a.objectid from group_tbl a, " +
                "user_group b where " +
                "b.user_oid = 1 " +
                "and a.objectId = b.group_oid " +
                "order by name desc", groups);

        for (int i = 0; i < groups.size(); i++)
        {
            Group  group = groups.getGroupAt(i);
            assertTrue(group.getUsers().contains(user));
        }
    }

    public void testChainedRelationship() throws SQLException
    {
        UserList users = new UserList(UserFinder.groups().locations().city().eq("New York").and(UserFinder.sourceId().eq(0)));
        assertTrue(users.size() > 0);
    }

    /*
    public void testManyToManyWithSourceId() throws Exception
    {
        User user = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        GroupList groups = user.getSourceGroups();
        compareGroups("select a.objectid from group_tbl a, " +
                "user_group b where " +
                "b.user_oid = 1 " +
                "and a.objectId = b.group_oid ", user.getSourceGroups());
        for (int i = 0; i < groups.size(); i++)
        {
            Group  group = groups.getGroupAt(i);
            assertTrue(group.getSourceUsers().contains(user));
        }

    }
    */

    private void compareGroups(String sql, GroupList groups)
            throws SQLException
    {
        List actualIds = getActualIds(groups);
        List expectedIds = getIds(sql);
        assertEquals(expectedIds, actualIds);
    }

    private List getActualIds(GroupList groups)
    {
        List actualIds = new ArrayList();
        for (int i = 0; i < groups.size(); i++)
        {
            Group group = groups.getGroupAt(i);
            actualIds.add(new Integer(group.getId()));
        }
        return actualIds;
    }

    private List getIds(String sql)
            throws SQLException
    {
        Connection conn = this.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        List expectedNames = new ArrayList();
        while(rs.next())
        {
            expectedNames.add(new Integer(rs.getInt(1)));
        }
        conn.close();
        return expectedNames;
    }

    public void testOneToManyWithDifferentSource() throws SQLException
    {
        String trialId = "001A";
        Trial trial = TrialFinder.findOne(TrialFinder.trialId().eq(trialId));
        AccountList accounts = trial.getAccountsFromB();
        List expected = getExpectedAccounts(getConnection(SOURCE_B), trialId);
        List actual = new ArrayList();
        for (int i = 0; i < accounts.size(); i++)
        {
            Account account = accounts.getAccountAt(i);
            actual.add(account.getAccountNumber());
            assertNull(account.getTrialA());
            assertEquals(account.getTrialB(), trial);
        }
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

    private List getExpectedAccounts(Connection conn, String trialId)
            throws SQLException
    {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select ACCOUNT_NUMBER from account where trial_id = '" + trialId + "' order by ACCOUNT_NUMBER");
        List expected = new ArrayList();
        while(rs.next())
        {
            expected.add(rs.getString(1));
        };
        conn.close();
        return expected;
    }

    public void testDeepFetchToOne()
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.profile());
        ul.forceResolve();
        int oldCount = dbCalls();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            assertNotNull(u.getProfile());
        }
        assertEquals(oldCount, dbCalls());
    }

    public void testDeepFetchToOneToMany()
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.profile().users());
        ul.forceResolve();
        int oldCount = dbCalls();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            u.getProfile().getUsers().forceResolve();
        }
        assertEquals(oldCount, dbCalls());
    }

    public void testLateDeepFetchToOne()
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.forceResolve();
        ul.deepFetch(UserFinder.profile());
        int oldCount = dbCalls();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            assertNotNull(u.getProfile());
        }
        assertEquals(oldCount, dbCalls());
    }

    public void testLateDeepFetchToOneToMany()
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.forceResolve();
        ul.deepFetch(UserFinder.profile().users());
        int oldCount = dbCalls();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            u.getProfile().getUsers().forceResolve();
        }
        assertEquals(oldCount, dbCalls());
    }

    public void testDeepFetchToOneWithPreload()
    {
        ProfileList pl = new ProfileList(ProfileFinder.all().and(ProfileFinder.sourceId().eq(0)));
        pl.forceResolve();
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.profile());
        int oldCount = dbCalls();
        ul.forceResolve();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            assertNotNull(u.getProfile());
        }
        if (UserFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            assertEquals(oldCount, dbCalls());
        }
        else
        {
            assertEquals(oldCount + 1, dbCalls());
        }
    }

    public void testDeepFetchOneToMany()
    {
        GroupList gl = new GroupList(GroupFinder.id().lessThan(3).and(GroupFinder.sourceId().eq(0)));
        gl.deepFetch(GroupFinder.defaultUsers());
        gl.forceResolve();
        int oldCount = dbCalls();
        for(Iterator it = gl.iterator(); it.hasNext(); )
        {
            Group g = (Group) it.next();
            g.getDefaultUsers().forceResolve();
        }
        assertEquals(oldCount, dbCalls());
    }

	public void testRelationshipWithStringInOperation() throws SQLException
	{
		Book book = BookFinder.findOne(BookFinder.inventoryId().eq(1));
		SupplierList suppliersList = book.getSuppliersWithSpecificLocation();
		suppliersList.forceResolve();
		String sql = "select count(distinct C.SUPPLIER_ID) from BOOK A, SUPPLIER C, SUPPLIER_INVENTORY_ITEM D " +
				"where D.INVENTORY_ID = 1 and D.SUPPLIER_ID = C.SUPPLIER_ID and C.LOCATION in (?, ?)";
		Connection conn = getConnection();
		int count;
		try
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "New York");
			pstmt.setString(2, "London");
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			count = rs.getInt(1);
		}
		finally
		{
			if(conn != null) conn.close();
        }
		assertEquals(suppliersList.size(), count);
	}

	public void testRelationshipMixedInOperation() throws SQLException
	{
		Book book = BookFinder.findOne(BookFinder.inventoryId().eq(1));
		SupplierList suppliersList = book.getSuppliersSpecificIdAndLocation();
		suppliersList.forceResolve();
		String sql = "select count(distinct C.SUPPLIER_ID) from BOOK A, SUPPLIER C, SUPPLIER_INVENTORY_ITEM D " +
				"where D.INVENTORY_ID = 1 and D.SUPPLIER_ID = C.SUPPLIER_ID and C.LOCATION in (?, ?) and C.SUPPLIER_ID in (?, ?)";
		Connection conn = getConnection();
		int count;
		try
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "New York");
			pstmt.setString(2, "London");
			pstmt.setInt(3, 1);
			pstmt.setInt(4, 2);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			count = rs.getInt(1);
		}
		finally
		{
			if(conn != null) conn.close();
        }
		assertEquals(suppliersList.size(), count);
	}

	public void testRelationshipWithIntegerInOperation() throws SQLException
	{
		Book book = BookFinder.findOne(BookFinder.inventoryId().eq(1));
		SupplierList suppliersList = book.getSuppliersWithSpecificId();
		suppliersList.forceResolve();
		String sql = "select count(distinct SUPPLIER_ID) from SUPPLIER_INVENTORY_ITEM " +
				"where INVENTORY_ID = 1 and SUPPLIER_ID in (?, ?)";
		Connection conn = getConnection();
		int count;
		try
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, 1);
			pstmt.setInt(2, 2);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			count = rs.getInt(1);
		}
		finally
		{
            if(conn != null) conn.close();
		}
		assertEquals(suppliersList.size(), count);
	}

    public void testRelationshipLookup()
    {
        assertEquals(BookFinder.manufacturer().location(), BookFinder.getFinderInstance().getRelationshipFinderByName("manufacturer.location"));
    }

    public void testInRelationshipFinder() throws SQLException
	{
		BookList books = new BookList(BookFinder.inventoryId().eq(1).and(BookFinder.supplierInventoryItemWithSpecificId().supplierId().eq(1)));
		books.deepFetch(BookFinder.supplierInventoryItemWithSpecificId());
		String sql = "select count(*) from SUPPLIER_INVENTORY_ITEM " +
				"where INVENTORY_ID = 1 and SUPPLIER_ID = ?";
		Connection conn = getConnection();
		int count;
		try
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, 1);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			count = rs.getInt(1);
		}
		finally
		{
            if(conn != null) conn.close();
		}
		assertEquals(books.size(), count);
	}

    public void testManyToManyRelationshipDeepFetch() throws SQLException
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.groups());
        ul.forceResolve();
        int oldCount = dbCalls();
        checkGroupsFromUsers(ul);
        assertEquals(oldCount, dbCalls());

    }

    public void testManyToManyNotExists()
    {
        UserList ul = new UserList(UserFinder.groups().notExists().and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.groupsWithManagers());
        ul.deepFetch(UserFinder.profile());
        assertEquals(1, ul.size());
    }

    private void checkGroupsFromUsers(UserList ul)
    {
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            u.getGroups().forceResolve();
        }
    }

    private void checkUsersFromGroups(GroupList gl)
    {
        for(int i=0;i<gl.size();i++) gl.get(i).getUsers().forceResolve();
    }

    public void testManyToManyRelationshipTripleDeepFetch() throws SQLException
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.groups().users().groups().users());
        ul.deepFetch(UserFinder.groups().users().groups2());
        ul.forceResolve();
        int oldCount = dbCalls();
        ul.getGroups().forceResolve();
        ul.getGroups().getUsers().forceResolve();
        ul.getGroups().getUsers().getGroups().forceResolve();
        ul.getGroups().getUsers().getGroups().getUsers().forceResolve();
        ul.getGroups().getUsers().getGroups2().forceResolve();
        assertEquals(oldCount, dbCalls());

        oldCount = dbCalls();
        checkGroupsFromUsers(ul);
        checkUsersFromGroups(ul.getGroups());
        checkGroupsFromUsers(ul.getGroups().getUsers());
        checkUsersFromGroups(ul.getGroups().getUsers().getGroups());
        checkGroupsFromUsers(ul.getGroups().getUsers().getGroups().getUsers());
        assertEquals(oldCount, dbCalls());

        oldCount = dbCalls();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            u.getGroups().forceResolve();
            u.getGroups().getUsers().forceResolve();
            u.getGroups().getUsers().getGroups2().forceResolve();
            u.getGroups().getUsers().getGroups().forceResolve();
        }
        assertEquals(oldCount, dbCalls());
    }

    public void testFakeManyToManyRelationshipDeepFetch() throws SQLException
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.userGroups().group());
        ul.forceResolve();
        int oldCount = dbCalls();
        checkGroupsFromUsers(ul);
        assertEquals(oldCount, dbCalls());
    }

    public void testManyToManyRelationshipDeepFetchNegativeCaching() throws SQLException
    {
        UserList ul = new UserList(UserFinder.id().eq(50).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.groups());
        ul.forceResolve();
        int oldCount = dbCalls();
        checkGroupsFromUsers(ul);
        assertEquals(oldCount, dbCalls());
    }

    public void testFakeManyToManyRelationshipDeepFetchNegativeCaching() throws SQLException
    {
        UserList ul = new UserList(UserFinder.id().eq(50).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.userGroups().group());
        ul.forceResolve();
        int oldCount = dbCalls();
        checkGroupsFromUsers(ul);
        assertEquals(oldCount, dbCalls());
    }

    public void testToManyTwice()
    {
        OrderList orders = new OrderList(OrderFinder.orderId().in(IntHashSet.newSetWith(new int[] { 1, 2, 3, 4})));
        orders.deepFetch(OrderFinder.items());
        orders.forceResolve();
        int oldCount = dbCalls();
        orders = new OrderList(OrderFinder.orderId().in(IntHashSet.newSetWith(new int[] { 1, 4})));
        orders.deepFetch(OrderFinder.items());
        orders.forceResolve();
        assertEquals(oldCount, dbCalls());
    }

    public void testFakeManyToManyRelationshipDeepFetch2() throws SQLException
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.userGroups2().group());
        ul.forceResolve();
        int oldCount = dbCalls();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            u.getGroups2().forceResolve();
        }
        assertEquals(oldCount, dbCalls());

    }

    public void testFakeManyToManyRelationshipDeepFetchNegativeCaching2() throws SQLException
    {
        UserList ul = new UserList(UserFinder.id().eq(2).and(UserFinder.sourceId().eq(0)));
        ul.deepFetch(UserFinder.userGroups2().group());
        ul.forceResolve();
        int oldCount = dbCalls();
        for(Iterator it = ul.iterator(); it.hasNext(); )
        {
            User u = (User) it.next();
            u.getGroups2().forceResolve();
        }
        assertEquals(oldCount, dbCalls());

    }

	public void testDeepRelationshipWithInOperation() throws SQLException
	{
		BookList books = new BookList(BookFinder.inventoryId().eq(1));
		books.deepFetch(BookFinder.supplierInventoryItemWithSpecificId());
		Book book = books.getBookAt(0);
		SupplierInventoryItemList suppliersList = book.getSupplierInventoryItemWithSpecificId();
		String sql = "select count(distinct SUPPLIER_ID) from SUPPLIER_INVENTORY_ITEM " +
				"where INVENTORY_ID = 1 and SUPPLIER_ID in (?, ?)";
		Connection conn = getConnection();
		int count;
		try
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, 1);
			pstmt.setInt(2, 2);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			count = rs.getInt(1);
		}
		finally
		{
            if(conn != null) conn.close();
		}
		assertEquals(suppliersList.size(), count);
	}

    public void testParametrizedRelationship()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        assertNotNull(order);
        OrderItem item = order.getItemForProduct(1);
        assertNotNull(item);
        assertEquals(1, item.getProductId());
        item = order.getItemForProduct(2);
        assertNotNull(item);
        assertEquals(2, item.getProductId());
    }

    public void testParametrizedRelationshipDeepFetch()
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        list.deepFetch(OrderFinder.itemForProduct(1));
        list.forceResolve();
        int count = dbCalls();
        for(int i=0;i<list.size();i++)
        {
            Order o = list.getOrderAt(i);
            o.getItemForProduct(1);
        }
        assertEquals(count, dbCalls());
    }

    public void testParametrizedWithNonEqualityRelationshipDeepFetch()
    {
        OrderList list = new OrderList(OrderFinder.orderId().eq(2));
        list.deepFetch(OrderFinder.expensiveItems(11.0));
        list.forceResolve();
        int count = dbCalls();
        assertEquals(1, list.size());
        Order order = list.getOrderAt(0);
        OrderItemList expensiveOrderItems = order.getExpensiveItems(11.0);
        for(int i=0;i<expensiveOrderItems.size();i++)
        {
            assertTrue(expensiveOrderItems.getOrderItemAt(i).getOriginalPrice() >= 11.0);
        }
        assertEquals(count, dbCalls());
    }

    public void testNullAttributeInRelationship()
    {
        AccountList list = new AccountList(AccountFinder.deskId().eq("A").and(AccountFinder.pnlGroupId().eq("NULLTEST")));
        list.deepFetch(AccountFinder.trial());
        list.forceResolve();
    }

    public void testSelfJoinWithConstant1()
    {
        SelfJoinConstantList list = new SelfJoinConstantList(SelfJoinConstantFinder.payReceive().eq("P"));
        list.deepFetch(SelfJoinConstantFinder.matchingReceiveDetail());
        for(int i=0;i<list.size();i++)
        {
            SelfJoinConstant matchingReceiveDetail = list.getSelfJoinConstantAt(i).getMatchingReceiveDetail();
            assertNotNull(matchingReceiveDetail);
            assertEquals("R", matchingReceiveDetail.getPayReceive());
        }
    }

    public void testSelfJoinWithConstant2()
    {
        SelfJoinConstantList list = new SelfJoinConstantList(SelfJoinConstantFinder.payReceive().eq("R"));
        list.deepFetch(SelfJoinConstantFinder.matchingPayDetail());
        for(int i=0;i<list.size();i++)
        {
            SelfJoinConstant matchingPayDetail = list.getSelfJoinConstantAt(i).getMatchingPayDetail();
            assertNotNull(matchingPayDetail);
            assertEquals("P", matchingPayDetail.getPayReceive());
        }
    }

    public void testSelfJoinWithConstant3()
    {
        SelfJoinConstantList list = new SelfJoinConstantList(SelfJoinConstantFinder.referenceNumber().eq("1"));
        list.deepFetch(SelfJoinConstantFinder.matchingPayDetail());
        list.deepFetch(SelfJoinConstantFinder.matchingReceiveDetail());
        for(int i=0;i<list.size();i++)
        {
            SelfJoinConstant matchingPayDetail = list.getSelfJoinConstantAt(i).getMatchingPayDetail();
            assertNotNull(matchingPayDetail);
            assertEquals("P", matchingPayDetail.getPayReceive());
            SelfJoinConstant matchingReceiveDetail = list.getSelfJoinConstantAt(i).getMatchingReceiveDetail();
            assertNotNull(matchingReceiveDetail);
            assertEquals("R", matchingReceiveDetail.getPayReceive());
        }
    }

    public void testRemoveDependentObjectByIndex()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        assertNotNull(order);
        OrderItemList items = order.getItems();
        items.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        int originalSize = items.size();
        assertTrue(originalSize > 1);
        OrderItem item = items.getOrderItemAt(0);
        int orderItemId = item.getId();
        assertSame(items.remove(0), item);
        assertEquals(originalSize - 1, items.size());
        assertEquals(originalSize - 1, order.getItems().size());
        assertNull(OrderItemFinder.findOne(OrderItemFinder.id().eq(orderItemId)));
        assertNull(OrderItemStatusFinder.findOne(OrderItemStatusFinder.itemId().eq(orderItemId)));
    }

    public void testRemoveDependentObjectByObject()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        assertNotNull(order);
        OrderItemList items = order.getItems();
        items.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        int originalSize = items.size();
        assertTrue(originalSize > 2);
        OrderItem item = items.getOrderItemAt(1);
        int orderItemId = item.getId();
        assertTrue(items.remove(item));
        assertEquals(originalSize - 1, items.size());
        assertEquals(originalSize - 1, order.getItems().size());
        assertNull(OrderItemFinder.findOne(OrderItemFinder.id().eq(orderItemId)));
        assertNull(OrderItemStatusFinder.findOne(OrderItemStatusFinder.itemId().eq(orderItemId)));
    }

    public void testDanglingJoinGet()
    {
        User user = UserFinder.findOne(UserFinder.id().eq(40).and(UserFinder.sourceId().eq(0)));
        assertNotNull(user);
        GroupList allGroups = user.getGroups();
        assertEquals(2, allGroups.size());
        GroupList groupsWithManagers = user.getGroupsWithManagers();
        assertEquals(1, groupsWithManagers.size());
    }

    public void testOneToManyDeepFetchOneAtATime()
    {
        OrderItemList items1 = OrderItemFinder.findMany(OrderItemFinder.orderId().eq(1));
        OrderItemList items2 = OrderItemFinder.findMany(OrderItemFinder.orderId().eq(2));
        items1.forceResolve();
        items2.forceResolve();

        int count = dbCalls();
        OrderList orders = OrderFinder.findMany(OrderFinder.orderId().eq(1).or(OrderFinder.orderId().eq(2)));
        orders.deepFetch(OrderFinder.items().productInfo());
        for(int i=0;i<orders.size();i++)
        {
            OrderItemList items = orders.get(i).getItems();
            for(int j=0;j<items.size();j++)
            {
                items.get(j).getProductInfo();
            }
        }
        if (!OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            assertEquals(count + 2, dbCalls());
        }

    }

    public void testDanglingJoinDeepFetch()
    {
        UserList list = new UserList(UserFinder.id().greaterThanEquals(40).and(UserFinder.id().lessThan(42)).and(UserFinder.sourceId().eq(0)));
        list.deepFetch(UserFinder.groupsWithManagers());
        assertEquals(1, list.size());
        int count = dbCalls();
        assertEquals(1, list.getGroupsWithManagers().size());
        User user = list.getUserAt(0);
        GroupList groupsWithManagers = user.getGroupsWithManagers();
        assertEquals(1, groupsWithManagers.size());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testDanglingJoinInOperation()
    {
        UserList list = new UserList(UserFinder.groupsWithManagers().id().eq(3).and(UserFinder.sourceId().eq(0)));
        assertEquals(3, list.size());
    }

    public void testCollapsedDeepFetch()
    {
        OrderItemList list = new OrderItemList();
        for(int i=0;i<2000;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i+1);
            item.setId(i+2000);
            item.setOrderId(1);
            list.add(item);
        }
        list.insertAll();
        OrderItemList list2 = new OrderItemList(OrderItemFinder.discountPrice().lessThan(5000));
        list2.deepFetch(OrderItemFinder.order());
        assertTrue(list2.size() > 1000);
        assertTrue(list2.getOrders().size() < 1000);
    }

    private void createItems()
    {
        OrderItemList list = new OrderItemList();
        for(int i=0;i<2000;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i+1);
            item.setId(i+2000);
            item.setOrderId(i+5000);
            list.add(item);
        }
        list.insertAll();
    }

    private void createOrders()
    {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        OrderList orderList = new OrderList();
        for(int i=0;i<2000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+5000);
            order.setTrackingId("x"+i);
            order.setOrderDate(timestamp);
            orderList.add(order);
        }
        orderList.insertAll();
        OrderFinder.clearQueryCache();
    }

    private void createOrderStatuses()
    {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        OrderStatusList statusList = new OrderStatusList();
        for(int i=0;i<2000;i++)
        {
            OrderStatus status = new OrderStatus();
            status.setOrderId(i+5000);
            status.setLastUser("xyz"+i);
            status.setLastUpdateTime(timestamp);
            statusList.add(status);
        }
        statusList.insertAll();
        OrderStatusFinder.clearQueryCache();
    }

    public void testFlippedDeepFetch()
    {
        createOrders();
        createOrderStatuses();
        createItems();
        Operation op = OrderItemFinder.discountPrice().lessThan(5000);
        op = op.and(OrderItemFinder.order().trackingId().startsWith("x"));
        OrderItemList list2 = new OrderItemList(op);
        list2.deepFetch(OrderItemFinder.order().orderStatus());
        assertTrue(list2.size() > 1000);
        assertEquals(2000, list2.getOrders().size());
    }

    public void testFlippedDeepFetchWithDeepQuery()
    {
        createOrders();
        createOrderStatuses();
        createItems();
        Operation op = OrderItemFinder.discountPrice().lessThan(5000);
        op = op.and(OrderItemFinder.order().orderStatus().lastUser().startsWith("xyz"));
        OrderItemList list2 = new OrderItemList(op);
        list2.deepFetch(OrderItemFinder.order().orderStatus());
        assertTrue(list2.size() > 1000);
        assertEquals(2000, list2.getOrders().size());
    }

    public void testRelationshipNames()
    {
        List<String> relationshipNames = new ArrayList<String>();
        List<String> parameterizedRelationshipNames = new ArrayList<String>();

        relationshipNames.add("items");
        relationshipNames.add("itemsForUserFive");
        relationshipNames.add("orderStatus");
        relationshipNames.add("itemsWithInterfaces");
        relationshipNames.add("orderStatusWithInterfaces");
        relationshipNames.add("productsWithoutInProgress");
        relationshipNames.add("itemsFilterByDate");
        relationshipNames.add("parentToChildAsParent");
        relationshipNames.add("parentToChildAsChild");
        relationshipNames.add("itemsforExistingStatus");
        relationshipNames.add("children");
        relationshipNames.add("parent");
        relationshipNames.add("itemsOrTest");
        relationshipNames.add("itemsInTest");

        parameterizedRelationshipNames.add("itemForProduct");
        parameterizedRelationshipNames.add("itemForProductSet");
        parameterizedRelationshipNames.add("expensiveItems");
        parameterizedRelationshipNames.add("cheapItems");
        parameterizedRelationshipNames.add("cheapItemsWithCheck");

        List<RelatedFinder> relationshipFinders = OrderFinder.allRelatedFinders();
        for(int i = 0 ; i < relationshipFinders.size(); i++)
        {
            RelatedFinder finder = relationshipFinders.get(i);
            String name = ((DeepRelationshipAttribute)finder).getRelationshipName();
            assertTrue(relationshipNames.contains(name));
            assertFalse(parameterizedRelationshipNames.contains(name));
        }

        List<RelatedFinder> dependentRelationshipFinders = OrderFinder.allDependentRelatedFinders();
        for(int i = 0 ; i < dependentRelationshipFinders.size(); i++)
        {
            RelatedFinder finder = dependentRelationshipFinders.get(i);
            String name = ((DeepRelationshipAttribute)finder).getRelationshipName();
            assertTrue(relationshipNames.contains(name));
            assertFalse(parameterizedRelationshipNames.contains(name));
        }
    }

    public void testSelfLoop()
    {
        assertEquals(2, OrderFinder.findMany(OrderFinder.parent().description().eq("Order number five")).size());
    }

    public void testNoneCacheDeepFetch()
    {
        IntHashSet ids = IntHashSet.newSetWith(new int[]{1, 2, 4});
        TestConflictCheckImplList checkList;
        checkList = new TestConflictCheckImplList(TestConflictCheckImplFinder.id().in(ids));

        int count = dbCalls();

        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().sellSideDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().nonAcquisitionDealDetail());

        assertEquals(3, checkList.size());

        assertEquals(4, dbCalls() - count);

        checkList = new TestConflictCheckImplList(TestConflictCheckImplFinder.id().in(ids));

        count = dbCalls();

        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().sellSideDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().nonAcquisitionDealDetail());

        assertEquals(3, checkList.size());

        assertEquals(4, dbCalls() - count);
    }

    public void testNoneCacheDeepFetchInParallel()
    {
        IntHashSet ids = IntHashSet.newSetWith(new int[]{1, 2, 4});
        TestConflictCheckImplList checkList;
        checkList = new TestConflictCheckImplList(TestConflictCheckImplFinder.id().in(ids));

        int count = dbCalls();

        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().sellSideDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().nonAcquisitionDealDetail());
        checkList.setNumberOfParallelThreads(3);

        assertEquals(3, checkList.size());

        assertEquals(4, dbCalls() - count);

        checkList = new TestConflictCheckImplList(TestConflictCheckImplFinder.id().in(ids));

        count = dbCalls();

        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().sellSideDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().nonAcquisitionDealDetail());
        checkList.setNumberOfParallelThreads(3);

        assertEquals(3, checkList.size());

        assertEquals(4, dbCalls() - count);
    }

    public void testManyToManyDeepFetch()
    {
        IntHashSet ids = IntHashSet.newSetWith(new int[]{4, 5, 6, 7});
        TestConflictCheckImplList checkList;
        checkList = new TestConflictCheckImplList(TestConflictCheckImplFinder.id().in(ids));

        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().sellSideDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().nonAcquisitionDealDetail());

        checkList.deepFetch(TestConflictCheckImplFinder.securitiesDealDetail().checkDesks());
        if (GsDeskFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            checkList.deepFetch(TestConflictCheckImplFinder.securitiesDealDetail().checkDesks().gsDesk());
        }

        checkList.size();
        int dbCalls = dbCalls();

        checkList.get(0).getSecuritiesDealDetail();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getSecuritiesDealDetail().getCheckDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getSecuritiesDealDetail().getCheckDesks().getGsDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getDetachedCopy().getSecuritiesDealDetail();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getSecuritiesDealDetail().getDetachedCopy().getCheckDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getDetachedCopy().getSecuritiesDealDetail().getCheckDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getDetachedCopy().getSecuritiesDealDetail().getCheckDesks().getGsDesks();
        assertEquals(dbCalls, dbCalls());
    }

    public void testManyToManyDeepFetchInParallel()
    {
        IntHashSet ids = IntHashSet.newSetWith(new int[]{4, 5, 6, 7});
        TestConflictCheckImplList checkList;
        checkList = new TestConflictCheckImplList(TestConflictCheckImplFinder.id().in(ids));

        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().sellSideDealDetail());
        checkList.deepFetch(TestConflictCheckImplFinder.bankingDealDetail().nonAcquisitionDealDetail());

        checkList.deepFetch(TestConflictCheckImplFinder.securitiesDealDetail().checkDesks());
        if (GsDeskFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            checkList.deepFetch(TestConflictCheckImplFinder.securitiesDealDetail().checkDesks().gsDesk());
        }
        checkList.setNumberOfParallelThreads(4);

        checkList.size();
        int dbCalls = dbCalls();

        checkList.get(0).getSecuritiesDealDetail();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getSecuritiesDealDetail().getCheckDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getSecuritiesDealDetail().getCheckDesks().getGsDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getDetachedCopy().getSecuritiesDealDetail();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getSecuritiesDealDetail().getDetachedCopy().getCheckDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getDetachedCopy().getSecuritiesDealDetail().getCheckDesks();
        assertEquals(dbCalls, dbCalls());

        checkList.get(0).getDetachedCopy().getSecuritiesDealDetail().getCheckDesks().getGsDesks();
        assertEquals(dbCalls, dbCalls());
    }

    public void testManyToManyWithMissingMiddle()
    {
        TestSecuritiesDealDetailImplList secs = TestSecuritiesDealDetailImplFinder.findMany(TestSecuritiesDealDetailImplFinder.checkId().eq(7));
        secs.deepFetch(TestSecuritiesDealDetailImplFinder.associatedDesks());
        secs.forceResolve();
    }

    public void testDeepFetchInClauseWithBypassCache()
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            Log4JRecordingAppender itemsAppender = setupRecordingAppender(OrderItem.class);
            Log4JRecordingAppender statusAppender = setupRecordingAppender(OrderStatus.class);
            OrderList orders = new OrderList(OrderFinder.orderId().greaterThan(1));
            orders.deepFetch(OrderFinder.items());
            orders.deepFetch(OrderFinder.orderStatus());
            orders.setBypassCache(true);

            int dbCalls = dbCalls();
            orders.forceResolve();

            LoggingEvent itemEvent = itemsAppender.getEvents().get(0);
            assertTrue(itemEvent.getMessage().toString().contains(" t0")); // make sure this is the sql log
            assertFalse(itemEvent.getMessage().toString().contains(" t1")); // make sure we don't hit the order table

            LoggingEvent statusEvent = statusAppender.getEvents().get(0);
            assertTrue(statusEvent.getMessage().toString().contains(" t0")); // make sure this is the sql log
            assertFalse(statusEvent.getMessage().toString().contains(" t1")); // make sure we don't hit the order table

            assertEquals(3, dbCalls() - dbCalls);
            for(int i=0;i<orders.size();i++)
            {
                Order order = orders.get(i);
                order.getItems();
                order.getOrderStatus();
            }
            orders.getItems();
            orders.getOrderStatus();
            assertEquals(3, dbCalls() - dbCalls);
            tearDownRecordingAppender(OrderItem.class);
            tearDownRecordingAppender(OrderStatus.class);
        }
    }

    public void testQueryCacheTimedExpiration()
    {
        if (OrderItemWiFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            OrderWi order = OrderWiFinder.findOne(OrderWiFinder.orderId().eq(1));

            assertTrue(order.getItems().size() > 0);
            assertNotNull(order.getOrderStatus());
            CacheClock.forceTick();
            sleep(1100);
            CacheClock.forceTick();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            assertTrue(order.getItems().size() > 0);
            assertNotNull(order.getOrderStatus());
            assertEquals(count + 2, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testManyToManyWithNotFilter()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertEquals(0, order.getProductsWithoutInProgress().size());
    }

    public void testMultiLevelSimplifiedInClause()
    {
        OrderStatus status = new OrderStatus();
        status.setOrderId(2);
        status.setLastUser("moh");
        status.setLastUpdateTime(new Timestamp(System.currentTimeMillis()));
        status.insert();
        OrderItemList itemList = new OrderItemList();
        ProductList prodList = new ProductList();
        for(int i=0;i<10000;i++)
        {
            OrderItem item = new OrderItem();
            item.setId(1000+i);
            item.setOrderId((i % 2) + 1);
            item.setDiscountPrice(i);
            item.setState("foo");
            item.setProductId(2000+i);

            itemList.add(item);

            Product p = new Product();
            p.setProductId(2000 + i);
            p.setDailyProductionRate(1.0f);
            p.setManufacturerId(5+i);
            p.setProductCode("abc");
            p.setProductDescription("foo");

            prodList.add(p);
        }
        itemList.insertAll();
        prodList.insertAll();
        ProductFinder.clearQueryCache();
        OrderStatusFinder.clearQueryCache();
        Log4JRecordingAppender recordingAppender = setupRecordingAppender(Product.class);
        try
        {
            OrderStatusList statusList = new OrderStatusList(OrderStatusFinder.orderId().in(IntHashSet.newSetWith(new int[] { 1, 2 } )));
            statusList.deepFetch(OrderStatusFinder.order().items().productInfo());
            statusList.forceResolve();
            if (ProductFinder.getMithraObjectPortal().isPartiallyCached())
            {
                LoggingEvent productEvent = recordingAppender.getEvents().get(0);
                assertTrue(productEvent.getMessage().toString().contains(" t0")); // make sure this is the sql log
                assertTrue(productEvent.getMessage().toString().contains(" t1")); // we should join to item
                assertFalse(productEvent.getMessage().toString().contains(" t2")); // but nothing else
            }
        }
        finally
        {
            tearDownRecordingAppender(Product.class);
        }
    }

    public void testNullJoinDeepFetch() throws ParseException
    {
        ExchangeRateList exchangeRateList = this.createNewExchangeRateList(100);
        exchangeRateList.bulkInsertAll();
        final Operation op = ExchangeRateFinder.acmapCode().eq("A");
        int dbCount = this.getRetrievalCount();
        exchangeRateList = new ExchangeRateList(op);
        exchangeRateList.deepFetch(ExchangeRateFinder.children());
        exchangeRateList.forceResolve();
        for(ExchangeRate r: exchangeRateList) r.getChildren();
        if (!ExchangeRateFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            assertEquals(2, this.getRetrievalCount() - dbCount);
        }
    }

    public void testImplicitJoin()
    {
        OrderList list = OrderFinder.findMany(OrderFinder.items().quantity().greaterThan(0.1));
        list.setForceImplicitJoin(true);
        list.deepFetch(OrderFinder.items());
        assertEquals(3, list.size());
    }

    private ExchangeRateList createNewExchangeRateList(int setSize) throws ParseException
    {
        ExchangeRateList result = new ExchangeRateList();
        Timestamp now = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        for(int i=0;i<setSize;i++)
        {
            ExchangeRate b = new ExchangeRate();
            b.setAcmapCode("A");
            b.setSourceNull();
            b.setCurrency(""+i);
            b.setDate(now);
            result.add(b);
        }
        return result;
    }

    public void testSourceAttributeInheritance() throws Exception
    {
        Timestamp buzDate = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        Operation op = LewContractFinder.acctId().eq(7).and(LewContractFinder.instrumentId().eq(1)).and(LewContractFinder.region().eq("A"));
        op = op.and(LewContractFinder.businessDate().eq(buzDate));
        LewContract contract = LewContractFinder.findOne(op);
        assertNotNull(contract);
        assertNotNull(contract.getLewTransaction());
        int count = this.getRetrievalCount();
        assertNotNull(contract.getLewTransaction());
        assertEquals(count, this.getRetrievalCount());
    }

    public void testSourceAttributeInheritancePreLoad() throws Exception
    {
        Timestamp buzDate = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        Operation op = LewContractFinder.acctId().eq(7).and(LewContractFinder.instrumentId().eq(1)).and(LewContractFinder.region().eq("A"));
        op = op.and(LewContractFinder.businessDate().eq(buzDate));
        LewContract contract = LewContractFinder.findOne(op);
        assertNotNull(contract);
        op = LewTransactionFinder.tranId().eq(23).and(LewTransactionFinder.region().eq("A"));
        op = op.and(LewTransactionFinder.businessDate().eq(buzDate));
        assertNotNull(LewTransactionFinder.findOne(op));
        int count = this.getRetrievalCount();
        assertNotNull(contract.getLewTransaction());
        assertEquals(count, this.getRetrievalCount());
    }
    
    public void testAsOfToTimestampJoin()
    {
        Timestamp busDate = Timestamp.valueOf("2011-05-01 23:59:00.0");
        
        assertEquals(2, TestAsOfToTimestampJoinObjectBFinder.findOne(TestAsOfToTimestampJoinObjectBFinder.id().eq(1).and(TestAsOfToTimestampJoinObjectBFinder.businessDate().eq(busDate))).getA().getId());
        assertEquals(2, TestAsOfToTimestampJoinObjectBFinder.findOne(TestAsOfToTimestampJoinObjectBFinder.id().eq(2).and(TestAsOfToTimestampJoinObjectBFinder.businessDate().eq(busDate))).getA().getId());

        assertEquals(1, TestAsOfToTimestampJoinObjectAFinder.findOne(TestAsOfToTimestampJoinObjectAFinder.id().eq(1).and(TestAsOfToTimestampJoinObjectAFinder.businessDate().eq(busDate))).getB().getId());
        assertEquals(1, TestAsOfToTimestampJoinObjectAFinder.findOne(TestAsOfToTimestampJoinObjectAFinder.id().eq(2).and(TestAsOfToTimestampJoinObjectAFinder.businessDate().eq(busDate))).getB().getId());
    }

    public void testOverSpecifiedWithSetParameter()
    {
        OrderItem item = OrderItemFinder.findOne(OrderItemFinder.id().eq(1));

        UnifiedSet descriptions = new UnifiedSet();
        descriptions.add("foo");
        descriptions.add("bar");

        assertNull(item.getOverSpecifiedOrder(descriptions));

        descriptions.add("First order");
        assertNotNull(item.getOverSpecifiedOrder(descriptions));
        assertEquals(1, item.getOverSpecifiedOrder(descriptions).getOrderId());
    }

    public void testRelationshipSetsSourceAttribute()
    {
        Profile existingProfile = ProfileFinder.findOne(ProfileFinder.sourceId().eq(1).and(ProfileFinder.id().eq(2)));
        assertNotNull(existingProfile);
        Group newGroup = new Group();
        newGroup.setProfile(existingProfile);
        assertEquals(2, newGroup.getProfileId());
        assertEquals(1, newGroup.getSourceId());
    }

    public void testAdhocListManyToOneUniqueness()
    {
        IntHashSet orderIdSet = IntHashSet.newSetWith(new int[] { 2, 55 });
        OrderItemList orderItems = new OrderItemList(OrderItemFinder.orderId().in(orderIdSet));
        int orderId = orderItems.get(0).getOrderId();
        boolean pass = false;
        for(int i=1;i<orderItems.size();i++)
        {
            if (orderId != orderItems.get(i).getOrderId())
            {
                pass = true;
                break;
            }
        }
        assertTrue(pass);

        OrderItemList orderItemsAdhoc = new OrderItemList(orderItems);

        OrderList orders = orderItemsAdhoc.getOrders();
        assertEquals(2, orders.size());
    }

    public void testAdhocListRelationshipOrderBy()
    {
        User user = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        assertNotNull(user);
        UserList usersAdhoc = new UserList();
        usersAdhoc.add(user);

        GroupList groups = user.getGroups();  // relationship has orderBy="name DESC"

        assertEquals(3, groups.size());
        assertEquals("PARA", groups.get(0).getName());
        assertEquals("IT", groups.get(1).getName());
        assertEquals("Controllers", groups.get(2).getName());
    }

    public void testJoinViaRelationshipInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            @Override
            public Object executeTransaction(final MithraTransaction tx)
            {
                OrderList orders = OrderFinder.findMany(OrderFinder.orderStatus().status().eq(10)
                        .and(OrderFinder.items().productId().eq(1)));
                assertEquals(1, orders.size());
                assertEquals(1, orders.get(0).getOrderId());
                return null;
            }
        });
    }

    public void testAuditedTree()
    {
        AuditedTreeList rootTree = AuditedTreeFinder.findMany(AuditedTreeFinder.children().name().startsWith("Level 1"));
        assertEquals(1, rootTree.size());
        assertEquals(1, rootTree.get(0).getNodeId());
        AuditedTreeList nonLeaves = AuditedTreeFinder.findMany(AuditedTreeFinder.children().exists());
        assertEquals(2, nonLeaves.size());
        AuditedTreeList leaves = AuditedTreeFinder.findMany(AuditedTreeFinder.children().notExists());
        assertEquals(2, leaves.size());
    }

    public void testRelationshipWithInClause()
    {
        ProductList list = ProductFinder.findMany(ProductFinder.all());
        list.deepFetch(ProductFinder.cusOrFooSynonyms());
        assertTrue(list.size() > 0);
        int count = this.getRetrievalCount();
        int sum = 0;
        for(int i=0;i<list.size();i++)
        {
            sum += list.get(i).getCusOrFooSynonyms() != null ? 1 : 0;
        }
        assertTrue(sum > 0);
        assertEquals(count, getRetrievalCount());
    }

    public void testListNavigationDeepFetchToZeroOne()
    {
        ProfileList profiles = ProfileFinder.findMany(ProfileFinder.id().greaterThan(0).and(ProfileFinder.sourceId().eq(0)));
        profiles.deepFetch(ProfileFinder.users().userGroupsForActive());
        profiles.forceResolve();
        int dbCalls = dbCalls();
        for(Profile profile: profiles)
        {
            profile.getUsers().getUserGroupsForActives().forceResolve();
        }
        assertEquals(dbCalls, dbCalls());
    }

    public void testOverspecifiedRelationshipsOne()
    {
        OrderItem item = OrderItemFinder.findByPrimaryKey(1);
        assertNotNull(item.getOrderWithOr("a", 1));
        assertNotNull(item.getOrderWithOr("a", 1));
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNull(item.getOrderWithOr("X", 12));
        assertNull(item.getOrderWithOr("X", 12));
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(item.getOrderWithOr("X", 1));
        assertNotNull(item.getOrderWithOr("X", 1));
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(item.getOrderWithOr("A", 12));
        assertNotNull(item.getOrderWithOr("A", 12));

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(item.getOrderWithAndOr("A", 1));
        assertNotNull(item.getOrderWithAndOr("A", 1));

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNull(item.getOrderWithAndOr("X", 1));
        assertNull(item.getOrderWithAndOr("X", 1));

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(item.getOrderWithAndOr("G", 1));
        assertNotNull(item.getOrderWithAndOr("G", 1));

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNull(item.getOrderWithAndOr("G", 12));
        assertNull(item.getOrderWithAndOr("G", 12));


        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNull(item.getOrderWithIsNull());
        assertNull(item.getOrderWithIsNull());

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(item.getOrderWithOrIsNull("A"));
        assertNotNull(item.getOrderWithOrIsNull("A"));
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNull(item.getOrderWithOrIsNull("X"));
        assertNull(item.getOrderWithOrIsNull("X"));


        item.getOrder().setDescription(null);

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(item.getOrderWithIsNull());
        assertNotNull(item.getOrderWithIsNull());

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(item.getOrderWithOrIsNull("X"));
        assertNotNull(item.getOrderWithOrIsNull("X"));
    }

    public void testOneToManyToManyToOneOnNonPrimaryAttributes()
    {
        final Timestamp businessDate = Timestamp.valueOf("2017-01-17 18:30:00.0");
        final int orderId = 1000;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>() {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable {
                BitemporalOrder order = new BitemporalOrder(businessDate);
                order.setOrderId(orderId);
                order.insert();
                return null;
            }
        });
        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderList orders = BitemporalOrderFinder.findMany(BitemporalOrderFinder.businessDate().eq(businessDate)
                .and(BitemporalOrderFinder.orderId().eq(orderId)));
        orders.deepFetch(BitemporalOrderFinder.items().productCategory());
        BitemporalOrderItemList items = orders.get(0).getItems();
        BitemporalProductCategoryList productCategories = items.getProductCategories();
        int retrievalCount = getRetrievalCount();
        productCategories.forceResolve();
        assertEquals(retrievalCount, getRetrievalCount());
    }
}
