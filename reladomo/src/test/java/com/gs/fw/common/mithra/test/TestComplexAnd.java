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

import com.gs.collections.impl.set.mutable.primitive.CharHashSet;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.collections.impl.set.mutable.primitive.LongHashSet;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;

public class TestComplexAnd
extends TestSqlDatatypes        
{

    public void testRetrieveCombo()
            throws Exception
    {
        HashSet inSet = new HashSet();
        inSet.add("lsd");
        inSet.add("swp");

        String sql = "select * from PARA_DESK where DESK_ID_STRING in ('lsd', 'swp') and STATUS_CHAR = 'A'";
        List desks = new ParaDeskList(ParaDeskFinder.deskIdString().in(inSet).and(ParaDeskFinder.statusChar().eq('A')));
        this.genericRetrievalTest(sql, desks);

        Connection connection = this.getConnection();
        sql = "select * from PARA_DESK where CONNECTION_LONG in (1000000, 2000000) and CREATE_TIMESTAMP in ( ?, ? )";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(2, new Timestamp(getDawnOfTime().getTime()));

        LongHashSet longSet = new LongHashSet();
        longSet.add(1000000);
        longSet.add(2000000);

        inSet.clear();
        inSet.add(InfinityTimestamp.getParaInfinity());
        inSet.add(new Timestamp(getDawnOfTime().getTime()));

        desks = new ParaDeskList(ParaDeskFinder.connectionLong().in(longSet).and(ParaDeskFinder.createTimestamp().in(inSet)));
        this.genericRetrievalTest(ps, desks, connection);


        connection = this.getConnection();
        sql = "select * from PARA_DESK where STATUS_CHAR in ('A', 'Q', 'T', 'G') and CREATE_TIMESTAMP = ? ";
        ps = connection.prepareStatement(sql);
        ps.setTimestamp(1, getTestTimestamp());

        CharHashSet charSet = new CharHashSet();
        charSet.add('A');
        charSet.add('Q');
        charSet.add('T');
        charSet.add('G');

        desks = new ParaDeskList(ParaDeskFinder.statusChar().in(charSet).and(ParaDeskFinder.createTimestamp().eq(getTestTimestamp())));
        this.genericRetrievalTest(ps, desks, connection);


        connection = this.getConnection();
        sql = "select * from PARA_DESK where DESK_ID_STRING in ('rnd', 'lmn', 'cap') and ACTIVE_BOOLEAN = 1 and CONNECTION_LONG in (1000000) and CREATE_TIMESTAMP = ? ";
        ps = connection.prepareStatement(sql);
        ps.setTimestamp(1, getTestTimestamp());

        inSet.clear();
        inSet.add("rnd");
        inSet.add("abc");
        inSet.add("lmn");

        longSet.clear();
        longSet.add(1000000);

        desks = new ParaDeskList(ParaDeskFinder.deskIdString().in(inSet).and(ParaDeskFinder.activeBoolean().eq(true)).and(ParaDeskFinder.connectionLong().in(longSet)).and(ParaDeskFinder.createTimestamp().eq(getTestTimestamp())));
        this.genericRetrievalTest(ps, desks, connection);

        connection = this.getConnection();
        sql = "select * from PARA_DESK where DESK_ID_STRING in ('rnd', 'lmn', 'cap') and ACTIVE_BOOLEAN = 1 and CONNECTION_LONG > 100 and CREATE_TIMESTAMP = ? ";
        ps = connection.prepareStatement(sql);
        ps.setTimestamp(1, getTestTimestamp());

        inSet.clear();
        inSet.add("rnd");
        inSet.add("abc");
        inSet.add("lmn");

        desks = new ParaDeskList(ParaDeskFinder.deskIdString().in(inSet).and(ParaDeskFinder.activeBoolean().eq(true)).and(ParaDeskFinder.connectionLong().greaterThan(100)).and(ParaDeskFinder.createTimestamp().eq(getTestTimestamp())));
        this.genericRetrievalTest(ps, desks, connection);

    }

    public void testMultiEqualityWithIn()
    {
        User u1 = UserFinder.findOne(UserFinder.sourceId().eq(0).and(UserFinder.id().eq(1)));
        assertNotNull(u1);
        User u2 = UserFinder.findOne(UserFinder.sourceId().eq(0).and(UserFinder.id().eq(2)));
        assertNotNull(u2);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        IntHashSet set = new IntHashSet();
        set.add(1);
        set.add(2);
        UserList ul = new UserList(UserFinder.sourceId().eq(0).and(UserFinder.id().in(set)));
        ul.forceResolve();
        assertEquals(2, ul.size());
        assertTrue(ul.contains(u1));
        assertTrue(ul.contains(u2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testMultiEqualityDuplicateDetection()
    {
        Operation op = UserFinder.sourceId().eq(0);
        op = op.and(UserFinder.id().eq(1));
        op = op.and(UserFinder.id().eq(1)); // repeat
        op = op.and(UserFinder.sourceId().eq(0));
        User ul = UserFinder.findOne(op);
        assertNotNull(ul);
        
    }
    
    public void testDomainEq()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Product prod = ProductFinder.findOne(ProductFinder.productId().eq(1));
        assertNotNull(OrderItemFinder.findOne(OrderItemFinder.order().eq(order).and(OrderItemFinder.productInfo().eq(prod))));
    }

    public void testNone()
    {
        Operation multiOne = OrderFinder.userId().eq(1).and(OrderFinder.description().eq("abc"));
        Operation andOp = multiOne.and(OrderFinder.state().greaterThan("12A"));
        Operation multiTwo = OrderFinder.userId().eq(2).and(OrderFinder.description().eq("def"));
        assertTrue(andOp.and(multiTwo).zIsNone());
    }

}
