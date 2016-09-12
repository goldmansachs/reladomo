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

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskList;

import java.sql.SQLException;
import java.util.Set;



public class TestStringLike extends TestSqlDatatypes
{

    public void testStringLike() throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING like 'g%'";
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.deskIdString().startsWith("g"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like 'gn%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().startsWith("gn"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%q'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().endsWith("q"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%sd'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().endsWith("sd"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%a%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().contains("a"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%gn%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().contains("gn"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%r%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().contains("r"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%w%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().contains("w"));
        this.genericRetrievalTest(sql, desks);

    }

    public void testStringNotLike() throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING not like 'g%'";
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.deskIdString().notStartsWith("g"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like 'gn%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notStartsWith("gn"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%q'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notEndsWith("q"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%sd'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notEndsWith("sd"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%a%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notContains("a"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%gn%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notContains("gn"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%r%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notContains("r"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%w%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notContains("w"));
        this.genericRetrievalTest(sql, desks);

    }
    
    public void testStringWildEq() throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING like 'g%'";
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardEq("g*"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like 'a_'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardEq("a?"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%a%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardEq("*a*"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '%a'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardEq("*a"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING like '_a%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardEq("?a*"));
        this.genericRetrievalTest(sql, desks);
    }

    public void testStringLikeEscapes()
    {
        Order o = new Order();
        o.setOrderId(10000);
        o.setDescription("with_underscore");
        o.insert();
        o = new Order();
        o.setOrderId(10001);
        o.setDescription("with%percent");
        o.insert();
        o = new Order();
        o.setOrderId(10002);
        o.setDescription("with[openbrace");
        o.insert();
        o = new Order();
        o.setOrderId(10003);
        o.setDescription("with]closebrace");
        o.insert();
        o = new Order();
        o.setOrderId(10004);
        o.setDescription("with\\backslash");
        o.insert();
        o = new Order();
        o.setOrderId(10005);
        o.setDescription("with[x]twobrace");
        o.insert();
        o = new Order();
        o.setOrderId(10006);
        o.setDescription("with=equals");
        o.insert();

        assertEquals(10000, OrderFinder.findOne(OrderFinder.description().wildCardEq("*?_?*")).getOrderId());
        assertEquals(10001, OrderFinder.findOne(OrderFinder.description().wildCardEq("*?%?*")).getOrderId());
        assertEquals(10002, OrderFinder.findOne(OrderFinder.description().wildCardEq("*?[?*").and(OrderFinder.orderId().notEq(10005))).getOrderId());
        assertEquals(10003, OrderFinder.findOne(OrderFinder.description().wildCardEq("*?]?*").and(OrderFinder.orderId().notEq(10005))).getOrderId());
        assertEquals(10004, OrderFinder.findOne(OrderFinder.description().wildCardEq("*?\\?*")).getOrderId());
        assertEquals(10005, OrderFinder.findOne(OrderFinder.description().wildCardEq("*?[x]?*")).getOrderId());
        assertEquals(10006, OrderFinder.findOne(OrderFinder.description().wildCardEq("*?=?*")).getOrderId());

        assertEquals(10000, OrderFinder.findOne(OrderFinder.description().wildCardEq("*_*")).getOrderId());
        assertEquals(10001, OrderFinder.findOne(OrderFinder.description().wildCardEq("*%*")).getOrderId());
        assertEquals(10002, OrderFinder.findOne(OrderFinder.description().wildCardEq("*[*").and(OrderFinder.orderId().notEq(10005))).getOrderId());
        assertEquals(10003, OrderFinder.findOne(OrderFinder.description().wildCardEq("*]*").and(OrderFinder.orderId().notEq(10005))).getOrderId());
        assertEquals(10004, OrderFinder.findOne(OrderFinder.description().wildCardEq("*\\*")).getOrderId());
        assertEquals(10005, OrderFinder.findOne(OrderFinder.description().wildCardEq("*[x]*")).getOrderId());
        assertEquals(10006, OrderFinder.findOne(OrderFinder.description().wildCardEq("*=*")).getOrderId());
    }

    public void testStringWildIn() throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING like 'g%' or DESK_ID_STRING like 'a%' or DESK_ID_STRING like 'b%'" +
                " or DESK_ID_STRING like 'a_' or DESK_ID_STRING like '%a%' or DESK_ID_STRING like '%b%' or DESK_ID_STRING like '%a'" +
                " or DESK_ID_STRING like '%b' or DESK_ID_STRING like '_a%' or DESK_ID_STRING like '_b%'";
        Set<String> patterns = UnifiedSet.newSet();
        patterns.add("g*");
        patterns.add("a*");
        patterns.add("b*");
        patterns.add("a?");
        patterns.add("*a*");
        patterns.add("*b*");
        patterns.add("*a");
        patterns.add("*b");
        patterns.add("?a*");
        patterns.add("?b*");
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardIn(patterns));
        this.genericRetrievalTest(sql, desks);
    }

    public void testStringWildNotEq() throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING not like 'g%'";
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardNotEq("g*"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like 'a_'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardNotEq("a?"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%a%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardNotEq("*a*"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '%a'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardNotEq("*a"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING not like '_a%'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardNotEq("?a*"));
        this.genericRetrievalTest(sql, desks);
    }
    
    public void testStringWildEqNoSql()
    {
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardEq("?a*"));
        assertEquals(1, desks.size());
        assertEquals("cap", desks.get(0).getDeskIdString());
    }

    public void testStringWildNotEqNoSql()
    {
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.deskIdString().wildCardNotEq("?a*"));
        for(int i=0;i<desks.size();i++)
        {
            assertFalse('a' == desks.get(i).getDeskIdString().charAt(1));
        }
    }
}
