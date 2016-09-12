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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.child.ChildType;
import com.gs.fw.common.mithra.test.domain.child.ChildTypeFinder;
import com.gs.fw.common.mithra.test.domain.child.ChildTypeList;
import com.gs.fw.common.mithra.test.domain.criters.PetType;
import com.gs.fw.common.mithra.test.domain.criters.PetTypeFinder;
import com.gs.fw.common.mithra.test.util.Log4JRecordingAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.sql.Timestamp;



public class TestExists extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        return new Class[] { Account.class, Trial.class, ChildType.class, PetType.class,
                Order.class, OrderItem.class, OrderStatus.class,
                AuditedUser.class, AuditedGroup.class, AuditedUserGroup.class};
    }

    public void testToOne()
    {
        assertNull(AccountFinder.findOne(AccountFinder.deskId().eq("A").and(AccountFinder.trial().exists()).and(AccountFinder.accountNumber().eq("null trial"))));
        assertNotNull(AccountFinder.findOne(AccountFinder.deskId().eq("A").and(AccountFinder.trial().exists()).and(AccountFinder.accountNumber().eq("with trial"))));
    }

    public void testToOneWithOrExistsNotExists()
    {
        OrderStatus newStatus = new OrderStatus();
        newStatus.setOrderId(2);
        newStatus.setLastUser("Moh");
        newStatus.setLastUpdateTime(new Timestamp(System.currentTimeMillis()));
        newStatus.setStatus(17);
        newStatus.insert();
        Operation op = OrderFinder.orderStatus().notExists();
        op = op.or(OrderFinder.orderStatus().lastUser().eq("Fred"));
        assertEquals(6, OrderFinder.findMany(op).size());
    }

    public void testToMany()
    {
        assertNull(ChildTypeFinder.findOne(ChildTypeFinder.id().eq(102).and(ChildTypeFinder.pets().exists())));
        assertNotNull(ChildTypeFinder.findOne(ChildTypeFinder.id().eq(101).and(ChildTypeFinder.pets().exists())));
    }

    public void testOrAndExists()
    {
        Operation op = ChildTypeFinder.id().eq(102).and(ChildTypeFinder.pets().exists());

        Operation op2 = ChildTypeFinder.id().eq(101).and(ChildTypeFinder.pets().exists());
        assertNotNull(ChildTypeFinder.findOne(op.or(op2)));
    }

    public void testOrAndExistsWithSecondExists() throws Exception
    {
        if (OrderStatusFinder.getMithraObjectPortal().isPartiallyCached())
        {
            Log4JRecordingAppender appender = this.setupRecordingAppender(OrderStatus.class);
            try
            {
                Operation op = OrderStatusFinder.order().items().otherInProgressItems().exists();
                Operation op2 = OrderStatusFinder.order().items().otherExpensiveItems().exists();
                Operation orOp = op.or(op2);
                OrderStatusFinder.findMany(orOp).forceResolve();
                LoggingEvent event = appender.getEvents().get(0);
                String msg = event.getMessage().toString();
                int start = 0;
                for(int i=0;i<3;i++)
                {
                    start = msg.indexOf("left join", start);
                    assertTrue(start >= 0);
                }
            }
            finally
            {
                tearDownRecordingAppender(OrderStatus.class);
            }
        }
    }

    public void testOrAndExistsWithSecondExistsWithOrAndAnd() throws Exception
    {
        if (OrderStatusFinder.getMithraObjectPortal().isPartiallyCached())
        {
            Log4JRecordingAppender appender = this.setupRecordingAppender(OrderStatus.class);
            try
            {
                Operation op = OrderStatusFinder.order().items().otherInProgressItems().quantity().greaterThan(0.001);
                Operation op2 = OrderStatusFinder.order().items().otherExpensiveItems().exists();
                Operation orOp = op.or(op2);
                Operation op3 = OrderStatusFinder.order().items().otherInProgressItems().quantity().lessThan(1000000);
                Operation orAndOp = orOp.and(op3).and(OrderStatusFinder.status().notEq(-10));
                OrderStatusFinder.findMany(orAndOp).forceResolve();
                LoggingEvent event = appender.getEvents().get(0);
                String msg = event.getMessage().toString();
                int start = 0;
                for(int i=0;i<3;i++)
                {
                    start = msg.indexOf("left join", start);
                    assertTrue(start >= 0);
                }
            }
            finally
            {
                tearDownRecordingAppender(OrderStatus.class);
            }
        }
    }

    public void testToOneNotExists()
    {
        assertNotNull(AccountFinder.findOne(AccountFinder.deskId().eq("A").and(AccountFinder.trial().notExists()).and(AccountFinder.accountNumber().eq("null trial"))));
        assertNull(AccountFinder.findOne(AccountFinder.deskId().eq("A").and(AccountFinder.trial().notExists()).and(AccountFinder.accountNumber().eq("with trial"))));
    }

    public void testToManyNotExists()
    {
        assertNotNull(ChildTypeFinder.findOne(ChildTypeFinder.id().eq(104).and(ChildTypeFinder.pets().notExists())));
        assertNull(ChildTypeFinder.findOne(ChildTypeFinder.id().eq(101).and(ChildTypeFinder.pets().notExists())));
    }

    public void testToManyNotExistsNotEq()
    {
        ChildTypeList list = ChildTypeFinder.findMany(ChildTypeFinder.pets().notExists(PetTypeFinder.genome().notEq("Feline")).and(ChildTypeFinder.pets().exists()));
        assertEquals(1, list.size());
    }

    public void testManyToManyNotExistsAudited()
    {
        Operation op = AuditedUserFinder.active().eq(true).and(AuditedUserFinder.groups().notExists());
        AuditedUserFinder.findMany(AuditedUserFinder.sourceId().eq(1).and(op)).forceResolve();
    }

    public void testManyToManyExistsAudited()
    {
        Operation op = AuditedUserFinder.active().eq(true).and(AuditedUserFinder.groups().exists());
        AuditedUserFinder.findMany(AuditedUserFinder.sourceId().eq(1).and(op)).forceResolve();
    }

    public void testToOneToMany()
    {
        OrderItem newItem = new OrderItem();
        newItem.setOrderId(1);
        newItem.setId(1000);
        newItem.setProductId(1);
        newItem.setState("In-Progress");
        newItem.insert();
        Operation op = OrderStatusFinder.order().items().state().eq("In-Progress");
        op = op.and(OrderStatusFinder.orderId().eq(1));
        assertNotNull(OrderStatusFinder.findOne(op));
    }

    public void testNotExistsThroughTwoRelationships()
    {
        Operation op = OrderStatusFinder.order().items().notExists(OrderItemFinder.state().eq("done"));
        op = op.and(OrderStatusFinder.orderId().eq(1));

        assertNotNull(OrderStatusFinder.findOne(op));
    }

    public void testMultiOrExists()
    {
        Operation op = OrderFinder.items().quantity().greaterThan(0);
        op = op.or(OrderFinder.items().exists().and(OrderFinder.expensiveItems(1.0).quantity().greaterThan(1)));

        assertEquals(3, OrderFinder.findMany(op).size());
    }
}
