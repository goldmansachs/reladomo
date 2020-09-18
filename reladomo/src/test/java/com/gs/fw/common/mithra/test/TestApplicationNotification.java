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
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.test.domain.Division;
import com.gs.fw.common.mithra.test.domain.Employee;
import com.gs.fw.common.mithra.test.domain.EmployeeFinder;
import com.gs.fw.common.mithra.test.domain.EmployeeList;
import com.gs.fw.common.mithra.test.domain.FullyCachedTinyBalance;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.Player;
import com.gs.fw.common.mithra.test.domain.PlayerFinder;
import com.gs.fw.common.mithra.test.domain.SpecialAccount;
import com.gs.fw.common.mithra.test.domain.Team;
import com.gs.fw.common.mithra.test.domain.TeamFinder;
import com.gs.fw.common.mithra.test.domain.TeamList;
import org.eclipse.collections.api.block.function.primitive.IntFunction;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;



public class TestApplicationNotification extends RemoteMithraNotificationTestCase
{
    private static Logger LOGGER = LoggerFactory.getLogger(TestApplicationNotification.class.getName());

    protected Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        Order.class,
                        OrderItem.class,
                        Employee.class,
                        Team.class,
                        Player.class,
                        Division.class,
                        SpecialAccount.class,
                        FullyCachedTinyBalance.class // just to suppress errors
                };
    }

    private interface Notifiable
    {
        boolean isNotified();
    }

    private static class TestMithraApplicationClassLevelNotificationListener implements MithraApplicationClassLevelNotificationListener, Notifiable
    {
        public static final Predicate<MithraNotificationEvent> INSERT_PREDICATE = new Predicate<MithraNotificationEvent>()
        {
            @Override
            public boolean accept(MithraNotificationEvent event)
            {
                return event.getDatabaseOperation() == MithraNotificationEvent.INSERT;
            }
        };

        public static final Predicate<MithraNotificationEvent> DELETE_PREDICATE = new Predicate<MithraNotificationEvent>()
        {
            @Override
            public boolean accept(MithraNotificationEvent event)
            {
                return event.getDatabaseOperation() == MithraNotificationEvent.DELETE;
            }
        };

        public static final Predicate<MithraNotificationEvent> MASS_DELETE_PREDICATE = new Predicate<MithraNotificationEvent>()
        {
            @Override
            public boolean accept(MithraNotificationEvent event)
            {
                return event.getDatabaseOperation() == MithraNotificationEvent.MASS_DELETE;
            }
        };

        public static final Predicate<MithraNotificationEvent> UPDATE_PREDICATE = new Predicate<MithraNotificationEvent>()
        {
            @Override
            public boolean accept(MithraNotificationEvent event)
            {
                return event.getDatabaseOperation() == MithraNotificationEvent.UPDATE;
            }
        };

        public static final IntFunction<MithraNotificationEvent> DATA_RECORD_COUNT_FUNCTION = new IntFunction<MithraNotificationEvent>()
        {
            @Override
            public int intValueOf(MithraNotificationEvent event)
            {
                return event.getDataObjects().length;
            }
        };

        private MutableList<MithraNotificationEvent> notificationEvents = FastList.newList();
        private int totalNotificationEventCount = 0;
        private boolean throwException = false;

        public TestMithraApplicationClassLevelNotificationListener withThrowException()
        {
            this.throwException = true;
            return this;
        }

        @Override
        public void processNotificationEvent(MithraNotificationEvent notificationEvent)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("*******Listener received a notification event object*************");
            }
            this.notificationEvents.add(notificationEvent);
            this.totalNotificationEventCount++;

            if (throwException)
            {
                throw new RuntimeException("Simulate exception thrown by application listener implementation");
            }
        }

        public List<MithraNotificationEvent> getNotificationEvents()
        {
            return notificationEvents;
        }

        @Override
        public boolean isNotified()
        {
            return this.notificationEvents.size() > 0;
        }

        public boolean isInserted()
        {
            return this.notificationEvents.anySatisfy(INSERT_PREDICATE);
        }

        public boolean isDeleted()
        {
            return this.notificationEvents.anySatisfy(DELETE_PREDICATE);
        }

        public boolean isMassDeleted()
        {
            return this.notificationEvents.anySatisfy(MASS_DELETE_PREDICATE);
        }

        public boolean isUpdated()
        {
            return this.notificationEvents.anySatisfy(UPDATE_PREDICATE);
        }

        public void reset()
        {
            this.notificationEvents.clear();
        }

        public int getInsertedEventCount()
        {
            return this.notificationEvents.count(INSERT_PREDICATE);
        }

        public int getDeletedEventCount()
        {
            return this.notificationEvents.count(DELETE_PREDICATE);
        }

        public int getUpdatedEventCount()
        {
            return this.notificationEvents.count(UPDATE_PREDICATE);
        }

        public long getInsertedObjectCount()
        {
            return this.notificationEvents.select(INSERT_PREDICATE).sumOfInt(DATA_RECORD_COUNT_FUNCTION);
        }

        public long getDeletedObjectCount()
        {
            return this.notificationEvents.select(DELETE_PREDICATE).sumOfInt(DATA_RECORD_COUNT_FUNCTION);
        }

        public long getUpdatedObjectCount()
        {
            return this.notificationEvents.select(UPDATE_PREDICATE).sumOfInt(DATA_RECORD_COUNT_FUNCTION);
        }

        public int getTotalNotificationCount()
        {
            return this.totalNotificationEventCount;
        }
    }

    private class TestMithraApplicationNotificationListener implements MithraApplicationNotificationListener, Notifiable
    {
        private boolean inserted = false;
        private boolean deleted = false;
        private boolean updated = false;
        private boolean notified = false;
        private int insertedCount = 0;
        private int deletedCount = 0;
        private int updatedCount = 0;
        private int totalNotificationCount = 0;
        private boolean throwException = false;

        public TestMithraApplicationNotificationListener withThrowException()
        {
            this.throwException = true;
            return this;
        }

        @Override
        public boolean isNotified()
        {
            return notified;
        }

        public boolean isInserted()
        {
            return inserted;
        }

        public boolean isDeleted()
        {
            return deleted;
        }

        public boolean isUpdated()
        {
            return updated;
        }

        public int getInsertedCount()
        {
            return insertedCount;
        }

        public int getDeletedCount()
        {
            return deletedCount;
        }

        public int getUpdatedCount()
        {
            return updatedCount;
        }

        public int getTotalNotificationCount()
        {
            return totalNotificationCount;
        }

        public void updated()
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("*******Listener received an update notification*************");
            }
            updated = true;
            notified = true;
            updatedCount++;
            totalNotificationCount++;

            throwExceptionIfApplicable();
        }

        public void deleted()
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("*******Listener received an delete notification*************");
            }
            deleted = true;
            notified = true;
            deletedCount++;
            totalNotificationCount++;

            throwExceptionIfApplicable();
        }

        public void inserted()
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("*******Listener received an insert notification*************");
            }
            inserted = true;
            notified = true;
            insertedCount++;
            totalNotificationCount++;

            throwExceptionIfApplicable();
        }

        private void throwExceptionIfApplicable()
        {
            if (throwException)
            {
                throw new RuntimeException("Simulate exception thrown by application listener implementation");
            }
        }

        public void reset()
        {
            inserted = false;
            updated = false;
            deleted = false;
            notified = false;
            insertedCount = 0;
            deletedCount = 0;
            updatedCount = 0;
        }
    }

    public void testOperationBasedListModifiedByInsert()
            throws Exception
    {
        int orderId = 5;
        int userId = 5;
        String state = "In-Progress";
        String newTrackingId = "127";

        Operation op = OrderFinder.userId().eq(userId);
        OrderList orderList = new OrderList(op);
        assertEquals(0, orderList.size());

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        orderList.registerForNotification(listener);

        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        OrderFinder.registerForNotification(classLevelListener);

        waitForRegistrationToComplete();

        assertFalse(orderList.isStale());

        int orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverInsertOrder",
                new Class[]{int.class, int.class, String.class, String.class},
                new Object[]{new Integer(orderId), new Integer(userId), state, newTrackingId});
        waitForMessages(orderUpdateClassCount, OrderFinder.getMithraObjectPortal());

        // insert notification is not implemented for list-based listeners - only for class-level listeners
        waitForNotification(classLevelListener);

        assertTrue(orderList.isStale());

        OrderList orderList2 = new OrderList(op);
        assertEquals(1, orderList2.size());

        assertFalse(listener.isInserted()); // insert notification is not implemented for list-based listeners - only for class-level listeners

        assertTrue(classLevelListener.isInserted());
        assertEquals(1, classLevelListener.getInsertedEventCount());
        assertEquals(1, classLevelListener.getInsertedObjectCount());
    }

    public void testOperationBasedListNotModifiedByInsert()
            throws Exception
    {
        int newOrderId = 5;
        int newUserId = 6;
        String state = "In-Progress";
        String newTrackingId = "127";

        Operation op = OrderFinder.userId().eq(5);
        OrderList orderList = new OrderList(op);
        assertEquals(0, orderList.size());
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        int orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverInsertOrder",
                new Class[]{int.class, int.class, String.class, String.class},
                new Object[]{new Integer(newOrderId), new Integer(newUserId), state, newTrackingId});
        waitForMessages(orderUpdateClassCount, OrderFinder.getMithraObjectPortal());

        // insert notification is not implemented for list-based listeners - only for class-level listeners
        waitForNotification(classLevelListener);

        assertEquals(0, orderList.size()); // no change
        OrderList orderList2 = new OrderList(op);
        assertEquals(0, orderList2.size());
        assertFalse(listener.isInserted()); // insert notification is not implemented for list-based listeners - only for class-level listeners

        assertTrue(classLevelListener.isInserted());
        assertEquals(1, classLevelListener.getInsertedEventCount());
        assertEquals(1, classLevelListener.getInsertedObjectCount());
    }

    public void testClassBasedListenerSourceFilter()
            throws Exception
    {
        int sourceId1 = 0;
        int sourceId2 = 1;
        int newId = 5;
        String email = "dummy@dummy.com";

        TestMithraApplicationClassLevelNotificationListener classLevelListener1 = new TestMithraApplicationClassLevelNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener2 = new TestMithraApplicationClassLevelNotificationListener();
        EmployeeFinder.registerForNotification(sourceId1, classLevelListener1);
        EmployeeFinder.registerForNotification(sourceId2, classLevelListener2);
        waitForRegistrationToComplete();

        this.getRemoteWorkerVm().executeMethod("serverInsertEmployee",
                new Class[]{int.class, int.class, String.class},
                new Object[]{new Integer(sourceId1), new Integer(newId), email});

        waitForNotification(classLevelListener1);

        assertTrue(classLevelListener1.isInserted());
        assertEquals(1, classLevelListener1.getInsertedEventCount());
        assertEquals(1, classLevelListener1.getInsertedObjectCount());

        assertFalse(classLevelListener2.isNotified());

        classLevelListener1.reset();
        classLevelListener2.reset();

        this.getRemoteWorkerVm().executeMethod("serverInsertEmployee",
                new Class[]{int.class, int.class, String.class},
                new Object[]{new Integer(sourceId2), new Integer(newId), email});

        waitForNotification(classLevelListener2);

        assertFalse(classLevelListener1.isNotified());

        assertTrue(classLevelListener2.isInserted());
        assertEquals(1, classLevelListener2.getInsertedEventCount());
        assertEquals(1, classLevelListener2.getInsertedObjectCount());
    }

    public void testOperationBasedListModifiedByUpdate()
            throws Exception
    {
        int orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int orderId = 4;
        int userId = 2;
        String state = "In-Progress";
        String newTrackingId = "127";

        Operation op = OrderFinder.userId().eq(userId);
        OrderList orderList = new OrderList(op);
        assertEquals(1, orderList.size());
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(orderId), state, new Integer(userId),newTrackingId});
        waitForMessages(orderUpdateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(orderList.isStale());

        OrderList orderList2 = new OrderList(op);
        assertEquals(1, orderList2.size());

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
        assertEquals(1, classLevelListener.getUpdatedEventCount());
        assertEquals(1, classLevelListener.getUpdatedObjectCount());
    }

    public void testOperationBasedListNotModifiedByUpdate()
            throws Exception
    {
        int userIdAttributeUpdateCount = OrderFinder.userId().getUpdateCount();
        int orderId = 3;
        int userId = 2;
        int newUserId = 3;
        String state = "In-Progress";
        String trackingId = "125";

        Operation op = OrderFinder.userId().eq(userId);
        OrderList orderList = new OrderList(op);
        assertEquals(1, orderList.size());
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(orderId), state, new Integer(newUserId),trackingId});
        waitForAttributeUpdate(userIdAttributeUpdateCount, OrderFinder.userId());

        waitForNotification(classLevelListener);

        assertTrue(orderList.isStale());

        OrderList orderList2 = new OrderList(op);
        assertEquals(1, orderList2.size());

        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
        assertEquals(1, classLevelListener.getUpdatedEventCount());
        assertEquals(1, classLevelListener.getUpdatedObjectCount());
    }

    public void testOperationBasedListModifiedByDelete()
            throws Exception
    {
        int orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();

        Operation op = OrderFinder.userId().eq(1);
        OrderList orderList = new OrderList(op);
        assertEquals(3, orderList.size());
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverDeleteOrder",
                new Class[]{int.class},
                new Object[]{new Integer(1)});
        waitForMessages(orderUpdateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(orderList.isStale());

        OrderList orderList2 = new OrderList(op);
        assertEquals(2, orderList2.size());

        assertTrue(listener.isDeleted());

        assertTrue(classLevelListener.isDeleted());
        assertEquals(1, classLevelListener.getDeletedEventCount());
        assertEquals(1, classLevelListener.getDeletedObjectCount());
    }

    public void testOperationBasedListNotModifiedByDelete()
            throws Exception
    {
        int orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();

        Operation op = OrderFinder.userId().eq(1);
        OrderList orderList = new OrderList(op);
        assertEquals(3, orderList.size());
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverDeleteOrder",
                new Class[]{int.class},
                new Object[]{new Integer(4)});
        waitForMessages(orderUpdateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(classLevelListener);

        assertTrue(orderList.isStale());

        OrderList orderList2 = new OrderList(op);
        assertEquals(3, orderList2.size());

        assertFalse(listener.isDeleted());

        assertTrue(classLevelListener.isDeleted());
        assertEquals(1, classLevelListener.getDeletedEventCount());
        assertEquals(1, classLevelListener.getDeletedObjectCount());
    }

    public void testMultipleRegistrationOnOperationBasedList()
            throws Exception
    {
        int updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = TeamFinder.teamId().lessThan(1000).and(TeamFinder.sourceId().eq("A"));
        TeamList teamList = new TeamList(op);
        assertEquals(3, teamList.size());
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        teamList.registerForNotification(listener);
        TeamFinder.registerForNotification("A", classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(998), "New Team Name"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());
        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());
        TestMithraApplicationNotificationListener listener2 = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener2 = new TestMithraApplicationClassLevelNotificationListener();
        teamList.registerForNotification(listener2);
        TeamFinder.registerForNotification("A", classLevelListener2);
        waitForRegistrationToComplete();
        updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverDeleteTeam",
                new Class[]{String.class, int.class},
                new Object[]{"A", new Integer(998)});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(listener2);
        waitForNotification(classLevelListener);
        waitForNotification(classLevelListener2);

        assertTrue(listener.isDeleted());
        assertTrue(listener2.isDeleted());
        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isDeleted());
        assertTrue(classLevelListener2.isDeleted());
        assertFalse(classLevelListener.isUpdated());
    }

    public void testRegisteringMultipleOperatioBasedLists()
            throws Exception
    {
        int updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op0 = TeamFinder.divisionId().eq(100).and(TeamFinder.sourceId().eq("A"));
        Operation op1 = TeamFinder.divisionId().eq(101).and(TeamFinder.sourceId().eq("A"));
        TeamList teamList0 = new TeamList(op0);
        TeamList teamList1 = new TeamList(op1);
        assertEquals(2, teamList0.size());
        assertEquals(1, teamList1.size());
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        teamList0.registerForNotification(listener);
        teamList1.registerForNotification(listener);
        TeamFinder.registerForNotification("A", classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(998), "New Team Name"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());
        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());
        updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverDeleteTeam",
                new Class[]{String.class, int.class},
                new Object[]{"A", new Integer(997)});

        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isDeleted());
        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isDeleted());
        assertFalse(classLevelListener.isUpdated());

    }

//    public void testUpdatingMappedOperationBasedList()
//            throws Exception
//    {
//        int playerUpdateClassCount = PlayerFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
//        Operation op0 = TeamFinder.players().name().eq("John Doe").and(TeamFinder.sourceId().eq("A"));
//        TeamList teamList0 = new TeamList(op0);
//
//        assertEquals(1, teamList0.size());
//        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
//        teamList0.registerForNotification(listener);
//
//        this.getRemoteWorkerVm().executeMethod("serverUpdatePlayerName",
//                new Class[]{String.class, int.class,String.class},
//                new Object[]{"A", new Integer(100), "John C. Doe"});
//        waitForMessages(playerUpdateClassCount, PlayerFinder.getMithraObjectPortal());
//        Thread.sleep(200);
//        assertTrue(listener.isUpdated());
//    }

    public void testOperationBasedListMassDeleteNotification()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();

        Order order1 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order1);

        OrderList orderList = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(3, orderList.size());

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverMassDeleteOrdersForUser",
                new Class[]{int.class},
                new Object[]{new Integer(1)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isDeleted());
        assertTrue(classLevelListener.isMassDeleted());

        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNull(order2);
    }

    public void testSimpleListMassDeleteNotification()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverMassDeleteOrdersForUser",
                new Class[]{int.class},
                new Object[]{new Integer(1)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isDeleted());
        assertTrue(classLevelListener.isMassDeleted());

        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNull(order2);
    }

    public void testSimpleListNotNotifiedByMassDelete()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverMassDeleteOrdersForUser",
                new Class[]{int.class},
                new Object[]{new Integer(2)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(classLevelListener);

        assertFalse(listener.isDeleted());

        assertTrue(classLevelListener.isMassDeleted());
    }

    public void testSimpleListNotModifiedByInsert()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int orderId = 10;
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();
        assertEquals(5, orderList.size());

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverInsertOrder",
                new Class[]{int.class, int.class, String.class, String.class},
                new Object[]{new Integer(orderId), new Integer(userId), state, trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        // insert notification is not implemented for list-based listeners - only for class-level listeners. In any case inserts would never be relevant to a simple list (of fixed elements).
        waitForNotification(classLevelListener);

        assertEquals(5, orderList.size()); // no change is expected

        assertFalse(listener.isInserted());

        assertTrue(classLevelListener.isInserted());
    }

    public void testSimpleListUpdateNotification()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testSimpleListNotModifiedByUpdate()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(1), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(classLevelListener);

        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }


    public void testSimpleListModifiedByDelete()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverDeleteOrder",
                new Class[]{int.class}, new Object[]{new Integer(5)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isDeleted());

        assertTrue(classLevelListener.isDeleted());
    }

    public void testSimpleListNotModifiedByDelete()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverDeleteOrder",
                new Class[]{int.class}, new Object[]{new Integer(1)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(classLevelListener);

        assertFalse(listener.isDeleted());

        assertTrue(classLevelListener.isDeleted());
    }

    public void testSimpleListNotificationWithMultipleSourceAttributes()
            throws Exception
    {
        int updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        TeamList teamList = this.createTeamListWithMultipleSourceAttributes(1000, 2000);
        teamList.insertAll();
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        teamList.registerForNotification(listener);
        TeamFinder.registerForNotification(Sets.mutable.of("A", "B"), classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"B", new Integer(1003), "New Team 1003 B"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(1004), "New Team 1004 A"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testEmptyListRegistration()
            throws Exception
    {
        int teamNameAttributeUpdateCount = TeamFinder.name().getUpdateCount();
        Team aTeam = TeamFinder.findOne(TeamFinder.sourceId().eq("A").and(TeamFinder.teamId().eq(1)));
        assertNull(aTeam);
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        TeamList teamList = new TeamList();
        teamList.registerForNotification(listener);
        TeamFinder.registerForNotification("A", classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(999), "New Team Name"});
        waitForAttributeUpdate(teamNameAttributeUpdateCount, TeamFinder.name());

        waitForNotification(classLevelListener);

        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        for(int i = 1; i <= 10; i++)
        {
            Team team = new Team();
            team.setSourceId("A");
            team.setTeamId(i);
            team.setDivisionId(1);
            team.setName("Team"+i);
            teamList.add(team);
        }
        teamList.insertAll();
        int updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(1), "New Team Name"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testSimpleListNotificationWithMultipleSourceAttributesAddingObjectAfterRegistration()
            throws Exception
    {
        int updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        TeamList teamList = this.createTeamListWithMultipleSourceAttributes(1000, 2000);
        teamList.insertAll();
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        teamList.registerForNotification(listener);
        TeamFinder.registerForNotification(Sets.mutable.of("A", "B"), classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"B", new Integer(1003), "New Team 1003 B"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(1004), "New Team 1004 A"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());

        //add a new team to the registered list
        Team team = new Team();
        team.setTeamId(10999);
        team.setSourceId("B");
        team.setDivisionId(1);
        team.setName("Team 10999");
        team.insert();
        teamList.add(team);
        updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        //server-side update the newly added team
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"B", new Integer(10999), "New Team 999"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        //notification expected
        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());
        //server-side update a team which is not in the list
        int teamNameAttributeUpdateCount = TeamFinder.name().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(999), "New Team 999A"});
        waitForAttributeUpdate(teamNameAttributeUpdateCount, TeamFinder.name());

        waitForNotification(classLevelListener);

        //notification is not expected
        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testSimpleListNotificationAddingObjectAfterRegistration()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());

        Order order = new Order();
        order.setOrderId(999);
        order.setState(state);
        order.setTrackingId(trackingId);
        order.setOrderDate(ts);
        order.insert();
        orderList.add(order);
        updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(999), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testSimpleListNotificationAddingObjectByIndexAfterRegistration()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());

        Order order = new Order();
        order.setOrderId(999);
        order.setState(state);
        order.setTrackingId(trackingId);
        order.setOrderDate(ts);
        order.insert();
        orderList.add(2, order);
        updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(999), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testSimpleListNotificationAddingAllAfterRegistration()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        OrderList newOrderList = new OrderList();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        newOrderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        newOrderList.addAll(orderList);
        newOrderList.insertAll();
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());

        Order order = new Order();
        order.setOrderId(999);
        order.setState(state);
        order.setTrackingId(trackingId);
        order.setOrderDate(ts);
        order.insert();
        newOrderList.add(order);
        updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(999), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testSimpleListNotificationAddingAllByIndexAfterRegistration()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";


        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        OrderList newOrderList = new OrderList();
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        newOrderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);

        Order order = new Order();
        order.setOrderId(999);
        order.setState(state);
        order.setTrackingId(trackingId);
        order.setOrderDate(ts);
        newOrderList.add(order);
        newOrderList.addAll(1, orderList);
        newOrderList.insertAll();
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class, String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }


    public void testSimpleListNotificationRemovingObjectAfterRegistration()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId,5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        orderList.clear();
        listener.reset();
        classLevelListener.reset();
        assertFalse(listener.isUpdated());
        assertFalse(classLevelListener.isUpdated());

        updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int stateAttributeUpdateCount = OrderFinder.state().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "In-Progress", new Integer(userId), trackingId});
        waitForAttributeUpdate(stateAttributeUpdateCount, OrderFinder.state());

        waitForNotification(classLevelListener);

        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }

    public void testRegistrationEntryGarbageCollection()
            throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        orderList = null;
        listener.reset();
        classLevelListener.reset();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();

        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "In-Progress", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        assertFalse(listener.isUpdated());
    }

    public void testMultipleRegistrationOnSameList()
            throws Exception
    {
        int teamNameAttributeUpdateCount = TeamFinder.name().getUpdateCount();
        Team aTeam = TeamFinder.findOne(TeamFinder.sourceId().eq("A").and(TeamFinder.teamId().eq(1)));
        assertNull(aTeam);
        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();

        TeamList teamList = new TeamList();
        teamList.registerForNotification(listener);
        TeamFinder.registerForNotification("A", classLevelListener);
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(999), "New Team Name"});
        waitForAttributeUpdate(teamNameAttributeUpdateCount, TeamFinder.name());

        waitForNotification(classLevelListener);

        assertFalse(listener.isUpdated());
        assertEquals(0, listener.getUpdatedCount());
        assertEquals(0, listener.getTotalNotificationCount());

        assertTrue(classLevelListener.isUpdated());
        assertEquals(1, classLevelListener.getUpdatedEventCount());
        assertEquals(1, classLevelListener.getUpdatedObjectCount());
        assertEquals(1, classLevelListener.getTotalNotificationCount());

        for(int i = 1; i <= 10; i++)
        {
            Team team = new Team();
            team.setSourceId("A");
            team.setTeamId(i);
            team.setDivisionId(1);
            team.setName("Team"+i);
            teamList.add(team);
        }
        int  updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        teamList.insertAll(); // the notification this generates is ignored because it is stamped with our local VM ID as the requestor. This is because insertAll() internally creates a transaction.
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        assertEquals(1, classLevelListener.getTotalNotificationCount());

        listener.reset();
        classLevelListener.reset();

        updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(1), "New Team Name"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());
        assertEquals(1, listener.getUpdatedCount());
        assertEquals(1, listener.getTotalNotificationCount());

        assertTrue(classLevelListener.isUpdated());
        assertEquals(1, classLevelListener.getUpdatedEventCount());
        assertEquals(1, classLevelListener.getUpdatedObjectCount());
        assertEquals(2, classLevelListener.getTotalNotificationCount());

        listener.reset();
        classLevelListener.reset();

        TestMithraApplicationNotificationListener listener2 = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener2 = new TestMithraApplicationClassLevelNotificationListener();
        teamList.registerForNotification(listener2);
        TeamFinder.registerForNotification("A", classLevelListener2);

        teamNameAttributeUpdateCount = TeamFinder.name().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(1), "Newest Team Name"});
        waitForAttributeUpdate(teamNameAttributeUpdateCount, TeamFinder.name());

        waitForNotification(listener);
        waitForNotification(listener2);
        waitForNotification(classLevelListener);
        waitForNotification(classLevelListener2);

        assertTrue(listener.isUpdated());
        assertEquals(1, listener.getUpdatedCount());
        assertEquals(2, listener.getTotalNotificationCount());

        assertTrue(listener2.isUpdated());
        assertEquals(1, listener2.getUpdatedCount());
        assertEquals(1, listener2.getTotalNotificationCount());

        assertTrue(classLevelListener.isUpdated());
        assertEquals(1, classLevelListener.getUpdatedEventCount());
        assertEquals(3, classLevelListener.getTotalNotificationCount());

        assertTrue(classLevelListener2.isUpdated());
        assertEquals(1, classLevelListener2.getUpdatedEventCount());
        assertEquals(1, classLevelListener2.getTotalNotificationCount());

        listener.reset();
        listener2.reset();
        classLevelListener.reset();
        classLevelListener2.reset();

        Team team = new Team();
        team.setSourceId("A");
        team.setTeamId(1000);
        team.setDivisionId(1);
        team.setName("Team 999");
        team.insert(); // the notification this generates DOES get processed because it is not stamped with our local VM ID. The remote Mithra server does not stamp our local VM ID because this operation is not part of a transaction. This is a peculiarity of using the middle tier and would not happen if we did the insert locally.
        teamList.add(team);

        waitForNotification(classLevelListener);
        waitForNotification(classLevelListener2);

        assertFalse(listener.isNotified());

        assertFalse(listener2.isNotified());

        assertEquals(1, classLevelListener.getInsertedEventCount());
        assertEquals(4, classLevelListener.getTotalNotificationCount());

        assertEquals(1, classLevelListener2.getInsertedEventCount());
        assertEquals(2, classLevelListener2.getTotalNotificationCount());

        listener.reset();
        listener2.reset();
        classLevelListener.reset();
        classLevelListener2.reset();

        updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverUpdateTeamName",
                new Class[]{String.class, int.class,String.class},
                new Object[]{"A", new Integer(1000), "Newest Team Name 999"});
        waitForMessages(updateClassCount, TeamFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(listener2);
        waitForNotification(classLevelListener);
        waitForNotification(classLevelListener2);

        assertTrue(listener.isUpdated());
        assertEquals(1, listener.getUpdatedCount());
        assertEquals(3, listener.getTotalNotificationCount());

        assertTrue(listener2.isUpdated());
        assertEquals(1, listener2.getUpdatedCount());
        assertEquals(2, listener2.getTotalNotificationCount());

        assertTrue(classLevelListener.isUpdated());
        assertEquals(1, classLevelListener.getUpdatedEventCount());
        assertEquals(5, classLevelListener.getTotalNotificationCount());

        assertTrue(classLevelListener2.isUpdated());
        assertEquals(1, classLevelListener2.getUpdatedEventCount());
        assertEquals(3, classLevelListener2.getTotalNotificationCount());
    }

    public void testRegisteringMultipleLists()
            throws Exception
    {
        int updateClassCount = TeamFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();

        //Create a list of 10 employees with employeeId starting at 1000.
        EmployeeList employeeList0 = this.createEmployeeList(0, 1000, 10);
        employeeList0.insertAll();
        //Create a list of 10 employees with employeeId starting at 2000.
        EmployeeList employeeList1 = this.createEmployeeList(0, 2000, 10);
        employeeList1.insertAll();
        //register each list with its own listener
        TestMithraApplicationNotificationListener listener0 = new TestMithraApplicationNotificationListener();
        TestMithraApplicationNotificationListener listener1 = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        employeeList0.registerForNotification(listener0);
        employeeList1.registerForNotification(listener1);
        // register two class-level listeners
        EmployeeFinder.registerForNotification(0, classLevelListener);
        assertFalse(listener0.isDeleted());
        assertFalse(listener1.isDeleted());
        assertFalse(classLevelListener.isDeleted());
        waitForRegistrationToComplete();
        //server-side delete employee from list 0
        this.getRemoteWorkerVm().executeMethod("serverDeleteEmployee",
                new Class[]{int.class, int.class},
                new Object[]{new Integer(0), new Integer(1001)});
        waitForMessages(updateClassCount, EmployeeFinder.getMithraObjectPortal());

        waitForNotification(listener0);
        waitForNotification(classLevelListener);

        //listener0 should be notified, listener 1 shouldn't
        assertTrue(listener0.isDeleted());
        assertFalse(listener1.isDeleted());

        assertTrue(classLevelListener.isDeleted());

        listener0.reset();
        listener1.reset();
        classLevelListener.reset();
        assertFalse(listener0.isDeleted());
        assertFalse(listener1.isDeleted());
        assertFalse(classLevelListener.isDeleted());

        //server-side delete employee from list 1
        this.getRemoteWorkerVm().executeMethod("serverDeleteEmployee",
                new Class[]{int.class, int.class},
                new Object[]{new Integer(0), new Integer(2005)});
        waitForMessages(updateClassCount, EmployeeFinder.getMithraObjectPortal());

        waitForNotification(listener1);
        waitForNotification(classLevelListener);

        //listener0 shouldn't be notified, listener 1 should be notified
        assertFalse(listener0.isDeleted());
        assertTrue(listener1.isDeleted());

        assertTrue(classLevelListener.isDeleted());
    }

    public void testRegisteringListInTransaction()
            throws Exception
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        String state = "In-Progress";
        int userId = 1;
        String trackingId = "123";

        OrderList orderList = createOrderList(ts, state, userId, trackingId, 5, 5);
        orderList.insertAll();

        TestMithraApplicationNotificationListener listener = new TestMithraApplicationNotificationListener();
        TestMithraApplicationClassLevelNotificationListener classLevelListener = new TestMithraApplicationClassLevelNotificationListener();
        orderList.registerForNotification(listener);
        OrderFinder.registerForNotification(classLevelListener);
        tx.commit();
        waitForRegistrationToComplete();
        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "Completed", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener);
        waitForNotification(classLevelListener);

        assertTrue(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());

        orderList = null;
        listener.reset();
        classLevelListener.reset();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();

        this.getRemoteWorkerVm().executeMethod("serverUpdateOrder",
                new Class[]{int.class,String.class, int.class, String.class},
                new Object[]{new Integer(5), "In-Progress", new Integer(userId), trackingId});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(classLevelListener);

        assertFalse(listener.isUpdated());

        assertTrue(classLevelListener.isUpdated());
    }


    public void testExceptionInApplicationListener()
            throws Exception
    {
        Operation op = OrderFinder.orderId().in(IntHashSet.newSetWith(1, 2, 3, 4));
        OrderList orderList = new OrderList(op);
        assertEquals(4, orderList.size());

        TestMithraApplicationNotificationListener listener1 = new TestMithraApplicationNotificationListener().withThrowException();
        TestMithraApplicationNotificationListener listener2 = new TestMithraApplicationNotificationListener().withThrowException();

        orderList.registerForNotification(listener1);
        orderList.registerForNotification(listener2);

        TestMithraApplicationClassLevelNotificationListener classLevelListener1 = new TestMithraApplicationClassLevelNotificationListener().withThrowException();
        TestMithraApplicationClassLevelNotificationListener classLevelListener2 = new TestMithraApplicationClassLevelNotificationListener().withThrowException();

        OrderFinder.registerForNotification(classLevelListener1);
        OrderFinder.registerForNotification(classLevelListener2);

        waitForRegistrationToComplete();

        int orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverDeleteOrder",
                new Class[]{int.class}, new Object[]{new Integer(1)});
        waitForMessages(orderUpdateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener1);
        waitForNotification(listener2);

        waitForNotification(classLevelListener1);
        waitForNotification(classLevelListener2);

        assertTrue(orderList.isStale());
        assertEquals(4, orderList.size());

        OrderList orderList2 = new OrderList(op);
        assertEquals(3, orderList2.size());
        assertFalse(orderList2.isStale());

        assertEquals(1, listener1.getDeletedCount());
        assertEquals(1, listener1.getTotalNotificationCount());

        assertEquals(1, listener2.getDeletedCount());
        assertEquals(1, listener2.getTotalNotificationCount());

        assertEquals(1, classLevelListener1.getDeletedEventCount());
        assertEquals(1, classLevelListener1.getTotalNotificationCount());

        assertEquals(1, classLevelListener2.getDeletedEventCount());
        assertEquals(1, classLevelListener2.getTotalNotificationCount());

        listener1.reset();
        listener2.reset();
        classLevelListener1.reset();
        classLevelListener2.reset();

        orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("serverDeleteOrder",
                new Class[]{int.class}, new Object[]{new Integer(2)});
        waitForMessages(orderUpdateClassCount, OrderFinder.getMithraObjectPortal());

        waitForNotification(listener1);
        waitForNotification(listener2);

        waitForNotification(classLevelListener1);
        waitForNotification(classLevelListener2);

        assertEquals(4, orderList.size());
        assertTrue(orderList.isStale());

        assertEquals(3, orderList2.size());
        assertTrue(orderList2.isStale());

        OrderList orderList3 = new OrderList(op);
        assertEquals(2, orderList3.size());
        assertFalse(orderList3.isStale());

        assertEquals(1, listener1.getDeletedCount());
        assertEquals(2, listener1.getTotalNotificationCount());

        assertEquals(1, listener2.getDeletedCount());
        assertEquals(2, listener2.getTotalNotificationCount());

        assertEquals(1, classLevelListener1.getDeletedEventCount());
        assertEquals(2, classLevelListener1.getTotalNotificationCount());

        assertEquals(1, classLevelListener2.getDeletedEventCount());
        assertEquals(2, classLevelListener2.getTotalNotificationCount());
    }


    private TeamList createTeamListWithMultipleSourceAttributes(int initialTeamId, int listSize)
    {
        TeamList teamList = new TeamList();
        int limit = initialTeamId + listSize;
        for(int i = initialTeamId; i < limit; i++)
        {
            String sourceId = i%2 == 0?"A":"B";
            Team team = new Team();
            team.setTeamId(i);
            team.setSourceId(sourceId);
            team.setDivisionId(1);
            team.setName("Team"+i);
            teamList.add(team);
        }
        return teamList;
    }

    private EmployeeList createEmployeeList(int sourceId, int initialEmployeeId, int listSize)
    {
        EmployeeList employeeList = new EmployeeList();
        int limit = initialEmployeeId + listSize;
        for(int i = initialEmployeeId; i < limit; i++)
        {
            Employee employee = new Employee();
            employee.setNullablePrimitiveAttributesToNull();
            employee.setId(i);
            employee.setSourceId(sourceId);
            employee.setEmail("employee"+i+"@xyz.com");
            employeeList.add(employee);
        }
        return employeeList;
    }

    private OrderList createOrderList(Timestamp ts, String state, int userId, String trackingId, int initialOrderId, int listSize)
    {
        OrderList orderList = new OrderList();
        int limit = initialOrderId + listSize;
        for(int i = initialOrderId; i < limit; i++)
        {
            Order order = new Order();
            order.setOrderId(i);
            order.setDescription("Order "+i);
            order.setOrderDate(ts);
            order.setState(state);
            order.setUserId(userId);
            order.setTrackingId(trackingId);
            orderList.add(order);
        }
        return orderList;
    }

    private void waitForNotification(Notifiable listener)
            throws Exception
    {
        for(int j = 0; j < 300; j++)
        {
            Thread.sleep(100);
            if(listener.isNotified())
            {
                break;
            }
        }
        if(!listener.isNotified())
        {
            LOGGER.warn(listener.getClass().getSimpleName() + " was not notified");
        }
    }

    public void serverMassDeleteOrdersForUser(int userId)
    {
        Operation op = OrderFinder.userId().eq(userId);
        OrderList orderList = new OrderList(op);
        orderList.deleteAll();
    }
    public void serverInsertOrderItem(int orderItemId, int orderId, double originalPrice)
    {
        OrderItem item = new OrderItem();
        item.setId(orderItemId);
        item.setOrderId(orderId);
        item.setOriginalPrice(originalPrice);
        item.setDiscountPrice(originalPrice);
        item.setProductId(20);
        item.setState("In-Progress");
        item.insert();
    }

    public void serverInsertOrder(int orderId, int userId, String state, String trackingId)
    {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setDescription("Order "+orderId);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState(state);
        order.setUserId(userId);
        order.setTrackingId(trackingId);
        order.insert();
    }

    public void serverUpdateOrder(int orderId, String newState, int newUserId, String newTrackingId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.setState(newState);
        order.setUserId(newUserId);
        order.setTrackingId(newTrackingId);
        tx.commit();
    }

    public void serverUpdateTeamName(String sourceId, int teamId, String newName)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Team team = TeamFinder.findOne(TeamFinder.sourceId().eq(sourceId).and(TeamFinder.teamId().eq(teamId)));
        team.setName(newName);
        tx.commit();
    }

    public void serverUpdatePlayerName(String sourceId, int id, String newName)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Player player = PlayerFinder.findOne(PlayerFinder.sourceId().eq(sourceId).and(PlayerFinder.id().eq(id)));
        player.setName(newName);
        tx.commit();
    }

    public void serverDeleteTeam(String sourceId, int teamId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Team team = TeamFinder.findOne(TeamFinder.sourceId().eq(sourceId).and(TeamFinder.teamId().eq(teamId)));
        team.delete();
        tx.commit();
    }

    public void serverDeleteOrder(int orderId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.delete();
        tx.commit();
    }

    public void serverCascadeInsertOrderAndItem(int orderItemId, int orderId, double originalPrice)
    {
        OrderItem item = new OrderItem();
        item.setId(orderItemId);
        item.setOrderId(orderId);
        item.setOriginalPrice(originalPrice);
        item.setDiscountPrice(originalPrice);
        item.setProductId(20);
        item.setState("In-Progress");
        OrderItemList itemList = new OrderItemList();
        itemList.add(item);

        Order order = new Order();
        order.setOrderId(orderId);
        order.setDescription("Order "+orderId);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("In-Progress");
        order.setUserId(1);
        order.setTrackingId("123");
        order.setItems(itemList);
        order.cascadeInsert();
    }

    public void serverInsertEmployee(int sourceId, int id, String email)
    {
        Employee employee = new Employee();
        employee.setSourceId(sourceId);
        employee.setId(id);
        employee.setEmail(email);
        employee.insert();
    }

    public void serverUpdateEmployee(int acmapCode, int employeeId, String newName, String newPhone, String newDesignation, String newEmail)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Employee employee = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(acmapCode).and(EmployeeFinder.id().eq(employeeId)));
        employee.setName(newName);
        employee.setPhone(newPhone);
        employee.setEmail(newEmail);
        employee.setDesignation(newDesignation);
        tx.commit();
    }

    public void serverDeleteEmployee(int acmapCode, int employeeId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Employee employee = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(acmapCode).and(EmployeeFinder.id().eq(employeeId)));
        employee.delete();
        tx.commit();

    }

    private void waitForAttributeUpdate(int attributeUpdateCount, Attribute attribute)
            throws InterruptedException
    {
        boolean messageProcessed = false;
        for(int j = 0; j < 300; j++)
        {
            Thread.sleep(100);
            int newAttributeUpdateCount = attribute.getUpdateCount();
            if(newAttributeUpdateCount > attributeUpdateCount)
            {
                messageProcessed = true;
                break;
            }
        }
        if(!messageProcessed)
        {
            LOGGER.info("***************MITHRA NOTIFICATION MESSAGE WAS NOT RECEIVED NOR PROCESSED*******************");
        }

    }

}
