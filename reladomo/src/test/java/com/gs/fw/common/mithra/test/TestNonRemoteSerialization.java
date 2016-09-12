
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

import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrial;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrialFinder;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import junit.framework.Assert;

import java.io.*;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

public class TestNonRemoteSerialization
extends MithraTestAbstract
{

    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final Class[] RESTRICTED_CLASS_LIST =
            {
                AuditedOrder.class,
                AuditedOrderItem.class,
                AuditedOrderItemStatus.class,
                AuditedOrderStatus.class,
                AuditedOrderStatusTwo.class,
                Order.class,
                OrderItem.class,
                OrderItemWi.class,
                OrderStatus.class,
                OrderStatusWi.class,
                OrderItemStatus.class,
                OrderParentToChildren.class,
                PkTimezoneTest.class,
                TinyBalance.class,
                TinyBalanceUtc.class,
                TimezoneTest.class,
                TestTamsMithraTrial.class,
                DatedWithNullablePK.class,
                ParaDesk.class,
                TamsAccount.class,
            };

    public Class[] getRestrictedClassList()
    {
        return RESTRICTED_CLASS_LIST;
    }

    private <T> T serializeAndDeserialize(T original)
    {
        byte[] pileOfBytes = serialize(original);

        return (T) deserialize(pileOfBytes);
    }

    private Object deserialize(byte[] pileOfBytes)
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(pileOfBytes);
        Object deserialized = null;

        try
        {
            ObjectInputStream objectStream = new ObjectInputStream(bais);
            deserialized = objectStream.readObject();
        }
        catch (Exception e)
        {
            MithraTestAbstract.getLogger().error("could not deserialize object", e);
            Assert.fail("could not deserialize object");
        }

        return deserialized;
    }

    private byte[] serialize(Object original)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos);
            objectOutputStream.writeObject(original);
            objectOutputStream.flush();
            objectOutputStream.close();
            baos.close();
        }
        catch (IOException e)
        {
            MithraTestAbstract.getLogger().error("could not serialize object", e);
            Assert.fail("could not serialize object");
        }

        byte[] pileOfBytes = baos.toByteArray();
        return pileOfBytes;
    }

    public void testReadOnlySerializationOfExistingObject()
    {
        final ParaDesk existingDesk = ParaDeskFinder.findOne(ParaDeskFinder.deskIdString().eq("rnd"));
        Assert.assertNotNull(existingDesk);
        Assert.assertFalse(existingDesk instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertFalse(existingDesk instanceof MithraDatedObject);  // i.e. "not dated"
        final ParaDesk copyOfExisting = (ParaDesk) serializeAndDeserialize(existingDesk);
        Assert.assertSame(existingDesk, copyOfExisting);
    }

    public void testReadOnlySerializationOfNewObject()
    {
        final ParaDesk newDesk = new ParaDesk();
        Assert.assertFalse(newDesk instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertFalse(newDesk instanceof MithraDatedObject);  // i.e. "not dated"
        newDesk.setDeskIdString("tis");
        newDesk.setActiveBoolean(false);
        newDesk.setSizeDoubleNull();
        newDesk.setConnectionLong(123456);
        newDesk.setTagInt(4);
        newDesk.setStatusChar('X');
        newDesk.setCreateTimestamp(new Timestamp(1000000));
        newDesk.setLocationByte((byte)8);
        newDesk.setClosedDate(new Date(1002000));
        newDesk.setMaxFloat((float)128.64);
        newDesk.setMinShort((short)2);

        final ParaDesk copyOfNewDesk = (ParaDesk) serializeAndDeserialize(newDesk);
        Assert.assertNotSame(newDesk, copyOfNewDesk);
        Assert.assertEquals(newDesk.getDeskIdString()   , copyOfNewDesk.getDeskIdString());
        Assert.assertEquals(newDesk.isActiveBoolean()   , copyOfNewDesk.isActiveBoolean());
        Assert.assertTrue(copyOfNewDesk.isSizeDoubleNull());
        Assert.assertEquals(newDesk.getConnectionLong() , copyOfNewDesk.getConnectionLong());
        Assert.assertEquals(newDesk.getTagInt()         , copyOfNewDesk.getTagInt());
        Assert.assertEquals(newDesk.getStatusChar()     , copyOfNewDesk.getStatusChar());
        Assert.assertEquals(newDesk.getCreateTimestamp(), copyOfNewDesk.getCreateTimestamp());
        Assert.assertEquals(newDesk.getLocationByte()   , copyOfNewDesk.getLocationByte());
        Assert.assertEquals(newDesk.getClosedDate()     , copyOfNewDesk.getClosedDate());
        Assert.assertEquals(newDesk.getMaxFloat()       , copyOfNewDesk.getMaxFloat(), 0.0);
        Assert.assertEquals(newDesk.getMinShort()       , copyOfNewDesk.getMinShort());
    }


    public void testDatedReadOnlySerializationOfExistingObject()
    {
        final Timestamp asOf = new Timestamp(System.currentTimeMillis());
        final TamsAccount existingAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                                                     TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        Assert.assertNotNull(existingAccount);
        Assert.assertFalse(existingAccount instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertTrue(existingAccount instanceof MithraDatedObject);  // i.e. "dated"
        final TamsAccount copyOfExisting = (TamsAccount) serializeAndDeserialize(existingAccount);
        Assert.assertSame(existingAccount, copyOfExisting);
    }

    public void testDatedReadOnlySerializationOfNewObject()
    {
        final Timestamp asOf = new Timestamp(System.currentTimeMillis());
        final TamsAccount newAccount = new TamsAccount(asOf, asOf);
        Assert.assertFalse(newAccount instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertTrue(newAccount instanceof MithraDatedObject);  // i.e. "dated"
        newAccount.setDeskId("tis");
        newAccount.setCode("87654321");
        newAccount.setTrialId("TID");
        newAccount.setPnlGroupId("PNLGID");

        final TamsAccount copyOfNewAccount = (TamsAccount) serializeAndDeserialize(newAccount);
        Assert.assertNotSame(newAccount, copyOfNewAccount);
        Assert.assertEquals(newAccount.getDeskId()        , copyOfNewAccount.getDeskId());
        Assert.assertEquals(newAccount.getBusinessDate()  , copyOfNewAccount.getBusinessDate());
        Assert.assertEquals(newAccount.getProcessingDate(), copyOfNewAccount.getProcessingDate());
        Assert.assertEquals(newAccount.getCode()          , copyOfNewAccount.getCode());
        Assert.assertEquals(newAccount.getTrialId()       , copyOfNewAccount.getTrialId());
        Assert.assertEquals(newAccount.getPnlGroupId()    , copyOfNewAccount.getPnlGroupId());
    }

    public void testTransactionalSerialization()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Assert.assertNotNull(order);
        Order order2 = (Order) serializeAndDeserialize(order);
        Assert.assertSame(order, order2);

        order = new Order();
        order.setOrderId(17);
        order.setDescription("D1");
        order.setOrderDate(new Timestamp(1000000));
        order.setState("S1");
        order.setTrackingId(null);
        order.setUserIdNull();

        order2 = (Order) serializeAndDeserialize(order);
        Assert.assertNotSame(order, order2);
        Assert.assertEquals(order.getOrderId(), order2.getOrderId());
        Assert.assertEquals(order.getDescription(), order2.getDescription());
        Assert.assertEquals(order.getOrderDate(), order2.getOrderDate());
        Assert.assertEquals(order.getTrackingId(), order2.getTrackingId());
        Assert.assertTrue(order2.isUserIdNull());
    }
    private TinyBalance findTinyBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                            .and(TinyBalanceFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceFinder.businessDate().eq(businessDate)));
    }

    private DatedWithNullablePK findDatedWithNullablePKForBusinessDate(int id, Timestamp businessDate)
    {
        return DatedWithNullablePKFinder.findOne(DatedWithNullablePKFinder.objectId().eq(id)
                .and(DatedWithNullablePKFinder.businessDate().eq(businessDate)));
    }

    public void testDatedTransactionalSerialization() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance tb2 = (TinyBalance) serializeAndDeserialize(tb);
        Assert.assertSame(tb, tb2);

        tb = new TinyBalance(businessDate, InfinityTimestamp.getParaInfinity());
        tb.setAcmapCode("A");
        tb.setBalanceId(2000);
        tb.setQuantity(12.5);

        tb2 = (TinyBalance) serializeAndDeserialize(tb);
        Assert.assertNotSame(tb, tb2);
        Assert.assertEquals(tb.getBalanceId(), tb2.getBalanceId());
        Assert.assertEquals(tb.getAcmapCode(), tb2.getAcmapCode());
        Assert.assertEquals(tb.getQuantity(), tb2.getQuantity(), 0);
        Assert.assertEquals(tb.getBusinessDate(), tb2.getBusinessDate());
        Assert.assertEquals(tb.getProcessingDate(), tb2.getProcessingDate());
    }

    public void testDatedTransactionalSerializationWithNullablePrimaryKey() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-08-14 00:00:00").getTime());

        DatedWithNullablePK obj = this.findDatedWithNullablePKForBusinessDate(1237,businessDate);
        DatedWithNullablePK obj2 = (DatedWithNullablePK) serializeAndDeserialize(obj);
        Assert.assertSame(obj, obj2);

        obj = new DatedWithNullablePK(businessDate, InfinityTimestamp.getParaInfinity());
        obj.setObjectId(2000);
        obj.setQuantity(12.5);

        obj2 = (DatedWithNullablePK) serializeAndDeserialize(obj);
        Assert.assertNotSame(obj, obj2);
        Assert.assertEquals(obj.getObjectId(), obj2.getObjectId());
        Assert.assertEquals(obj.getQuantity(), obj2.getQuantity(), 0);
        Assert.assertEquals(obj.getBusinessDate(), obj2.getBusinessDate());
        Assert.assertEquals(obj.getProcessingDate(), obj2.getProcessingDate());
    }

    public void testDatedNonTransactionalSerialization() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int id = 1;
        TestTamsMithraTrial trial = TestTamsMithraTrialFinder.findOne(TestTamsMithraTrialFinder.trialNodeId().eq(id).and(
                TestTamsMithraTrialFinder.businessDate().eq(businessDate)));
        Assert.assertNotNull(trial);
        TestTamsMithraTrial trial2 = (TestTamsMithraTrial) serializeAndDeserialize(trial);
        Assert.assertSame(trial, trial2);

        trial = new TestTamsMithraTrial(businessDate, InfinityTimestamp.getTamsInfinity());
        trial.setTrialId("abc");
        trial.setTrialNodeId(1234);

        trial2 = (TestTamsMithraTrial) serializeAndDeserialize(trial);
        Assert.assertNotSame(trial, trial2);
        Assert.assertEquals(trial.getTrialId(), trial2.getTrialId());
        Assert.assertEquals(trial.getTrialNodeId(), trial2.getTrialNodeId());
        Assert.assertEquals(trial.getBusinessDate(), trial2.getBusinessDate());
        Assert.assertEquals(trial.getProcessingDate(), trial2.getProcessingDate());
    }

    public void testTimezoneSerialization()
    {
        PkTimezoneTest pkTimezoneTest = PkTimezoneTestFinder.findOne(PkTimezoneTestFinder.value().eq(1));
        PkTimezoneTest pkTimezoneTest2 = (PkTimezoneTest) serializeAndDeserialize(pkTimezoneTest);
        Assert.assertSame(pkTimezoneTest, pkTimezoneTest2);

        TimezoneTest timezoneTest = TimezoneTestFinder.findOne(TimezoneTestFinder.timezoneTestId().eq(1));
        TimezoneTest timezoneTest2 = (TimezoneTest) serializeAndDeserialize(timezoneTest);
        Assert.assertSame(timezoneTest, timezoneTest2);

        pkTimezoneTest = pkTimezoneTest.getNonPersistentCopy();
        pkTimezoneTest2 = (PkTimezoneTest) serializeAndDeserialize(pkTimezoneTest);
        Assert.assertNotSame(pkTimezoneTest, pkTimezoneTest2);

        Assert.assertEquals(pkTimezoneTest.getInsensitiveDate(), pkTimezoneTest2.getInsensitiveDate());
        Assert.assertEquals(pkTimezoneTest.getDatabaseDate(), pkTimezoneTest2.getDatabaseDate());
        Assert.assertEquals(pkTimezoneTest.getUtcDate(), pkTimezoneTest2.getUtcDate());

        timezoneTest = timezoneTest.getNonPersistentCopy();
        timezoneTest2 = (TimezoneTest) serializeAndDeserialize(timezoneTest);
        Assert.assertNotSame(timezoneTest, timezoneTest2);

        Assert.assertEquals(timezoneTest.getInsensitiveTimestamp(), timezoneTest2.getInsensitiveTimestamp());
        Assert.assertEquals(timezoneTest.getDatabaseTimestamp(), timezoneTest2.getDatabaseTimestamp());
        Assert.assertEquals(timezoneTest.getUtcTimestamp(), timezoneTest2.getUtcTimestamp());

        Assert.assertEquals(timezoneTest.getInsensitiveDate(), timezoneTest2.getInsensitiveDate());
        Assert.assertEquals(timezoneTest.getDatabaseDate(), timezoneTest2.getDatabaseDate());
        Assert.assertEquals(timezoneTest.getUtcDate(), timezoneTest2.getUtcDate());

        TinyBalanceUtc tinyBalanceUtc = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                            .and(TinyBalanceUtcFinder.balanceId().eq(1))
                            .and(TinyBalanceUtcFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));
        TinyBalanceUtc tinyBalanceUtc2 = (TinyBalanceUtc) serializeAndDeserialize(tinyBalanceUtc);
        Assert.assertSame(tinyBalanceUtc, tinyBalanceUtc2);
    }

    public void testNoneSerialization()
    {
        Operation op = OrderFinder.description().in(new HashSet());
        Assert.assertTrue(op instanceof None);
        serializeAndDeserialize(op);
    }

    public void testOperationBasedListSerialization()
    {
        OrderList list = OrderFinder.findMany(OrderFinder.orderId().greaterThan(1));
        OrderList newList = (OrderList) serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
        OrderItemList list2 = list.getItems();
        OrderItemList newList2 = (OrderItemList) serializeAndDeserialize(list2);
        assertTrue(newList2.size() > 1);
        list.forceResolve();
        newList = (OrderList) serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
    }

    public void testOperationBasedListSerializationWithOrderBy()
    {
        OrderList list = OrderFinder.findMany(OrderFinder.orderId().greaterThan(1));
        list.addOrderBy(OrderFinder.orderId().ascendingOrderBy());
        OrderList newList = serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
        OrderItemList list2 = list.getItems();
        OrderItemList newList2 = serializeAndDeserialize(list2);
        assertTrue(newList2.size() > 1);
        list.forceResolve();
        newList = serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
    }

    public void testOperationBasedListSerializationWithDeepFetch()
    {
        OrderList list = OrderFinder.findMany(OrderFinder.orderId().greaterThan(1));
        list.deepFetch(OrderFinder.items());
        OrderList newList = serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
        OrderItemList list2 = list.getItems();
        OrderItemList newList2 = serializeAndDeserialize(list2);
        assertTrue(newList2.size() > 1);
        list.forceResolve();
        newList = serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
    }

    public void testNonOperationBasedListSerialization()
    {
        OrderList list = new OrderList(OrderFinder.findMany(OrderFinder.orderId().greaterThan(1)));
        OrderList newList = (OrderList) serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
        OrderItemList list2 = new OrderItemList(list.getItems());
        OrderItemList newList2 = (OrderItemList) serializeAndDeserialize(list2);
        assertTrue(newList2.size() > 1);
        newList = (OrderList) serializeAndDeserialize(list.getDetachedCopy());
        assertTrue(newList.size() > 1);
        Order order = newList.get(0);
        int orderId = order.getOrderId();
        newList.remove(0);
        newList.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(orderId)));
    }

    public void testNonOperationBasedListSerializationWithRelationships()
    {
        Order detachedOrder = OrderFinder.findOne(OrderFinder.orderId().eq(2)).getDetachedCopy();
        int items = detachedOrder.getItems().size();
        detachedOrder.getItems().remove(0);
        detachedOrder = (Order) serializeAndDeserialize(detachedOrder);
        assertEquals(items - 1, detachedOrder.getItems().size());
        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        OrderItemList orderItemList = OrderFinder.findOne(OrderFinder.orderId().eq(2)).getItems();
        orderItemList.setBypassCache(true);
        assertEquals(items - 1, orderItemList.size());
    }

    public void testAuditedNonOperationBasedListSerializationWithRelationships()
    {
        AuditedOrder detachedAuditedOrder = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2)).getDetachedCopy();
        int items = detachedAuditedOrder.getItems().size();
        detachedAuditedOrder.getItems().remove(0);
        detachedAuditedOrder = (AuditedOrder) serializeAndDeserialize(detachedAuditedOrder);
        assertEquals(items - 1, detachedAuditedOrder.getItems().size());
        detachedAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        AuditedOrderItemList orderItemList = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2)).getItems();
        orderItemList.setBypassCache(true);
        assertEquals(items - 1, orderItemList.size());
    }

    public void testAuditedOperationBasedListSerialization()
    {
        AuditedOrderList list = AuditedOrderFinder.findMany(AuditedOrderFinder.orderId().greaterThan(1));
        AuditedOrderList newList = (AuditedOrderList) serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
        AuditedOrderItemList list2 = list.getItems();
        AuditedOrderItemList newList2 = (AuditedOrderItemList) serializeAndDeserialize(list2);
        assertTrue(newList2.size() > 1);
        list.forceResolve();
        newList = (AuditedOrderList) serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
    }

    public void testAuditedNonOperationBasedListSerialization()
    {
        AuditedOrderList list = new AuditedOrderList(AuditedOrderFinder.findMany(AuditedOrderFinder.orderId().greaterThan(1)));
        AuditedOrderList newList = (AuditedOrderList) serializeAndDeserialize(list);
        assertTrue(newList.size() > 1);
        AuditedOrderItemList list2 = new AuditedOrderItemList(list.getItems());
        AuditedOrderItemList newList2 = (AuditedOrderItemList) serializeAndDeserialize(list2);
        assertTrue(newList2.size() > 1);
        newList = (AuditedOrderList) serializeAndDeserialize(list.getDetachedCopy());
        assertTrue(newList.size() > 1);
        AuditedOrder order = newList.get(0);
        int orderId = order.getOrderId();
        newList.remove(0);
        newList.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        assertNull(AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(orderId)));
    }

    private byte[] fromHexString(String hex)
    {
        byte[] result = new byte[hex.length() / 2];
        for(int i=0;i<hex.length();i+=2)
        {
            int x = Integer.parseInt(hex.substring(i, i+2), 16);
            result[i/2] = (byte) x;
        }
        return result;
    }

    private String toHexString(byte[] bytes)
    {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for(int i=0;i<bytes.length;i++)
        {
            int x = ((int)bytes[i]) & 0xFF;
            String xstr = Integer.toHexString(x);
            if (xstr.length() == 1) builder.append('0');
            builder.append(xstr);
        }
        return builder.toString();
    }

    public void testSerialization()
    {
        MithraTimestamp original = new MithraTimestamp(12345678L, true);
        //Below represents the byte format of the above MithraTimestamp object created using MithraTimestamp as in mithra version 11.1.0
        //Changes to MithraTimestamp's serialization should be caught to ensure that consumers
        //and service providers use compatible versions of MithraTimestamp.
        String encodedObject = "aced00057372002c636f6d2e67732e66772e636f6d6d6f6e2e6d69746872612e7574696c2e4d697468726154696d657374616d701d1bd2250dd0e8e50300015a001174696d657a6f6e6553656e736974697665787200126a6176612e73716c2e54696d657374616d702618d5c80153bf650200014900056e616e6f737872000e6a6176612e7574696c2e44617465686a81014b597419030000787077080000000000bc5ea87828697580770d010000000000bc614e2869758078";
        MithraTimestamp copy = (MithraTimestamp) deserialize(fromHexString(encodedObject));

        assertTrue("MithraTimestamp serialization format as in mithra-10.11.0 does not match with current version.", original.equals(copy));
        assertEquals("MithraTimestamp serialization format as in mithra-10.11.0 does not match with current version.", original.getNanos(), copy.getNanos());
        assertEquals("MithraTimestamp serialization format as in mithra-10.11.0 does not match with current version.", original.getTime(), copy.getTime());
    }

}
