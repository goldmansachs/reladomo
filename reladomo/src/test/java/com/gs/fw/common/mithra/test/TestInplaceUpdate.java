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

import java.sql.*;

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;



public class TestInplaceUpdate extends MithraTestAbstract
{

    private  AuditedOrderList getAllAuditedOrders()
    {
        Operation op = AuditedOrderFinder.all();
        AuditedOrderList list = new AuditedOrderList(op);
        assertTrue(list.size() >  0);

        return list;
    }

    public void testAuditedWithMultupleInPlaceUpdateAndTerminate()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderList list = AuditedOrderFinder.findMany(AuditedOrderFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));
                assert(list.size() > 1);
                for(int i=0;i<list.size();i++)
                {
                    list.get(i).setDescriptionUsingInPlaceUpdate("inplace");
                }
                list.get(0).cascadeTerminate();
                return null;
            }
        });
    }
    
    public void testAuditedWithInplaceUpdate()
    {
        AuditedOrderList list = getAllAuditedOrders();
        final AuditedOrder auditedOrder = list.get(0);

        Operation initOp = AuditedOrderFinder.processingDate().equalsEdgePoint()
                                .and(AuditedOrderFinder.processingDateFrom().greaterThan(new Timestamp(1)));
        int originalSize = (new AuditedOrderList(initOp).size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    auditedOrder.setDescriptionUsingInPlaceUpdate("Young");
                    return null;
                }
        });
        Operation op4 = AuditedOrderFinder.processingDate().equalsEdgePoint()
                                .and(AuditedOrderFinder.processingDateFrom().greaterThan(new Timestamp(1)));
        int resultSize = new AuditedOrderList(op4).size();
        assertEquals(originalSize, resultSize);

        Operation op3 = AuditedOrderFinder.description().eq("Young");
        AuditedOrderList list3 = new AuditedOrderList(op3);
        assertTrue(list3.size() == 1) ;
    }

    public void testAuditedWithRegularUpdate()
    {
        AuditedOrderList list = getAllAuditedOrders();
        final AuditedOrder auditedOrder = list.get(0);

        Operation initOp = AuditedOrderFinder.processingDate().equalsEdgePoint()
                                .and(AuditedOrderFinder.processingDateFrom().greaterThan(new Timestamp(1)));
        int originalSize = (new AuditedOrderList(initOp).size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand(){
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    auditedOrder.setDescription("Ryu");
                    return null;
                }
        });
        Operation op4 = AuditedOrderFinder.processingDate().equalsEdgePoint()
                                .and(AuditedOrderFinder.processingDateFrom().greaterThan(new Timestamp(1)));
        int resultSize = new AuditedOrderList(op4).size();
        assertTrue(originalSize < resultSize);

        Operation op5 = AuditedOrderFinder.description().eq("Ryu");
        AuditedOrderList list5 = new AuditedOrderList(op5);
        assertTrue(list5.size() == 1);
    }

    protected BitemporalOrderList getAllBitemporalOrders()
    {
        Operation op = BitemporalOrderFinder.all()
                            .and(BitemporalOrderFinder.businessDate().eq(createReferenceBusinessDate(2000, 1, 1)));
        BitemporalOrderList bitemporalOrders = new BitemporalOrderList(op);
        assertTrue(bitemporalOrders.size() > 0);

        return bitemporalOrders;
    }

    public void testBitemporalWithInplaceUpdate()
    {
        Operation initOp = BitemporalOrderFinder.processingDate().equalsEdgePoint()
                                .and(BitemporalOrderFinder.businessDate().equalsEdgePoint())
                                .and(BitemporalOrderFinder.processingDateFrom().greaterThan(new Timestamp(1))
                                .and(BitemporalOrderFinder.businessDateFrom().greaterThan(new Timestamp(1))));
        int originalSize = (new AuditedOrderList(initOp).size());

        final BitemporalOrder bitemproalOrder = getAllBitemporalOrders().get(0);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand(){
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    bitemproalOrder.setDescriptionUsingInPlaceUpdate("Description InplaceUpdate");
                    return null;
                }
        });
        Operation resultOp = BitemporalOrderFinder.processingDate().equalsEdgePoint()
                                .and(BitemporalOrderFinder.businessDate().equalsEdgePoint())
                                .and(BitemporalOrderFinder.processingDateFrom().greaterThan(new Timestamp(1))
                                .and(BitemporalOrderFinder.businessDateFrom().greaterThan(new Timestamp(1))));
        int resultSize = (new BitemporalOrderList(resultOp).size());
        assertEquals(originalSize, resultSize);

        Operation op3 = BitemporalOrderFinder.description().eq("Description InplaceUpdate")
                            .and(BitemporalOrderFinder.businessDate().eq(createReferenceBusinessDate(2000, 1, 1)));
        BitemporalOrderList list3 = new BitemporalOrderList(op3);
        assertEquals(list3.size(), 1);
    }

    public void testBitemporalWithRegularUpdate()
    {
        Operation initOp = BitemporalOrderFinder.processingDate().equalsEdgePoint()
                                .and(BitemporalOrderFinder.businessDate().equalsEdgePoint())
                                .and(BitemporalOrderFinder.processingDateFrom().greaterThan(new Timestamp(1))
                                .and(BitemporalOrderFinder.businessDateFrom().greaterThan(new Timestamp(1))));
        int originalSize = (new AuditedOrderList(initOp).size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand(){
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    getAllBitemporalOrders().get(0).setDescription("Description Regular Update");
                    return null;
                }
        });
        Operation resultOp = BitemporalOrderFinder.processingDate().equalsEdgePoint()
                                .and(BitemporalOrderFinder.businessDate().equalsEdgePoint())
                                .and(BitemporalOrderFinder.processingDateFrom().greaterThan(new Timestamp(1))
                                .and(BitemporalOrderFinder.businessDateFrom().greaterThan(new Timestamp(1))));
        int resultSize = (new BitemporalOrderList(resultOp).size());
        assertTrue(originalSize < resultSize);

        Operation op4 = BitemporalOrderFinder.description().eq("Description Regular Update")
                            .and(BitemporalOrderFinder.businessDate().eq(createReferenceBusinessDate(2000, 1, 1)));
        BitemporalOrderList list4 = new BitemporalOrderList(op4);
        assertTrue(list4.size() == 1);
    }

    public void testBitemporalWithMixedUpdates()
    {
        Operation initOp = BitemporalOrderFinder.processingDate().equalsEdgePoint()
                                .and(BitemporalOrderFinder.businessDate().equalsEdgePoint())
                                .and(BitemporalOrderFinder.processingDateFrom().greaterThan(new Timestamp(1))
                                .and(BitemporalOrderFinder.businessDateFrom().greaterThan(new Timestamp(1))));
        int originalSize = (new AuditedOrderList(initOp).size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand(){
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    getAllBitemporalOrders().get(0).setDescriptionUsingInPlaceUpdate("Description MIX Update");
                    getAllBitemporalOrders().get(0).setUserId(-666);
                    return null;
                }
        });
        Operation resultOp = BitemporalOrderFinder.processingDate().equalsEdgePoint()
                                .and(BitemporalOrderFinder.businessDate().equalsEdgePoint())
                                .and(BitemporalOrderFinder.processingDateFrom().greaterThan(new Timestamp(1))
                                .and(BitemporalOrderFinder.businessDateFrom().greaterThan(new Timestamp(1))));
        int resultSize = (new BitemporalOrderList(resultOp).size());
        assertTrue(originalSize < resultSize);

        Operation op5 = BitemporalOrderFinder.description().eq("Description MIX Update")
                            .and(BitemporalOrderFinder.businessDate().eq(createReferenceBusinessDate(2000, 1, 1)))
                            .and(BitemporalOrderFinder.userId().eq(-666));
        BitemporalOrderList list5 = new BitemporalOrderList(op5);
        assertTrue(list5.size() == 1);
    }

    public void testAuditedWithInplaceUpdateAndDelete()
    {
        AuditedOrderList list = getAllAuditedOrders();
        final AuditedOrder auditedOrder = list.get(0);

        Operation initOp = AuditedOrderFinder.processingDate().equalsEdgePoint()
                                .and(AuditedOrderFinder.processingDateFrom().greaterThan(new Timestamp(1)));
        int originalSize = (new AuditedOrderList(initOp).size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand(){
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    auditedOrder.setDescriptionUsingInPlaceUpdate("Young");
                    auditedOrder.terminate();
                    return null;
                }
        });
        Operation op4 = AuditedOrderFinder.processingDate().equalsEdgePoint()
                                .and(AuditedOrderFinder.processingDateFrom().greaterThan(new Timestamp(1)));
        int resultSize = new AuditedOrderList(op4).size();
        assertEquals(originalSize, resultSize);

        Operation op3 = AuditedOrderFinder.description().eq("Young");
        AuditedOrderList list3 = new AuditedOrderList(op3);
        assertEquals(0, list3.size());
    }
}
