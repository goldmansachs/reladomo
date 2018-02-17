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

import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TemporaryContext;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.ParaProduct;
import com.gs.fw.common.mithra.test.domain.ParaProductDriver;
import com.gs.fw.common.mithra.test.domain.ParaProductDriverFinder;
import com.gs.fw.common.mithra.test.domain.ParaProductDriverList;
import com.gs.fw.common.mithra.test.domain.ParaProductFinder;
import com.gs.fw.common.mithra.test.domain.ParaProductList;
import com.gs.fw.common.mithra.test.domain.PositionDriver;
import com.gs.fw.common.mithra.test.domain.PositionDriverFinder;
import com.gs.fw.common.mithra.test.domain.PositionDriverList;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductFinder;
import com.gs.fw.common.mithra.test.domain.ProductList;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistory;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistoryFinder;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistoryList;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantity;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityFinder;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityList;
import com.gs.fw.common.mithra.util.MithraArrayTupleTupleSet;
import com.gs.fw.common.mithra.util.TupleSet;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.sql.Timestamp;
import java.util.List;


public class TestTupleIn extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        return new Class[] { Product.class, ParaProduct.class, ParaProductDriver.class, PositionQuantity.class, PositionDriver.class, PositionAdjustmentHistory.class};
    }

    private TupleAttribute getProductTupleAttribute()
    {
        return ParaProductFinder.gsn().tupleWith(ParaProductFinder.cusip());
    }

    public void testSmallTupleIn()
    {
        ParaProductDriverList drivers = new ParaProductDriverList();
        drivers.add(createParaProductDriver("A","ABC123","12345"));

        Operation op = getProductTupleAttribute().in(drivers, new Extractor[] { ParaProductDriverFinder.gsn(), ParaProductDriverFinder.cusip()});
        op = op.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products = new ParaProductList(op);
        assertEquals(0, products.size());

        ParaProductDriverList drivers2 = new ParaProductDriverList();
        drivers2.add(createParaProductDriver("A","ABC124","12346"));
        drivers2.add(createParaProductDriver("A","ABC124","12347"));

        Operation op2 = getProductTupleAttribute().in(drivers2, new Extractor[] { ParaProductDriverFinder.gsn(), ParaProductDriverFinder.cusip()});
        op2 = op2.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products2 = new ParaProductList(op2);
        assertEquals(2, products2.size());

        drivers2 = new ParaProductDriverList();
        drivers2.add(createParaProductDriver("A","ABC124","12346"));
        drivers2.add(createParaProductDriver("A","ABC124","12347"));
        drivers2.add(createParaProductDriver("A","ABC125","12347"));

        op2 = getProductTupleAttribute().in(drivers2, new Extractor[] { ParaProductDriverFinder.gsn(), ParaProductDriverFinder.cusip()});
        op2 = op2.and(ParaProductFinder.acmapCode().eq("A"));
        products2 = new ParaProductList(op2);
        assertEquals(2, products2.size());
    }

    public void testSmallTupleInWithSubstring()
    {
        ParaProductDriverList drivers = new ParaProductDriverList();
        drivers.add(createParaProductDriver("A","ABC123","12345"));

        Operation op = getProductTupleAttribute().in(drivers, new Extractor[] { ParaProductDriverFinder.gsn(), ParaProductDriverFinder.cusip()});
        op = op.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products = new ParaProductList(op);
        assertEquals(0, products.size());

        ParaProductDriverList drivers2 = new ParaProductDriverList();
        drivers2.add(createParaProductDriver("A","ABC124","12346"));
        drivers2.add(createParaProductDriver("A","ABC124","12347"));

        TupleAttribute tupleAttribute = ParaProductFinder.gsn().substring(0,6).tupleWith(ParaProductFinder.cusip());
        Operation op2 = tupleAttribute.in(drivers2, new Extractor[]{ParaProductDriverFinder.gsn(), ParaProductDriverFinder.cusip()});
        op2 = op2.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products2 = new ParaProductList(op2);
        assertEquals(2, products2.size());

        drivers2 = new ParaProductDriverList();
        drivers2.add(createParaProductDriver("A","ABC124","12346"));
        drivers2.add(createParaProductDriver("A", "ABC124", "12347"));
        drivers2.add(createParaProductDriver("A","ABC125","12347"));

        op2 = tupleAttribute.in(drivers2, new Extractor[]{ParaProductDriverFinder.gsn(), ParaProductDriverFinder.cusip()});
        op2 = op2.and(ParaProductFinder.acmapCode().eq("A"));
        products2 = new ParaProductList(op2);
        assertEquals(2, products2.size());
    }

    public void testSmallTupleInWithSet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("ABC123","12345");

        Operation op = getProductTupleAttribute().in(set);
        op = op.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products = new ParaProductList(op);
        assertEquals(0, products.size());

        TupleSet set2 = new MithraArrayTupleTupleSet();
        set2.add("ABC124","12346");
        set2.add("ABC124","12347");

        Operation op2 = getProductTupleAttribute().in(set2);
        op2 = op2.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products2 = new ParaProductList(op2);
        assertEquals(2, products2.size());
    }

    public void testTupleInInTransactionWithSource()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testSmallTupleInWithSet();
                return null;
            }
        });
    }

    public void testTupleInInTransactionWithSourceTwice()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testSmallTupleInWithSet();
                testSmallTupleInWithSet();
                testMediumTupleInWithSet();
                testMediumTupleInWithSet();
                return null;
            }
        });
    }

    public void testMediumTupleInViaAggregateQuery()
    {
        TemporaryContext tempContext = PositionDriverFinder.createTemporaryContext("A");
        try
        {
            PositionDriverList drivers = createPositionDriverA();
            drivers.insertAll();
            AggregateList aggList = new AggregateList(PositionDriverFinder.acmapCode().eq("A"));
            aggList.addGroupBy("acct", PositionDriverFinder.accountId());
            aggList.addGroupBy("prod", PositionDriverFinder.productId());
            Operation op = PositionQuantityFinder.acmapCode().eq("A");
            op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
            op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(aggList, "acct", "prod"));
            PositionQuantityList pqList = new PositionQuantityList(op);
            assertEquals(5, pqList.size());
        }
        finally
        {
            tempContext.destroy();
        }
    }

    public void testMediumTupleInViaAggregateQuery2()
    {
        TemporaryContext tempContext = PositionDriverFinder.createTemporaryContext("A");
        try
        {
            PositionDriverList drivers = createPositionDriverB();
            drivers.insertAll();
            AggregateList aggList = new AggregateList(PositionDriverFinder.acmapCode().eq("A"));
            aggList.addGroupBy("acct", PositionDriverFinder.accountId());
            aggList.addGroupBy("prod", PositionDriverFinder.productId());
            Operation op = PositionQuantityFinder.acmapCode().eq("A");
            op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
            op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(aggList, "acct", "prod"));
            PositionQuantityList pqList = new PositionQuantityList(op);
            assertEquals(6, pqList.size());
        }
        finally
        {
            tempContext.destroy();
        }
    }

    public void testMediumTupleIn()
    {
        List drivers = createPositionDriverA();
        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(drivers, new Extractor[] { PositionDriverFinder.accountId(), PositionDriverFinder.productId()} ));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(5, pqList.size());
    }

    public void testMediumTupleIn2()
    {
        List drivers = createPositionDriverB();
        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(drivers, new Extractor[] { PositionDriverFinder.accountId(), PositionDriverFinder.productId()} ));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(6, pqList.size());
    }

    public void testMediumTupleInWithTruncation()
    {
        List drivers = new OrderList();
        drivers.add(createOrder("7616150501", 1522));
        drivers.add(createOrder("7616030301", 1522));
        drivers.add(createOrder("7616030401", 1522));
        drivers.add(createOrder("7616030601", 1522));
        drivers.add(createOrder("7616030701", 1522));
        drivers.add(createOrder("7616030801", 1523));
        drivers.add(createOrder("longString12345678901234567890", 1220));
        drivers.add(createOrder("longString12345678901234567890xyz", 1221));

        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(drivers, new Extractor[] { OrderFinder.description(), OrderFinder.userId()} ));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(6, pqList.size());
    }

    private Order createOrder(String s, int i)
    {
        Order order = new Order();
        order.setDescription(s);
        order.setUserId(i);
        return order;
    }

    public void testMediumTupleIn2Substring()
    {
        List drivers = createPositionDriverC();
        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().substring(0,8).tupleWith(PositionQuantityFinder.productId()).in(drivers, new Extractor[] { PositionDriverFinder.accountId(), PositionDriverFinder.productId()} ));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(6, pqList.size());
    }

    public void testMediumTupleInWithAsOfAttribute()
    {
        //todo: fullcache: implement MultiEquality + MultiIn as one operation
        if (PositionQuantityFinder.getMithraObjectPortal().isPartiallyCached())
        {
            List drivers = PositionAdjustmentHistoryFinder.findMany(PositionAdjustmentHistoryFinder.acmapCode().eq("A"));
            Operation op = PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).tupleWith(PositionQuantityFinder.businessDate()).tupleWith(PositionQuantityFinder.acmapCode()).
                    in(drivers, new Extractor[] { PositionAdjustmentHistoryFinder.accountId(), PositionAdjustmentHistoryFinder.productId(), PositionAdjustmentHistoryFinder.businessDate(), PositionAdjustmentHistoryFinder.acmapCode()} );
            PositionQuantityList pqList = new PositionQuantityList(op);
            assertEquals(6, pqList.size());
        }
    }

    public void testMediumTupleInWithAsOfAttributeAndNull()
    {
        //todo: fullcache: implement MultiEquality + MultiIn as one operation
        if (PositionQuantityFinder.getMithraObjectPortal().isPartiallyCached())
        {
            List drivers = FastList.newList(PositionAdjustmentHistoryFinder.findMany(PositionAdjustmentHistoryFinder.acmapCode().eq("A")));
            PositionAdjustmentHistory history = new PositionAdjustmentHistory();
            history.setAcmapCode("A");
            drivers.add(0, history); // nulls
            Operation op = PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).tupleWith(PositionQuantityFinder.businessDate()).tupleWith(PositionQuantityFinder.acmapCode()).
                    inIgnoreNulls(drivers, new Extractor[]{PositionAdjustmentHistoryFinder.accountId(), PositionAdjustmentHistoryFinder.productId(), PositionAdjustmentHistoryFinder.businessDate(), PositionAdjustmentHistoryFinder.acmapCode()});
            PositionQuantityList pqList = new PositionQuantityList(op);
            assertEquals(6, pqList.size());
        }
    }

    public void testMediumTupleInWithSet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("7616150501", 1522);
        set.add("7616030301", 1522);
        set.add("7616030401", 1522);
        set.add("7616030601", 1522);
        set.add("7616030701", 1522);

        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(set));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(5, pqList.size());
    }

    public void testMediumTupleInWithSet2()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("7616150501", 1522);
        set.add("7616030301", 1522);
        set.add("7616030401", 1522);
        set.add("7616030601", 1522);
        set.add("7616030701", 1522);
        set.add("7616030801", 1523);

        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(set));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(6, pqList.size());
    }

    public void testMediumTupleInWithSet2AndSourceIn()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("7616150501", 1522);
        set.add("7616030301", 1522);
        set.add("7616030401", 1522);
        set.add("7616030601", 1522);
        set.add("7616030701", 1522);
        set.add("7616030801", 1523);

        Operation op = PositionQuantityFinder.acmapCode().in(UnifiedSet.newSetWith("A", "B"));
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(set));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(6, pqList.size());
    }

    public void testMediumTupleInWithSetAndTruncation()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("7616150501", 1522);
        set.add("7616030301", 1522);
        set.add("7616030401", 1522);
        set.add("7616030601", 1522);
        set.add("7616030701", 1522);
        set.add("7616030801", 1523);
        set.add("longString12345678901234567890", 1220);
        set.add("longString12345678901234567890xyz", 1221);

        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
        op = op.and(PositionQuantityFinder.accountId().tupleWith(PositionQuantityFinder.productId()).in(set));
        PositionQuantityList pqList = new PositionQuantityList(op);
        assertEquals(6, pqList.size());
    }

    public void testTupleInWithRelationship()
    {
        List drivers = createPositionDriverA();
        Operation op = PositionAdjustmentHistoryFinder.acmapCode().eq("A");
        op = op.and(PositionAdjustmentHistoryFinder.positionQuantity().accountId().tupleWith(PositionAdjustmentHistoryFinder.positionQuantity().productId()).in(drivers, new Extractor[] { PositionDriverFinder.accountId(), PositionDriverFinder.productId()}));
        PositionAdjustmentHistoryList pqList = new PositionAdjustmentHistoryList(op);
        assertEquals(5, pqList.size());
    }

    public void testTupleInWithRelationship2()
    {
        List drivers = createPositionDriverB();
        Operation op = PositionAdjustmentHistoryFinder.acmapCode().eq("A");
        op = op.and(PositionAdjustmentHistoryFinder.positionQuantity().accountId().tupleWith(PositionAdjustmentHistoryFinder.positionQuantity().productId()).in(drivers, new Extractor[] { PositionDriverFinder.accountId(), PositionDriverFinder.productId()}));
        PositionAdjustmentHistoryList pqList = new PositionAdjustmentHistoryList(op);
        assertEquals(6, pqList.size());
    }

    private PositionDriverList createPositionDriverA()
    {
        PositionDriverList list = new PositionDriverList();
        list.add(createPositionDriver("A", "7616150501", 1522));
        list.add(createPositionDriver("A", "7616030301", 1522));
        list.add(createPositionDriver("A", "7616030401", 1522));
        list.add(createPositionDriver("A", "7616030601", 1522));
        list.add(createPositionDriver("A", "7616030701", 1522));
        return list;
    }

    private PositionDriverList createPositionDriverB()
    {
        PositionDriverList list = new PositionDriverList();
        list.add(createPositionDriver("A", "7616150501", 1522));
        list.add(createPositionDriver("A", "7616030301", 1522));
        list.add(createPositionDriver("A", "7616030401", 1522));
        list.add(createPositionDriver("A", "7616030601", 1522));
        list.add(createPositionDriver("A", "7616030701", 1522));
        list.add(createPositionDriver("A", "7616030801", 1523));
        return list;
    }

    private PositionDriverList createPositionDriverC()
    {
        PositionDriverList list = new PositionDriverList();
        list.add(createPositionDriver("A", "76161505", 1522));
        list.add(createPositionDriver("A", "76160303", 1522));
        list.add(createPositionDriver("A", "76160304", 1522));
        list.add(createPositionDriver("A", "76160306", 1522));
        list.add(createPositionDriver("A", "76160307", 1522));
        list.add(createPositionDriver("A", "76160308", 1523));
        return list;
    }

    private PositionDriver createPositionDriver(String source, String account, int productId)
    {
        PositionDriver pd = new PositionDriver();
        pd.setAcmapCode(source);
        pd.setAccountId(account);
        pd.setProductId(productId);
        return pd;
    }

    private ParaProductDriver createParaProductDriver(String source, String gsn, String cusip)
    {
        ParaProductDriver driver = new ParaProductDriver();
        driver.setAcmapCode(source);
        driver.setGsn(gsn);
        driver.setCusip(cusip);
        return driver;
    }

    public void testTupleIn()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("AA", "Product 1", 1);
        set.add("AA", "Product 2", 1);
        set.add("AB", "Product 3", 2);
        set.add("AB", "Product 4", 3);

        ProductList list = new ProductList(ProductFinder.productCode().tupleWith(ProductFinder.productDescription(),ProductFinder.manufacturerId()).in(set));
        assertEquals(4, list.size());
    }

    public void testMediumTupleInWithSourceToSourcelessRelationship() throws Exception
    {
        Timestamp buzDate = new Timestamp(timestampFormat.parse("2010-10-11 00:00:00").getTime());
        ProductFinder.ProductSingleFinderForRelatedClasses<PositionQuantity, Product, PositionQuantity> pf = PositionQuantityFinder.product();
        TupleAttribute tupleAttribute = pf.productCode().tupleWith(pf.manufacturerId());

        Operation op = tupleAttribute.in(createPositionDriverB(), new Extractor[]{PositionDriverFinder.accountId(), PositionDriverFinder.productId()});
        op = op.and(PositionQuantityFinder.acmapCode().eq(SOURCE_A));
        op = op.and(PositionQuantityFinder.businessDate().eq(buzDate));
        PositionQuantityList list = PositionQuantityFinder.findMany(op);
        list.forceResolve();
    }

    public void testMediumTupleInWithSourcelessToSourceRelationship() throws Exception
    {
        Timestamp buzDate = new Timestamp(timestampFormat.parse("2010-10-11 00:00:00").getTime());
        PositionQuantityFinder.PositionQuantityCollectionFinderForRelatedClasses<Product, PositionQuantityList, Product> positions = ProductFinder.positions(SOURCE_A, buzDate);
        TupleAttribute tupleAttribute = positions.accountId().tupleWith(positions.productId());
        Operation op = tupleAttribute.in(createPositionDriverB(), new Extractor[] { PositionDriverFinder.accountId(), PositionDriverFinder.productId()});
        ProductList list = ProductFinder.findMany(op);
        list.forceResolve();
    }
}