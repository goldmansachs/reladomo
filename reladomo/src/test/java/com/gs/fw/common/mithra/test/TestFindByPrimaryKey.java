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

import com.gs.fw.common.mithra.cache.bean.I3O3L3;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.ExchangeRate;
import com.gs.fw.common.mithra.test.domain.ExchangeRateFinder;
import com.gs.fw.common.mithra.test.domain.ListEntryContactsImpl;
import com.gs.fw.common.mithra.test.domain.ListEntryContactsImplFinder;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.TestCheckGsDesk;
import com.gs.fw.common.mithra.test.domain.TestCheckGsDeskFinder;
import com.gs.fw.common.mithra.test.domain.UserGroup;
import com.gs.fw.common.mithra.test.domain.UserGroupFinder;
import com.gs.fw.common.mithra.test.domain.VariousTypes;
import com.gs.fw.common.mithra.test.domain.VariousTypesFinder;

import java.sql.Timestamp;


public class TestFindByPrimaryKey extends MithraTestAbstract
{

    public TestFindByPrimaryKey(String s)
    {
        super(s);
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
                Order.class,
                UserGroup.class,
                ExchangeRate.class,
                AuditedOrder.class,
                BitemporalOrder.class,
                ListEntryContactsImpl.class,
                TestCheckGsDesk.class,
                VariousTypes.class,
        };
    }

    private void assertCorrectCounts(int objectCacheHits, int queryCacheHits, RelatedFinder finderInstance)
    {
        if (!finderInstance.getMithraObjectPortal().isPartiallyCached())
        {
            assertEquals(objectCacheHits, finderInstance.getMithraObjectPortal().getPerformanceData().getObjectCacheHits());
            assertEquals(queryCacheHits, finderInstance.getMithraObjectPortal().getPerformanceData().getQueryCacheHits());
        }
    }

    public void testSingleInt()
    {
        int objectCacheHits = OrderFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = OrderFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();
        Order order = OrderFinder.findByPrimaryKey(1);
        assertNotNull(order);
        assertEquals(1, order.getOrderId());
        assertCorrectCounts(objectCacheHits, queryCacheHits, OrderFinder.getFinderInstance());
    }

    public void testNoneCache()
    {
        int start = getRetrievalCount();
        assertNotNull(TestCheckGsDeskFinder.findByPrimaryKey(4, 1));
        assertNotNull(TestCheckGsDeskFinder.findByPrimaryKey(4, 1));
        assertNotNull(TestCheckGsDeskFinder.findByPrimaryKey(4, 1));
        assertEquals(start + 3, getRetrievalCount());
    }

    public void testThreeInts()
    {
        int objectCacheHits = UserGroupFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = UserGroupFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();
        UserGroup userGroup = UserGroupFinder.findByPrimaryKey(5, 3, 0);
        assertNotNull(userGroup);
        assertEquals(5, userGroup.getOwnerId());
        assertEquals(3, userGroup.getDependentId());
        assertEquals(0, userGroup.getSourceId());
        assertCorrectCounts(objectCacheHits, queryCacheHits, UserGroupFinder.getFinderInstance());
    }

    public void testThreeObjectAndOneInt()
    {
        int objectCacheHits = ExchangeRateFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = ExchangeRateFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();
        Timestamp date = Timestamp.valueOf("2004-09-30 18:30:00.0");
        ExchangeRate exchangeRate = ExchangeRateFinder.findByPrimaryKey("USD", 10, date, "A");
        assertNotNull(exchangeRate);
        assertEquals("USD", exchangeRate.getCurrency());
        assertEquals(10, exchangeRate.getSource());
        assertEquals(date, exchangeRate.getDate());
        assertEquals("A", exchangeRate.getAcmapCode());
        assertCorrectCounts(objectCacheHits, queryCacheHits, ExchangeRateFinder.getFinderInstance());
    }

    public void testAuditedSingleInt()
    {
        Timestamp date = Timestamp.valueOf("2010-01-05 10:06:00.0");
        int objectCacheHits = AuditedOrderFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = AuditedOrderFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();
        AuditedOrder order = AuditedOrderFinder.findByPrimaryKey(1, date);
        assertNotNull(order);
        assertEquals(1, order.getOrderId());
        assertCorrectCounts(objectCacheHits, queryCacheHits, AuditedOrderFinder.getFinderInstance());
    }

    public void testBitemporalSingleInt()
    {
        Timestamp date = Timestamp.valueOf("2010-01-05 10:06:00.0");
        int objectCacheHits = BitemporalOrderFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = BitemporalOrderFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();
        BitemporalOrder order = BitemporalOrderFinder.findByPrimaryKey(1, date, date);
        assertNotNull(order);
        assertEquals(1, order.getOrderId());
        assertCorrectCounts(objectCacheHits, queryCacheHits, BitemporalOrderFinder.getFinderInstance());
    }

    public void testByteArray()
    {
        int objectCacheHits = ListEntryContactsImplFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = ListEntryContactsImplFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();
        byte[] data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);
        ListEntryContactsImpl contacts = ListEntryContactsImplFinder.findByPrimaryKey(data, "two", "y");
        assertNotNull(contacts);
        assertEquals("two", contacts.getEmplId());
        assertEquals("y", contacts.getListEntryRole());
        assertCorrectCounts(objectCacheHits, queryCacheHits, ListEntryContactsImplFinder.getFinderInstance());
    }

    public void testFloat()
    {
        int objectCacheHits = VariousTypesFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = VariousTypesFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();

        VariousTypes types = VariousTypesFinder.findByFloatColumn(15.0f);
        assertNotNull(types);
        assertEquals(1, types.getId());
        assertCorrectCounts(objectCacheHits, queryCacheHits, VariousTypesFinder.getFinderInstance());
    }

    public void testDouble()
    {
        int objectCacheHits = VariousTypesFinder.getMithraObjectPortal().getPerformanceData().getObjectCacheHits();
        int queryCacheHits = VariousTypesFinder.getMithraObjectPortal().getPerformanceData().getQueryCacheHits();

        VariousTypes types = VariousTypesFinder.findByDoubleColumn(10.0);
        assertNotNull(types);
        assertEquals(1, types.getId());
        assertCorrectCounts(objectCacheHits, queryCacheHits, VariousTypesFinder.getFinderInstance());

    }

    public void testI3O3L3ConversionRange()
    {
        I3O3L3 bean = new I3O3L3(-1);
        short s = Short.MIN_VALUE;
        bean.setI1AsInteger(s);
        assertEquals(s, bean.getI1AsShort());
        s = Short.MAX_VALUE;
        bean.setI1AsInteger(s);
        assertEquals(s, bean.getI1AsShort());
        byte b = Byte.MIN_VALUE;
        bean.setI1AsInteger(b);
        assertEquals(b, bean.getI1AsByte());
        b = Byte.MAX_VALUE;
        bean.setI1AsInteger(b);
        assertEquals(b, bean.getI1AsByte());
        bean.setI1AsBoolean(true);
        assertTrue(bean.getI1AsBoolean());
        bean.setI1AsBoolean(false);
        assertFalse(bean.getI1AsBoolean());
    }
}
