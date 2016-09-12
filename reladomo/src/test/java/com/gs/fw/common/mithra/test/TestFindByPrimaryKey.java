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

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.cache.CacheClock;
import com.gs.fw.common.mithra.cache.bean.I3O3L3;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.glew.LewContract;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.test.glew.LewTransaction;
import com.gs.fw.common.mithra.test.glew.LewTransactionFinder;
import com.gs.fw.common.mithra.test.util.Log4JRecordingAppender;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import org.apache.log4j.spi.LoggingEvent;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


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
