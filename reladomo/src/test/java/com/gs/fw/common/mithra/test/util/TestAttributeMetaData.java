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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.remote.RemoteTransactionId;
import com.gs.fw.common.mithra.test.domain.Account;
import com.gs.fw.common.mithra.test.domain.AccountFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItemFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItemStatus;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItemStatusFinder;
import com.gs.fw.common.mithra.test.domain.Employee;
import com.gs.fw.common.mithra.test.domain.EmployeeFinder;
import com.gs.fw.common.mithra.test.domain.FirmFinder;
import com.gs.fw.common.mithra.test.domain.LocationFinder;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderStatusFinder;
import com.gs.fw.common.mithra.test.domain.PersonFinder;
import com.gs.fw.common.mithra.test.domain.ProductSynonymFinder;
import com.gs.fw.common.mithra.test.domain.RestrictedEntity;
import com.gs.fw.common.mithra.test.domain.TestAbstractMessage;
import com.gs.fw.common.mithra.test.domain.TestSecurityJournalFinder;
import junit.framework.TestCase;

import java.io.*;
import java.sql.Timestamp;


public class TestAttributeMetaData extends TestCase
{
    public void testMetaData()
    {
        assertTrue(OrderFinder.description().getMetaData().isNullable());
        assertFalse(OrderFinder.orderId().getMetaData().isNullable());
        assertEquals("ORDER_ID", OrderFinder.orderId().getMetaData().getColumnName());

        assertEquals(Boolean.TRUE, TestSecurityJournalFinder.sourceSystem().getProperty(TestAbstractMessage.BOOLEAN_KEY));
        assertEquals(TestAbstractMessage.VALUE, TestSecurityJournalFinder.sourceSystem().getProperty(TestAbstractMessage.KEY));
        assertEquals("items.orderId", OrderFinder.items().orderId().getAttributeName());
        assertEquals("items.productInfo.productCode", OrderFinder.items().productInfo().productCode().getAttributeName());
    }

    public void testOwningRelationshipMetaData()
    {
        // primary key
        assertNull(OrderFinder.orderId().getOwningRelationship());
        assertNull(OrderFinder.orderId().getOwningReverseRelationshipOwnerPackage());
        assertNull(OrderFinder.orderId().getOwningReverseRelationshipOwner());
        assertNull(OrderFinder.orderId().getOwningReverseRelationshipName());

        // many-to-one with reverse
        assertSame(OrderItemFinder.order(), OrderItemFinder.orderId().getOwningRelationship());
        assertEquals("com.gs.fw.common.mithra.test.domain", OrderItemFinder.orderId().getOwningReverseRelationshipOwnerPackage());
        assertEquals("Order", OrderItemFinder.orderId().getOwningReverseRelationshipOwner());
        assertEquals("items", OrderItemFinder.orderId().getOwningReverseRelationshipName());

        // many-to-one without reverse
        assertSame(OrderItemFinder.productInfo(), OrderItemFinder.productId().getOwningRelationship());
        assertNull(OrderItemFinder.productId().getOwningReverseRelationshipOwnerPackage());
        assertNull(OrderItemFinder.productId().getOwningReverseRelationshipOwner());
        assertNull(OrderItemFinder.productId().getOwningReverseRelationshipName());

        // one-to-one, relatedIsDependent, with reverse
        assertSame(OrderStatusFinder.order(), OrderStatusFinder.orderId().getOwningRelationship());
        assertEquals("com.gs.fw.common.mithra.test.domain", OrderStatusFinder.orderId().getOwningReverseRelationshipOwnerPackage());
        assertEquals("Order", OrderStatusFinder.orderId().getOwningReverseRelationshipOwner());
        assertEquals("orderStatus", OrderStatusFinder.orderId().getOwningReverseRelationshipName());

        // one-to-one, relatedIsDependent, reverse only
        assertNull(AuditedOrderItemStatusFinder.itemId().getOwningRelationship());
        assertEquals("com.gs.fw.common.mithra.test.domain", AuditedOrderItemStatusFinder.itemId().getOwningReverseRelationshipOwnerPackage());
        assertEquals("AuditedOrderItem", AuditedOrderItemStatusFinder.itemId().getOwningReverseRelationshipOwner());
        assertEquals("auditedOrderItemStatus", AuditedOrderItemStatusFinder.itemId().getOwningReverseRelationshipName());

        // one-to-many
        assertNull(ProductSynonymFinder.productId().getOwningRelationship());
        assertEquals("com.gs.fw.common.mithra.test.domain", ProductSynonymFinder.productId().getOwningReverseRelationshipOwnerPackage());
        assertEquals("Product", ProductSynonymFinder.productId().getOwningReverseRelationshipOwner());
        assertEquals("synonyms", ProductSynonymFinder.productId().getOwningReverseRelationshipName());

        // one-to-many, non-transactional
        assertNull(LocationFinder.groupId().getOwningRelationship());
        assertEquals("com.gs.fw.common.mithra.test.domain", LocationFinder.groupId().getOwningReverseRelationshipOwnerPackage());
        assertEquals("Group", LocationFinder.groupId().getOwningReverseRelationshipOwner());
        assertEquals("locations", LocationFinder.groupId().getOwningReverseRelationshipName());
    }
}
