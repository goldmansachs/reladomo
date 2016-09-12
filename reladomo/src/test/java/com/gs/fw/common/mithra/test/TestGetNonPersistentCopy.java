
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

import java.sql.Timestamp;

import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.child.LifeStatus;
import com.gs.fw.common.mithra.test.domain.child.LifeStatusFinder;
import com.gs.fw.common.mithra.test.domain.parent.ParentType;
import com.gs.fw.common.mithra.test.domain.parent.ParentTypeFinder;

public class TestGetNonPersistentCopy
        extends MithraTestAbstract
{
    private static final Timestamp AS_OF = Timestamp.valueOf("2005-02-01 00:00:00.0");

    public void testWithReadOnlyObjectWithRelationships()
    {
        ParentType parentType = ParentTypeFinder.findOne(ParentTypeFinder.id().eq(1));
        assertEquals("James Kim", parentType.getName());
        assertEquals(1, parentType.getNameEquals("James Kim").size());
        ParentType copy = parentType.getNonPersistentCopy();
        assertTrue(copy.getLiveTest().isAlive());
        assertEquals("James Kim", copy.getName());
        ParentType fakeType = new ParentType();
        assertEquals(1, copy.getNameEquals("James Kim").size());
        
        fakeType.setId(-100);
        assertNotSame(fakeType, copy.getLiveTest());
        copy.setLiveTest(fakeType);

        assertEquals(-100, copy.getId());
        assertSame(fakeType, copy.getLiveTest());
    }

    public void testWithSimpleReadOnlyObject()
    {
        LifeStatus lifeStatus = LifeStatusFinder.findOne(LifeStatusFinder.isAlive().eq(true));
        assertNotNull(lifeStatus);
        LifeStatus copy = lifeStatus.getNonPersistentCopy();
        assertNotSame(copy, lifeStatus);
        assertTrue(copy.isIsAlive());
        copy.setIsAlive(false);
        assertFalse(copy.isIsAlive());
    }

    public void testWithReadOnlyDatedObject()
    {
        DatedEntity datedEntity =
                DatedEntityFinder.findOne(DatedEntityFinder.id().eq(5).and(DatedEntityFinder.processingDate().eq(AS_OF)));
        assertEquals(5, datedEntity.getDatedEntityDesc().getId());
        assertEquals(1.1, datedEntity.getRate(), 0.001);
        DatedEntity copy = datedEntity.getNonPersistentCopy();
        assertEquals(5, copy.getDatedEntityDesc().getId());
        assertEquals(1.1, copy.getRate(), 0.001);
        assertEquals(1, copy.getExpensiveEntities(1).size());
        DatedEntityDesc newDesc = new DatedEntityDesc(new Timestamp(0), new Timestamp(9999999));
        newDesc.setId(-900);
        copy.setRate(100);
        copy.setDatedEntityDesc(newDesc);
        assertSame(newDesc, copy.getDatedEntityDesc());
        assertEquals(100, copy.getRate(), 0.001);
        assertEquals(-900, copy.getId());
    }

    public void testWithWriteableObject()
    {
        Book book = BookFinder.findOne(BookFinder.inventoryId().eq(1));
        assertEquals("Design Patterns", book.getDescription());
        Book copy = book.getNonPersistentCopy();
        assertEquals("Design Patterns", copy.getDescription());
        copy.setDescription("new description");
        assertEquals("new description", copy.getDescription());
        assertEquals("Design Patterns", book.getDescription());
    }

    public void testWithWriteableDatedObject()
    {
        AuditedOrderItem orderItem = AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.id().eq(3).and(AuditedOrderItemFinder.processingDate().eq(AS_OF)));
        assertEquals(2, orderItem.getProductInfo().getProductId());
        AuditedOrderItem copy = orderItem.getNonPersistentCopy();
        assertEquals(2, copy.getProductInfo().getProductId());
        Product product = new Product();
        product.setProductId(999);
        copy.setProductInfo(product);
        assertSame(product, copy.getProductInfo());
        assertEquals(999, copy.getProductId());
    }
}
