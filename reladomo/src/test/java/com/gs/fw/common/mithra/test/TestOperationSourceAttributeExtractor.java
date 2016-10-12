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

import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;

import java.util.Map;
import java.util.Set;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import org.junit.Assert;


public class TestOperationSourceAttributeExtractor extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Team.class,
            Player.class,
            Division.class,
            Order.class,
            OrderItem.class,
            Product.class,
            Book.class,
            InventoryItem.class,
            Supplier.class,
            Manufacturer.class,
            Location.class,
            SupplierInventoryItem.class
        };
    }

    public void testSourceAttributeMissing()
    {
        try
        {
            Player player1 = new Player();
            player1.setId(100);
            player1.insert();
        }
        catch (Exception e)
        {
            Assert.assertTrue(e instanceof NullPointerException);
            Assert.assertEquals("sourceAttribute expected for class: 'com.gs.fw.common.mithra.test.domain.Player', please ensure operation or new object correctly specifies the sourceAttribute!",
                    e.getMessage());
        }

        try
        {
            Player player2 = new Player();
            player2.setId(100);
            player2.setSourceId("NewSource");
            player2.insert();
        } catch (Exception e)
        {
            Assert.assertTrue(e instanceof NullPointerException);
            Assert.assertEquals("Could not find connection for class com.gs.fw.common.mithra.test.domain.Player Make sure the class is added to the test xml file. Double check that it's added to the correct connection mananger.  If the object has a source attribute, also ensure that the operation correctly specifies a value for it. No connection manager found. for database: NewSource",
                    e.getMessage());
        }
    }

   public void testWeirdRelationships()
   {
       Operation op = BookFinder.manufacturer().location().city().eq("New York").and(BookFinder.suppliers().location().eq("New York"));
       MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
       Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);

   }

    public void testDbidDefaultSourceAttribute()
    {
        Map expectedDbidMap = new UnifiedMap();
        MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(null, OrderFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        Operation op = OrderFinder.orderId().eq(999999);
        MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
        Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);
        assertEquals(expectedDbidMap, dbidMap);
    }

    public void testDbidWithRelationshipAndDefaultSourceAttribute()
    {
        Map expectedDbidMap = new UnifiedMap();
        MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(null, OrderFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(null, OrderItemFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(null, ProductFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        Operation op = OrderFinder.items().productInfo().productCode().eq("AA");
        MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
        Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);
        assertEquals(expectedDbidMap, dbidMap);
    }

    public void testDbIdExtractionWithOperation()
    {
        Map expectedDbidMap = new UnifiedMap();
        MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A", PlayerFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");

        Operation op = PlayerFinder.sourceId().eq("A").and(PlayerFinder.name().eq("Rafael"));
        MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
        Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);
        assertEquals(expectedDbidMap, dbidMap);
    }

    public void testDbIdExtractionWithMultipleSourceAttributes()
    {
        Map expectedDbidMap = new UnifiedMap();
        MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A", PlayerFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("B", PlayerFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:B");

        Set sourceAttributeSet = new UnifiedSet();
        sourceAttributeSet.add("A");
        sourceAttributeSet.add("B");

        Operation op = PlayerFinder.sourceId().in(sourceAttributeSet).and(PlayerFinder.name().eq("Rafael"));
        MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
        Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);
        assertEquals(expectedDbidMap, dbidMap);
    }

    public void testDbIdExtractionWithRelationship()
    {
        Map expectedDbidMap = new UnifiedMap();
        MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A", PlayerFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A", TeamFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");

        Operation op = TeamFinder.sourceId().eq("A").and(TeamFinder.players().name().eq("Rafael"));
        MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
        Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);
        assertEquals(expectedDbidMap, dbidMap);
    }

    public void testDbIdExtractionWithRelationshipAndMultipleSourceAttributes()
    {
        Map expectedDbidMap = new UnifiedMap();
        MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A", PlayerFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A",TeamFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("B", PlayerFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:B");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("B", TeamFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:B");

        Set sourceAttributeSet = new UnifiedSet();
        sourceAttributeSet.add("A");
        sourceAttributeSet.add("B");
        Operation op = TeamFinder.sourceId().in(sourceAttributeSet).and(TeamFinder.players().name().eq("Rafael"));
        MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
        Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);
        assertEquals(expectedDbidMap, dbidMap);
    }

    public void testDbIdExtractionWithNestedRelationship()
    {
        Map expectedDbidMap = new UnifiedMap();
        MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A",PlayerFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A", TeamFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");
        key = new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey("A", DivisionFinder.getFinderInstance());
        expectedDbidMap.put(key, "localhost:A");

        Operation op = DivisionFinder.sourceId().eq("A").and(DivisionFinder.teams().players().name().eq("Rafael"));
        MithraDatabaseIdentifierExtractor dbidExtractor = new MithraDatabaseIdentifierExtractor();
        Map dbidMap = dbidExtractor.extractDatabaseIdentifierMap(op);
        assertEquals(expectedDbidMap, dbidMap);
    }

}
