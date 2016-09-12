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
import com.gs.fw.common.mithra.MithraDatedTransactionalList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.DatedWithNullablePK;
import com.gs.fw.common.mithra.test.domain.DatedWithNullablePKFinder;
import com.gs.fw.common.mithra.test.domain.DatedWithNullablePKList;
import com.gs.fw.common.mithra.test.domain.NotDatedWithNullablePK;
import com.gs.fw.common.mithra.test.domain.NotDatedWithNullablePKFinder;
import com.gs.fw.common.mithra.test.domain.NotDatedWithNullablePKList;
import com.gs.fw.common.mithra.test.domain.Synonym;
import com.gs.fw.common.mithra.test.domain.SynonymFinder;
import com.gs.fw.common.mithra.test.domain.SynonymList;

import java.sql.Timestamp;
import java.util.List;


public class TestNullPrimaryKeyColumn extends MithraTestAbstract
{

        protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new SynonymResultSetComparator());
    }

   public Class[] getRestrictedClassList()
   {
       return new Class[]
       {
           Synonym.class,
           NotDatedWithNullablePK.class,
           DatedWithNullablePK.class
       };
   }

    public void testRetrieval() throws Exception
    {
        Operation dateCondition = SynonymFinder.synonymEndDate().isNull();
        dateCondition = dateCondition.or(SynonymFinder.synonymEndDate().greaterThan(Timestamp.valueOf("2005-01-01 00:00:00")));

        Operation op = SynonymFinder.productId().eq(1234);
        op = op.and(dateCondition);

        List fakeList = new SynonymList(op);

        String sql = "select * from SYNONYM where PRODUCT_ID = 1234 and (EXPIRY_DATE is null or EXPIRY_DATE > '2005-01-01 00:00:00')";
        this.genericRetrievalTest(sql, fakeList, 1);
    }

    public void testFindOriginal() throws Exception
    {
        Operation op = NotDatedWithNullablePKFinder.objectId().eq(1000);
        op = op.and(NotDatedWithNullablePKFinder.nullablePKComponent().isNull());

        NotDatedWithNullablePK obj = NotDatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj);

        obj = obj.getNonPersistentCopy();
        obj.setQuantity(obj.getQuantity() * 2);

        NotDatedWithNullablePK original = obj.zFindOriginal();
        assertNotNull(original);
        assertTrue(original.isNullablePKComponentNull());
        assertEquals(1000, original.getObjectId());
        assertEquals(obj.getQuantity(), original.getQuantity() * 2, 0.0001);
    }

    public void testUpdateObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.objectId().eq(1000);
        op = op.and(NotDatedWithNullablePKFinder.nullablePKComponent().isNull());

        NotDatedWithNullablePK obj = NotDatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertNotNull(NotDatedWithNullablePKFinder.findOne(op));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        assertTrue(obj.isNullablePKComponentNull());
        assertEquals(1, obj.getQuantity(),0);
        obj.setQuantity(100);

        NotDatedWithNullablePKFinder.clearQueryCache();

        NotDatedWithNullablePK obj2 = NotDatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj2);
        assertTrue(obj2.isNullablePKComponentNull());
        assertEquals(100, obj2.getQuantity(), 0);
    }

    public void testMultiUpdateObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.nullablePKComponent().isNull();

        NotDatedWithNullablePKList list = new NotDatedWithNullablePKList(op);
        assertEquals(3, list.size());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        for(int i=0;i < list.size();i++)
        {
            NotDatedWithNullablePK obj = (NotDatedWithNullablePK) list.get(i);
            obj.setQuantity(1000.00);
            obj.setPrice(100.00);
        }
        tx.commit();

        NotDatedWithNullablePKFinder.clearQueryCache();
        NotDatedWithNullablePKList list2 = new NotDatedWithNullablePKList(op);
        for(int i=0;i < list2.size();i++)
        {
            NotDatedWithNullablePK obj = (NotDatedWithNullablePK) list2.get(i);
            assertEquals(1000.00,obj.getQuantity(),0);
            assertEquals(100.00, obj.getPrice(),0);
        }
    }

    public void testMultiDeleteObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.nullablePKComponent().isNull();

        NotDatedWithNullablePKList list = new NotDatedWithNullablePKList(op);
        assertEquals(3, list.size());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        for(int i=0;i < list.size();i++)
        {
            NotDatedWithNullablePK obj = (NotDatedWithNullablePK) list.get(i);
            obj.delete();
        }
        tx.commit();

        NotDatedWithNullablePKFinder.clearQueryCache();
        NotDatedWithNullablePKList list2 = new NotDatedWithNullablePKList(op);
        assertEquals(0,list2.size());
    }

    public void testDatedTransactionalDeleteObjectWithNullablePrimaryKey()
    {
        Operation op = DatedWithNullablePKFinder.businessDate().eq(Timestamp.valueOf("2006-08-16 00:00:00"));
        op = op.and(DatedWithNullablePKFinder.objectId().in(IntHashSet.newSetWith(1234,1235,1236,1237)));

        DatedWithNullablePKList datedWithNullablePKs = new DatedWithNullablePKList(op);
        MithraDatedTransactionalList<DatedWithNullablePK> mithraDatedTransactionalList = new DatedWithNullablePKList();
        mithraDatedTransactionalList.addAll(datedWithNullablePKs);
        mithraDatedTransactionalList.purgeAll();

        DatedWithNullablePKFinder.clearQueryCache();
        DatedWithNullablePKList list2 = new DatedWithNullablePKList(op);
        assertEquals(0, list2.size());
    }

    public void testTransactionalDeleteObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.all();

        NotDatedWithNullablePKList datedWithNullablePKs = new NotDatedWithNullablePKList(op);
        datedWithNullablePKs.deleteAll();

        NotDatedWithNullablePKFinder.clearQueryCache();
        NotDatedWithNullablePKList list2 = new NotDatedWithNullablePKList(op);
        assertEquals(0, list2.size());
    }


    public void testMixedMultiUpdateObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.all();

        NotDatedWithNullablePKList list = new NotDatedWithNullablePKList(op);
        assertEquals(6, list.size());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        for(int i=0;i < list.size();i++)
        {
            NotDatedWithNullablePK obj = (NotDatedWithNullablePK) list.get(i);
            obj.setQuantity(1000.00);
            obj.setPrice(100.00);
        }
        tx.commit();

        NotDatedWithNullablePKFinder.clearQueryCache();
        NotDatedWithNullablePKList list2 = new NotDatedWithNullablePKList(op);
        for(int i=0;i < list2.size();i++)
        {
            NotDatedWithNullablePK obj = (NotDatedWithNullablePK) list2.get(i);
            assertEquals(1000.00,obj.getQuantity(),0);
            assertEquals(100.00, obj.getPrice(),0);
        }
    }

    public void testMixedMultiDeleteObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.all();

        NotDatedWithNullablePKList list = new NotDatedWithNullablePKList(op);
        assertEquals(6, list.size());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        for(int i=0;i < list.size();i++)
        {
            NotDatedWithNullablePK obj = (NotDatedWithNullablePK) list.get(i);
            obj.delete();
        }
        tx.commit();

        NotDatedWithNullablePKFinder.clearQueryCache();
        NotDatedWithNullablePKList list2 = new NotDatedWithNullablePKList(op);
        assertEquals(0, list2.size());
    }

    public void testDeleteObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.objectId().eq(1000);
        op = op.and(NotDatedWithNullablePKFinder.nullablePKComponent().isNull());

        NotDatedWithNullablePK obj = NotDatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj);
        assertTrue(obj.isNullablePKComponentNull());
        obj.delete();

        NotDatedWithNullablePKFinder.clearQueryCache();

        NotDatedWithNullablePK obj2 = NotDatedWithNullablePKFinder.findOne(op);
        assertNull(obj2);
    }

    public void testRefreshObjectWithNullablePrimaryKey()
    {
        Operation op = NotDatedWithNullablePKFinder.objectId().eq(1000);
        op = op.and(NotDatedWithNullablePKFinder.nullablePKComponent().isNull());
        NotDatedWithNullablePK obj= NotDatedWithNullablePKFinder.findOne(op);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
            obj.setQuantity(10000.00);
        tx.commit();

        NotDatedWithNullablePK obj2 = NotDatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj2);
        assertTrue(obj2.isNullablePKComponentNull());
        assertEquals(10000.00, obj2.getQuantity(), 0);
    }

    public void testRefreshDatedObjectWithNullablePrimaryKey()
    {
        Operation op = DatedWithNullablePKFinder.objectId().eq(1237);
        op = op.and(DatedWithNullablePKFinder.nullablePKComponent().isNull());
        op = op.and(DatedWithNullablePKFinder.businessDate().eq(Timestamp.valueOf("2006-08-10 00:00:00")));
        DatedWithNullablePK obj= DatedWithNullablePKFinder.findOne(op);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
            obj.setQuantity(10000.00);
        tx.commit();

        DatedWithNullablePK obj2 = DatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj2);
        assertTrue(obj2.isNullablePKComponentNull());
        assertEquals(10000.00, obj2.getQuantity(), 0);
    }

    public void testUpdateDatedObjectWithNullablePrimaryKey()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = DatedWithNullablePKFinder.objectId().eq(1234);
        op = op.and(DatedWithNullablePKFinder.nullablePKComponent().isNull());
        op = op.and(DatedWithNullablePKFinder.businessDate().eq(Timestamp.valueOf("2006-01-01 00:00:00")));

        DatedWithNullablePK obj = DatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj);
        assertTrue(obj.isNullablePKComponentNull());
        assertEquals(200.00, obj.getQuantity(),0);

        obj.setQuantity(300.00);

        tx.commit();

        DatedWithNullablePK obj2 = DatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj2);
        assertTrue(obj2.isNullablePKComponentNull());
        assertEquals(300.00, obj2.getQuantity(), 0);
    }

    public void testMultiUpdateDatedObjectWithNullablePrimaryKey()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = DatedWithNullablePKFinder.nullablePKComponent().isNull();
        op = op.and(DatedWithNullablePKFinder.businessDate().eq(Timestamp.valueOf("2006-01-01 00:00:00")));

        DatedWithNullablePKList list = new DatedWithNullablePKList(op);
        assertEquals(3, list.size());

        for(int i = 0 ; i < list.size(); i++)
        {
            DatedWithNullablePK obj = (DatedWithNullablePK) list.get(i);
            obj.setQuantity(1000.00);
            obj.setPrice(100.00);
        }
        tx.commit();

        DatedWithNullablePKList list2 = new DatedWithNullablePKList(op);
        assertEquals(3, list2.size());

        for(int i = 0 ; i < list.size(); i++)
        {
            DatedWithNullablePK obj = (DatedWithNullablePK) list.get(i);
            assertEquals(1000.00, obj.getQuantity(),0);
            assertEquals(100.00, obj.getPrice(), 0);
        }
    }

    public void testMixedMultiUpdateDatedObjectWithNullablePrimaryKey()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = DatedWithNullablePKFinder.businessDate().eq(Timestamp.valueOf("2006-01-01 00:00:00"));

        DatedWithNullablePKList list = new DatedWithNullablePKList(op);
        assertEquals(6, list.size());

        for(int i = 0 ; i < list.size(); i++)
        {
            DatedWithNullablePK obj = (DatedWithNullablePK) list.get(i);
            obj.setQuantity(1000.00);
            obj.setPrice(100.00);
        }
        tx.commit();

        DatedWithNullablePKList list2 = new DatedWithNullablePKList(op);
        assertEquals(6, list2.size());

        for(int i = 0 ; i < list.size(); i++)
        {
            DatedWithNullablePK obj = (DatedWithNullablePK) list2.get(i);
            assertEquals(1000.00, obj.getQuantity(),0);
            assertEquals(100.00, obj.getPrice(), 0);
        }
    }

    public void testGetForDateRangeWithNullablePrimaryKey()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = DatedWithNullablePKFinder.objectId().eq(1234);
        op = op.and(DatedWithNullablePKFinder.nullablePKComponent().isNull());
        op = op.and(DatedWithNullablePKFinder.businessDate().eq(Timestamp.valueOf("2005-12-14 18:30:00.0")));

        DatedWithNullablePK obj = DatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj);
        assertTrue(obj.isNullablePKComponentNull());
        assertEquals(100.00, obj.getQuantity(),0);

        obj.setQuantity(300.00);

        tx.commit();

        DatedWithNullablePK obj2 = DatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj2);
        assertTrue(obj2.isNullablePKComponentNull());
        assertEquals(300.00, obj2.getQuantity(), 0);
    }


    public void testTerminateDatedObjectWithNullablePrimaryKey()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = DatedWithNullablePKFinder.objectId().eq(1234);
        op = op.and(DatedWithNullablePKFinder.nullablePKComponent().isNull());
        op = op.and(DatedWithNullablePKFinder.businessDate().eq(Timestamp.valueOf("2006-01-01 00:00:00")));

        DatedWithNullablePK obj = DatedWithNullablePKFinder.findOne(op);
        assertNotNull(obj);
        assertTrue(obj.isNullablePKComponentNull());
        assertEquals(200.00, obj.getQuantity(), 0);

        obj.terminate();
        tx.commit();

        DatedWithNullablePK obj2 = DatedWithNullablePKFinder.findOne(op);
        assertNull(obj2);
    }

}
