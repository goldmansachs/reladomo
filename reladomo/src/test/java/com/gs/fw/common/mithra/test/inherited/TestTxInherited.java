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

package com.gs.fw.common.mithra.test.inherited;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.inherited.*;

import java.sql.Timestamp;


public class TestTxInherited
        extends MithraTestAbstract
{
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
                TxAnimal.class,
                TxMammal.class,
                TxMonkey.class,
                TxCow.class,
        };
    }

    public void testReadMonkeyById()
    {
        TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testReadMonkeyByName()
    {
        TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.name().eq("moh"));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testReadMonkeyByBodyTemp()
    {
        TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.bodyTemp().eq(95.3));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testReadMonkeyByBodyTailLength()
    {
        TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.tailLength().eq(9.5));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testPolymorphicAnimalById()
    {
        TxAnimal a = TxAnimalFinder.findOne(TxAnimalFinder.animalId().eq(1));
        assertNotNull(a);
        assertTrue(a instanceof TxMonkey);
        TxMonkey m = (TxMonkey) a;
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);

        TxAnimal b = TxAnimalFinder.findOne(TxAnimalFinder.animalId().eq(2));
        assertNotNull(b);
        assertTrue(b instanceof TxCow);
        TxCow c = (TxCow) b;
        assertEquals(2, c.getAnimalId());
        assertEquals("nelly", c.getName());
        assertEquals(90.4, c.getBodyTemp(), 0.0);
        assertEquals("green acres", c.getFarm());
    }

    public void testPolymorphicAnimal()
    {
        TxAnimalList list = new TxAnimalList(TxAnimalFinder.animalId().lessThan(3));
        list.setOrderBy(TxAnimalFinder.animalId().ascendingOrderBy());
        assertEquals(2, list.size());
        TxAnimal a = list.get(0);
        assertNotNull(a);
        assertTrue(a instanceof TxMonkey);
        TxMonkey m = (TxMonkey) a;
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);

        TxAnimal b = list.get(1);
        assertNotNull(b);
        assertTrue(b instanceof TxCow);
        TxCow c = (TxCow) b;
        assertEquals(2, c.getAnimalId());
        assertEquals("nelly", c.getName());
        assertEquals(90.4, c.getBodyTemp(), 0.0);
        assertEquals("green acres", c.getFarm());
    }

    public void testPolymorphicMammalById()
    {
        TxMammal a = TxMammalFinder.findOne(TxMammalFinder.animalId().eq(1));
        assertNotNull(a);
        assertTrue(a instanceof TxMonkey);
        TxMonkey m = (TxMonkey) a;
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);

        TxMammal b = TxMammalFinder.findOne(TxMammalFinder.animalId().eq(2));
        assertNotNull(b);
        assertTrue(b instanceof TxCow);
        TxCow c = (TxCow) b;
        assertEquals(2, c.getAnimalId());
        assertEquals("nelly", c.getName());
        assertEquals(90.4, c.getBodyTemp(), 0.0);
        assertEquals("green acres", c.getFarm());
    }

    public void testInsertAnimal()
    {
        TxAnimal a = new TxAnimal();
        a.setAnimalId(1000);
        a.setName("fred");

        a.insert();
        a = null;
        clearCache();
        a = TxAnimalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000));
        assertNotNull(a);
        assertEquals(1000, a.getAnimalId());
        assertEquals("fred", a.getName());
    }

    public void testInsertMammal()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMammal a = new TxMammal();
                a.setAnimalId(1000);
                a.setName("fred");
                a.setBodyTemp(12.4);
                a.insert();
                return null;
            }
        });
        clearCache();
        TxMammal a = TxMammalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000));
        assertNotNull(a);
        assertEquals(1000, a.getAnimalId());
        assertEquals("fred", a.getName());
        assertEquals(12.4, a.getBodyTemp(), 0.0);
    }

    public void testInsertMonkey()
    {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMonkey a = new TxMonkey();
                a.setAnimalId(1000);
                a.setName("fred");
                a.setBodyTemp(12.4);
                a.setAlphaMaleDate(now);
                a.setTailLength(2.4);
                a.insert();
                return null;
            }
        });
        clearCache();
        TxMonkey a = TxMonkeyFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000));
        assertNotNull(a);
        assertEquals(1000, a.getAnimalId());
        assertEquals("fred", a.getName());
        assertEquals(12.4, a.getBodyTemp(), 0.0);
        assertEquals(now, a.getAlphaMaleDate());
        assertEquals(2.4, a.getTailLength(), 0.0);
    }

    public void testInsertTwelveMonkies()
    {
        final long now = System.currentTimeMillis();
        TxMonkeyList list = new TxMonkeyList();
        for(int i=0;i<12;i++)
        {
            TxMonkey a = new TxMonkey();
            a.setAnimalId(1000+i);
            a.setName("fred"+i);
            a.setBodyTemp(12.4+i);
            a.setAlphaMaleDate(new Timestamp(now + i*1000));
            a.setTailLength(2.4+i);
            list.add(a);
        }
        list.insertAll();
        list = null;
        clearCache();
        list = new TxMonkeyList(TxAnimalFinder.animalId().greaterThanEquals(1000));
        list.setBypassCache(true);
        assertEquals(12, list.size());
        for(int i=0;i<12;i++)
        {
            TxMonkey a = list.get(i);
            assertEquals(1000+i, a.getAnimalId());
            assertEquals("fred"+i, a.getName());
            assertEquals(12.4+i, a.getBodyTemp(), 0.0);
            assertEquals(now+i*1000, a.getAlphaMaleDate().getTime());
            assertEquals(2.4+i, a.getTailLength(), 0.0);
        }
    }

    public void testDeleteAnimal()
    {
        testInsertAnimal();
        TxAnimal a = TxAnimalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000));
        a.delete();
        assertNull(TxAnimalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000)));
    }

    public void testDeleteMammal()
    {
        testInsertMammal();
        final TxAnimal a = TxAnimalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                a.delete();
                return null;
            }
        });
        assertNull(TxAnimalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000)));
    }

    public void testDeleteMonkey()
    {
        testInsertMonkey();
        final TxAnimal a = TxAnimalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                a.delete();
                return null;
            }
        });
        assertNull(TxAnimalFinder.findOneBypassCache(TxAnimalFinder.animalId().eq(1000)));
    }

    public void testBatchDelete()
    {
        this.testInsertTwelveMonkies();
        TxMonkeyList list = new TxMonkeyList(TxAnimalFinder.animalId().greaterThanEquals(1000));
        list.deleteAll();
        list = null;
        clearCache();
        list = new TxMonkeyList(TxAnimalFinder.animalId().greaterThanEquals(1000));
        list.setBypassCache(true);
        assertEquals(0, list.size());
    }

    public void testUpdateAnimal()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
                m.setName("new name");
                return null;
            }
        });
        clearCache();
        TxMonkey m = TxMonkeyFinder.findOneBypassCache(TxMonkeyFinder.animalId().eq(1));
        assertEquals("new name", m.getName());
    }

    public void testUpdateMammal()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
                m.setBodyTemp(7.343);
                return null;
            }
        });
        clearCache();
        TxMonkey m = TxMonkeyFinder.findOneBypassCache(TxMonkeyFinder.animalId().eq(1));
        assertEquals(7.343, m.getBodyTemp(), 0.0);
    }

    public void testNonTxUpdateMonkey()
    {
        TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
        m.setName("new name");
        TxMonkey m2 = TxMonkeyFinder.findOneBypassCache(TxMonkeyFinder.animalId().eq(1));
        assertEquals("new name", m2.getName());
    }

    public void testNonTxUpdateMonkeyInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
                m.setName("new name");
                return null;
            }
        });
        TxMonkey m2 = TxMonkeyFinder.findOneBypassCache(TxMonkeyFinder.animalId().eq(1));
        assertEquals("new name", m2.getName());
    }

    public void testUpdateMonkey()
    {
        final long now = System.currentTimeMillis();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
                m.setName("new name");
                m.setTailLength(45.4);
                m.setAlphaMaleDate(new Timestamp(now));
                return null;
            }
        });
        clearCache();
        TxMonkey m = TxMonkeyFinder.findOneBypassCache(TxMonkeyFinder.animalId().eq(1));
        assertEquals("new name", m.getName());
        assertEquals(45.4, m.getTailLength(), 0.0);
        assertEquals(now, m.getAlphaMaleDate().getTime());
    }

    public void testMultiUpdateMonkey()
    {
        final long now = System.currentTimeMillis();
        this.testInsertTwelveMonkies();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMonkeyList list = new TxMonkeyList(TxMonkeyFinder.animalId().greaterThanEquals(1000));
                for(int i=0;i<list.size();i++)
                {
                    TxMonkey m = list.get(i);
                    m.setTailLength(45.4);
                    m.setAlphaMaleDate(new Timestamp(now));
                }
                return null;
            }
        });
        clearCache();
        TxMonkeyList list = new TxMonkeyList(TxMonkeyFinder.animalId().greaterThanEquals(1000));
        list.setBypassCache(true);
        for(int i=0;i<list.size();i++)
        {
            TxMonkey m = list.get(i);
            assertEquals(45.4, m.getTailLength(), 0.0);
            assertEquals(now, m.getAlphaMaleDate().getTime());
        }
    }

    public void testBatchUpdateMonkey()
    {
        final long now = System.currentTimeMillis()+5000;
        this.testInsertTwelveMonkies();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TxMonkeyList list = new TxMonkeyList(TxMonkeyFinder.animalId().greaterThanEquals(1000));
                for(int i=0;i<list.size();i++)
                {
                    TxMonkey m = list.get(i);
                    m.setTailLength(45.4+i);
                    m.setAlphaMaleDate(new Timestamp(now + i * 1000));
                }
                return null;
            }
        });
        clearCache();
        TxMonkeyList list = new TxMonkeyList(TxMonkeyFinder.animalId().greaterThanEquals(1000));
        list.setBypassCache(true);
        for(int i=0;i<list.size();i++)
        {
            TxMonkey m = list.get(i);
            assertEquals(45.4 + i, m.getTailLength(), 0.0);
            assertEquals(now + i * 1000, m.getAlphaMaleDate().getTime());
        }
    }

    private void clearCache()
    {
        TxAnimalFinder.clearQueryCache();
        TxMammalFinder.clearQueryCache();
        TxMonkeyFinder.clearQueryCache();
        TxCowFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
    }

    public void testUniquenessTopFirst()
    {
        TxAnimal a = TxAnimalFinder.findOne(TxAnimalFinder.animalId().eq(1));
        TxMammal b = TxMammalFinder.findOne(TxMammalFinder.animalId().eq(1));
        TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
        assertSame(a, b);
        assertSame(a, m);
    }

    public void testUniquenessBottomFirst()
    {
        TxMonkey m = TxMonkeyFinder.findOne(TxMonkeyFinder.animalId().eq(1));
        TxMammal b = TxMammalFinder.findOne(TxMammalFinder.animalId().eq(1));
        TxAnimal a = TxAnimalFinder.findOne(TxAnimalFinder.animalId().eq(1));
        assertSame(a, b);
        assertSame(a, m);
    }

}
