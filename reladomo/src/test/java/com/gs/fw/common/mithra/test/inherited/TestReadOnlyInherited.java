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

import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.inherited.*;


public class TestReadOnlyInherited
        extends MithraTestAbstract
{
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
                ReadOnlyAnimal.class,
                ReadOnlyMammal.class,
                ReadOnlyMonkey.class,
                ReadOnlyCow.class,
        };
    }

    public void testReadMonkeyById()
    {
        ReadOnlyMonkey m = ReadOnlyMonkeyFinder.findOne(ReadOnlyMonkeyFinder.animalId().eq(1));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testReadMonkeyByName()
    {
        ReadOnlyMonkey m = ReadOnlyMonkeyFinder.findOne(ReadOnlyMonkeyFinder.name().eq("moh"));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testReadMonkeyByBodyTemp()
    {
        ReadOnlyMonkey m = ReadOnlyMonkeyFinder.findOne(ReadOnlyMonkeyFinder.bodyTemp().eq(95.3));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testReadMonkeyByBodyTailLength()
    {
        ReadOnlyMonkey m = ReadOnlyMonkeyFinder.findOne(ReadOnlyMonkeyFinder.tailLength().eq(9.5));
        assertNotNull(m);
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);
    }

    public void testPolymorphicAnimalById()
    {
        ReadOnlyAnimal a = ReadOnlyAnimalFinder.findOne(ReadOnlyAnimalFinder.animalId().eq(1));
        assertNotNull(a);
        assertTrue(a instanceof ReadOnlyMonkey);
        ReadOnlyMonkey m = (ReadOnlyMonkey) a;
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);

        ReadOnlyAnimal b = ReadOnlyAnimalFinder.findOne(ReadOnlyAnimalFinder.animalId().eq(2));
        assertNotNull(b);
        assertTrue(b instanceof ReadOnlyCow);
        ReadOnlyCow c = (ReadOnlyCow) b;
        assertEquals(2, c.getAnimalId());
        assertEquals("nelly", c.getName());
        assertEquals(90.4, c.getBodyTemp(), 0.0);
        assertEquals("green acres", c.getFarm());
    }

    public void testPolymorphicAnimal()
    {
        ReadOnlyAnimalList list = new ReadOnlyAnimalList(ReadOnlyAnimalFinder.animalId().lessThan(3));
        list.setOrderBy(ReadOnlyAnimalFinder.animalId().ascendingOrderBy());
        assertEquals(2, list.size());
        ReadOnlyAnimal a = list.get(0);
        assertNotNull(a);
        assertTrue(a instanceof ReadOnlyMonkey);
        ReadOnlyMonkey m = (ReadOnlyMonkey) a;
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);

        ReadOnlyAnimal b = list.get(1);
        assertNotNull(b);
        assertTrue(b instanceof ReadOnlyCow);
        ReadOnlyCow c = (ReadOnlyCow) b;
        assertEquals(2, c.getAnimalId());
        assertEquals("nelly", c.getName());
        assertEquals(90.4, c.getBodyTemp(), 0.0);
        assertEquals("green acres", c.getFarm());
    }

    public void testPolymorphicMammalById()
    {
        ReadOnlyMammal a = ReadOnlyMammalFinder.findOne(ReadOnlyMammalFinder.animalId().eq(1));
        assertNotNull(a);
        assertTrue(a instanceof ReadOnlyMonkey);
        ReadOnlyMonkey m = (ReadOnlyMonkey) a;
        assertEquals(1, m.getAnimalId());
        assertEquals("moh", m.getName());
        assertEquals(95.3, m.getBodyTemp(), 0.0);
        assertEquals(9.5, m.getTailLength(), 0.0);

        ReadOnlyMammal b = ReadOnlyMammalFinder.findOne(ReadOnlyMammalFinder.animalId().eq(2));
        assertNotNull(b);
        assertTrue(b instanceof ReadOnlyCow);
        ReadOnlyCow c = (ReadOnlyCow) b;
        assertEquals(2, c.getAnimalId());
        assertEquals("nelly", c.getName());
        assertEquals(90.4, c.getBodyTemp(), 0.0);
        assertEquals("green acres", c.getFarm());
    }

    public void testUniquenessTopFirst()
    {
        ReadOnlyAnimal a = ReadOnlyAnimalFinder.findOne(ReadOnlyAnimalFinder.animalId().eq(1));
        ReadOnlyMammal b = ReadOnlyMammalFinder.findOne(ReadOnlyMammalFinder.animalId().eq(1));
        ReadOnlyMonkey m = ReadOnlyMonkeyFinder.findOne(ReadOnlyMonkeyFinder.animalId().eq(1));
        assertSame(a, b);
        assertSame(a, m);
    }

    public void testUniquenessBottomFirst()
    {
        ReadOnlyMonkey m = ReadOnlyMonkeyFinder.findOne(ReadOnlyMonkeyFinder.animalId().eq(1));
        ReadOnlyMammal b = ReadOnlyMammalFinder.findOne(ReadOnlyMammalFinder.animalId().eq(1));
        ReadOnlyAnimal a = ReadOnlyAnimalFinder.findOne(ReadOnlyAnimalFinder.animalId().eq(1));
        assertSame(a, b);
        assertSame(a, m);
    }

}
