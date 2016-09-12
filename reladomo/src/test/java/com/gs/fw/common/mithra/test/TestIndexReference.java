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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.FullNonDatedCache;
import com.gs.fw.common.mithra.cache.IndexReference;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;



/**
 * TestIndexReference
 */
public class TestIndexReference extends TestCase
{
    public void testIsForCache()
    {
        Cache c1 = new FullNonDatedCache(new Attribute[0], null);
        Cache c2 = new FullNonDatedCache(new Attribute[0], null);
        assertTrue(new IndexReference(c1, 0).isForCache(c1));
        assertFalse(new IndexReference(c1, 0).isForCache(c2));
    }

    public void testIsValid()
    {
        Cache c1 = new FullNonDatedCache(new Attribute[0], null);
        assertFalse(new IndexReference(c1, -1).isValid());
        assertFalse(new IndexReference(c1, 0).isValid());
        assertTrue(new IndexReference(c1, 1).isValid());
        assertTrue(new IndexReference(c1, 5).isValid());
    }

    public void testSerializatioon() throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(new IndexReference(new FullNonDatedCache(new Attribute[0], null), 0));
        assertNull(new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())).readObject());
    }
}
