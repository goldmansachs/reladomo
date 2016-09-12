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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.child.LifeStatus;
import com.gs.fw.common.mithra.test.domain.child.LifeStatusFinder;
import com.gs.fw.common.mithra.test.domain.parent.ParentType;
import com.gs.fw.common.mithra.test.domain.parent.ParentTypeFinder;
import com.gs.fw.common.mithra.test.domain.parent.ParentTypeList;



public class TestBasicByteOperation extends MithraTestAbstract
{

    public TestBasicByteOperation()
    throws Exception
    {
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            ParentType.class,
            LifeStatus.class
        };
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new ParentTypeResultSetComparator());
    }

    public void testEqWithByteAttribute()
    throws Exception
    {
        Operation op;

        op = ParentTypeFinder.taxCode().isAlive().eq(false);
        ParentTypeList parents = new ParentTypeList(op);
        parents.deepFetch(ParentTypeFinder.taxCode());
        assertEquals(1, parents.size());
        assertEquals("Nabi Lee", parents.get(0).getName());
        assertTrue(parents.get(0).getTaxCode().get(0).getAliveByte() == 0);

        op = ParentTypeFinder.aliveByte().eq(LifeStatusFinder.aliveByte());
        parents = new ParentTypeList(op);
        assertEquals(3, parents.size());

        op = ParentTypeFinder.all();
        parents = new ParentTypeList(op);
        assertFalse(ParentTypeFinder.aliveByte().valueEquals(parents.get(0), parents.get(1)));
    }

    public void testNotEqWithByteAttribute()
    throws Exception
    {
        Operation op;
        op = ParentTypeFinder.taxCode().isAlive().notEq(true);
        ParentTypeList parents = new ParentTypeList(op);
        parents.deepFetch(ParentTypeFinder.taxCode());
        assertEquals(1, parents.size());
        assertEquals("Nabi Lee", parents.get(0).getName());

        try
        {
            op = ParentTypeFinder.aliveByte().notEq(LifeStatusFinder.aliveByte());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        op = ParentTypeFinder.aliveByte().notEq(ParentTypeFinder.aliveByte());
        parents = new ParentTypeList(op);
        assertEquals(0, parents.size());
    }
}
