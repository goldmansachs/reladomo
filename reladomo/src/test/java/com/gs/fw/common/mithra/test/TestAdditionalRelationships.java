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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.child.ChildType;
import com.gs.fw.common.mithra.test.domain.child.ChildTypeFinder;
import com.gs.fw.common.mithra.test.domain.child.ChildTypeList;
import com.gs.fw.common.mithra.test.domain.criters.PetType;
import com.gs.fw.common.mithra.test.domain.criters.PetTypeFinder;
import com.gs.fw.common.mithra.test.domain.criters.PetTypeList;
import com.gs.fw.common.mithra.test.domain.parent.ParentType;
import com.gs.fw.common.mithra.test.domain.parent.ParentTypeFinder;
import com.gs.fw.common.mithra.test.domain.parent.ParentTypeList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.Iterator;



public class TestAdditionalRelationships extends MithraTestAbstract
{
    public TestAdditionalRelationships(String s)
    {
        super(s);
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        ParentType.class,
                        ChildType.class,
                        PetType.class
                };
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        super.setMithraTestObjectToResultSetComparator(new ParentTypeResultSetComparator());
    }

    public void testThreeLayerRelationship()
            throws Exception
    {
        // test a.b.c relationship
        Operation op;
        op = ParentTypeFinder.alive().eq(true);

        ParentTypeList parents = new ParentTypeList(op);
        assertEquals(2, parents.size());
        assertEquals(3, parents.getChildrens().size());
        assertEquals(2, parents.getChildrens().getPets().size());

        // test list relationship
        ChildTypeList children = parents.getChildrens();
        PetTypeList pets = children.getPets();
        int cnt = 0;
        int[] petIds = new int[children.getPets().size()];
        for (Iterator i = pets.iterator(); i.hasNext(); cnt++)
        {
            int petId = ((PetType) i.next()).getId();
            petIds[cnt] = petId;
        }

        Operation op2 = PetTypeFinder.id().in(IntHashSet.newSetWith(petIds));
        PetTypeList petsTwo = new PetTypeList(op2);
        assertEquals(pets.size(), petsTwo.size());
    }

    public void testDeepFetchRelationship()
            throws Exception
    {
        // test a.b.c relationship
        Operation op;
        op = ParentTypeFinder.alive().eq(true);

        ParentTypeList parents = new ParentTypeList(op);
        parents.deepFetch(ParentTypeFinder.children());
        parents.deepFetch(ParentTypeFinder.children().pets());
        assertEquals(2, parents.size());
        assertEquals(3, parents.getChildrens().size());
        assertEquals(2, parents.getChildrens().getPets().size());

        ParentTypeFinder.clearQueryCache();
        ChildTypeFinder.clearQueryCache();
        PetTypeFinder.clearQueryCache();

        // test list relationship
        ChildTypeList children = parents.getChildrens();
        int cnt = 0;
        int[] childIds = new int[children.size()];
        for (Iterator i = children.iterator(); i.hasNext(); cnt++)
        {
            int childId = ((ChildType) i.next()).getId();
            childIds[cnt] = childId;
        }
        Operation op2 = ChildTypeFinder.id().in(IntHashSet.newSetWith(childIds));
        ChildTypeList childrenTwo = new ChildTypeList(op2);
        childrenTwo.deepFetch(ChildTypeFinder.pets());
        assertEquals(children.getPets().size(), childrenTwo.getPets().size());
    }
}
