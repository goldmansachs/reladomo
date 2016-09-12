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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;



public class TestByteArray extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        return new Class[]
        {
                TestBinaryArray.class,
                WallCrossImpl.class,
                ListEntryContactsImpl.class,
                DisplayText.class
        } ;
    }

    public void testDatedBynaryArrayColumnUpdate()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = DisplayTextFinder.displayTextI().eq(1234);
                op = op.and(DisplayTextFinder.businessDate().eq(Timestamp.valueOf("2005-12-31 18:30:00.0")));
                DisplayText displayText = DisplayTextFinder.findOne(op);
                assertEquals("AAAAAA",convertToString(displayText.getDisplayTextT()));

                byte[] newData = new byte[5];
                newData[0] = toByte(0xAA);
                newData[1] = toByte(0xBB);
                newData[2] = toByte(0x99);
                newData[3] = toByte(0x11);
                newData[4] = 0;

                displayText.setDisplayTextTUntil(newData, Timestamp.valueOf("2007-01-01 00:00:00.0"));

                Operation op2 = DisplayTextFinder.displayTextI().eq(1234);
                op2 = op2.and(DisplayTextFinder.businessDate().eq(Timestamp.valueOf("2005-12-30 18:30:00.0")));
                DisplayText text2 = DisplayTextFinder.findOne(op2);
                assertEquals("AAAAAA",convertToString(text2.getDisplayTextT()));

                Operation op3 = DisplayTextFinder.displayTextI().eq(1234);
                op3 = op3.and(DisplayTextFinder.businessDate().eq(Timestamp.valueOf("2006-01-01 18:30:00.0")));
                DisplayText text3 = DisplayTextFinder.findOne(op3);
                assertEquals("AABB991100",convertToString(text3.getDisplayTextT()));

                Operation op4 = DisplayTextFinder.displayTextI().eq(1234);
                op4 = op4.and(DisplayTextFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
                DisplayText text4 = DisplayTextFinder.findOne(op4);
                assertEquals("AAAAAA",convertToString(text4.getDisplayTextT()));

                return null;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = DisplayTextFinder.displayTextI().eq(1234);
                op = op.and(DisplayTextFinder.businessDate().eq(Timestamp.valueOf("2005-12-30 18:30:00.0")));
                DisplayText displayText = DisplayTextFinder.findOne(op);
                assertEquals("AAAAAA",convertToString(displayText.getDisplayTextT()));

                byte[] newData = new byte[5];
                newData[0] = toByte(0xAA);
                newData[1] = toByte(0xBB);
                newData[2] = toByte(0x99);
                newData[3] = toByte(0x11);
                newData[4] = 0;

                displayText.setDisplayTextTUntil(newData, Timestamp.valueOf("2007-01-01 00:00:00.0"));
                return null;
            }
        });



    }

    public void testRetrieve()
    {
        TestBinaryArray testBinaryArray = TestBinaryArrayFinder.findOne(TestBinaryArrayFinder.testBinaryArrayId().eq(1));
        assertNotNull(testBinaryArray);
        byte[] data = testBinaryArray.getPictureData();
        assertEquals(4, data.length);
        String asString = convertToString(data);
        assertEquals("FF010200", asString);
    }

    private String convertToString(byte[] data)
    {
        String asString = "";
        for(int i=0;i<data.length;i++)
        {
            String s = Integer.toHexString(((int) data[i]) & 0xFF);
            if (s.length() == 1) asString += "0";
            asString += s;
        }
        return asString.toUpperCase();
    }

    public void testUpdate()
    {
        TestBinaryArray testBinaryArray = TestBinaryArrayFinder.findOne(TestBinaryArrayFinder.testBinaryArrayId().eq(1));
        assertNotNull(testBinaryArray);
        byte[] newData = new byte[5];
        newData[0] = toByte(0xAA);
        newData[1] = toByte(0xBB);
        newData[2] = toByte(0x99);
        newData[3] = toByte(0x11);
        newData[4] = 0;

        testBinaryArray.setPictureData(newData);
        testBinaryArray = null;
        TestBinaryArrayFinder.clearQueryCache();
        testBinaryArray = TestBinaryArrayFinder.findOne(TestBinaryArrayFinder.testBinaryArrayId().eq(1));
        assertEquals("AABB991100", convertToString(testBinaryArray.getPictureData()));
    }

    public void testInsert()
    {
        TestBinaryArray testBinaryArray = new TestBinaryArray();
        testBinaryArray.setTestBinaryArrayId(2);
        byte[] newData = new byte[5];
        newData[0] = toByte(0xAA);
        newData[1] = toByte(0xBB);
        newData[2] = toByte(0x99);
        newData[3] = toByte(0x11);
        newData[4] = 0;

        testBinaryArray.setPictureData(newData);
        testBinaryArray.insert();
        testBinaryArray = null;
        TestBinaryArrayFinder.clearQueryCache();
        testBinaryArray = TestBinaryArrayFinder.findOne(TestBinaryArrayFinder.testBinaryArrayId().eq(2));
        assertEquals("AABB991100", convertToString(testBinaryArray.getPictureData()));
    }

    public void testFind()
    {
        byte[] data = new byte[4];
        data[0] = toByte(0xFF);
        data[1] = 1;
        data[2] = 2;
        data[3] = 0;
        TestBinaryArray testBinaryArray = TestBinaryArrayFinder.findOne(TestBinaryArrayFinder.pictureData().eq(data));
        assertNotNull(testBinaryArray);
        assertEquals(1, testBinaryArray.getTestBinaryArrayId());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertSame(testBinaryArray, TestBinaryArrayFinder.findOne(TestBinaryArrayFinder.pictureData().eq(data)));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testIn()
    {
        ByteArraySet set = new ByteArraySet();
        ByteArraySet set2 = new ByteArraySet();

        byte[] data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);
        set.add(data);
        set2.add(data);

        data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);

        set.add(data);
        set2.add(data);
        assertEquals(1, set.size());

        data = new byte[3];
        data[0] = toByte(0xAA);
        data[1] = toByte(0xAA);
        data[2] = toByte(0xAA);

        set.add(data);
        set2.add(data);
        assertEquals(2, set.size());

        assertEquals(set, set2);
        assertEquals(set.hashCode(), set2.hashCode());

        WallCrossImplList list = new WallCrossImplList(WallCrossImplFinder.listId().in(set));
        assertEquals(2, list.size());
    }

    public void testNotIn()
    {
        ByteArraySet set = new ByteArraySet();

        byte[] data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);
        set.add(data);

        data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);

        set.add(data);
        assertEquals(1, set.size());

        data = new byte[3];
        data[0] = toByte(0xAA);
        data[1] = toByte(0xAA);
        data[2] = toByte(0xAA);

        set.add(data);
        assertEquals(2, set.size());

        WallCrossImplList list = new WallCrossImplList(WallCrossImplFinder.listId().notIn(set));
        assertEquals(1, list.size());
    }

    public void testPrimaryKey()
    {
        WallCrossImplList list = new WallCrossImplList(WallCrossImplFinder.all());
        assertEquals(3, list.size());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        byte[] data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);
        WallCrossImpl first = WallCrossImplFinder.findOne(WallCrossImplFinder.listId().eq(data));
        assertNotNull(first);
        assertEquals(1, first.getEntityid());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        ListEntryContactsImpl contacts = ListEntryContactsImplFinder.findOne(ListEntryContactsImplFinder.listId().eq(data).and(
                ListEntryContactsImplFinder.emplId().eq("one")));
        assertNotNull(contacts);
        assertEquals("x", contacts.getListEntryRole());

        count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertSame(contacts, ListEntryContactsImplFinder.findOne(ListEntryContactsImplFinder.listId().eq(data).and(
                ListEntryContactsImplFinder.emplId().eq("one"))));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testMutliUpdate()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                ListEntryContactsImplList list = new ListEntryContactsImplList(ListEntryContactsImplFinder.all());
                list.setQuantity(5);
                return null;
            }
        });
        ListEntryContactsImplFinder.clearQueryCache();
        ListEntryContactsImplList list = new ListEntryContactsImplList(ListEntryContactsImplFinder.all());
        for(int i=0;i<list.size();i++)
        {
            assertEquals(5, list.get(i).getQuantity());
        }
    }

    public void testToManyRelationship()
    {
        byte[] data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);
        WallCrossImpl wall = WallCrossImplFinder.findOne(WallCrossImplFinder.listId().eq(data));
        assertNotNull(wall);
        assertEquals(3, wall.getListEntryContacts().size());

        data[0] = toByte(0xAA);
        data[1] = toByte(0xAA);
        data[2] = toByte(0xAA);
        wall = WallCrossImplFinder.findOne(WallCrossImplFinder.listId().eq(data));
        assertNotNull(wall);
        assertEquals(1, wall.getListEntryContacts().size());

        data[0] = toByte(0xBB);
        data[1] = toByte(0xBB);
        data[2] = toByte(0xBB);
        wall = WallCrossImplFinder.findOne(WallCrossImplFinder.listId().eq(data));
        assertNotNull(wall);
        assertEquals(0, wall.getListEntryContacts().size());
    }

    public void testDeepFetch()
    {
        WallCrossImplList list = new WallCrossImplList(WallCrossImplFinder.all());
        list.deepFetch(WallCrossImplFinder.listEntryContacts());
        assertEquals(3, list.size());
        int retrieveCount = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        for(int i=0; i< list.size();i++)
        {
            WallCrossImpl wci = list.get(i);
            ListEntryContactsImplList lecil = wci.getListEntryContacts();
            lecil.size();
        }
        assertEquals(retrieveCount, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testToOneRelationship()
    {
        byte[] data = new byte[3];
        data[0] = toByte(0xFF);
        data[1] = toByte(0xFF);
        data[2] = toByte(0xFF);
        ListEntryContactsImpl contacts = ListEntryContactsImplFinder.findOne(ListEntryContactsImplFinder.listId().eq(data).and(
                ListEntryContactsImplFinder.emplId().eq("one")));
        assertNotNull(contacts.getWallCrossImpl());
        contacts = ListEntryContactsImplFinder.findOne(ListEntryContactsImplFinder.listId().eq(data).and(
                ListEntryContactsImplFinder.emplId().eq("two")));
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertNotNull(contacts.getWallCrossImpl());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }
}
