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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.KeepOnlySpecifiedDatesFilter;



public class TestCache extends TestCase
{
    private static final String USER_ID_INDEX = "by userId";
    private static final String USER_NAME_INDEX = "by userName";
    private static final String PARA_DESK_TAG_INDEX = "by tag";
    private static final String PARA_DESK_SIZE_INDEX = "by size";
    private static final String PARA_DESK_CONNECTIONS_INDEX = "by connections";

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void testClearingFullCache()
    {
        UserDatabaseObject udo = new UserDatabaseObject();
        FullNonDatedCache fullCache = new FullNonDatedCache(UserFinder.getPrimaryKeyAttributes(), udo);
        fullCache.addUniqueIndex(USER_ID_INDEX, new Attribute[] { UserFinder.userId() } );
        fullCache.addIndex(USER_NAME_INDEX, new Attribute[] { UserFinder.name() } );

        UserData userData0 = createUserData(0, "moh");
        User user0 = (User) fullCache.getObjectFromData(userData0);
        fullCache.getObjectFromData(createUserData(1, "moh"));
        fullCache.getObjectFromData(createUserData(2, "moh"));
        fullCache.getObjectFromData(createUserData(3, "doh"));
        fullCache.getObjectFromData(createUserData(4, "poh"));
        fullCache.getObjectFromData(createUserData(5, "joh"));
        fullCache.getObjectFromData(createUserData(6, null));
        fullCache.getObjectFromData(createUserData(7, null));

        for (int i=100;i<200;i++)
        {
            fullCache.getObjectFromData(createUserData(i, "name "+i%10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = UserFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int pkIndexRef = fullCache.getBestIndexReference(pkAttributes).indexReference;
        int userIdIndexRef = fullCache.getIndexRef(UserFinder.userId()).indexReference;
        int userNameIndexRef = fullCache.getIndexRef(UserFinder.name()).indexReference;

        assertSame(user0, fullCache.getObjectFromData(createUserData(0, "moh")));
        assertSame(user0, fullCache.get(userIdIndexRef, userData0.getUserId()).get(0));
        assertSame(user0, fullCache.get(pkIndexRef, user0, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false).get(0));
        assertEquals(3, fullCache.get(userNameIndexRef, userData0.getName()).size());
        assertEquals(1, fullCache.get(userNameIndexRef, "doh").size());
        assertEquals(2, fullCache.getNulls(userNameIndexRef).size());

        fullCache.clear();

        assertEquals(0, fullCache.getAll().size());
        assertEquals(0, fullCache.size());
        assertNotSame(user0, fullCache.getObjectFromData(createUserData(0, "moh")));
        assertNotSame(user0, fullCache.get(userIdIndexRef, userData0.getUserId()).get(0));
        assertNotSame(user0, fullCache.get(pkIndexRef, user0, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false).get(0));
        assertTrue(3 != fullCache.get(userNameIndexRef, userData0.getName()).size());
        assertTrue(1 != fullCache.get(userNameIndexRef, "doh").size());
        assertTrue(2 != fullCache.getNulls(userNameIndexRef).size());
    }


    public void testFullCacheWithUser()
    {
        UserDatabaseObject udo = new UserDatabaseObject();
        FullNonDatedCache fullCache = new FullNonDatedCache(UserFinder.getPrimaryKeyAttributes(), udo);
        fullCache.addUniqueIndex(USER_ID_INDEX, new Attribute[] { UserFinder.userId() } );
        fullCache.addIndex(USER_NAME_INDEX, new Attribute[] { UserFinder.name() } );

        UserData userData0 = createUserData(0, "moh");
        User user0 = (User) fullCache.getObjectFromData(userData0);
        fullCache.getObjectFromData(createUserData(1, "moh"));
        fullCache.getObjectFromData(createUserData(2, "moh"));
        fullCache.getObjectFromData(createUserData(3, "doh"));
        fullCache.getObjectFromData(createUserData(4, "poh"));
        fullCache.getObjectFromData(createUserData(5, "joh"));
        fullCache.getObjectFromData(createUserData(6, null));
        fullCache.getObjectFromData(createUserData(7, null));

        for (int i=100;i<200;i++)
        {
            fullCache.getObjectFromData(createUserData(i, "name "+i%10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = UserFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int pkIndexRef = fullCache.getBestIndexReference(pkAttributes).indexReference;
        int userIdIndexRef = fullCache.getIndexRef(UserFinder.userId()).indexReference;
        int userNameIndexRef = fullCache.getIndexRef(UserFinder.name()).indexReference;

        assertSame(user0, fullCache.getObjectFromData(createUserData(0, "moh")));
        assertSame(user0, fullCache.get(userIdIndexRef, userData0.getUserId()).get(0));
        assertSame(user0, fullCache.get(pkIndexRef, user0, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false).get(0));
        assertSame(user0, fullCache.getAsOne(user0, pkAttributes));
        assertSame(user0, fullCache.getAsOne(user0, pkArray));
        assertEquals(3, fullCache.get(userNameIndexRef, userData0.getName()).size());
        assertEquals(1, fullCache.get(userNameIndexRef, "doh").size());
        assertEquals(2, fullCache.getNulls(userNameIndexRef).size());

    }

    public void testLargeFullCache()
    {
        int max = 1;
//        int max = 10;
        for(int i=0;i<max;i++)
        {
            long startTime = System.nanoTime();
            runFullCachePerformance();
            long runTimes = System.nanoTime() - startTime;
            System.out.println("full cache (1:10 write:read) "+((double)runTimes)/2000000 + " ns per");
            System.gc();
            Thread.yield();
            System.gc();
            Thread.yield();
        }
    }

    public void runFullCachePerformance()
    {
        UserDatabaseObject udo = new UserDatabaseObject();
        FullNonDatedCache fullCache = new FullNonDatedCache(UserFinder.getPrimaryKeyAttributes(), udo);
        fullCache.addUniqueIndex(USER_ID_INDEX, new Attribute[] { UserFinder.userId() } );

        UserData userData0 = createUserData(0, "moh");
        User user0 = (User) fullCache.getObjectFromData(userData0);
        fullCache.getObjectFromData(createUserData(1, "moh"));
        fullCache.getObjectFromData(createUserData(2, "moh"));
        fullCache.getObjectFromData(createUserData(3, "doh"));
        fullCache.getObjectFromData(createUserData(4, "poh"));
        fullCache.getObjectFromData(createUserData(5, "joh"));
        fullCache.getObjectFromData(createUserData(6, null));
        fullCache.getObjectFromData(createUserData(7, null));

        int max = 200000;
        int loop = 1;
        for (int i=100;i< max;i++)
        {
            fullCache.getObjectFromData(createUserData(i, "name "+i%10));
        }

        Attribute[] pkArray = UserFinder.getPrimaryKeyAttributes();
        UserData tempData = createUserData(0, "moh");
        for(int j=0;j<loop;j++)
        {
            for(int i=100;i< max;i++)
            {
                tempData.setId(i);
                User user = (User) fullCache.getAsOne(tempData, pkArray);
                assertEquals(i, user.getId());
            }
        }
    }

    public void testFullCacheWithParaDesk()
    {
        ParaDeskDatabaseObject pddo = new ParaDeskDatabaseObject();
        FullNonDatedCache fullCache = new FullNonDatedCache(ParaDeskFinder.getPrimaryKeyAttributes(), pddo);
        fullCache.addUniqueIndex(PARA_DESK_SIZE_INDEX, new Attribute[] { ParaDeskFinder.sizeDouble() } );
        fullCache.addUniqueIndex(PARA_DESK_TAG_INDEX, new Attribute[] { ParaDeskFinder.tagInt() } );
        fullCache.addUniqueIndex(PARA_DESK_CONNECTIONS_INDEX, new Attribute[] { ParaDeskFinder.connectionLong() } );

        ParaDeskData paraDeskData0 = createParaDeskData(1, 1);
        ParaDesk paraDesk0 = (ParaDesk) fullCache.getObjectFromData( paraDeskData0);

        for (int i=100;i<200;i++)
        {
            fullCache.getObjectFromData(createParaDeskData(i, i*10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = ParaDeskFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int pkIndexRef = fullCache.getBestIndexReference(pkAttributes).indexReference;
        int sizeRef = fullCache.getIndexRef(ParaDeskFinder.sizeDouble()).indexReference;
        int tagRef = fullCache.getIndexRef(ParaDeskFinder.tagInt()).indexReference;
        int deskConRef = fullCache.getIndexRef(ParaDeskFinder.connectionLong()).indexReference;

        assertSame(paraDesk0, fullCache.getObjectFromData(createParaDeskData(1, 1)));
        assertSame(paraDesk0, fullCache.get(sizeRef, 1.0).get(0));
        assertSame(paraDesk0, fullCache.get(tagRef, 1).get(0));
        assertSame(paraDesk0, fullCache.get(deskConRef, 1L).get(0));

    }

    public void testPartialCacheWithParaDesk()
    {
        PartialNonDatedCache partialCache = createPartialParaDeskCache(0, 0);

        ParaDeskData paraDeskData0 = createParaDeskData(1, 1);
        ParaDesk paraDesk0 = (ParaDesk) partialCache.getObjectFromData(paraDeskData0);

        for (int i=100;i<200;i++)
        {
            partialCache.getObjectFromData(createParaDeskData(i, i*10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = ParaDeskFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int pkIndexRef = partialCache.getBestIndexReference(pkAttributes).indexReference;
        int sizeRef = partialCache.getIndexRef(ParaDeskFinder.sizeDouble()).indexReference;
        int tagRef = partialCache.getIndexRef(ParaDeskFinder.tagInt()).indexReference;
        int deskConRef = partialCache.getIndexRef(ParaDeskFinder.connectionLong()).indexReference;

        assertSame(paraDesk0, partialCache.getObjectFromData(createParaDeskData(1, 1)));
        assertSame(paraDesk0, partialCache.get(sizeRef, 1.0).get(0));
        assertSame(paraDesk0, partialCache.get(tagRef, 1).get(0));
        assertSame(paraDesk0, partialCache.get(deskConRef, 1L).get(0));

    }

    public void testPartialCacheWithParaDeskAfterClearAndReload()
    {
        PartialNonDatedCache partialCache = createPartialParaDeskCache(0, 0);

        ParaDeskData paraDeskData0 = createParaDeskData(1, 1);
        ParaDesk paraDesk0 = (ParaDesk) partialCache.getObjectFromData(paraDeskData0);

        for (int i=100;i<200;i++)
        {
            partialCache.getObjectFromData(createParaDeskData(i, i*10));
        }

        partialCache.clear();
        assertSame(paraDesk0, partialCache.getObjectFromData(paraDeskData0));
        for (int i=100;i<200;i++)
        {
            partialCache.getObjectFromData(createParaDeskData(i, i*10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = ParaDeskFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int pkIndexRef = partialCache.getBestIndexReference(pkAttributes).indexReference;
        int sizeRef = partialCache.getIndexRef(ParaDeskFinder.sizeDouble()).indexReference;
        int tagRef = partialCache.getIndexRef(ParaDeskFinder.tagInt()).indexReference;
        int deskConRef = partialCache.getIndexRef(ParaDeskFinder.connectionLong()).indexReference;

        assertSame(paraDesk0, partialCache.getObjectFromData(createParaDeskData(1, 1)));
        assertSame(paraDesk0, partialCache.get(sizeRef, 1.0).get(0));
        assertSame(paraDesk0, partialCache.get(tagRef, 1).get(0));
        assertSame(paraDesk0, partialCache.get(deskConRef, 1L).get(0));

    }

    public void sleep(long millis)
    {
        long now = System.currentTimeMillis();
        long target = now + millis;
        while(now < target)
        {
            try
            {
                Thread.sleep(target-now);
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            now = System.currentTimeMillis();
        }
    }

    public void testPartialCacheWithParaDeskWithExpiration()
    {
        int count = 0;
        while(!tryPartialCacheWithParaDeskWithExpiration() && count < 10)
        {
            count++;
        }
        assertTrue(count < 10);
    }

    public boolean tryPartialCacheWithParaDeskWithExpiration()
    {
        CacheClock.forceTick();
        long start = CacheClock.getTime();
        long timeToLive = 200;
        PartialNonDatedCache partialCache = createPartialParaDeskCache(timeToLive, 0);

        ParaDeskData paraDeskData0 = createParaDeskData(1, 1);
        ParaDesk paraDesk0 = (ParaDesk) partialCache.getObjectFromData(paraDeskData0);

        for (int i=100;i<200;i++)
        {
            partialCache.getObjectFromData(createParaDeskData(i, i*10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = ParaDeskFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int pkIndexRef = partialCache.getBestIndexReference(pkAttributes).indexReference;
        int sizeRef = partialCache.getIndexRef(ParaDeskFinder.sizeDouble()).indexReference;
        int tagRef = partialCache.getIndexRef(ParaDeskFinder.tagInt()).indexReference;
        int deskConRef = partialCache.getIndexRef(ParaDeskFinder.connectionLong()).indexReference;

        assertSame(paraDesk0, partialCache.getObjectFromData(createParaDeskData(1, 1)));
        List list1 = partialCache.get(sizeRef, 101.0);
        List list2 = partialCache.get(tagRef, 1010);
        List list3 = partialCache.get(deskConRef, 1010L);
        CacheClock.forceTick();
        if (!(CacheClock.getTime() < start + timeToLive))
        {
            return false;
        }
        assertEquals(101.0, ((ParaDesk)list1.get(0)).getSizeDouble(), 0.0);
        assertEquals(1010, ((ParaDesk)list2.get(0)).getTagInt());
        assertEquals(1010L, ((ParaDesk)list3.get(0)).getConnectionLong());
        sleep(timeToLive + 100);
        CacheClock.forceTick();
        assertSame(paraDesk0, partialCache.getObjectFromData(createParaDeskData(1, 1)));
        assertSame(paraDesk0, partialCache.getAsOne(createParaDeskData(1, 1), new Extractor[] {ParaDeskFinder.deskIdString()}));
        assertEquals(0, partialCache.get(sizeRef, 101.0).size());
        assertEquals(0, partialCache.get(tagRef, 1010).size());
        assertEquals(0, partialCache.get(deskConRef, 1010L).size());
        return true;
    }

    public void testPartialCacheWithParaDeskWithRelationshipExpiration()
    {
        long start = System.currentTimeMillis();
        long timeToLive = 200;
        PartialNonDatedCache partialCache = createPartialParaDeskCache(timeToLive, timeToLive);

        ParaDeskData paraDeskData0 = createParaDeskData(1, 1);
        ParaDesk paraDesk0 = (ParaDesk) partialCache.getObjectFromData(paraDeskData0);

        for (int i=100;i<200;i++)
        {
            partialCache.getObjectFromData(createParaDeskData(i, i*10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = ParaDeskFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int pkIndexRef = partialCache.getBestIndexReference(pkAttributes).indexReference;
        int sizeRef = partialCache.getIndexRef(ParaDeskFinder.sizeDouble()).indexReference;
        int tagRef = partialCache.getIndexRef(ParaDeskFinder.tagInt()).indexReference;
        int deskConRef = partialCache.getIndexRef(ParaDeskFinder.connectionLong()).indexReference;

        Object paraDesk1 = partialCache.getObjectFromData(createParaDeskData(1, 1));
        List list1 = partialCache.get(sizeRef, 101.0);
        List list2 = partialCache.get(tagRef, 1010);
        List list3 = partialCache.get(deskConRef, 1010L);
        if (System.currentTimeMillis() < start + timeToLive)
        {
            assertSame(paraDesk0, paraDesk1);
            assertEquals(101.0, ((ParaDesk)list1.get(0)).getSizeDouble(), 0.0);
            assertEquals(1010, ((ParaDesk)list2.get(0)).getTagInt());
            assertEquals(1010L, ((ParaDesk)list3.get(0)).getConnectionLong());
        }
        sleep(timeToLive + 100);
        CacheClock.forceTick();
        assertSame(paraDesk0, partialCache.getObjectFromData(createParaDeskData(1, 1)));
        assertNull(partialCache.getAsOne(createParaDeskData(1, 2), new Extractor[] {ParaDeskFinder.deskIdString()}));
        assertEquals(0, partialCache.get(sizeRef, 101.0).size());
        assertEquals(0, partialCache.get(tagRef, 1010).size());
        assertEquals(0, partialCache.get(deskConRef, 1010L).size());
    }

    private PartialNonDatedCache createPartialParaDeskCache(long timeToLive, long relTimeToLive)
    {
        ParaDeskDatabaseObject pddo = new ParaDeskDatabaseObject();
        PartialNonDatedCache partialCache = new PartialNonDatedCache(ParaDeskFinder.getPrimaryKeyAttributes(), pddo, timeToLive, relTimeToLive);
        partialCache.addUniqueIndex(PARA_DESK_SIZE_INDEX, new Attribute[] { ParaDeskFinder.sizeDouble() } );
        partialCache.addUniqueIndex(PARA_DESK_TAG_INDEX, new Attribute[] { ParaDeskFinder.tagInt() } );
        partialCache.addUniqueIndex(PARA_DESK_CONNECTIONS_INDEX, new Attribute[] { ParaDeskFinder.connectionLong() } );
        return partialCache;
    }

    public void testNonUniqueIndexWithParaDesk()
    {
        ParaDeskDatabaseObject pddo = new ParaDeskDatabaseObject();
        FullNonDatedCache fullCache = new FullNonDatedCache(ParaDeskFinder.getPrimaryKeyAttributes(), pddo);
        fullCache.addIndex(PARA_DESK_SIZE_INDEX, new Attribute[] { ParaDeskFinder.sizeDouble() } );
        fullCache.addIndex(PARA_DESK_TAG_INDEX, new Attribute[] { ParaDeskFinder.tagInt() } );
        fullCache.addIndex(PARA_DESK_CONNECTIONS_INDEX, new Attribute[] { ParaDeskFinder.connectionLong() } );

        ParaDeskData paraDeskData0 = createParaDeskData("desk 1", 1, 1);
        ParaDesk paraDesk0 = (ParaDesk) fullCache.getObjectFromData(paraDeskData0);
        fullCache.getObjectFromData(createParaDeskData("desk 2", 1, 1));
        fullCache.getObjectFromData(createParaDeskData("desk 3", 2, 2));

        for (int i=100;i<200;i++)
        {
            fullCache.getObjectFromData(createParaDeskData(i, i*10));
        }

        ArrayList pkAttributes = new ArrayList();
        Attribute[] pkArray = ParaDeskFinder.getPrimaryKeyAttributes();
        for(int i=0;i<pkArray.length;i++)
        {
            pkAttributes.add(pkArray[i]);
        }
        int sizeRef = fullCache.getIndexRef(ParaDeskFinder.sizeDouble()).indexReference;
        int tagRef = fullCache.getIndexRef(ParaDeskFinder.tagInt()).indexReference;
        int deskConRef = fullCache.getIndexRef(ParaDeskFinder.connectionLong()).indexReference;

        assertEquals(fullCache.get(sizeRef, 1.0).size(), 2);
        assertEquals(fullCache.get(tagRef, 1).size(), 2);
        assertEquals(fullCache.get(deskConRef, 1L).size(), 2);
        assertTrue(fullCache.get(sizeRef, 1.0).contains(paraDesk0));
        assertTrue(fullCache.get(tagRef, 1).contains(paraDesk0));
        assertTrue(fullCache.get(deskConRef, 1L).contains(paraDesk0));
        assertEquals(fullCache.get(sizeRef, 2.0).size(), 1);
        assertEquals(fullCache.get(tagRef, 2).size(), 1);
        assertEquals(fullCache.get(deskConRef, 2L).size(), 1);

    }

    public void testFullCacheRemoveAll()
    {
        UserDatabaseObject udo = new UserDatabaseObject();
        FullNonDatedCache fullCache = new FullNonDatedCache(UserFinder.getPrimaryKeyAttributes(), udo);
        fullCache.addUniqueIndex(USER_ID_INDEX, new Attribute[] { UserFinder.userId() } );
        fullCache.addIndex(USER_NAME_INDEX, new Attribute[] { UserFinder.name() } );

        for(int i=0;i<10000;i++)
        {
            UserData userData = createUserData(i, "name"+(i/4));
            User user = (User) fullCache.getObjectFromData(userData);
        }
        List<Attribute> pkAttributes = Arrays.asList(UserFinder.getPrimaryKeyAttributes());
        int pkIndexRef = fullCache.getBestIndexReference(pkAttributes).indexReference;
        int userIdIndexRef = fullCache.getIndexRef(UserFinder.userId()).indexReference;
        int userNameIndexRef = fullCache.getIndexRef(UserFinder.name()).indexReference;

        User u = new User();

        for(int i=0;i<10000;i++)
        {
            u.setId(i);
            List list = fullCache.get(pkIndexRef, u, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false);
            assertEquals(1, list.size());
            assertEquals(i, ((User)list.get(0)).getId());
            assertEquals(1, fullCache.get(userIdIndexRef, "User"+i).size());
            assertEquals(4, fullCache.get(userNameIndexRef, "name"+(i/4)).size());
        }
        fullCache.removeAll(new Filter()
        {
            public boolean matches(Object o)
            {
                User u = (User) o;
                return u.getId() % 2 == 1;
            }
        });
        for(int i=0;i<10000;i+=2)
        {
            u.setId(i);
            List list = fullCache.get(pkIndexRef, u, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false);
            assertEquals(1, list.size());
            assertEquals(i, ((User)list.get(0)).getId());
            assertEquals(1, fullCache.get(userIdIndexRef, "User"+i).size());
            assertEquals(2, fullCache.get(userNameIndexRef, "name"+(i/4)).size());
        }
        for(int i=1;i<10000;i+=2)
        {
            u.setId(i);
            List list = fullCache.get(pkIndexRef, u, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false);
            assertEquals(0, list.size());
            assertEquals(0, fullCache.get(userIdIndexRef, "User"+i).size());
            assertEquals(2, fullCache.get(userNameIndexRef, "name"+(i/4)).size());
        }
    }

    public void testPartialCacheRemoveAll()
    {
        UserDatabaseObject udo = new UserDatabaseObject();
        PartialNonDatedCache partialCache = new PartialNonDatedCache(UserFinder.getPrimaryKeyAttributes(), udo, 0, 0);
        partialCache.addUniqueIndex(USER_ID_INDEX, new Attribute[] { UserFinder.userId() } );

        ArrayList hardRefs = new ArrayList(10000);
        for(int i=0;i<10000;i++)
        {
            UserData userData = createUserData(i, "name"+(i/4));
            User user = (User) partialCache.getObjectFromData(userData);
            hardRefs.add(user);
        }
        List<Attribute> pkAttributes = Arrays.asList(UserFinder.getPrimaryKeyAttributes());
        int pkIndexRef = partialCache.getBestIndexReference(pkAttributes).indexReference;
        int userIdIndexRef = partialCache.getIndexRef(UserFinder.userId()).indexReference;

        User u = new User();

        for(int i=0;i<10000;i++)
        {
            u.setId(i);
            List list = partialCache.get(pkIndexRef, u, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false);
            assertEquals(1, list.size());
            assertEquals(i, ((User)list.get(0)).getId());
            assertEquals(1, partialCache.get(userIdIndexRef, "User"+i).size());
        }
        partialCache.removeAll(new Filter()
        {
            public boolean matches(Object o)
            {
                User u = (User) o;
                return u.getId() % 2 == 1;
            }
        });
        for(int i=0;i<10000;i+=2)
        {
            u.setId(i);
            List list = partialCache.get(pkIndexRef, u, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false);
            assertEquals(1, list.size());
            assertEquals(i, ((User)list.get(0)).getId());
        }
        for(int i=1;i<10000;i+=2)
        {
            u.setId(i);
            List list = partialCache.get(pkIndexRef, u, (Extractor[]) pkAttributes.toArray(new Extractor[pkAttributes.size()]), false);
            assertEquals(0, list.size());
            assertEquals(0, partialCache.get(userIdIndexRef, "User"+i).size());
        }
    }

    public void testDatedFullCacheWithExtraIndex() throws Exception
    {
        BitemporalOrderDatabaseObject dbo = new BitemporalOrderDatabaseObject();
        Attribute[] primaryKeyAttributes = BitemporalOrderFinder.getPrimaryKeyAttributes();
        AsOfAttribute[] asOfAttributes = BitemporalOrderFinder.getAsOfAttributes();
        FullDatedCache fullCache = new FullDatedCache(primaryKeyAttributes,
                asOfAttributes, dbo, primaryKeyAttributes);
        int indexRef = fullCache.addUniqueIndex("", new Extractor[] { BitemporalOrderFinder.userId()});
        Timestamp jan = new Timestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new Timestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
        Timestamp mar = new Timestamp(timestampFormat.parse("2003-03-15 00:00:00").getTime());
        Timestamp apr = new Timestamp(timestampFormat.parse("2003-04-15 00:00:00").getTime());
        Timestamp may = new Timestamp(timestampFormat.parse("2003-05-15 00:00:00").getTime());
        Timestamp jun = new Timestamp(timestampFormat.parse("2003-06-15 00:00:00").getTime());
        Timestamp in = new Timestamp(System.currentTimeMillis());
        Timestamp out = new Timestamp(in.getTime()+1);

        BitemporalOrderData data1 = BitemporalOrderDatabaseObject.allocateOnHeapData();
        data1.setOrderId(1);
        data1.setUserId(1);
        data1.setBusinessDateFrom(jan);
        data1.setBusinessDateTo(DefaultInfinityTimestamp.getDefaultInfinity());
        data1.setProcessingDateFrom(in);
        data1.setProcessingDateTo(out);
        BitemporalOrder order1 = (BitemporalOrder) fullCache.getObjectFromData(data1, new Timestamp[] {jan, in});

        Extractor[] byUserId = new Extractor[] { BitemporalOrderFinder.userId(), BitemporalOrderFinder.businessDateFrom(), BitemporalOrderFinder.processingDateFrom()};
        List list = fullCache.get(indexRef, data1, byUserId, false);
        assertEquals(1, list.size());
        assertSame(order1, list.get(0));
        
        BitemporalOrderData data2 = BitemporalOrderDatabaseObject.allocateOnHeapData();
        data2.setOrderId(2);
        data2.setUserId(1);
        data2.setBusinessDateFrom(jan);
        data2.setBusinessDateTo(DefaultInfinityTimestamp.getDefaultInfinity());
        data2.setProcessingDateFrom(in);
        data2.setProcessingDateTo(out);
        BitemporalOrder order2 = (BitemporalOrder) fullCache.getObjectFromData(data2, new Timestamp[] {jan, in});

        list = fullCache.get(indexRef, data1, byUserId, false);
        assertEquals(1, list.size());
        assertSame(order2, list.get(0));

        fullCache.clear();
        list = fullCache.get(indexRef, data1, byUserId, false);
        assertEquals(0, list.size());

    }

    public void testDatedFullCacheRemoveAll() throws Exception
    {
        TinyBalanceDatabaseObject dbo = new TinyBalanceDatabaseObject();
        Attribute[] primaryKeyAttributes = TinyBalanceFinder.getPrimaryKeyAttributes();
        AsOfAttribute[] asOfAttributes = TinyBalanceFinder.getAsOfAttributes();
        FullDatedCache fullCache = new FullDatedCache(primaryKeyAttributes,
                asOfAttributes, dbo, primaryKeyAttributes);
        Timestamp jan = new Timestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new Timestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
        Timestamp mar = new Timestamp(timestampFormat.parse("2003-03-15 00:00:00").getTime());
        Timestamp apr = new Timestamp(timestampFormat.parse("2003-04-15 00:00:00").getTime());
        Timestamp may = new Timestamp(timestampFormat.parse("2003-05-15 00:00:00").getTime());
        Timestamp jun = new Timestamp(timestampFormat.parse("2003-06-15 00:00:00").getTime());
        Timestamp in = new Timestamp(System.currentTimeMillis());
        Timestamp[] asOfDates = new Timestamp[2];
        asOfDates[0] = feb;
        asOfDates[1] = InfinityTimestamp.getParaInfinity();
        fullCache.getObjectFromData(createTinyBalanceData(1, jan, mar, in), asOfDates);
        asOfDates[0] = apr;
        fullCache.getObjectFromData(createTinyBalanceData(1, mar, may, in), asOfDates);
        asOfDates[0] = InfinityTimestamp.getParaInfinity();
        fullCache.getObjectFromData(createTinyBalanceData(1, may, InfinityTimestamp.getParaInfinity(), in), asOfDates);

        List extractorList = new ArrayList();
        for(int i=0;i<primaryKeyAttributes.length;i++)
        {
            extractorList.add(primaryKeyAttributes[i]);
        }
        for(int i=0;i<asOfAttributes.length;i++)
        {
            extractorList.add(asOfAttributes[i]);
        }
        Extractor[] extractors = (Extractor[]) extractorList.toArray(new Extractor[extractorList.size()]);
        int indexRef = fullCache.getBestIndexReference(extractorList).indexReference;

        assertEquals(1, fullCache.get(indexRef, createTinyBalance(feb), extractors, false).size());
        assertEquals(1, fullCache.get(indexRef, createTinyBalance(apr), extractors, false).size());
        assertEquals(1, fullCache.get(indexRef, createTinyBalance(jun), extractors, false).size());

        List toKeep = new ArrayList();
        toKeep.add(feb);
        toKeep.add(jun);
        KeepOnlySpecifiedDatesFilter filter = new KeepOnlySpecifiedDatesFilter(TinyBalanceFinder.businessDate(), toKeep);
        fullCache.removeAll(filter);

        assertEquals(1, fullCache.get(indexRef, createTinyBalance(feb), extractors, false).size());
        assertEquals(0, fullCache.get(indexRef, createTinyBalance(apr), extractors, false).size());
        assertEquals(1, fullCache.get(indexRef, createTinyBalance(jun), extractors, false).size());

    }

    public void testDatedPartialCacheWithExpiration() throws Exception
    {
        long timeToLive = 1000;
        TinyBalanceDatabaseObject dbo = new TinyBalanceDatabaseObject();
        Attribute[] primaryKeyAttributes = TinyBalanceFinder.getPrimaryKeyAttributes();
        AsOfAttribute[] asOfAttributes = TinyBalanceFinder.getAsOfAttributes();
        PartialDatedCache cache = new PartialDatedCache(primaryKeyAttributes,
                asOfAttributes, dbo, primaryKeyAttributes, timeToLive, timeToLive);
        List extractorList = new ArrayList();
        for(int i=0;i<primaryKeyAttributes.length;i++)
        {
            extractorList.add(primaryKeyAttributes[i]);
        }
        for(int i=0;i<asOfAttributes.length;i++)
        {
            extractorList.add(asOfAttributes[i]);
        }
        Extractor[] extractors = (Extractor[]) extractorList.toArray(new Extractor[extractorList.size()]);
        int indexRef = cache.getBestIndexReference(extractorList).indexReference;

        Timestamp jan = new Timestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new Timestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
        Timestamp mar = new Timestamp(timestampFormat.parse("2003-03-15 00:00:00").getTime());
        Timestamp apr = new Timestamp(timestampFormat.parse("2003-04-15 00:00:00").getTime());
        Timestamp may = new Timestamp(timestampFormat.parse("2003-05-15 00:00:00").getTime());
        Timestamp jun = new Timestamp(timestampFormat.parse("2003-06-15 00:00:00").getTime());
        Timestamp in = new Timestamp(System.currentTimeMillis());
        Timestamp[] asOfDates = new Timestamp[2];
        asOfDates[0] = feb;
        asOfDates[1] = InfinityTimestamp.getParaInfinity();
        TinyBalance febBalance = (TinyBalance) cache.getObjectFromData(createTinyBalanceData(1, jan, mar, in), asOfDates);
        asOfDates[0] = apr;
        cache.getObjectFromData(createTinyBalanceData(1, mar, may, in), asOfDates);
        asOfDates[0] = InfinityTimestamp.getParaInfinity();
        cache.getObjectFromData(createTinyBalanceData(1, may, InfinityTimestamp.getParaInfinity(), in), asOfDates);

        assertSame(febBalance, cache.get(indexRef, createTinyBalance(feb), extractors, false).get(0));
        assertEquals(1, cache.get(indexRef, createTinyBalance(apr), extractors, false).size());
        assertEquals(1, cache.get(indexRef, createTinyBalance(jun), extractors, false).size());
        sleep(timeToLive + 100);
        assertEquals(0, cache.get(indexRef, createTinyBalance(feb), extractors, false).size());
        assertEquals(0, cache.get(indexRef, createTinyBalance(apr), extractors, false).size());
        assertEquals(0, cache.get(indexRef, createTinyBalance(jun), extractors, false).size());
    }

    public void xtestDatedPartialCacheSoftReferenceCollection() throws Exception
    {
        TinyBalanceDatabaseObject dbo = new TinyBalanceDatabaseObject();
        Attribute[] primaryKeyAttributes = TinyBalanceFinder.getPrimaryKeyAttributes();
        AsOfAttribute[] asOfAttributes = TinyBalanceFinder.getAsOfAttributes();
        PartialDatedCache cache = new PartialDatedCache(primaryKeyAttributes,
                asOfAttributes, dbo, primaryKeyAttributes, 0, 0);
        List extractors = new ArrayList();
        for(int i=0;i<primaryKeyAttributes.length;i++)
        {
            extractors.add(primaryKeyAttributes[i]);
        }
        for(int i=0;i<asOfAttributes.length;i++)
        {
            extractors.add(asOfAttributes[i]);
        }
        int indexRef = cache.getBestIndexReference(extractors).indexReference;

        List dataExtractors = new ArrayList(extractors);
        for(int i=0;i<asOfAttributes.length;i++)
        {
            dataExtractors.set(primaryKeyAttributes.length + i, asOfAttributes[i].getFromAttribute());
        }
        Extractor[] arrayExtractors = (Extractor[]) dataExtractors.toArray(new Extractor[dataExtractors.size()]);
        Timestamp jan = new Timestamp(timestampFormat.parse("2003-01-15 00:00:00").getTime());
        Timestamp feb = new Timestamp(timestampFormat.parse("2003-02-15 00:00:00").getTime());
        Timestamp mar = new Timestamp(timestampFormat.parse("2003-03-15 00:00:00").getTime());
        Timestamp in = new Timestamp(0);
        Timestamp[] asOfDates = new Timestamp[2];
        asOfDates[0] = feb;
        asOfDates[1] = InfinityTimestamp.getParaInfinity();
        TinyBalance febBalance = (TinyBalance) cache.getObjectFromData(createTinyBalanceData(1, jan, mar, in), asOfDates);
        TinyBalanceData forSearch = createTinyBalanceData(1, jan, mar, in);
        for(int i=0;i<100000000;i++)
        {
            if (i % 1000000 == 0)
            {
                febBalance = null;
            }
            assertNotNull(cache.get(indexRef, forSearch, arrayExtractors, false));
            forSearch.setProcessingDateFrom(new Timestamp(i+1));
        }
    }

    private TinyBalance createTinyBalance(Timestamp apr)
    {
        TinyBalance bal = new TinyBalance(apr, InfinityTimestamp.getParaInfinity());
        bal.setAcmapCode("A");
        bal.setBalanceId(1);
        return bal;
    }

    public TinyBalanceData createTinyBalanceData(int balanceId, Timestamp from, Timestamp to, Timestamp in)
    {
        TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
        data.setAcmapCode("A");
        data.setBalanceId(balanceId);
        data.setBusinessDateFrom(from);
        data.setBusinessDateTo(to);
        data.setProcessingDateFrom(in);
        data.setProcessingDateTo(InfinityTimestamp.getParaInfinity());
        return data;
    }

    public ParaDeskData createParaDeskData(double size, int tag)
    {
        return createParaDeskData("deskId"+tag, size, tag);
    }

    public ParaDeskData createParaDeskData(String deskId, double size, int tag)
    {
        ParaDeskData pdd = new ParaDeskData();
        pdd.setDeskIdString(deskId);
        pdd.setSizeDouble(size);
        pdd.setTagInt(tag);
        pdd.setConnectionLong(tag);
        return pdd;
    }

    public UserData createUserData(int id, String name)
    {
        UserData userData = new UserData();
        userData.setId(id);
        userData.setUserId("User"+id);
        userData.setName(name);
        return userData;
    }
}
