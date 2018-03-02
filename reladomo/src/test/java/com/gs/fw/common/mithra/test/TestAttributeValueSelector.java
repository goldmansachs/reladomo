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

import com.gs.fw.common.mithra.test.domain.Group;
import com.gs.fw.common.mithra.test.domain.GroupFinder;
import com.gs.fw.common.mithra.test.domain.GroupList;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.User;
import com.gs.fw.common.mithra.test.domain.UserFinder;
import com.gs.fw.common.mithra.test.domain.UserList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;



public class TestAttributeValueSelector extends TestSqlDatatypes
{

    public void testSimpleAttributeSelectors()
    {
//        insert into PARA_DESK values ('rnd', 1, 10.54, 1000000, 100, 'O', '1981-06-08 02:01', 120, '1981-06-08', 999999.9, 4545 )
        ParaDesk paraDesk = ParaDeskFinder.findOne(ParaDeskFinder.deskIdString().eq("rnd"));
        assertNotNull(paraDesk);
        assertEquals(ParaDeskFinder.deskIdString().stringValueOf(paraDesk), paraDesk.getDeskIdString());
        assertEquals(ParaDeskFinder.activeBoolean().booleanValueOf(paraDesk), paraDesk.isActiveBoolean());
        assertEquals(ParaDeskFinder.closedDate().dateValueOf(paraDesk), paraDesk.getClosedDate());
        assertEquals(ParaDeskFinder.connectionLong().longValueOf(paraDesk), paraDesk.getConnectionLong());
        assertEquals(ParaDeskFinder.createTimestamp().timestampValueOf(paraDesk), paraDesk.getCreateTimestamp());
        assertEquals(ParaDeskFinder.locationByte().byteValueOf(paraDesk), paraDesk.getLocationByte());
        assertTrue(ParaDeskFinder.maxFloat().floatValueOf(paraDesk) == paraDesk.getMaxFloat());
        assertEquals(ParaDeskFinder.minShort().shortValueOf(paraDesk), paraDesk.getMinShort());
        assertTrue(ParaDeskFinder.sizeDouble().doubleValueOf(paraDesk) == paraDesk.getSizeDouble());
        assertEquals(ParaDeskFinder.statusChar().charValueOf(paraDesk), paraDesk.getStatusChar());
        assertEquals(ParaDeskFinder.tagInt().intValueOf(paraDesk), paraDesk.getTagInt());
    }

    public void testRelationshipSelectors()
    {
//        insert into user (objectid, userid, name, active, last_update, profile_oid, default_group_oid)
//        values (2,'rezeam','Mohammad Rezaei',0,'2004-05-11 00:00:00.0',1,1);

        User user = UserFinder.findOne(UserFinder.id().eq(2).and(UserFinder.sourceId().eq(0)));
        assertNotNull(user);
        Group defaultGroup = user.getDefaultGroup();
        assertNotNull(defaultGroup);
        assertNotNull(defaultGroup.getManager());
        assertSame(UserFinder.defaultGroup().valueOf(user), defaultGroup);
        assertEquals(UserFinder.defaultGroup().id().intValueOf(user), defaultGroup.getId());
        assertEquals(UserFinder.defaultGroup().managerId().intValueOf(user), defaultGroup.getManagerId());
        assertEquals(UserFinder.defaultGroup().name().stringValueOf(user), defaultGroup.getName());
        assertSame(UserFinder.defaultGroup().manager().valueOf(user), defaultGroup.getManager());
        assertSame(UserFinder.defaultGroup().manager().name().valueOf(user), defaultGroup.getManager().getName());
    }

    public void testListSelectors()
    {
        UserList ul = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        ul.getGroups().forceResolve();
        GroupList gl = (GroupList) UserFinder.groups().listValueOf(ul);
        Group g = GroupFinder.findOne(GroupFinder.id().eq(1).and(GroupFinder.sourceId().eq(0)));
        assertTrue(gl.contains(g));
    }

    public void testListSelectorsTwoDeep()
    {
        UserList ul = new UserList(UserFinder.id().eq(2).and(UserFinder.sourceId().eq(0)));
        UserList allUsersWithThisProfile = (UserList) UserFinder.profile().users().listValueOf(ul);
        assertTrue(allUsersWithThisProfile.contains(ul.get(0)));
    }

    public void testMaxLength()
    {
        assertEquals(50, OrderFinder.description().getMaxLength());
    }

    public void testAttributeAsSet()
    {
        UserList users = new UserList(UserFinder.sourceId().eq(0));

        assertEquals(IntHashSet.newSetWith(new int[] { 1, 2 }), UserFinder.profileId().asGscSet(users));
        assertEquals(IntHashSet.newSetWith(1, 2), UserFinder.profileId().asGscSet(users));

        IntHashSet gscIntHashSet = IntHashSet.newSetWith(new int[] { -42 });
        UserFinder.profileId().asSet(users, gscIntHashSet);
        assertEquals(IntHashSet.newSetWith(new int[]{1, 2, -42}), gscIntHashSet);

        MutableIntSet intSet = IntHashSet.newSetWith(-42);
        assertEquals(IntHashSet.newSetWith(1, 2, -42), UserFinder.profileId().asSet(users, intSet));
    }
}
