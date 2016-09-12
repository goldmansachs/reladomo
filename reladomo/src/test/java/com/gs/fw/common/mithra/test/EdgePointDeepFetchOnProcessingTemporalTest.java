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
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;

import java.util.HashSet;
import java.util.Set;

public class EdgePointDeepFetchOnProcessingTemporalTest extends MithraTestAbstract
{
    @Override
    protected Class[] getRestrictedClassList()
    {
        Set<Class> result = new HashSet<Class>();
        result.add(Projito.class);
        result.add(ProjitoVersion.class);
        result.add(ProjitoMembership.class);
        result.add(ProjitoMeasureOfSuccess.class);
        result.add(ProjitoEmployee.class);
        result.add(ProjitoAddress.class);

        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    public void testEdgePointDeepFetchOnProcessingTemporal()
    {
        ProjitoVersionList projitoVersions = this.executeProjitoVersionQuery();
        assertEquals(1, projitoVersions.size());
        assertEquals("name 1", projitoVersions.get(0).getVersionedRoot().getName());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                ProjitoList projitos = executeProjitoQuery();
                assertEquals(1, projitos.size());

                Projito projito = projitos.get(0);
                projito.setName("New name, yo.");
                projito.getVersion().setNumber(projito.getVersion().getNumber() + 1L);
                return null;
            }
        });

        projitoVersions = this.executeProjitoVersionQuery();
        assertEquals(2, projitoVersions.size());

        int countBeforeGetters = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        assertEquals("New name, yo.", projitoVersions.get(0).getVersionedRoot().getName());
        assertEquals("tester1", projitoVersions.get(0).getVersionedRoot().getMemberships().get(0).getKerberos());
        assertEquals("name 1", projitoVersions.get(1).getVersionedRoot().getName());
        assertEquals("tester1", projitoVersions.get(1).getVersionedRoot().getMemberships().get(0).getKerberos());

        assertEquals(countBeforeGetters, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    private ProjitoVersionList executeProjitoVersionQuery()
    {
        Operation op = ProjitoVersionFinder.processingDate().equalsEdgePoint();
        op = op.and(ProjitoVersionFinder.versionedRoot().id().eq(1L));
        op = op.and(ProjitoVersionFinder.versionedRoot().memberships().kerberos().eq("tester1"));

        ProjitoVersionList projitoVersions = ProjitoVersionFinder.findMany(op);

        projitoVersions.deepFetch(ProjitoVersionFinder.versionedRoot().memberships());

        projitoVersions.setOrderBy(ProjitoVersionFinder.processingDateFrom().descendingOrderBy());

        return projitoVersions;
    }

    private ProjitoList executeProjitoQuery()
    {
        ProjitoList projitos = ProjitoFinder.findMany(
            ProjitoFinder.id().eq(1L).and(
            ProjitoFinder.processingDate().eq(DefaultInfinityTimestamp.getSybaseIqInfinity())));
        projitos.deepFetch(ProjitoFinder.memberships());

        return projitos;
    }

    public void testEdgePointDeepFetchOnProcessingTemporalOverlappingDeepFetchNode()
    {
        ProjitoVersionList projitoVersions = this.executeProjitoVersionQuery2();
        assertEquals(2, projitoVersions.size());

        int countBeforeGetters = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        ProjitoAddressList addresses = projitoVersions.get(0).getVersionedRoot().getMemberships().get(0).getEmployee().getAddresses();
        assertEquals(2, addresses.size());

        ProjitoAddressList addresses2 = projitoVersions.get(1).getVersionedRoot().getMemberships().get(0).getEmployee().getAddresses();
        assertEquals(2, addresses2.size());

        ProjitoMeasureOfSuccessList measuresOfSuccess = projitoVersions.get(0).getVersionedRoot().getMeasuresOfSuccess();
        assertEquals(2, measuresOfSuccess.size());
        assertEquals(2, measuresOfSuccess.get(0).getAccountableEmployee().getAddresses().size());
        assertEquals(1, measuresOfSuccess.get(1).getAccountableEmployee().getAddresses().size());

        ProjitoMeasureOfSuccessList measuresOfSuccess2 = projitoVersions.get(1).getVersionedRoot().getMeasuresOfSuccess();
        assertEquals(2, measuresOfSuccess2.size());
        assertEquals(2, measuresOfSuccess2.get(0).getAccountableEmployee().getAddresses().size());
        assertEquals(1, measuresOfSuccess2.get(1).getAccountableEmployee().getAddresses().size());

        assertEquals(countBeforeGetters, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    private ProjitoVersionList executeProjitoVersionQuery2()
    {
        Operation op = ProjitoVersionFinder.processingDate().equalsEdgePoint();
        op = op.and(ProjitoVersionFinder.versionedRoot().id().eq(2L));

        ProjitoVersionList projitoVersions = ProjitoVersionFinder.findMany(op);

        projitoVersions.deepFetch(ProjitoVersionFinder.versionedRoot().memberships().employee().addresses());
        projitoVersions.deepFetch(ProjitoVersionFinder.versionedRoot().measuresOfSuccess().accountableEmployee().addresses());

        projitoVersions.setOrderBy(ProjitoVersionFinder.number().descendingOrderBy());

        return projitoVersions;
    }

    public void testEdgePointDeepFetchWithExtraOrOperation()
    {
        ProjitoVersionList projitoVersions = this.executeProjitoVersionQuery3();
        assertEquals(2, projitoVersions.size());

        int countBeforeGetters = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        ProjitoAddressList addresses = projitoVersions.get(0).getVersionedRoot().getMemberships().get(0).getEmployee().getAddresses();
        assertEquals(2, addresses.size());

        ProjitoAddressList addresses2 = projitoVersions.get(1).getVersionedRoot().getMemberships().get(0).getEmployee().getAddresses();
        assertEquals(2, addresses2.size());

        ProjitoMeasureOfSuccessList measuresOfSuccess = projitoVersions.get(0).getVersionedRoot().getMeasuresOfSuccess();
        assertEquals(2, measuresOfSuccess.size());
        assertEquals(2, measuresOfSuccess.get(0).getAccountableEmployee().getAddresses().size());
        assertEquals(1, measuresOfSuccess.get(1).getAccountableEmployee().getAddresses().size());

        ProjitoMeasureOfSuccessList measuresOfSuccess2 = projitoVersions.get(1).getVersionedRoot().getMeasuresOfSuccess();
        assertEquals(2, measuresOfSuccess2.size());
        assertEquals(2, measuresOfSuccess2.get(0).getAccountableEmployee().getAddresses().size());
        assertEquals(1, measuresOfSuccess2.get(1).getAccountableEmployee().getAddresses().size());

        assertEquals(countBeforeGetters, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    private ProjitoVersionList executeProjitoVersionQuery3()
    {
        Operation authOp = ProjitoVersionFinder.versionedRoot().bvp().eq("bvp 2");
        authOp = authOp.or(ProjitoVersionFinder.versionedRoot().memberships().employee().kerberos().eq("tester2"));

        Operation op = ProjitoVersionFinder.processingDate().equalsEdgePoint();
        op = op.and(ProjitoVersionFinder.versionedRoot().id().eq(2L));
        op = op.and(authOp);

        ProjitoVersionList projitoVersions = new ProjitoVersionList(ProjitoVersionFinder.findMany(op));

        projitoVersions.deepFetch(ProjitoVersionFinder.versionedRoot().memberships().employee().addresses());
        projitoVersions.deepFetch(ProjitoVersionFinder.versionedRoot().measuresOfSuccess().accountableEmployee().addresses());

        projitoVersions.setOrderBy(ProjitoVersionFinder.number().descendingOrderBy());

        return projitoVersions;
    }
}
