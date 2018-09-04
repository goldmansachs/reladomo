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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.util.HashSet;
import java.util.Set;
import java.sql.Timestamp;

public class MultiThreadDeepFetchTest extends MithraTestAbstract
{
    @Override
    protected Class[] getRestrictedClassList ()
    {
        Set<Class> result = new HashSet<Class> ();
        result.add (Projito.class);
        result.add (ProjitoVersion.class);
        result.add (ProjitoMembership.class);
        result.add (ProjitoMeasureOfSuccess.class);
        result.add (ProjitoEmployee.class);
        result.add (ProjitoAddress.class);

        Class[] array = new Class[result.size ()];
        result.toArray (array);
        return array;
    }


    public void testFiveThreadDeepFetch ()
    {
        Operation op = ProjitoVersionFinder.processingDate ().eq (Timestamp.valueOf ("2018-08-28 00:00:00")).and (ProjitoVersionFinder.versionedRoot ().id ().eq (2L));

        ProjitoVersionList projitoVersions = ProjitoVersionFinder.findManyBypassCache (op);

        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().memberships ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().memberships ().employee ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().memberships ().employee ().addresses ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().measuresOfSuccess ().accountableEmployee ().addresses ());

        projitoVersions.setOrderBy (ProjitoVersionFinder.number ().descendingOrderBy ());
        projitoVersions.setNumberOfParallelThreads (5);
        assertEquals (1, projitoVersions.size ());

        int countBeforeGetters = MithraManagerProvider.getMithraManager ().getDatabaseRetrieveCount ();

        ProjitoAddressList addresses = projitoVersions.get (0).getVersionedRoot ().getMemberships ().get (0).getEmployee ().getAddresses ();
        assertEquals (2, addresses.size ());

        ProjitoMeasureOfSuccessList measuresOfSuccess = projitoVersions.get (0).getVersionedRoot ().getMeasuresOfSuccess ();
        assertEquals (2, measuresOfSuccess.size ());
        assertEquals (2, measuresOfSuccess.get (0).getAccountableEmployee ().getAddresses ().size ());
        assertEquals (1, measuresOfSuccess.get (1).getAccountableEmployee ().getAddresses ().size ());

        assertEquals (countBeforeGetters, MithraManagerProvider.getMithraManager ().getDatabaseRetrieveCount ());
    }

    public void testCompletesAfterExceptionThrown ()
    {
        Operation op = ProjitoFinder.processingDate ().eq (Timestamp.valueOf ("2018-08-28 00:00:00")).and (ProjitoFinder.id ().eq (2L));

        ProjitoList projitoVersions = ProjitoFinder.findManyBypassCache (op);

        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().memberships ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().memberships ().employee ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().memberships ().employee ().addresses ());
        projitoVersions.deepFetch (ProjitoVersionFinder.versionedRoot ().measuresOfSuccess ().accountableEmployee ().addresses ());

        projitoVersions.setOrderBy (ProjitoVersionFinder.number ().descendingOrderBy ());
        projitoVersions.setNumberOfParallelThreads (5);
        try
        {
            projitoVersions.size ();
            fail();
        } catch (ClassCastException e)
        {
            // expected exception
        }

    }

}
