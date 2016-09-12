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

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.tax.Address;
import com.gs.fw.common.mithra.test.tax.AddressFinder;
import com.gs.fw.common.mithra.test.tax.AddressList;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;

import java.util.Set;

public class TestIsNullOrLargeInOperation extends MithraTestAbstract
{
    private static int addressId = 0;

    //Tests a bug found with using 2 large in sets, and an isNull operation.
    public void testIsNullOrLargeInOperation()
    {
        createAddress("abc", "def");
        createAddress("aabbcc", null);
        createAddress("xyz", "def");
        createAddress("xxyyzz", null);

        Set<String> cities = createSet(200);
        Set<String> states = createSet(300);

        cities.add("abc");
        cities.add("aabbcc");
        states.add("def");

        Operation addressOperation = AddressFinder.city().in(cities);
        addressOperation = addressOperation.and(AddressFinder.state().isNull().or(AddressFinder.state().in(states)));

        AddressList addressList = AddressFinder.findMany(addressOperation);

        assertTrue(addressList.size() == 2);
        Set<String> resultCities = UnifiedSet.newSet();
        for(Address a: addressList)
        {
            resultCities.add(a.getCity());
        }
        assertTrue(resultCities.contains("abc"));
        assertTrue(resultCities.contains("aabbcc"));
        assertFalse(resultCities.contains("xyz"));
        assertFalse(resultCities.contains("xxyyzz"));
    }

    private Set<String> createSet(int size)
    {
        Set<String> values = UnifiedSet.newSet(size);
        for(int i = 0; i<=size; i++)
        {
            values.add(Integer.toString(i));
        }

        return values;
    }

    private void createAddress(final String city, final String state)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Address address = new Address(DefaultInfinityTimestamp.getDefaultInfinity());
                address.setCity(city);
                if(state != null)
                    address.setState(state);

                address.setAddressId(addressId++);
                address.setChangedBy("testusr");

                address.insert();
                return null;
            }
        });
    }
}
