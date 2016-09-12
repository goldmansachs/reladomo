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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.VerboseSerializer;

import java.io.*;
import java.util.List;


public class TestVerboseSerializer extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            DifferentDataTypes.class,
            TinyBalance.class
        };
    }

    public void testVerboseSerializerWithDifferentData() throws IOException, ClassNotFoundException
    {
        DifferentDataTypesList list = new DifferentDataTypesList(DifferentDataTypesFinder.all());
        assertTrue(list.size() > 0);
        byte[] bytes = this.serializeMessage(list, DifferentDataTypesFinder.getFinderInstance());
        List dataObjects = this.deserialize(bytes, DifferentDataTypesFinder.getFinderInstance(), DifferentDataTypesData.class);
        assertEquals(list.size(), dataObjects.size());
        for(int i=0;i<list.size();i++)
        {
            DifferentDataTypesData o = (DifferentDataTypesData) list.getDifferentDataTypesAt(i).zGetCurrentData();

            MithraDataObject newData = (MithraDataObject) dataObjects.get(i);
            assertTrue(o.hasSamePrimaryKeyIgnoringAsOfAttributes(newData));
            assertFalse(o.changed(newData));
        }
    }

    public void testVerboseSerializerSkippingColumns() throws IOException, ClassNotFoundException
    {
        DifferentDataTypesList list = new DifferentDataTypesList(DifferentDataTypesFinder.all());
        assertTrue(list.size() > 0);
        byte[] bytes = this.serializeMessage(list, DifferentDataTypesFinder.getFinderInstance());
        List dataObjects = this.deserialize(bytes, OldDifferentDataTypesFinder.getFinderInstance(), OldDifferentDataTypesData.class);
        assertEquals(list.size(), dataObjects.size());
        for(int i=0;i<list.size();i++)
        {
            DifferentDataTypesData o = (DifferentDataTypesData) list.getDifferentDataTypesAt(i).zGetCurrentData();

            OldDifferentDataTypesData newData = (OldDifferentDataTypesData) dataObjects.get(i);
            assertEquals(o.getId(), newData.getId());
        }
    }


    public void testVerboseSerializerWithOrders() throws IOException, ClassNotFoundException
    {
        OrderList list = new OrderList(OrderFinder.all());
        assertTrue(list.size() > 0);
        byte[] bytes = this.serializeMessage(list, OrderFinder.getFinderInstance());
        List dataObjects = this.deserialize(bytes, OrderFinder.getFinderInstance(), OrderData.class);
        assertEquals(list.size(), dataObjects.size());
        for(int i=0;i<list.size();i++)
        {
            OrderData o = (OrderData) list.getOrderAt(i).zGetCurrentData();

            MithraDataObject newData = (MithraDataObject) dataObjects.get(i);
            assertTrue(o.hasSamePrimaryKeyIgnoringAsOfAttributes(newData));
            assertFalse(o.changed(newData));
        }
    }

    public void testVerboseSerializerWithSourceAttribute() throws IOException, ClassNotFoundException
    {
        TinyBalanceList list = new TinyBalanceList(TinyBalanceFinder.businessDate().equalsEdgePoint().and(TinyBalanceFinder.acmapCode().eq("A")));
        assertTrue(list.size() > 0);
        byte[] bytes = this.serializeMessage(list, TinyBalanceFinder.getFinderInstance());
        List dataObjects = this.deserialize(bytes, TinyBalanceFinder.getFinderInstance(), TinyBalanceData.class);
        assertEquals(list.size(), dataObjects.size());
        for(int i=0;i<list.size();i++)
        {
            TinyBalanceData o = (TinyBalanceData) list.getTinyBalanceAt(i).zGetCurrentData();

            MithraDataObject newData = (MithraDataObject) dataObjects.get(i);
            assertTrue(o.hasSamePrimaryKeyIgnoringAsOfAttributes(newData));
            assertFalse(o.changed(newData));
        }
    }


    public byte[] serializeMessage(List objects, RelatedFinder finder) throws IOException
    {
        byte[] pileOfBytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2000);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        VerboseSerializer serializer = new VerboseSerializer(finder);
        serializer.writeObjects(objects, oos);
        oos.flush();
        bos.flush();
        pileOfBytes = bos.toByteArray();
        bos.close();
        return pileOfBytes;
    }

    public List deserialize(byte[] input, RelatedFinder finder, Class dataClass) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bis  = new ByteArrayInputStream(input);
        ObjectInputStream ois = new ObjectInputStream(bis);
        VerboseSerializer deserializer = new VerboseSerializer(finder, dataClass);

        List result = deserializer.readObjectsAsDataObjects(ois);
        ois.close();
        bis.close();
        return result;
    }

}
