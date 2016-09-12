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

package com.gs.fw.common.mithra.portal;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectDeserializer;
import com.gs.fw.common.mithra.MithraPureObjectFactory;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.finder.Operation;

import java.io.IOException;
import java.io.ObjectInput;
import java.sql.Timestamp;
import java.util.List;


public abstract class MithraAbstractPureDatedObjectFactory implements MithraObjectDeserializer, MithraPureObjectFactory
{

    private String factoryParameter;

    public String getFactoryParameter()
    {
        return factoryParameter;
    }

    public void setFactoryParameter(String factoryParameter)
    {
        this.factoryParameter = factoryParameter;
    }

    public List deserializeList(Operation op, ObjectInput in, boolean weak) throws IOException, ClassNotFoundException
    {
        Cache cache = this.getMithraObjectPortal().getCache();
        int size = in.readInt();
        FastList result = new FastList(size);
        Timestamp[] asOfDates = this.getAsOfDates();
        for(int i=0;i < size;i++)
        {
            MithraDataObject data = this.deserializeFullData(in);
            this.deserializeAsOfAttributes(in, asOfDates);
            if (weak)
            {
                result.add(cache.getObjectFromDataWithoutCaching(data, asOfDates));
            }
            else
            {
                result.add(cache.getObjectFromData(data, asOfDates));
            }
        }
        return result;
    }

    public void deserializeForReload(ObjectInput in) throws IOException, ClassNotFoundException
    {
        Cache cache = this.getMithraObjectPortal().getCache();
        PrimaryKeyIndex fullUniqueIndex = cache.getPrimayKeyIndexCopy();
        int size = in.readInt();
        FastList newDataList = new FastList();
        FastList updatedDataList = new FastList();
        for(int i=0;i < size;i++)
        {
            MithraDataObject data = this.deserializeFullData(in);
            deserializeAsOfAttributes(in, this.getAsOfDates());
            this.analyzeChangeForReload(fullUniqueIndex, data, newDataList, updatedDataList);
        }
        List deletedData = fullUniqueIndex.getAll();
        cache.updateCache(newDataList, updatedDataList, deletedData);
    }

    public void reloadFullCache()
    {
        // hook for subclass to override
    }

    public void loadFullCache()
    {
        // hook for subclass to override
    }

    public void analyzeChangeForReload(PrimaryKeyIndex fullUniqueIndex, MithraDataObject data, List newDataList, List updatedDataList)
    {
        MithraDataObject existingData = (MithraDataObject) fullUniqueIndex.removeUsingUnderlying(data);
        if (existingData == null)
        {
            newDataList.add(data);
        }
        else
        {
            if (existingData.changed(data))
            {
                updatedDataList.add(data);
            }
        }
    }

    public abstract void deserializeAsOfAttributes(ObjectInput in, Timestamp[] asof) throws IOException, ClassNotFoundException;
    protected abstract Timestamp[] getAsOfDates();

}
