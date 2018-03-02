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

package com.gs.fw.common.mithra.portal;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectDeserializer;
import com.gs.fw.common.mithra.MithraPureObjectFactory;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.finder.Operation;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.IOException;
import java.io.ObjectInput;
import java.sql.Timestamp;
import java.util.List;


public abstract class MithraAbstractPureObjectFactory implements MithraObjectDeserializer, MithraPureObjectFactory
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
        for(int i=0;i < size;i++)
        {
            MithraDataObject data = this.deserializeFullData(in);
            if (weak)
            {
                result.add(cache.getObjectFromDataWithoutCaching(data));
            }
            else
            {
                result.add(cache.getObjectFromData(data));
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
            this.analyzeChangeForReload(fullUniqueIndex, data, newDataList, updatedDataList);
        }
        List deletedData = fullUniqueIndex.getAll();
        cache.updateCache(newDataList, updatedDataList, deletedData);
    }

    protected Timestamp[] getAsOfDates()
    {
        return null;
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
        MithraTransactionalObject object = (MithraTransactionalObject) fullUniqueIndex.removeUsingUnderlying(data);
        if (object == null)
        {
            newDataList.add(data);
        }
        else
        {
            if (object.zUnsynchronizedGetData().changed(data))
            {
                updatedDataList.add(data);
            }
        }
    }

}
