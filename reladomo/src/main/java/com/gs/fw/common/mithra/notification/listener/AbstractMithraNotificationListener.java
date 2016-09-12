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

package com.gs.fw.common.mithra.notification.listener;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.cache.CacheRefresher;
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.OrOperation;
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.util.Arrays;
import java.util.List;



public abstract class AbstractMithraNotificationListener implements MithraNotificationListener
{

    private static final int BATCH_SIZE = 20;
    private static final int DATA_OBJECT_BATCH_SIZE = 1000;
    private String finderClassname;
    private RelatedFinder finder;
    private CacheRefresher cacheRefresher;


    public AbstractMithraNotificationListener(MithraObjectPortal mithraObjectPortal)
    {
        this.finder = mithraObjectPortal.getFinder();
        this.cacheRefresher = new CacheRefresher(mithraObjectPortal);
        this.finderClassname = finder.getFinderClassName();
    }

     public String getFinderClassname()
     {
        return finderClassname;
     }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return finder.getMithraObjectPortal();
    }

    public abstract void onInsert(MithraDataObject[] mithraDataObjects, Object sourceAttributeValue);
    public abstract void onUpdate(MithraDataObject[] mithraDataObjects, Attribute[] updatedAttributes, Object sourceAttributeValue);
    public abstract void onMassDelete(final Operation op);

    public void onDelete(MithraDataObject[] mithraDataObjects)
    {
        MithraObjectPortal mithraObjectPortal = this.getMithraObjectPortal();
        int size = mithraDataObjects.length;
        for(int i = 0; i < size; i++)
        {
             mithraObjectPortal.getCache().markDirty(mithraDataObjects[i]);
        }
        mithraObjectPortal.incrementClassUpdateCount();
    }

    protected void readObjectsFromDatabase(MithraDataObject[] dataObjects, Object sourceAttributeValue)
    {
        this.cacheRefresher.refreshObjectsFromServer(dataObjects, sourceAttributeValue);
    }

    protected void onUpdateForPartialCache(MithraDataObject[] mithraDataObjects, Attribute[] updatedAttributes, Object sourceAttribute)
    {
        Attribute attribute;
        int size = mithraDataObjects.length;
        int dirtyCount = 0;
        for(int i = 0; i < size; i++)
        {
            if (getMithraObjectPortal().getCache().markDirty(mithraDataObjects[i]))
            {
                dirtyCount++;
            }
        }
        if(updatedAttributes != null)
        {
            for(int i = 0; i < updatedAttributes.length; i++)
            {
                attribute = updatedAttributes[i];
                attribute.incrementUpdateCount();
            }
        }
        if (updatedAttributes == null || dirtyCount > 0)
        {
            getMithraObjectPortal().incrementClassUpdateCount();
        }

    }
}
