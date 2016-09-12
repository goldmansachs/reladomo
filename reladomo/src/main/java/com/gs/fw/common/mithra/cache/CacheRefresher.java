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

package com.gs.fw.common.mithra.cache;

import java.util.*;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.*;



public class CacheRefresher
{
    private static final int BATCH_SIZE = 20;
    private static final int DATA_OBJECT_BATCH_SIZE = 1000;
    private Attribute[] pks;
    private AsOfAttribute[] asOfAttributes;
    private RelatedFinder finder;


    public CacheRefresher(MithraObjectPortal mithraObjectPortal)
    {
        this.finder = mithraObjectPortal.getFinder();
        this.pks = finder.getPrimaryKeyAttributes();
        this.asOfAttributes = finder.getAsOfAttributes();
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return finder.getMithraObjectPortal();
    }

    public void refreshObjectsFromServer(MithraDataObject[] dataObjects, Object sourceAttributeValue)
    {
        Operation op = NoOperation.instance();

        if(sourceAttributeValue != null)
        {
            Attribute sourceAttribute = finder.getSourceAttribute();
            op = op.and(sourceAttribute.nonPrimitiveEq(sourceAttributeValue));
        }

        if(asOfAttributes != null)
        {
            for(int i = 0; i < asOfAttributes.length; i++)
            {
                op = op.and(asOfAttributes[i].equalsEdgePoint());
            }
        }

        if(pks.length == 1)
        {
            this.readDataObjectInSmallBatches(op, dataObjects);
        }
        else
        {
            Operation in = new TupleAttributeImpl(pks).in(Arrays.asList(dataObjects), pks);
            Operation newOp = op.and(in);
            MithraList list = finder.findMany(newOp);
            list.setBypassCache(true);
            list.forceResolve();
        }
    }

    private void readDataObjectInSmallBatches(Operation op, MithraDataObject[] dataObjects)
    {
        List dataObjectList = Arrays.asList(dataObjects);
        int dataObjectLength = dataObjects.length;
        int batchCount = dataObjectLength/DATA_OBJECT_BATCH_SIZE;
        if (dataObjectLength % DATA_OBJECT_BATCH_SIZE > 0)
        {
            batchCount++;
        }
        int initialPos = 0;
        int batchSize = DATA_OBJECT_BATCH_SIZE;
        for(int i = 0; i < batchCount; i++)
        {
            if((i + 1) == batchCount)
            {
                batchSize = dataObjectLength - initialPos;
            }
            readDataObjectBatch(op, dataObjectList.subList(initialPos, initialPos+batchSize));
            initialPos += batchSize;
        }
    }

    private void readDataObjectBatch(Operation op, List dataObjects)
    {
        op = op.and(pks[0].in(dataObjects, pks[0]));
        MithraList list = finder.findMany(op);
        list.setBypassCache(true);
        list.forceResolve();
    }

    protected List<List<MithraDataObject>> segregateBySourceAttribute(List<MithraDataObject> mithraObjects, Attribute sourceAttribute)
    {
        if (sourceAttribute == null || mithraObjects.size() == 1)
            return ListFactory.create(mithraObjects);
        MultiHashMap map = null;
        Object firstData = mithraObjects.get(0);
        for (int i = 0; i < mithraObjects.size(); i++)
        {
            MithraDataObject curData = mithraObjects.get(i);
            if (map != null)
            {
                map.put(sourceAttribute.valueOf(curData), curData);
            }
            else if (!sourceAttribute.valueEquals(firstData, curData))
            {
                map = new MultiHashMap();
                Object firstSource = sourceAttribute.valueOf(firstData);
                for (int j = 0; j < i; j++)
                {
                    map.put(firstSource, mithraObjects.get(j));
                }
                map.put(sourceAttribute.valueOf(curData), curData);
            }
        }

        if (map != null)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.create(mithraObjects);
        }
    }

    public void refreshObjectsFromServer(MithraFastList<MithraDataObject> toRefresh)
    {
        Attribute sourceAttribute = finder.getSourceAttribute();
        if (sourceAttribute == null)
        {
            refreshList(toRefresh, null);
        }
        else
        {
            List<List<MithraDataObject>> segregated = segregateBySourceAttribute(toRefresh, sourceAttribute);
            for(int i=0;i<segregated.size();i++)
            {
                refreshList(segregated.get(i), sourceAttribute.valueOf(segregated.get(i).get(0)));
            }
        }
    }

    private void refreshList(List<MithraDataObject> toRefresh, Object sourceAttributeValue)
    {
        MithraDataObject[] asArray = new MithraDataObject[toRefresh.size()];
        toRefresh.toArray(asArray);
        refreshObjectsFromServer(asArray, sourceAttributeValue);
    }
}
