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

/*
 *
 * Version     : : 1.1 $
 *
 *
 */
package com.gs.fw.common.mithra.util.dbextractor;

import java.util.*;

import com.gs.collections.api.block.*;
import com.gs.collections.api.block.function.*;
import com.gs.collections.impl.list.mutable.*;
import com.gs.collections.impl.map.mutable.*;
import com.gs.collections.impl.map.strategy.mutable.*;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.*;

public class ExtractResult
{
    private final UnifiedMapWithHashingStrategy<MithraObject, FullUniqueIndex> dataObjectsByFinder = UnifiedMapWithHashingStrategy.newMap(
            new HashingStrategy<MithraObject>()
            {
                @Override
                public int computeHashCode(MithraObject mithraObject)
                {
                    return HashUtil.combineHashes(HashUtil.hash(finder(mithraObject)), HashUtil.hash(source(mithraObject)));
                }

                @Override
                public boolean equals(MithraObject mithraObject, MithraObject mithraObject2)
                {
                    if (finder(mithraObject).equals(finder(mithraObject2)))
                    {
                        Object source = source(mithraObject);
                        Object source2 = source(mithraObject2);
                        return source == null ? source2 == null : source.equals(source2);
                    }
                    return false;
                }
            });

    public synchronized void addMithraObjects(List mithraList, List notExtracted)
    {
        for (Object object : mithraList)
        {
            final MithraObject mithraObject = MithraObject.class.cast(object);
            final RelatedFinder finder = mithraObject.zGetCurrentData().zGetMithraObjectPortal().getFinder();
            FullUniqueIndex existing = this.dataObjectsByFinder.getIfAbsentPut(mithraObject, new Function0<FullUniqueIndex>()
            {
                @Override
                public FullUniqueIndex value()
                {
                    Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
                    AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
                    Extractor[] extractors = new Extractor[primaryKeyAttributes.length + (asOfAttributes == null ? 0 : asOfAttributes.length)];
                    System.arraycopy(primaryKeyAttributes, 0, extractors, 0, primaryKeyAttributes.length);
                    if (asOfAttributes != null)
                    {
                        for (int i = 0; i < asOfAttributes.length; i++)
                        {
                            extractors[primaryKeyAttributes.length + i] = asOfAttributes[i].getFromAttribute();
                        }
                    }
                    return new FullUniqueIndex(extractors, 100);
                }
            });
            if (existing.put(mithraObject) == null && notExtracted != null)
            {
                notExtracted.add(mithraObject);
            }
        }
    }

    public Map<Pair<RelatedFinder, Object>, List<MithraDataObject>> getDataObjectsByFinderAndSource()
    {
        UnifiedMap<Pair<RelatedFinder, Object>, List<MithraDataObject>> result = UnifiedMap.newMap(this.dataObjectsByFinder.size());
        for (MithraObject key : this.dataObjectsByFinder.keySet())
        {
            FullUniqueIndex mithraObjects = this.dataObjectsByFinder.get(key);
            RelatedFinder finder = finder(key);
            Object source = source(key);
            List<MithraDataObject> mithraDataObjects = FastList.newList(mithraObjects.size());
            for (Object mithraObject : mithraObjects.getAll())
            {
                mithraDataObjects.add(((MithraObject) mithraObject).zGetCurrentData());
            }
            result.put(Pair.of(finder, source), mithraDataObjects);
        }
        return result;
    }

    private static Object source(MithraObject mithraObject)
    {
        Attribute sourceAttribute = finder(mithraObject).getSourceAttribute();
        return sourceAttribute == null ? null : sourceAttribute.valueOf(mithraObject);
    }

    private static RelatedFinder finder(MithraObject mithraObject)
    {
        return mithraObject.zGetCurrentData().zGetMithraObjectPortal().getFinder();
    }

    public void merge(ExtractResult branchResult)
    {
        for (MithraObject key : branchResult.dataObjectsByFinder.keySet())
        {
            this.addMithraObjects(branchResult.dataObjectsByFinder.get(key).getAll(), null);
        }
    }
}
