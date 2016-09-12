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

package com.gs.fw.common.mithra.finder;


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.extractor.Extractor;

import java.io.Serializable;
import java.util.List;

public class RelationshipMultiExtractor
{
    private List<Attribute> leftAttributes;
    private List<Extractor> extractors;
    private Extractor[] extractorArray;
    private volatile IndexReference bestIndexRef;
    private volatile UpdateCountHolder[] updateCountHolders;
    private volatile int[] originalValues;

    public RelationshipMultiExtractor(List<Attribute> leftAttributes, List<Extractor> extractors)
    {
        this.leftAttributes = leftAttributes;
        this.extractors = extractors;
        this.extractorArray = extractors.toArray(new Extractor[extractors.size()]);
    }

    public List<Attribute> getLeftAttributes()
    {
        return leftAttributes;
    }

    public IndexReference getBestIndexRef(Cache cache)
    {
        IndexReference localRef = this.bestIndexRef;

        if (localRef == null)
        {
            localRef = cache.getBestIndexReference(this.getLeftAttributes());
            this.bestIndexRef = localRef;
        }
        else if (!localRef.isForCache(cache))
        {
            this.updateCountHolders = createUpdateCountHolders();
            localRef = cache.getBestIndexReference(this.getLeftAttributes());
            this.bestIndexRef = localRef;
        }
        return localRef;
    }

    public Extractor[] getExtractorArray()
    {
        return extractorArray;
    }

    public List<Extractor> getExtractors()
    {
        return extractors;
    }
    
    public int[] getUpdateCountValues()
    {
        int[] currentValues = this.originalValues;
        if (expired(currentValues))
        {
            currentValues = createCurrentUpdateCounts();
            this.originalValues = currentValues;
        }
        return currentValues;
    }

    private boolean expired(int[] old)
    {
        if (old == null)
        {
            return true;
        }
        UpdateCountHolder[] countHolders = this.updateCountHolders;
        if (countHolders == null)
        {
            return true;
        }
        for(int i=0;i<countHolders.length;i++)
        {
            if (old[i] != countHolders[i].getUpdateCount())
            {
                return true;
            }
        }
        return false;
    }

    private int[] createCurrentUpdateCounts()
    {
        UpdateCountHolder[] countHolders = this.updateCountHolders;
        if (countHolders == null)
        {
            return null;
        }
        int[] current = new int[countHolders.length];
        for(int i=0;i<countHolders.length;i++)
        {
            current[i] = countHolders[i].getUpdateCount();
        }
        return getOwnerPortal().getPooledIntegerArray(current);
    }

    public UpdateCountHolder[] getUpdateCountHolders()
    {
        UpdateCountHolder[] localHolders = this.updateCountHolders;
        if (localHolders == null)
        {
            localHolders = createUpdateCountHolders();
            this.updateCountHolders = localHolders;
            if (localHolders == null)
            {
                return null;
            }
        }
        if (getOwnerPortal().getPerClassUpdateCountHolder() != localHolders[0])
        {
            localHolders = createUpdateCountHolders();
            this.updateCountHolders = localHolders;
        }
        return localHolders;
    }

    private MithraObjectPortal getOwnerPortal()
    {
        return leftAttributes.get(0).getOwnerPortal();
    }

    private UpdateCountHolder[] createUpdateCountHolders()
    {
        int size = 1 + this.leftAttributes.size();

        MithraObjectPortal ownerPortal = getOwnerPortal();
        if (ownerPortal.isForTempObject())
        {
            return null;
        }
        UpdateCountHolder[] holders = new UpdateCountHolder[size];
        holders[0] = ownerPortal.getPerClassUpdateCountHolder();
        for(int i=0;i<leftAttributes.size();i++)
        {
            holders[i+1] = leftAttributes.get(i);
        }
        return  holders;
    }

    public static RelationshipMultiExtractorBuilder withLeftAttributes(Attribute... attributes)
    {
        return new RelationshipMultiExtractorBuilder(attributes);
    }
    
    public static class RelationshipMultiExtractorBuilder
    {
        private List<Attribute> leftAttributes;

        public RelationshipMultiExtractorBuilder(Attribute... leftAttributes)
        {
            this.leftAttributes = FastList.newListWith(leftAttributes);
        }

        public RelationshipMultiExtractor withExtractors(Extractor... extractors)
        {
            return new RelationshipMultiExtractor(leftAttributes, FastList.newListWith(extractors));
        }
    }
}
