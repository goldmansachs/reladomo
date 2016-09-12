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

package com.gs.fw.common.mithra.cacheloader;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.util.AbstractBooleanFilter;
import com.gs.fw.common.mithra.util.Filter2;
import com.gs.fw.common.mithra.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CacheIndexBasedFilter extends AbstractBooleanFilter
{
    private static Logger logger = LoggerFactory.getLogger(CacheIndexBasedFilter.class);
    private Cache cache;
    private IndexReference indexReference;
    private Extractor[] keyHolderExtractors;
    private Operation filterOp;
    private Filter2 filter = null;
    private String description;

    public static CacheIndexBasedFilter create(Cache cache, final Map<Attribute, Attribute> relationshipAttributes,
                                               Operation filterOp, String description)
    {
        CacheIndexBasedFilter cacheIndexBasedFilter = new CacheIndexBasedFilter(cache, filterOp);
        cacheIndexBasedFilter.description = description;

        Map<Attribute, Attribute> fixedRelationshipAttributes = UnifiedMap.newMap();

        for (Map.Entry<Attribute, Attribute> each : relationshipAttributes.entrySet())
        {
            if (!timestampToAsofAttributeRelationiship(each))
            {
                fixedRelationshipAttributes.put(each.getKey(), each.getValue());
            }
        }

        IndexReference ref = cache.getBestIndexReference(FastList.newList(fixedRelationshipAttributes.keySet()));

        if (!ref.isValid() || ref.indexReference == IndexReference.AS_OF_PROXY_INDEX_ID)
        {
            throw new RuntimeException("Failed to find index for relationship " + description + "(" + relationshipAttributes + "). Consider adding index " +
                    fixedRelationshipAttributes.values() + " to the owner object or change the relationship for the load.");
        }

        cacheIndexBasedFilter.setIndexReference(ref);
        List<Pair<Extractor, Extractor>> additionalKeyHolderAttributes = cacheIndexBasedFilter.setupIndexExtractors(fixedRelationshipAttributes);
        cacheIndexBasedFilter.checkIterationOverhead();
        cacheIndexBasedFilter.setupOwnerFilter(filterOp, additionalKeyHolderAttributes);
        return cacheIndexBasedFilter;
    }

    // unsupported combination of businessDate timestamp -> businessDate asOf mapping. requires custom index
    private static boolean timestampToAsofAttributeRelationiship(Map.Entry<Attribute, Attribute> each)
    {
        return (each.getValue() != null && each.getValue().isAsOfAttribute()) && !each.getKey().isAsOfAttribute();
    }

    private void checkIterationOverhead()
    {
        if (filterOp == null)
        {
            return;
        }

        int expectedIterationSize = cache.getAverageReturnSize(indexReference.indexReference, 1);
        if (expectedIterationSize > 100)
        {
            logger.warn("Using index " + (indexReference.indexReference - 1) + " with filter " + this.filterOp +
                    " may result in high iteration volume. (" + expectedIterationSize + ").");
        }
    }

    private List<Pair<Extractor, Extractor>> setupIndexExtractors(Map<Attribute, Attribute> relationshipAttributes)
    {
        List<Pair<Extractor, Extractor>> additionalKeyHolderAttributes = null;
        List orderedKeyHolderAttributes = FastList.newList();
        Attribute[] indexAttributes = cache.getIndexAttributes(this.indexReference.indexReference);
        List missingIndexAttributes = FastList.newList();
        Set<? extends Extractor> missingRelationshipAttributes = UnifiedSet.newSet(relationshipAttributes.keySet());

        for (Attribute indexAttribute : indexAttributes)
        {
            Attribute keyHolderAttribute = relationshipAttributes.get(indexAttribute);
            if (keyHolderAttribute == null)
            {
                if (indexAttribute.isAsOfAttribute())
                {
                    orderedKeyHolderAttributes.add(new AlwaysTrueAsOfAttribute());
                }
                else
                {
                    missingIndexAttributes.add(indexAttribute);
                }
            }
            else
            {
                orderedKeyHolderAttributes.add(keyHolderAttribute.isAsOfAttribute() ? new AlwaysTrueAsOfAttribute() : keyHolderAttribute);
                missingRelationshipAttributes.remove(indexAttribute);
            }
        }

        if (missingIndexAttributes.size() > 0)
        {
            throw new RuntimeException("Cannot use index since relationship " + this.description + " does not have these attributes mapped: " + missingIndexAttributes);
        }
        if (missingRelationshipAttributes.size() > 0)
        {
            logger.warn("Relationship " + this.description + "(" + relationshipAttributes + ") has to filter owners based on the additional attributes from keyHolder: " +
                    missingRelationshipAttributes + " Not the most efficient relationship.");
            additionalKeyHolderAttributes = FastList.newList();
            for (Extractor each : missingRelationshipAttributes)
            {
                Attribute pairedAttribute = relationshipAttributes.get(each);
                if (each != null && pairedAttribute != null)
                {
                    additionalKeyHolderAttributes.add(new Pair(each, pairedAttribute));
                }
            }
        }

        Extractor[] extractors = new Extractor[orderedKeyHolderAttributes.size()];
        orderedKeyHolderAttributes.toArray(extractors);
        this.keyHolderExtractors = extractors;

        return additionalKeyHolderAttributes;
    }

    public CacheIndexBasedFilter(Cache cache, Operation filterOp)
    {
        this.cache = cache;
        this.filterOp = filterOp;

    }

    public void setIndexReference(IndexReference indexReference)
    {
        this.indexReference = indexReference;
    }

    private void setupOwnerFilter(Operation filterOp, List<Pair<Extractor, Extractor>> additionalKeyHolderAttributes)
    {
        if (filterOp != null || additionalKeyHolderAttributes != null)
        {
            this.filter = new FilterWithAdditionalKeyHolderAttributes(filterOp, additionalKeyHolderAttributes);

        }
    }


    @Override
    public boolean matches(final Object keyHolder)
    {
        return cache.contains(this.indexReference, keyHolder, this.keyHolderExtractors, this.filter);
    }

    private class FilterWithAdditionalKeyHolderAttributes implements Filter2
    {
        private Operation filterOp;
        private List<Pair<Extractor, Extractor>> additionalKeyHolderAttributes;

        private FilterWithAdditionalKeyHolderAttributes(Operation filterOp, List<Pair<Extractor, Extractor>> additionalKeyHolderAttributes)
        {
            this.filterOp = filterOp;
            this.additionalKeyHolderAttributes = additionalKeyHolderAttributes;
        }

        @Override
        public boolean matches(Object o, Object keyHolder)
        {
            if (filterOp != null && !filterOp.matches(o))
            {
                return false;
            }
            if (additionalKeyHolderAttributes != null)
            {
                for (int i = 0; i < additionalKeyHolderAttributes.size(); i++)
                {
                    Pair<Extractor, Extractor> pair = additionalKeyHolderAttributes.get(i);
                    if (!pair.getOne().valueEquals(o, keyHolder, pair.getTwo()))
                    {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private void reportError()
    {
        throw new RuntimeException("" + this.description);
    }


    public class AlwaysTrueAsOfAttribute implements AsOfExtractor
    {
        public Timestamp getDataSpecificValue(MithraDataObject data)
        {
            reportError();
            return null;
        }

        // is called by the FullSemiUniqueDatedIndex containsInNonDatedIfMatchAsOfDates
        public boolean dataMatches(Object data, Timestamp asOfDate, AsOfAttribute asOfAttribute)
        {
            return true;
        }

        // is called by the fullSemiUniqueDatedIndex containsInNonDatedMultiEntry
        public boolean matchesMoreThanOne()
        {
            return false;
        }

        // is called by the fullSemiUniqueDatedIndex containsInNonDatedMultiEntry. result is used by dataMatches and is ignored.
        public Timestamp timestampValueOf(Object o)
        {
            return null;
        }


        public long timestampValueOfAsLong(Object o)
        {
            reportError();
            return 0;
        }


        public void setTimestampValue(Object o, Timestamp newValue)
        {
            reportError();

        }


        public void setValue(Object o, Object newValue)
        {
            reportError();
        }


        public void setValueNull(Object o)
        {
            reportError();
        }


        public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
        {
            reportError();
        }


        public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
        {
            reportError();
        }


        public boolean isAttributeNull(Object o)
        {
            reportError();
            return false;
        }


        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            reportError();
            return false;
        }


        public int valueHashCode(Object object)
        {
            reportError();
            return 0;
        }


        public boolean valueEquals(Object first, Object second)
        {
            reportError();
            return false;
        }


        public Object valueOf(Object object)
        {
            reportError();
            return null;
        }
    }

}
