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

package com.gs.fw.common.mithra.querycache;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.UpdateCountHolder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.SmallSet;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.Arrays;
import java.util.List;



public class CachedQuery
{

    private Operation operation;
    private OrderBy orderBy;
    private List result;
    private UpdateCountHolder[] updateCountHolders;
    private int[] originalValues;
    private byte compactBools = 0;

    private static final int MAX_FULL_CACHE_RELATIONSHIPS_TO_KEEP = 100000;

    public CachedQuery(Operation op, OrderBy orderBy, CachedQuery first)
    {
        this(op, orderBy, first, false);
    }

    public CachedQuery(Operation op, OrderBy orderBy, CachedQuery first, boolean mergeUpdateCounters)
    {
        this.operation = op;
        this.orderBy = orderBy;

        if (first == null)
        {
            populateUpdateCounters();
        }
        else
        {
            if (mergeUpdateCounters)
            {
                populateUpdateCounters();
                mergeUpdateCounters(first);
            }
            else
            {
                this.updateCountHolders = first.updateCountHolders;
                this.originalValues = first.originalValues;
            }
        }
    }

    public CachedQuery(Operation op, OrderBy orderBy)
    {
        this.operation = op;
        this.orderBy = orderBy;
        populateUpdateCounters();
    }

    private CachedQuery(Operation operation, OrderBy orderBy, List result, UpdateCountHolder[] updateCountHolders, int[] originalValues)
    {
        this.operation = operation;
        this.orderBy = orderBy;
        this.result = result;
        this.updateCountHolders = updateCountHolders;
        this.originalValues = originalValues;
    }

    public void setReachedMaxRetrieveCount(boolean reachedMaxRetrieveCount)
    {
        if (reachedMaxRetrieveCount)
        {
            compactBools = (byte) ((int) compactBools | 1);
        }
        else
        {
            compactBools = (byte) ((int) compactBools & ~( 1));
        }
    }

    public void setOneQueryForMany(boolean oneQueryForMany)
    {
        if (oneQueryForMany)
        {
            compactBools = (byte) ((int) compactBools | 1 << 1);
        }
        else
        {
            compactBools = (byte) ((int) compactBools & ~( 1 << 1));
        }
    }

    public void setWasDefaulted()
    {
        compactBools = (byte)((int)compactBools | 1 << 2);
    }

    public void setIsModifiable()
    {
        compactBools = (byte)((int)compactBools | 1 << 3);
    }

    public void setIsSubquery()
    {
        compactBools = (byte)((int)compactBools | 1 << 4);
    }

//    public void setNullableFloatValueNull()
//    {
//        compactBools = (byte)((int)compactBools | 1 << 5);
//    }
//
//    public void setNullableDoubleValueNull()
//    {
//        compactBools = (byte)((int)compactBools | 1 << 6);
//    }
//
    public boolean reachedMaxRetrieveCount()
    {
        return (compactBools & 1) != 0 ;
    }

    public boolean isOneQueryForMany()
    {
        return (compactBools & 1 << 1) != 0 ;
    }

    public boolean wasDefaulted()
    {
        return (compactBools & 1 << 2) != 0 ;
    }

    public boolean isModifiable()
    {
        return (compactBools & 1 << 3) != 0 ;
    }

    public boolean isSubQuery()
    {
        return (compactBools & 1 << 4) != 0 ;
    }

//    public boolean isNullableFloatValueNull()
//    {
//        return (compactBools & 1 << 5) != 0 ;
//    }
//
//    public boolean isNullableDoubleValueNull()
//    {
//        return (compactBools & 1 << 6) != 0 ;
//    }
//
//    public boolean wasDefaulted()
//    {
//        return wasDefaulted;
//    }

    public List getPortalList()
    {
        SmallSet portalList = new SmallSet(3);
        this.operation.addDependentPortalsToSet(portalList);
        return portalList;
    }

    private void mergeUpdateCounters(CachedQuery first)
    {
        // Merges the update count holders and original values from first into this, eliminating duplicates.
        // If the same update count holder appears in both, the original values of first take precedence as they are guaranteed to be the oldest.

        if (first.updateCountHolders == null) return;
        if (this.updateCountHolders == null)
        {
            this.updateCountHolders = first.updateCountHolders;
            this.originalValues = first.originalValues;
            return;
        }

        final UnifiedSet<UpdateCountHolder> updateCountHoldersFromThisToAdd = new UnifiedSet<UpdateCountHolder>();
        updateCountHoldersFromThisToAdd.addAll(Arrays.asList(this.updateCountHolders));
        updateCountHoldersFromThisToAdd.removeAll(Arrays.asList(first.updateCountHolders));

        final int firstLength = first.updateCountHolders.length;
        final int combinedLength = firstLength + updateCountHoldersFromThisToAdd.size();

        final UpdateCountHolder[] combinedUpdateCountHolders = new UpdateCountHolder[combinedLength];
        final int[] combinedOriginalValues = new int[combinedLength];

        System.arraycopy(first.updateCountHolders, 0, combinedUpdateCountHolders, 0, firstLength);
        System.arraycopy(first.originalValues, 0, combinedOriginalValues, 0, firstLength);

        int insertPosition = firstLength;
        for (int i = 0; i < this.updateCountHolders.length; i++)
        {
            UpdateCountHolder updateCountHolder = this.updateCountHolders[i];
            if (updateCountHoldersFromThisToAdd.remove(updateCountHolder))
            {
                combinedUpdateCountHolders[insertPosition] = updateCountHolder;
                combinedOriginalValues[insertPosition] = this.originalValues[i];
                insertPosition++;
            }
        }

        if (insertPosition != combinedLength)
        {
            throw new IllegalStateException("Should not be possible - inconsistency while merging update counters");
        }

        this.updateCountHolders = combinedUpdateCountHolders;
        this.originalValues = combinedOriginalValues;
    }

    private void populateUpdateCounters()
    {
        if (this.operation instanceof CompactUpdateCountOperation)
        {
            CompactUpdateCountOperation compactOp = (CompactUpdateCountOperation) this.operation;
            this.updateCountHolders = compactOp.getUpdateCountHolders();
            this.originalValues = compactOp.getUpdateCountValues();
            return;
        }
        SmallSet portalOrAttributeSet = new SmallSet(6);
        this.operation.addDependentPortalsToSet(portalOrAttributeSet);
        int size1 = portalOrAttributeSet.size();
        this.operation.addDepenedentAttributesToSet(portalOrAttributeSet);
        if (orderBy != null)
        {
            orderBy.addDepenedentAttributesToSet(portalOrAttributeSet);
        }
        int size = portalOrAttributeSet.size();
        updateCountHolders = new UpdateCountHolder[size];

        for(int i=0;i<size1;i++)
        {
            Object object = portalOrAttributeSet.get(i);
            MithraObjectPortal portal = (MithraObjectPortal) object;
            if (portal.isForTempObject())
            {
                updateCountHolders = null;
                return;
            }
            updateCountHolders[i] = portal.getPerClassUpdateCountHolder();
        }
        for(int i=size1;i<size;i++)
        {
            Object object = portalOrAttributeSet.get(i);
            updateCountHolders[i] = (UpdateCountHolder) object;
        }
        originalValues = new int[size];
        for(int i=0;i<size;i++)
        {
            originalValues[i] = updateCountHolders[i].getUpdateCount();
        }
        MithraObjectPortal portal = this.operation.getResultObjectPortal();
        this.updateCountHolders = portal.getPooledUpdateCountHolders(updateCountHolders);
        this.originalValues = portal.getPooledIntegerArray(this.originalValues);
    }

    public boolean isExpired()
    {
        if (updateCountHolders == null) return false;
        for(int i=0;i<updateCountHolders.length;i++)
        {
            if (originalValues[i] != updateCountHolders[i].getUpdateCount()) return true;
        }
        return false;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public List getResult()
    {
        return result;
    }

    public void setResult(List result)
    {
        int size = result.size();
        switch(size)
        {
            case 0:
                this.result = ListFactory.EMPTY_LIST; // conserve memory
                break;
            case 1:
                this.result = ListFactory.create(result.get(0));
                break;
            default:
                this.result = result;
        }
    }

    public boolean prepareToCacheQuery(boolean forRelationship, QueryCache queryCache)
    {
        if (updateCountHolders == null) return false;
        if (this.operation instanceof CompactUpdateCountOperation)
        {
            CompactUpdateCountOperation compactOperation = (CompactUpdateCountOperation) this.operation;
            Operation substitute = compactOperation.getCachableOperation();
            if (substitute == null)
            {
                if (forRelationship && queryCache.roughSize() < MAX_FULL_CACHE_RELATIONSHIPS_TO_KEEP)
                {
                    substitute = compactOperation.forceGetCachableOperation();
                }
                else
                {
                    return false;
                }
            }
            this.operation = substitute;
        }
        return true;
    }

    public void cacheQuery(boolean forRelationship)
    {
        QueryCache queryCache = operation.getResultObjectPortal().getQueryCache();
        if (prepareToCacheQuery(forRelationship, queryCache))
        {
            if (forRelationship)
            {
                this.cacheQueryForRelationship();
            }
            else
                if (!reachedMaxRetrieveCount())
                {
                    queryCache.cacheQuery(this);
                }
        }
    }

    public void cacheQueryForRelationship()
    {
        if (updateCountHolders == null) return;
        QueryCache queryCache = operation.getResultObjectPortal().getQueryCache();
        queryCache.cacheQueryForRelationship(this);
    }

    public CachedQuery getCloneForEquivalentOperation(Operation op, OrderBy orderBy)
    {
        CachedQuery clone = new CachedQuery(op, orderBy, this);
        clone.result = this.result;
        clone.setWasDefaulted();
        if (!this.hasSameOrderBy(orderBy) && this.result.size() > 1)
        {
            FastList newOrderedList = new FastList(this.result);
            newOrderedList.sortThis(orderBy);
            clone.result = newOrderedList;
        }
        return clone;
    }

    public CachedQuery getCloneIfDifferentOrderBy(OrderBy orderBy)
    {
        CachedQuery clone = this;
        if (!this.hasSameOrderBy(orderBy) && this.result.size() > 1)
        {
            FastList newOrderedList = new FastList(this.result);
            newOrderedList.sortThis(orderBy);
            clone = new CachedQuery(this.operation, orderBy, newOrderedList, this.updateCountHolders, this.originalValues);
        }
        return clone;
    }

    public CachedQuery getModifiableClone()
    {
        List newOrderedList = new FastList(this.result);
        CachedQuery result = new CachedQuery(this.operation, orderBy, newOrderedList, this.updateCountHolders, this.originalValues);
        result.setIsModifiable();
        return result;
    }

    public boolean hasSameOrderBy(OrderBy orderBy)
    {
        return orderBy == null || orderBy.equals(this.orderBy);
    }

}
