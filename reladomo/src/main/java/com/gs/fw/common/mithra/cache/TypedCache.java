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

package com.gs.fw.common.mithra.cache;

import com.gs.collections.api.set.primitive.*;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.offheap.MasterCacheUplink;
import com.gs.fw.common.mithra.cache.offheap.MasterSyncResult;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.notification.listener.MithraNotificationListener;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Filter2;
import com.gs.fw.common.mithra.util.MithraTupleSet;

import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.*;


public class TypedCache implements Cache
{
    private Cache cache;
    private Class type;
    private Class underlyingType;

    public TypedCache(Cache cache, Class type)
    {
        this.cache = cache;
        this.type = type;
        try
        {
            //todo: offheap
            this.underlyingType = Class.forName(type.getName() + "Data");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("could not find underlying type");
        }
    }

    public List getAll()
    {
        return this.filterByType(this.cache.getAll());
    }

    public void forAll(final DoUntilProcedure procedure)
    {
        this.cache.forAll(new DoUntilProcedure()
        {
            public boolean execute(Object object)
            {
                if (TypedCache.this.type.isAssignableFrom(object.getClass()))
                {
                    return procedure.execute(object);
                }
                return false;
            }
        });
    }

    @Override
    public boolean contains(IndexReference indexRef, Object keyHolder, Extractor[] keyHolderNonDatedExtractors, final Filter2 filter)
    {
        return cache.contains(indexRef,  keyHolder, keyHolderNonDatedExtractors, filter);
    }

    @Override
    public int getId()
    {
        return this.cache.getId();
    }

    public IndexReference getIndexRef(Attribute attribute)
    {
        return this.cache.getIndexRef(attribute);
    }

    public IndexReference getBestIndexReference(List attributes)
    {
        return this.cache.getBestIndexReference(attributes);
    }

    public boolean mapsToUniqueIndex(List attributes)
    {
        return this.cache.mapsToUniqueIndex(attributes);
    }

    public int addIndex(String indexName, Extractor[] attributes)
    {
        return this.cache.addIndex(indexName, attributes);
    }

    public int addUniqueIndex(String indexName, Extractor[] attributes)
    {
        return this.cache.addUniqueIndex(indexName, attributes);
    }

    public int addTypedIndex(Extractor[] attributes, Class type, Class underlyingType)
    {
        return this.cache.addTypedIndex(attributes, type, underlyingType);
    }

    public int addTypedUniqueIndex(Extractor[] attributes, Class type, Class underlyingType)
    {
        return this.cache.addTypedUniqueIndex(attributes, type, underlyingType);
    }

    public boolean isFullCache()
    {
        return this.cache.isFullCache();
    }

    public boolean isPartialCache()
    {
        return this.cache.isPartialCache();
    }

    public void clear()
    {
        this.cache.clear();
    }

    public List getMany(int indexRef, MithraTupleSet dataHolders, Extractor[] extractors, boolean abortIfNotFound)
    {
        return this.filterByType(this.cache.getMany(indexRef, dataHolders, extractors, abortIfNotFound));
    }

    public void getManyDatedObjectsFromData(Object[] dataArray, int length, ObjectWithMapperStack[] asOfOpWithStacks)
    {
        this.cache.getManyDatedObjectsFromData(dataArray, length, asOfOpWithStacks);
    }

    @Override
    public int getAverageReturnSize(int indexRef, int multiplier)
    {
        return this.cache.getAverageReturnSize(indexRef, multiplier);
    }

    @Override
    public int getMaxReturnSize(int indexRef, int multiplier)
    {
        return this.cache.getMaxReturnSize(indexRef, multiplier);
    }

    public List get(int indexRef, Object dataHolder, Extractor[] extractors, boolean parallelAllowed)
    {
        return this.filterByType(this.cache.get(indexRef, dataHolder, extractors, parallelAllowed));
    }

    public Object getAsOne(Object dataHolder, List extractors)
    {
        return this.checkInstance(this.cache.getAsOne(dataHolder, extractors));
    }

    public Object getAsOne(Object dataHolder, Extractor[] extractors)
    {
        return this.checkInstance(this.cache.getAsOne(dataHolder, extractors));
    }

    public Object getAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        return this.checkInstance(cache.getAsOne(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1));
    }

    public Object getAsOneByIndex(int indexRef, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        return this.checkInstance(cache.getAsOneByIndex(indexRef, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1));
    }

    private Object checkInstance(Object result)
    {
        if (result != null && !this.type.isAssignableFrom(result.getClass()))
        {
            result = null;
        }
        return result;
    }

    public List get(int indexRef, Set indexValues)
    {
        return this.filterByType(this.cache.get(indexRef, indexValues));
    }

    public List get(int indexRef, ByteArraySet indexValues)
    {
        return this.filterByType(this.cache.get(indexRef, indexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, IntSet intSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, intSetIndexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.IntSet intSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, intSetIndexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, DoubleSet doubleSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, doubleSetIndexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.DoubleSet doubleSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, doubleSetIndexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, BooleanSet booleanSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, booleanSetIndexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.BooleanSet booleanSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, booleanSetIndexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, LongSet longSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, longSetIndexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.LongSet longSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, longSetIndexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, ByteSet byteSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, byteSetIndexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.ByteSet byteSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, byteSetIndexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, CharSet indexValues)
    {
        return this.filterByType(this.cache.get(indexRef, indexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.CharSet indexValues)
    {
        return this.filterByType(this.cache.get(indexRef, indexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, FloatSet floatSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, floatSetIndexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.FloatSet floatSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, floatSetIndexValues));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public List get(int indexRef, ShortSet shortSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, shortSetIndexValues));
    }

    @Override
    public List get(int indexRef, org.eclipse.collections.api.set.primitive.ShortSet shortSetIndexValues)
    {
        return this.filterByType(this.cache.get(indexRef, shortSetIndexValues));
    }

    public List get(int indexRef, Object indexValue)
    {
        return this.filterByType(this.cache.get(indexRef, indexValue));
    }

    public List get(int indexRef, int indexValue)
    {
        return this.filterByType(this.cache.get(indexRef, indexValue));
    }

    public List get(int indexRef, char indexValue)
    {
        return this.filterByType(this.cache.get(indexRef, indexValue));
    }

    public List get(int indexRef, double indexValue)
    {
        return this.filterByType(this.cache.get(indexRef, indexValue));
    }

    public List get(int indexRef, float indexValue)
    {
        return this.filterByType(this.cache.get(indexRef, indexValue));
    }

    public List get(int indexRef, boolean indexValue)
    {
        return this.filterByType(this.cache.get(indexRef, indexValue));
    }

    public List get(int indexRef, long indexValue)
    {
        return this.filterByType(this.cache.get(indexRef, indexValue));
    }

    public List getNulls(int indexRef)
    {
        return this.filterByType(this.cache.getNulls(indexRef));
    }

    public boolean isUnique(int indexReference)
    {
        return this.cache.isUnique(indexReference);
    }

    @Override
    public boolean isInitialized(int indexReference)
    {
        return this.cache.isInitialized(indexReference);
    }

    @Override
    public List<Object> collectMilestoningOverlaps()
    {
        return FastList.newList();
    }

    @Override
    public int estimateQuerySize()
    {
        return this.cache.estimateQuerySize();
    }

    public boolean isUniqueAndImmutable(int indexReference)
    {
        return this.cache.isUniqueAndImmutable(indexReference);
    }

    public void setMithraObjectPortal(MithraObjectPortal portal)
    {
        this.cache.setMithraObjectPortal(portal);
    }

    public Object getObjectFromData(MithraDataObject data)
    {
        return this.cache.getObjectFromData(data);
    }

    public void getManyObjectsFromData(Object[] dataArray, int length, boolean weak)
    {
        this.cache.getManyObjectsFromData(dataArray, length, weak);
    }

    public Object getObjectFromData(MithraDataObject data, Timestamp[] asOfDates)
    {
        return this.cache.getObjectFromData(data, asOfDates);
    }

    public Object getObjectFromDataWithoutCaching(MithraDataObject data)
    {
        return this.cache.getObjectFromDataWithoutCaching(data);
    }

    public Object getObjectFromDataWithoutCaching(MithraDataObject data, Timestamp[] asOfDates)
    {
        return this.cache.getObjectFromDataWithoutCaching(data, asOfDates);
    }

    public Attribute[] getIndexAttributes(int indexRef)
    {
        return this.cache.getIndexAttributes(indexRef);
    }

    public void reindex(MithraObject object, MithraDataObject newData, Object optionalBehavior, MithraDataObject optionalOldData)
    {
        this.cache.reindex(object, newData, optionalBehavior, optionalOldData);
    }

    public void reindex(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        this.cache.reindex(object, updateWrapper);
    }

    public void remove(MithraObject object)
    {
        this.cache.remove(object);
    }

    public Object put(MithraObject object)
    {
        return this.cache.put(object);
    }

    public void removeAll(List objects)
    {
        this.cache.removeAll(objects);
    }

    public void removeAll(Filter filter)
    {
        this.cache.removeAll(new TypedFilter(filter, this.type, this.underlyingType));
    }

    public void removeUsingData(MithraDataObject object)
    {
        this.cache.removeUsingData(object);
    }

    public boolean markDirty(MithraDataObject object)
    {
        return this.cache.markDirty(object);
    }

    @Override
    public void markNonExistent(int indexReference, Collection<Object> parentObjects, List<Extractor> extractors, List<Extractor> extraExtractors, Operation extraOperation)
    {
        this.cache.markNonExistent(indexReference, parentObjects, extractors, extraExtractors, extraOperation);
    }

    public MithraNotificationListener createNotificationListener(MithraObjectPortal portal)
    {
        return this.cache.createNotificationListener(portal);
    }

    public void reindexForTransaction(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        this.cache.reindexForTransaction(object, updateWrapper);
    }

    public void removeIgnoringTransaction(MithraObject object)
    {
        this.cache.removeIgnoringTransaction(object);
    }

    public void commit(MithraTransaction tx)
    {
        this.cache.commit(tx);
    }

    public void prepareForCommit(MithraTransaction tx)
    {
        this.cache.prepareForCommit(tx);
    }

    public void rollback(MithraTransaction tx)
    {
        this.cache.rollback(tx);
    }

    public Object preparePut(MithraObject obj)
    {
        return cache.preparePut(obj);
    }

    public void commitPreparedForIndex(Object index)
    {
        this.cache.commitPreparedForIndex(index);
    }

    public void commitRemovedObject(MithraDataObject data)
    {
        this.cache.commitRemovedObject(data);
    }

    public void commitObject(MithraTransactionalObject mithraObject, MithraDataObject oldData)
    {
        this.cache.commitObject(mithraObject, oldData);
    }

    public Object getObjectByPrimaryKey(MithraDataObject data, boolean evenIfDirty)
    {
        return this.cache.getObjectByPrimaryKey(data, evenIfDirty);
    }

    public boolean enrollDatedObject(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        return this.cache.enrollDatedObject(mithraObject, prevState, forWrite);
    }

    public boolean enrollDatedObjectForDelete(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        return this.cache.enrollDatedObjectForDelete(mithraObject, prevState, forWrite);
    }

    public TemporalContainer getOrCreateContainer(MithraDataObject mithraDataObject)
    {
        return this.cache.getOrCreateContainer(mithraDataObject);
    }

    public MithraDataObject getTransactionalDataFromData(MithraDataObject data)
    {
        return this.cache.getTransactionalDataFromData(data);
    }

    public boolean removeDatedData(MithraDataObject data)
    {
        return this.cache.removeDatedData(data);
    }

    public List getDatedDataIgnoringDates(MithraDataObject data)
    {
        return this.cache.getDatedDataIgnoringDates(data);
    }

    public void putDatedData(MithraDataObject data)
    {
        this.cache.putDatedData(data);
    }

    public PrimaryKeyIndex getPrimayKeyIndexCopy()
    {
        return this.cache.getPrimayKeyIndexCopy();
    }

    public void updateCache(List newDataList, List updatedDataList, List deletedData)
    {
        this.cache.updateCache(newDataList, updatedDataList, deletedData);
    }

    public void rollbackObject(MithraObject mithraObject)
    {
        this.cache.rollbackObject(mithraObject);
    }

    public MithraDataObject refreshOutsideTransaction(MithraDatedObject mithraObject, MithraDataObject data)
    {
        return this.cache.refreshOutsideTransaction(mithraObject, data);
    }

    public int size()
    {
        return this.cache.size();
    }

    public void markDirtyForReload(MithraDataObject object, MithraTransaction tx)
    {
        this.cache.markDirtyForReload(object, tx);
    }

    public void reloadDirty(MithraTransaction tx)
    {
        this.cache.reloadDirty(tx);
    }

    private List filterByType(List candidates)
    {
        if (candidates instanceof RandomAccess)
        {
            int size = candidates.size();
            for (int i = 0; i < size; i++)
            {
                Object candidate = candidates.get(i);
                if (!this.type.isAssignableFrom(candidate.getClass()))
                {
                    if (i != --size)
                    {
                        candidates.set(i--, candidates.get(size));
                    }
                    candidates.remove(size);
                }
            }
        }
        else
        {
            for (ListIterator li = candidates.listIterator(); li.hasNext();)
            {
                Object candidate = li.next();
                if (!this.type.isAssignableFrom(candidate.getClass()))
                {
                    li.remove();
                }
            }
        }
        return candidates;
    }

    public void archiveCache(ObjectOutput out)
    {
        throw new MithraBusinessException("Archiving of a hierarchy should only be used from the root of the hierarchy");
    }

    public void archiveCacheWithFilter(ObjectOutput out, Filter filterOfDatesToKeep)
    {
        throw new MithraBusinessException("Archiving of a hierarchy should only be used from the root of the hierarchy");
    }

    public long getCacheTimeToLive()
    {
        return cache.getCacheTimeToLive();
    }

    public long getRelationshipCacheTimeToLive()
    {
        return cache.getRelationshipCacheTimeToLive();
    }

    @Override
    public void destroy()
    {
        this.cache = null;
    }

    public boolean isOffHeap()
    {
        return this.cache.isOffHeap();
    }

    @Override
    public long getOffHeapAllocatedDataSize()
    {
        return cache.getOffHeapAllocatedDataSize();
    }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return cache.getOffHeapAllocatedIndexSize();
    }

    @Override
    public long getOffHeapUsedDataSize()
    {
        return cache.getOffHeapUsedDataSize();
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return cache.getOffHeapUsedIndexSize();
    }

    @Override
    public MasterSyncResult sendSyncResult(long maxReplicatedPageVersion)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isDated()
    {
        return cache.isDated();
    }

    @Override
    public boolean syncWithMasterCache(MasterCacheUplink uplink)
    {
        throw new RuntimeException("not implemented");
    }
}