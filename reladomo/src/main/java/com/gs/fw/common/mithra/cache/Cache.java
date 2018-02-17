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

import com.gs.fw.common.mithra.DatedTransactionalState;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
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
import org.eclipse.collections.api.set.primitive.BooleanSet;
import org.eclipse.collections.api.set.primitive.ByteSet;
import org.eclipse.collections.api.set.primitive.CharSet;
import org.eclipse.collections.api.set.primitive.DoubleSet;
import org.eclipse.collections.api.set.primitive.FloatSet;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.ShortSet;

import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public interface Cache
{
    public static final int WEAK_THRESHOLD = 50000;
    
    public int getId();
    
    public List getAll();

    public void forAll(DoUntilProcedure procedure);

    public IndexReference getIndexRef(Attribute attribute);

    public IndexReference getBestIndexReference(List attributes);

    public boolean mapsToUniqueIndex(List attributes);

    public int addIndex(String indexName, Extractor[] attributes);

    public int addUniqueIndex(String indexName, Extractor[] attributes);

    public int addTypedIndex(Extractor[] attributes, Class type, Class underlyingType);

    public int addTypedUniqueIndex(Extractor[] attributes, Class type, Class underlyingType);

    public boolean isFullCache();

    public boolean isPartialCache();

    public void clear();

    public List getMany(int indexRef, MithraTupleSet dataHolders, Extractor[] extractors, boolean abortIfNotFound);

    public void getManyDatedObjectsFromData(Object[] dataArray, int length, ObjectWithMapperStack[] asOfOpWithStacks);

    public int getAverageReturnSize(int indexRef, int multiplier);

    public int getMaxReturnSize(int indexRef, int multiplier);

    public List get(int indexRef, Object dataHolder, Extractor[] extractors, boolean parallelAllowed);

    public Object getAsOne(Object dataHolder, List extractors);

    public Object getAsOne(Object dataHolder, Extractor[] extractors);

    public Object getAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1);

    public Object getAsOneByIndex(int indexRef, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1);

    public List get(int indexRef, Set indexValues);

    public List get(int indexRef, ByteArraySet indexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.IntSet intSetIndexValues);

    public List get(int indexRef, IntSet intSetIndexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.DoubleSet doubleSetIndexValues);

    public List get(int indexRef, DoubleSet doubleSetIndexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.BooleanSet booleanSetIndexValues);

    public List get(int indexRef, BooleanSet booleanSetIndexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.LongSet longSetIndexValues);

    public List get(int indexRef, LongSet longSetIndexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.ByteSet byteSetIndexValues);

    public List get(int indexRef, ByteSet byteSetIndexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.CharSet charSetIndexValues);

    public List get(int indexRef, CharSet charSetIndexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.FloatSet floatSetIndexValues);

    public List get(int indexRef, FloatSet floatSetIndexValues);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public List get(int indexRef, com.gs.collections.api.set.primitive.ShortSet shortSetIndexValues);

    public List get(int indexRef, ShortSet shortSetIndexValues);

    public List get(int indexRef, Object indexValue);

    public List get(int indexRef, int indexValue);

    public List get(int indexRef, char indexValue);

    public List get(int indexRef, double indexValue);

    public List get(int indexRef, float indexValue);

    public List get(int indexRef, boolean indexValue);

    public List get(int indexRef, long indexValue);

    public List getNulls(int indexRef);

    /**
     *
     *
     *
     * @param indexRef refers NonUniqueIndex to be used to check presence
     * @param keyHolder
     * @param keyHolderNonDatedExtractors list of the extractors to hash keyHolder (note: order must match the order of the extractors on the Index extractors list or the hash code will not match.)
     * @param filter
     * @return
     */
    public boolean contains(IndexReference indexRef, Object keyHolder, Extractor[] keyHolderNonDatedExtractors, final Filter2 filter);

    public boolean isUnique(int indexReference);

    public boolean isUniqueAndImmutable(int indexReference);

    public void setMithraObjectPortal(MithraObjectPortal portal);

    public Object getObjectFromData(MithraDataObject data);

    public void getManyObjectsFromData(Object[] dataArray, int length, boolean weak);

    public Object getObjectFromData(MithraDataObject data, Timestamp[] asOfDates);

    public Object getObjectFromDataWithoutCaching(MithraDataObject data);

    public Object getObjectFromDataWithoutCaching(MithraDataObject data, Timestamp[] asOfDates);

    public Attribute[] getIndexAttributes(int indexRef);

    // only called when object is read from database
    public void reindex(MithraObject object, MithraDataObject newData, Object optionalBehavior, MithraDataObject optionalOldData);

    // this is called when a mithra object is changed via a setter method outside a transaction
    public void reindex(MithraObject object, AttributeUpdateWrapper updateWrapper);

    public void remove(MithraObject object);

    public Object put(MithraObject object);

    public void removeAll(List objects);

    public void removeAll(Filter filter);

    public void removeUsingData(MithraDataObject object);

    public boolean markDirty(MithraDataObject object);

    public void markNonExistent(int indexReference, Collection<Object> parentObjects, List<Extractor> extractors,
            List<Extractor> extraExtractors, Operation extraOperation);

    public MithraNotificationListener createNotificationListener(MithraObjectPortal portal);
    // transactional methods.
    // todo: rezaei: split into TransactionalCache interface

    // this is called when a mithra object is changed via a setter method inside a transaction
    public void reindexForTransaction(MithraObject object, AttributeUpdateWrapper updateWrapper);

    public void removeIgnoringTransaction(MithraObject object);

    public void commit(MithraTransaction tx);

    public void prepareForCommit(MithraTransaction tx);

    public void rollback(MithraTransaction tx);

    public Object preparePut(MithraObject obj);

    public void commitPreparedForIndex(Object index);

    public void commitRemovedObject(MithraDataObject data);

    public void commitObject(MithraTransactionalObject mithraObject, MithraDataObject oldData);

    public Object getObjectByPrimaryKey(MithraDataObject data, boolean evenIfDirty);

    public boolean enrollDatedObject(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite);

    public boolean enrollDatedObjectForDelete(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite);

    public TemporalContainer getOrCreateContainer(MithraDataObject mithraDataObject);

    public MithraDataObject getTransactionalDataFromData(MithraDataObject data);

    public boolean removeDatedData(MithraDataObject data);

    public List getDatedDataIgnoringDates(MithraDataObject data);

    public void putDatedData(MithraDataObject data);

    public PrimaryKeyIndex getPrimayKeyIndexCopy();

    public void updateCache(List newDataList, List updatedDataList, List deletedData);

    public void rollbackObject(MithraObject mithraObject);

    public MithraDataObject refreshOutsideTransaction(MithraDatedObject mithraObject, MithraDataObject data);

    public int size();

    public void markDirtyForReload(MithraDataObject object, MithraTransaction tx);

    public void reloadDirty(MithraTransaction tx);

    public void archiveCache(ObjectOutput out);

    public void archiveCacheWithFilter(ObjectOutput out, Filter filterOfDatesToKeep);

    public long getCacheTimeToLive();

    public long getRelationshipCacheTimeToLive();

    boolean isInitialized(int indexReference);

    public List<Object> collectMilestoningOverlaps();

    public int estimateQuerySize();

    public void destroy();

    public boolean isOffHeap();

    public long getOffHeapAllocatedDataSize();

    public long getOffHeapUsedDataSize();

    public long getOffHeapAllocatedIndexSize();

    public long getOffHeapUsedIndexSize();

    public boolean syncWithMasterCache(MasterCacheUplink uplink);

    public MasterSyncResult sendSyncResult(long maxReplicatedPageVersion);

    public boolean isDated();
}