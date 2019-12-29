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
    
    int getId();
    
    List getAll();

    void forAll(DoUntilProcedure procedure);

    IndexReference getIndexRef(Attribute attribute);

    IndexReference getBestIndexReference(List attributes);

    boolean mapsToUniqueIndex(List attributes);

    int addIndex(String indexName, Extractor[] attributes);

    int addUniqueIndex(String indexName, Extractor[] attributes);

    int addTypedIndex(Extractor[] attributes, Class type, Class underlyingType);

    int addTypedUniqueIndex(Extractor[] attributes, Class type, Class underlyingType);

    boolean isFullCache();

    boolean isPartialCache();

    void clear();

    List getMany(int indexRef, MithraTupleSet dataHolders, Extractor[] extractors, boolean abortIfNotFound);

    void getManyDatedObjectsFromData(Object[] dataArray, int length, ObjectWithMapperStack[] asOfOpWithStacks);

    int getAverageReturnSize(int indexRef, int multiplier);

    int getMaxReturnSize(int indexRef, int multiplier);

    List get(int indexRef, Object dataHolder, Extractor[] extractors, boolean parallelAllowed);

    Object getAsOne(Object dataHolder, List extractors);

    Object getAsOne(Object dataHolder, Extractor[] extractors);

    Object getAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1);

    Object getAsOneByIndex(int indexRef, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1);

    List get(int indexRef, Set indexValues);

    List get(int indexRef, ByteArraySet indexValues);

    List get(int indexRef, IntSet intSetIndexValues);

    List get(int indexRef, DoubleSet doubleSetIndexValues);

    List get(int indexRef, BooleanSet booleanSetIndexValues);

    List get(int indexRef, LongSet longSetIndexValues);

    List get(int indexRef, ByteSet byteSetIndexValues);

    List get(int indexRef, CharSet charSetIndexValues);

    List get(int indexRef, FloatSet floatSetIndexValues);

    List get(int indexRef, ShortSet shortSetIndexValues);

    List get(int indexRef, Object indexValue);

    List get(int indexRef, int indexValue);

    List get(int indexRef, char indexValue);

    List get(int indexRef, double indexValue);

    List get(int indexRef, float indexValue);

    List get(int indexRef, boolean indexValue);

    List get(int indexRef, long indexValue);

    List getNulls(int indexRef);

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
    boolean contains(IndexReference indexRef, Object keyHolder, Extractor[] keyHolderNonDatedExtractors, final Filter2 filter);

    boolean isUnique(int indexReference);

    boolean isUniqueAndImmutable(int indexReference);

    void setMithraObjectPortal(MithraObjectPortal portal);

    Object getObjectFromData(MithraDataObject data);

    void getManyObjectsFromData(Object[] dataArray, int length, boolean weak);

    Object getObjectFromData(MithraDataObject data, Timestamp[] asOfDates);

    Object getObjectFromDataWithoutCaching(MithraDataObject data);

    Object getObjectFromDataWithoutCaching(MithraDataObject data, Timestamp[] asOfDates);

    Attribute[] getIndexAttributes(int indexRef);

    // only called when object is read from database
    void reindex(MithraObject object, MithraDataObject newData, Object optionalBehavior, MithraDataObject optionalOldData);

    // this is called when a mithra object is changed via a setter method outside a transaction
    void reindex(MithraObject object, AttributeUpdateWrapper updateWrapper);

    void remove(MithraObject object);

    Object put(MithraObject object);

    void removeAll(List objects);

    void removeAll(Filter filter);

    void removeUsingData(MithraDataObject object);

    boolean markDirty(MithraDataObject object);

    void markNonExistent(int indexReference, Collection<Object> parentObjects, List<Extractor> extractors,
     List<Extractor> extraExtractors, Operation extraOperation);

    MithraNotificationListener createNotificationListener(MithraObjectPortal portal);
    // transactional methods.
    // todo: rezaei: split into TransactionalCache interface

    // this is called when a mithra object is changed via a setter method inside a transaction
    void reindexForTransaction(MithraObject object, AttributeUpdateWrapper updateWrapper);

    void removeIgnoringTransaction(MithraObject object);

    void commit(MithraTransaction tx);

    void prepareForCommit(MithraTransaction tx);

    void rollback(MithraTransaction tx);

    Object preparePut(MithraObject obj);

    void commitPreparedForIndex(Object index);

    void commitRemovedObject(MithraDataObject data);

    void commitObject(MithraTransactionalObject mithraObject, MithraDataObject oldData);

    Object getObjectByPrimaryKey(MithraDataObject data, boolean evenIfDirty);

    boolean enrollDatedObject(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite);

    boolean enrollDatedObjectForDelete(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite);

    TemporalContainer getOrCreateContainer(MithraDataObject mithraDataObject);

    MithraDataObject getTransactionalDataFromData(MithraDataObject data);

    boolean removeDatedData(MithraDataObject data);

    List getDatedDataIgnoringDates(MithraDataObject data);

    void putDatedData(MithraDataObject data);

    PrimaryKeyIndex getPrimayKeyIndexCopy();

    void updateCache(List newDataList, List updatedDataList, List deletedData);

    void rollbackObject(MithraObject mithraObject);

    MithraDataObject refreshOutsideTransaction(MithraDatedObject mithraObject, MithraDataObject data);

    int size();

    void markDirtyForReload(MithraDataObject object, MithraTransaction tx);

    void reloadDirty(MithraTransaction tx);

    void archiveCache(ObjectOutput out);

    void archiveCacheWithFilter(ObjectOutput out, Filter filterOfDatesToKeep);

    long getCacheTimeToLive();

    long getRelationshipCacheTimeToLive();

    boolean isInitialized(int indexReference);

    List<Object> collectMilestoningOverlaps();

    int estimateQuerySize();

    void destroy();

    boolean isOffHeap();

    long getOffHeapAllocatedDataSize();

    long getOffHeapUsedDataSize();

    long getOffHeapAllocatedIndexSize();

    long getOffHeapUsedIndexSize();

    boolean syncWithMasterCache(MasterCacheUplink uplink);

    MasterSyncResult sendSyncResult(long maxReplicatedPageVersion);

    boolean isDated();
}