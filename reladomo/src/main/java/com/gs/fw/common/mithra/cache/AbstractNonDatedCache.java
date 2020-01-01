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
import com.gs.fw.common.mithra.MithraObjectFactory;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.state.PersistedState;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MithraTupleSet;
import org.eclipse.collections.api.iterator.BooleanIterator;
import org.eclipse.collections.api.iterator.ByteIterator;
import org.eclipse.collections.api.iterator.CharIterator;
import org.eclipse.collections.api.iterator.DoubleIterator;
import org.eclipse.collections.api.iterator.FloatIterator;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.ShortIterator;
import org.eclipse.collections.api.set.primitive.BooleanSet;
import org.eclipse.collections.api.set.primitive.ByteSet;
import org.eclipse.collections.api.set.primitive.CharSet;
import org.eclipse.collections.api.set.primitive.DoubleSet;
import org.eclipse.collections.api.set.primitive.FloatSet;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.ShortSet;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public abstract class AbstractNonDatedCache extends AbstractCache
{
    private static Logger logger = LoggerFactory.getLogger(AbstractNonDatedCache.class);

    private final PrimaryKeyIndex primaryKeyIndex;

    private final InternalList monitoredAttributesList = new InternalList();
    private final Attribute[] primaryKeyAttributes;
    private final Attribute[] immutableAttributes;
    private final MithraObjectFactory factory;
    private MithraObjectPortal portal;
    private final long timeToLive;
    private final long relationshipTimeToLive;
    protected UnderlyingObjectGetter underlyingObjectGetter;

    public AbstractNonDatedCache(Attribute[] pkAttributes, MithraObjectFactory factory, long timeToLive, long relationshipTimeToLive)
    {
        this(pkAttributes, factory, pkAttributes, timeToLive, relationshipTimeToLive);
    }

    public AbstractNonDatedCache(Attribute[] pkAttributes, MithraObjectFactory factory, Attribute[] immutableAttributes, long timeToLive, long relationshipTimeToLive)
    {
        this(pkAttributes, factory, immutableAttributes, timeToLive, relationshipTimeToLive, null);
    }

    public AbstractNonDatedCache(Attribute[] pkAttributes, MithraObjectFactory factory, Attribute[] immutableAttributes,
            long timeToLive, long relationshipTimeToLive, UnderlyingObjectGetter underlyingObjectGetter)
    {
        super(1);
        this.underlyingObjectGetter = underlyingObjectGetter;
        this.factory = factory;
        this.primaryKeyAttributes = pkAttributes;
        this.timeToLive = timeToLive;
        this.relationshipTimeToLive = relationshipTimeToLive;
        this.primaryKeyIndex = this.createPrimaryKeyIndex(null, pkAttributes, timeToLive, relationshipTimeToLive);
        this.indices[0] = primaryKeyIndex;
        this.immutableAttributes = immutableAttributes;
        isIndexImmutable[0] = populateAttributeToIndexMap(pkAttributes, this.primaryKeyIndex);
        this.indexReferences[0] = new IndexReference(this, 1);
        if (timeToLive > 0) CacheClock.register(timeToLive);
        if (relationshipTimeToLive > 0) CacheClock.register(relationshipTimeToLive);
    }

    public long getRelationshipCacheTimeToLive()
    {
        return relationshipTimeToLive;
    }

    public long getCacheTimeToLive()
    {
        return timeToLive;
    }

    protected int addIndex(Index index, Extractor[] attributes)
    {
        int indexRef = this.addIndex(index);
        isIndexImmutable[indexRef] = this.populateAttributeToIndexMap(attributes, index);
        return indexRef;
    }

    protected MithraObjectFactory getFactory()
    {
        return factory;
    }

    public Attribute[] getPrimaryKeyAttributes()
    {
        return primaryKeyAttributes;
    }

    protected boolean populateAttributeToIndexMap(Extractor[] attributes, Index index)
    {
        boolean immutable = true;
        for (int i = 0; i < attributes.length; i++)
        {
            List existing = (List) attributeToIndexMap.get(attributes[i]);
            if (existing == null)
            {
                existing = new FastList();
                attributeToIndexMap.put(attributes[i], existing);
            }
            existing.add(index);
            if (!this.isImmutableAttribute((Attribute) attributes[i]))
            {
                if (this.monitoredAttributes.add(attributes[i]))
                {
                    this.monitoredAttributesList.add(attributes[i]);
                }
                immutable = false;
            }
        }
        return immutable;
    }

    private boolean isImmutableAttribute(Attribute attribute)
    {
        for (int i = 0; i < this.immutableAttributes.length; i++)
        {
            if (attribute.equals(this.immutableAttributes[i]))
            {
                return true;
            }
        }
        return false;
    }

    protected abstract Index createIndex(String indexName, Extractor[] extractors);

    protected abstract PrimaryKeyIndex createPrimaryKeyIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive);

    protected abstract Index createUniqueIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive);

    public int addIndex(String indexName, Extractor[] attributes)
    {
        int indexRef = -1;
        Index index = this.createIndex(indexName, attributes);
        if (index != null)
        {
            indexRef = this.addIndex(index, attributes);
        }
        return indexRef+1;
    }

    public int addTypedIndex(Extractor[] attributes, Class type, Class underlyingType)
    {
        int indexRef = -1;
        Index index = this.createIndex("", attributes);
        if (index != null)
        {
            TypedIndex typedIndex = new TypedIndex(index, type, underlyingType);
            indexRef = this.addIndex(typedIndex, attributes);
        }
        return indexRef+1;
    }

    public int addTypedUniqueIndex(Extractor[] attributes, Class type, Class underlyingType)
    {
        int indexRef = -1;
        Index index = this.createUniqueIndex("", attributes, timeToLive, relationshipTimeToLive);
        if (index != null)
        {
            TypedIndex typedIndex = new TypedIndex(index, type, underlyingType);
            indexRef = this.addIndex(typedIndex, attributes);
        }
        return indexRef+1;
    }

    public void clear()
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            for (int i = 0; i < indices.length; i++)
            {
                indices[i].clear();
            }
        }
        finally
        {
            this.readWriteLock.release();
        }

    }

    @Override
    public int getAverageReturnSize(int indexRef, int multiplier)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return this.getAverageReturnSize(this.indices[indexRef - 1], multiplier);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public int getMaxReturnSize(int indexRef, int multiplier)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            long expectedSize = this.indices[indexRef - 1].getMaxReturnSize(multiplier);
            long cacheSize = this.getPrimaryKeyIndex().size();
            if (this.isPartialCache())
            {
                cacheSize = 10 * multiplier;
            }
            return (int) Math.min(expectedSize, cacheSize);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    private int getAverageReturnSize(Index index, int multiplier)
    {
        long expectedSize = ((long) index.getAverageReturnSize()) * multiplier;
        long cacheSize = this.getPrimaryKeyIndex().size();
        if (this.isPartialCache())
        {
            cacheSize = 10 * multiplier;
        }
        return (int) Math.min(expectedSize, cacheSize);
    }

    public int addUniqueIndex(String indexName, Extractor[] attributes)
    {
        int indexRef = -1;
        Index uniqueIndex = this.createUniqueIndex(indexName, attributes, timeToLive, relationshipTimeToLive);
        if (uniqueIndex != null)
        {
            indexRef = this.addIndex(uniqueIndex, attributes);
        }
        return indexRef+1;
    }

    public List getAll()
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return this.primaryKeyIndex.getAll();
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void forAll(DoUntilProcedure procedure)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            this.primaryKeyIndex.forAll(procedure);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    protected IndexReference getIndexRefForSingleAttribute(Attribute attribute)
    {
        for (int i = 0; i < this.indices.length; i++)
        {
            Extractor[] extractors = indices[i].getExtractors();
            if (extractors.length == 1 && extractors[0].equals(attribute))
            {
                return this.getInitializedIndexReference(i, this.primaryKeyIndex);
            }
        }
        return this.noIndexReference;
    }

    public IndexReference getIndexRef(Attribute attribute)
    {
        return this.getIndexRefForSingleAttribute(attribute);
    }

    public Attribute[] getIndexAttributes(int indexReference)
    {
        return (Attribute[]) indices[indexReference - 1].getExtractors();
    }

    public IndexReference getBestIndexReference(List attributes)
    {
        return this.getBestIndex(attributes);
    }

    public boolean mapsToUniqueIndex(List attributes)
    {
        for (int i = 0; i < indices.length; i++)
        {
            Index candidate = this.indices[i];
            Extractor[] candidateAttributes = candidate.getExtractors();
            if (isSubset(attributes, candidateAttributes))
            {
                if (candidate.isUnique())
                {
                    return true;
                }
            }
        }
        return false;
    }

    protected IndexReference getBestIndex(List attributes)
    {
        int bestReference = this.getBestIndexReferenceBasedOnAttributes(attributes);
        if (bestReference >= 0 && bestReference < this.indices.length)
        {
            return this.getInitializedIndexReference(bestReference, this.primaryKeyIndex);
        }
        return this.noIndexReference;
    }

    @Override
    protected boolean isSubset(List all, Extractor[] mustBeSubset)
    {
        if (mustBeSubset.length <= all.size())
        {
            boolean allGood = true;
            for (int i = 0; i < mustBeSubset.length && allGood; i++)
            {
                boolean found = false;
                for (int j = 0; j < all.size() && !found; j++)
                {
                    if (mustBeSubset[i].equals(all.get(j)))
                    {
                        found = true;
                    }
                }
                allGood = found;
            }
            return allGood;
        }
        return false;
    }

    public boolean isUnique(int indexReference)
    {
        return indices[indexReference - 1].isUnique();
    }

    @Override
    public boolean isInitialized(int indexReference)
    {
        return indices[indexReference - 1].isInitialized();
    }

    @Override
    public int estimateQuerySize()
    {
        int size = this.size();
        if (size < 10)
        {
            size = 10;
        }
        return size;
    }

    public boolean isUniqueAndImmutable(int indexReference)
    {
        return indices[indexReference - 1].isUnique() && isIndexImmutable[indexReference - 1];
    }

    public void setMithraObjectPortal(MithraObjectPortal portal)
    {
        this.portal = portal;
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return portal;
    }

    public void reindex(MithraObject object, MithraDataObject newData, Object optionalBehavior, MithraDataObject optionalOldData)
    {
        if (this.monitoredAttributesList.size() > 0)
        {
            Object current = object;
            if (optionalOldData != null)
            {
                current = optionalOldData;
            }
            UnifiedSet affectedIndicies = null;
            for (int i=0;i<monitoredAttributesList.size();i++)
            {
                Attribute attribute = (Attribute) monitoredAttributesList.get(i);
                if (!attribute.valueEquals(current, newData))
                {
                    if (affectedIndicies == null) affectedIndicies = new UnifiedSet();
                    affectedIndicies.addAll((List) this.attributeToIndexMap.get(attribute));
                }
            }
            if (affectedIndicies != null && affectedIndicies.size() > 0)
            {
                try
                {
                    this.readWriteLock.acquireWriteLock();
                    reindexAffectedIndicesAndSetData(object, newData, affectedIndicies, optionalBehavior);
                }
                finally
                {
                    this.readWriteLock.release();
                }
                return;
            }
        }
        object.zSetData(newData, optionalBehavior);
    }

    protected void reindexAffectedIndicesAndSetData(MithraObject object, MithraDataObject newData,
            UnifiedSet affectedIndicies, Object optionalBehavior)
    {
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            Index index = (Index) it.next();
            index.remove(object);
        }
        object.zSetData(newData, optionalBehavior);
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            Index index = (Index) it.next();
            index.put(object);
        }
    }

    public void reindex(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        throw new RuntimeException("should not get here");
    }

    public void remove(MithraObject object)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            for (int i = 0; i < this.indices.length; i++)
            {
                indices[i].remove(object);
            }
        }
        finally
        {
            this.readWriteLock.release();
        }

    }

    public Object put(MithraObject object)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            return this.addToIndicies(object, false);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void reindexForTransaction(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        throw new RuntimeException("not implemented. expected to be overridden by transactional cache.");
    }

    public void removeIgnoringTransaction(MithraObject object)
    {
        this.remove(object);
    }

    public Object preparePut(MithraObject obj)
    {
        throw new RuntimeException("not implemented. expected to be overridden by transactional cache.");
    }

    public void commitPreparedForIndex(Object index)
    {
        throw new RuntimeException("not implemented. expected to be overridden by transactional cache.");
    }

    public void commitRemovedObject(MithraDataObject data)
    {
        throw new RuntimeException("not implemented. expected to be overridden by transactional cache.");
    }

    public void commitObject(MithraTransactionalObject mithraObject, MithraDataObject oldData)
    {
        throw new RuntimeException("not implemented. expected to be overridden by transactional cache.");
    }

    public Object getObjectByPrimaryKey(MithraDataObject data, boolean evenIfDirty)
    {
        throw new RuntimeException("not implemented. expected to be overridden by transactional cache.");
    }

    public boolean enrollDatedObject(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean enrollDatedObjectForDelete(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        throw new RuntimeException("not implemented");
    }

    public TemporalContainer getOrCreateContainer(MithraDataObject mithraDataObject)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject getTransactionalDataFromData(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean removeDatedData(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public List getDatedDataIgnoringDates(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public void putDatedData(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public PrimaryKeyIndex getPrimayKeyIndexCopy()
    {
        try
        {
            this.readWriteLock.acquireReadLock();
            return this.primaryKeyIndex.copy();
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void updateCache(List newDataList, List updatedDataList, List deletedData)
    {
        long start = 0;
        if (logger.isDebugEnabled())
        {
            start = System.currentTimeMillis();
        }
        Boolean lock = null;
        try
        {
            this.readWriteLock.acquireWriteLock();
            lock = Boolean.TRUE;
            FastList checkToReindexList = new FastList(updatedDataList.size());
            for (int i = 0; i < deletedData.size(); i++)
            {
                MithraObject object = (MithraObject) primaryKeyIndex.removeUsingUnderlying(deletedData.get(i));
                if (object != null)
                {
                    for (int j = 1; j < this.indices.length; j++)
                    {
                        Index index = this.indices[j];
                        index.remove(object);
                    }
                    this.markObjectAsDeleted(object);
                }
            }
            for (int i = 0; i < updatedDataList.size(); i++)
            {
                Object data = updatedDataList.get(i);
                MithraObject updatedObject = (MithraObject) primaryKeyIndex.getFromData(data);
                checkToReindexList.add(updatedObject);
            }
            ensureCapacity(primaryKeyIndex.size() + newDataList.size());
            updateCacheForNewObjects(newDataList, updatedDataList, checkToReindexList);
            this.readWriteLock.release();
            lock = null;
            for (int i = 0; i < checkToReindexList.size(); i++)
            {
                MithraObject obj = (MithraObject) checkToReindexList.get(i);
                obj.zReindexAndSetDataIfChanged((MithraDataObject) updatedDataList.get(i), this);
            }
        }
        finally
        {
            if (lock != null)
            {
                this.readWriteLock.release();
            }
        }
        if (logger.isDebugEnabled())
        {
            logger.debug("Cache update took "+(System.currentTimeMillis()-start)+" ms");
        }
    }

    protected void ensureCapacity(int capacity)
    {
        primaryKeyIndex.ensureExtraCapacity(capacity);
        for (int i = 1; i < this.indices.length; i++)
        {
            if (indices[i] instanceof PrimaryKeyIndex)
            {
                ((PrimaryKeyIndex)indices[i]).ensureExtraCapacity(capacity);
            }
        }
    }

    protected void updateCacheForNewObjects(List newDataList, List updatedDataList, FastList checkToReindexList)
    {
        if (newDataList.size() > 0)
        {
            if (newDataList.get(0) instanceof MithraObject)
            {
                updateCacheForNewObjectAsMithraObject(newDataList, updatedDataList, checkToReindexList);
            }
            else
            {
                updateCacheForNewObjectAsDataObject(newDataList, updatedDataList, checkToReindexList);
            }
        }
    }

    private void updateCacheForNewObjectAsMithraObject(List newDataList, List updatedDataList, FastList checkToReindexList)
    {
        NonNullMutableBoolean isDirty = new NonNullMutableBoolean();
        for (int i = 0; i < newDataList.size(); i++)
        {
            MithraObject newObject = (MithraObject) newDataList.get(i);
            MithraObject oldObject = (MithraObject) this.primaryKeyIndex.getFromDataEvenIfDirty(newObject, isDirty);
            if (oldObject == null)
            {
                addToIndicesUnderLockAfterCreate(newObject, false);
            }
            else
            {
                if (isDirty.value)
                {
                    addToIndicesUnderLockAfterCreate(oldObject, false);
                }
                updatedDataList.add(newObject.zGetCurrentData());
                checkToReindexList.add(oldObject);
            }
        }
    }

    private void updateCacheForNewObjectAsDataObject(List newDataList, List updatedDataList, FastList checkToReindexList)
    {
        NonNullMutableBoolean isDirty = new NonNullMutableBoolean();
        for (int i = 0; i < newDataList.size(); i++)
        {
            MithraDataObject dataObject = (MithraDataObject) newDataList.get(i);
            MithraObject oldObject = (MithraObject) this.primaryKeyIndex.getFromDataEvenIfDirty(dataObject, isDirty);
            if (oldObject == null)
            {
                addToIndicesUnderLockAfterCreate(this.getFactory().createObject(dataObject), false);
            }
            else
            {
                if (isDirty.value)
                {
                    addToIndicesUnderLockAfterCreate(oldObject, false);
                }
                updatedDataList.add(dataObject);
                checkToReindexList.add(oldObject);
            }
        }
    }

    public void rollbackObject(MithraObject mithraObject)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject refreshOutsideTransaction(MithraDatedObject mithraObject, MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public int size()
    {
        PrimaryKeyIndex pkIndex = this.getPrimaryKeyIndex();
        if (pkIndex.sizeRequiresWriteLock())
        {
            try
            {
                this.readWriteLock.acquireWriteLock();
                return pkIndex.size();
            }
            finally
            {
                this.readWriteLock.release();
            }
        }
        else
        {
            try
            {
                this.readWriteLock.acquireReadLock();
                return pkIndex.size();
            }
            finally
            {
                this.readWriteLock.release();
            }
        }
    }

    protected void markObjectAsDeleted(MithraObject object)
    {
        object.zSetNonTxPersistenceState(PersistedState.DELETED);
    }

    public void removeAll(List objects)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            for (int j = 0; j < this.indices.length; j++)
            {
                Index index = this.indices[j];
                for (int i = 0; i < objects.size(); i++)
                {
                    index.remove(objects.get(i));
                }
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void removeAll(Filter filter)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            List toRemove = this.primaryKeyIndex.removeAll(filter);
            for (int j = 1; j < this.indices.length; j++)
            {
                Index index = this.indices[j];
                for (int i = 0; i < toRemove.size(); i++)
                {
                    index.remove(toRemove.get(i));
                }
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void removeUsingData(MithraDataObject object)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            for (int i = 0; i < this.indices.length; i++)
            {
                indices[i].removeUsingUnderlying(object);
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public boolean markDirty(MithraDataObject object)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            MithraObject dirty;
            if (this.isFullCache())
            {
                dirty = (MithraObject) this.primaryKeyIndex.removeUsingUnderlying(object); // only get here onDelete
            }
            else
            {
                dirty = (MithraObject) this.primaryKeyIndex.markDirty(object);
            }
            if (dirty != null)
            {
                dirty.zMarkDirty();
                for (int i = 1; i < this.indices.length; i++)
                {
                    indices[i].removeUsingUnderlying(object);
                }
            }
            return dirty != null;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public void markNonExistent(int indexReference, Collection<Object> parentObjects, List<Extractor> extractors, List<Extractor> extraExtractors, Operation extraOperation)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            for(Iterator it = parentObjects.iterator(); it.hasNext(); )
            {
                Object parent = it.next();
                MithraObject object = (MithraObject) indices[indexReference - 1].get(parent, extractors);
                if (object != null && matchesExtraExtractorsAndOp(object, parent, extraExtractors, extraOperation))
                {
                    MithraObject dirty = null;
                    if (this.isFullCache())
                    {
                        dirty = (MithraObject) this.primaryKeyIndex.remove(object);
                    }
                    else if (object.zGetCurrentData() != null)
                    {
                        dirty = (MithraObject) this.primaryKeyIndex.markDirty(object.zGetCurrentData());
                    }
                    if (dirty != null)
                    {
                        dirty.zMarkDirty();
                        for (int i = 1; i < this.indices.length; i++)
                        {
                            indices[i].remove(object);
                        }
                    }
                }
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void markDirtyForReload(MithraDataObject object, MithraTransaction tx)
    {
        InternalList dirtyDataList = (InternalList) this.markedDirtyForReload.get(tx);
        if (dirtyDataList == null)
        {
            dirtyDataList = new InternalList();
            this.markedDirtyForReload.set(tx, dirtyDataList);
        }
        dirtyDataList.add(object);
    }

    public void reloadDirty(MithraTransaction tx)
    {
        InternalList dirtyDataList = (InternalList) this.markedDirtyForReload.get(tx);
        if (dirtyDataList != null)
        {
            if (this.isPartialCache())
            {
                for (int i = 0; i < dirtyDataList.size(); i++)
                {
                    this.markDirty((MithraDataObject) dirtyDataList.get(i));
                }
            }
            else
            {
//                this.portal.getMithraObjectPersister().refresh(, false)
                throw new RuntimeException("optimistic lock failure not supported with full cache");
            }
            dirtyDataList.clear();
        }
    }

    public void archiveCache(final ObjectOutput out)
    {
        try
        {
            this.readWriteLock.acquireReadLock();

            try
            {
                out.writeInt(this.primaryKeyIndex.size());
            }
            catch (IOException e)
            {
                throw new RuntimeException("serialization failed", e);
            }
            this.primaryKeyIndex.forAll(new DoUntilProcedure()
            {
                public boolean execute(Object object)
                {
                    MithraObject obj = (MithraObject) object;
                    try
                    {
                        obj.zSerializeFullData(out);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("serialization failed", e);
                    }
                    return false;
                }
            });
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    private void archiveList(List<MithraObject> list, ObjectOutput out)
    {
        try
        {
            out.writeInt(list.size());
            for (MithraObject each : list)
            {
                each.zSerializeFullData(out);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("serialization failed", e);
        }

    }

    public static int zLIST_CHUNK_SIZE = 1000;

    public void archiveCacheWithFilter(final ObjectOutput out, final Filter filterOfDatesToKeep)
    {
        try
        {
            this.readWriteLock.acquireReadLock();
            final List<MithraObject> list = FastList.newList(zLIST_CHUNK_SIZE);
            this.primaryKeyIndex.forAll(new DoUntilProcedure()
            {
                public boolean execute(Object object)
                {
                    if (filterOfDatesToKeep.matches(object))
                    {
                        return false;
                    }
                    list.add((MithraObject) object);
                    if (list.size() > zLIST_CHUNK_SIZE)
                    {
                        archiveList(list, out);
                        list.clear();
                    }
                    return false;
                }
            });

            archiveList(list, out);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public Object getObjectFromData(MithraDataObject data)
    {
        return getObjectFromData(data, false);
    }

    protected Object getObjectFromData(MithraDataObject data, boolean weak)
    {
        if (this.isFullCache()) weak = false;
        Boolean lock = null;
        MithraObject result = null;
        NonNullMutableBoolean isDirty = new NonNullMutableBoolean();
        try
        {
            this.readWriteLock.acquireReadLock();
            lock = Boolean.TRUE;
            result = getFromDataByPrimaryKeyForCreation(data, isDirty);
            boolean checkToReindex = true;
            if (result == null)
            {
                if (this.readWriteLock.upgradeToWriteLock())
                {
                    result = getFromDataByPrimaryKeyForCreation(data, isDirty); // not redundant, done under write lock
                }
                if (result != null && (isDirty.value || this.timeToLive > 0))
                {
                    addToIndicesUnderLockAfterCreate(result, weak);
                }
                if (result == null)
                {
                    result = factory.createObject(data);
                    addToIndicesUnderLockAfterCreate(result, weak);
                    checkToReindex = false;
                }
            }
            else if ((isDirty.value || this.timeToLive > 0) && result.zGetCurrentData() != null)
            {
                this.readWriteLock.upgradeToWriteLock();
                addToIndicesUnderLockAfterCreate(result, weak);
            }
            if (checkToReindex)
            {
                this.readWriteLock.release();
                lock = null;
                result.zReindexAndSetDataIfChanged(data, this);
            }
        }
        finally
        {
            if (lock != null)
            {
                this.readWriteLock.release();
            }
        }
        return result;
    }

    public void getManyObjectsFromData(Object[] dataArray, int length, boolean weak)
    {
        if (this.isFullCache()) weak = false;
        Boolean lock = null;
        boolean[] checkToReindex = new boolean[length];
        MithraDataObject[] originalData = null;
        NonNullMutableBoolean isDirty = new NonNullMutableBoolean();
        try
        {
            this.readWriteLock.acquireReadLock();
            lock = Boolean.TRUE; // true means read lock, false means write lock, null means no lock
            for(int i=0;i<length;i++)
            {
                MithraDataObject data = (MithraDataObject) dataArray[i];
                MithraObject result = getFromDataByPrimaryKeyForCreation(data, isDirty);
                checkToReindex[i] = true;
                if (result == null)
                {
                    if (lock == Boolean.TRUE && this.readWriteLock.upgradeToWriteLock())
                    {
                        lock = Boolean.FALSE;
                        result = getFromDataByPrimaryKeyForCreation(data, isDirty); // not redundant, done under write lock
                    }
                    if (result != null && (isDirty.value || this.timeToLive > 0))
                    {
                        if (lock == Boolean.TRUE)
                        {
                            this.readWriteLock.upgradeToWriteLock();
                            lock = Boolean.FALSE;
                        }
                        addToIndicesUnderLockAfterCreate(result, weak);
                    }

                    if (result == null)
                    {
                        result = factory.createObject(data);
                        addToIndicesUnderLockAfterCreate(result, weak);
                        checkToReindex[i] = false;
                    }
                }
                else if ((isDirty.value || this.timeToLive > 0) && result.zGetCurrentData() != null)
                {
                    if (lock == Boolean.TRUE)
                    {
                        this.readWriteLock.upgradeToWriteLock();
                        lock = Boolean.FALSE;
                    }
                    addToIndicesUnderLockAfterCreate(result, weak);
                }
                if (checkToReindex[i])
                {
                    if (originalData == null)
                    {
                        originalData = new MithraDataObject[length];
                    }
                    originalData[i] = (MithraDataObject) dataArray[i];
                }
                dataArray[i] = result;
            }
            this.readWriteLock.release();
            lock = null;
            for(int i=0;i<length;i++)
            {
                if (checkToReindex[i])
                {
                    ((MithraObject)dataArray[i]).zReindexAndSetDataIfChanged(originalData[i], this);
                }
            }
        }
        finally
        {
            if (lock != null)
            {
                this.readWriteLock.release();
            }
        }
    }

    public Object getObjectFromDataWithoutCaching(MithraDataObject data)
    {
        return getObjectFromData(data, true);
    }

    protected long getTimeToLive()
    {
        return timeToLive;
    }

    protected MithraObject getFromDataByPrimaryKeyForCreation(MithraDataObject data, NonNullMutableBoolean isDirty)
    {
        return (MithraObject) this.primaryKeyIndex.getFromDataEvenIfDirty(data, isDirty);
    }

    protected void addToIndicesUnderLockAfterCreate(MithraObject result, boolean weak)
    {
        this.addToIndicies(result, weak);
    }

    /* unsynchronized */
    protected Object addToIndicies(MithraObject result, boolean weak)
    {
        Object old = weak ? this.primaryKeyIndex.putWeak(result) : this.primaryKeyIndex.put(result);
        for (int i = 1; i < this.indices.length; i++)
        {
            indices[i].put(result);
        }
        return old;
    }

    public static Logger getLogger()
    {
        return logger;
    }

    public List get(int indexRef, Object dataHolder, Extractor[] extractors, boolean parallelAllowed)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(dataHolder, extractors));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List getMany(int indexRef, MithraTupleSet tupleSetDataHolders, final Extractor[] extractors, final boolean abortIfNotFound)
    {
        final Index index = indices[indexRef - 1];
        final FullUniqueIndex identitySet = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY, tupleSetDataHolders.size());
        this.readWriteLock.acquireReadLock();
        try
        {
            boolean notFound = tupleSetDataHolders.doUntil(new DoUntilProcedure()
            {
                public boolean execute(Object dataHolder)
                {
                    Object o = index.get(dataHolder, extractors);
                    if (abortIfNotFound && o == null) return true;
                    if (o != null) identitySet.put(o);
                    return false;
                }
            });
            if (notFound) return null;
            final MithraFastList arrayList = new MithraFastList(this.getAverageReturnSize(index, identitySet.size()));
            final DoUntilProcedure innerAddProcedure = new DoUntilProcedure()
            {
                public boolean execute(Object object)
                {
                    arrayList.add(object);
                    return false;
                }
            };

            DoUntilProcedure addProcedure = new DoUntilProcedure()
            {
                public boolean execute(Object object)
                {
                    if (object instanceof FullUniqueIndex)
                    {
                        ((FullUniqueIndex) object).forAll(innerAddProcedure);
                    }
                    else if (object instanceof List)
                    {
                        addAllListToList((List) object, arrayList);
                    }
                    else
                    {
                        arrayList.add(object);
                    }
                    return false;
                }
            };

            identitySet.forAll(addProcedure);

            return arrayList;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void getManyDatedObjectsFromData(Object[] dataArray, int length, ObjectWithMapperStack[] asOfOpWithStacks)
    {
        throw new RuntimeException("not implemented");
    }

    private void addAllListToList(List c, MithraFastList arrayList)
    {
        if (c.size() == 0) return;
        arrayList.addAll(c);
    }

    public Object getAsOne(Object dataHolder, List extractors)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return this.primaryKeyIndex.get(dataHolder, extractors);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public Object getAsOne(Object dataHolder, Extractor[] extractors)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return this.primaryKeyIndex.get(dataHolder, extractors);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public Object getAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return this.primaryKeyIndex.get(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public Object getAsOneByIndex(int indexRef, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        Index index = indices[indexRef - 1];
        this.readWriteLock.acquireReadLock();
        try
        {
            return index.get(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, Set indexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, indexValues.size()));
        Iterator it = indexValues.iterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get(it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, ByteArraySet indexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, indexValues.size()));
        Iterator it = indexValues.iterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get((byte[])it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, IntSet intSetIndexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, intSetIndexValues.size()));
        IntIterator it = intSetIndexValues.intIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get(it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, DoubleSet indexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, indexValues.size()));
        DoubleIterator it = indexValues.doubleIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get(it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, BooleanSet booleanSetIndexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, booleanSetIndexValues.size()));
        BooleanIterator it = booleanSetIndexValues.booleanIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get(it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, LongSet longSetIndexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, longSetIndexValues.size()));
        LongIterator it = longSetIndexValues.longIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get(it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, ByteSet byteSetIndexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, byteSetIndexValues.size()));
        ByteIterator it = byteSetIndexValues.byteIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get((int) it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, CharSet indexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, indexValues.size()));
        CharIterator it = indexValues.charIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get(it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, FloatSet floatSetIndexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, floatSetIndexValues.size()));
        FloatIterator it = floatSetIndexValues.floatIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get(it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public List get(int indexRef, ShortSet shortSetIndexValues)
    {
        Index index = indices[indexRef - 1];
        MithraFastList result = new MithraFastList(this.getAverageReturnSize(index, shortSetIndexValues.size()));
        ShortIterator it = shortSetIndexValues.shortIterator();
        this.readWriteLock.acquireReadLock();
        try
        {
            while (it.hasNext())
            {
                addAllListToList(wrapObjectInList(index.get((int) it.next())), result);
            }
            return result;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, Object indexValue)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(indexValue));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List getNulls(int indexRef)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].getNulls());
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, int indexValue)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(indexValue));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, char indexValue)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(indexValue));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, long indexValue)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(indexValue));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, double indexValue)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(indexValue));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, float indexValue)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(indexValue));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List get(int indexRef, boolean indexValue)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return wrapObjectInList(indices[indexRef - 1].get(indexValue));
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    private List wrapObjectInList(Object o)
    {
        if (o == null)
        {
            return ListFactory.EMPTY_LIST;
        }
        if (o instanceof List)
        {
            return new FastList((List)o); // must copy so that if the underlying list is changed the previous result stays the same
        }
        if (o instanceof FullUniqueIndex)
        {
            return ((FullUniqueIndex) o).getAll();
        }
        return ListFactory.create(o);
    }

    public Object getObjectFromData(MithraDataObject data, Timestamp[] asOfDates)
    {
        throw new RuntimeException("This cache does not support dated objects!");
    }

    public Object getObjectFromDataWithoutCaching(MithraDataObject data, Timestamp[] asOfDates)
    {
        throw new RuntimeException("This cache does not support dated objects!");
    }

    public void commit(MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    public void prepareForCommit(MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    public void rollback(MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    protected Index[] getIndices()
    {
        return indices;
    }

    protected PrimaryKeyIndex getPrimaryKeyIndex()
    {
        return primaryKeyIndex;
    }

    protected UnifiedMap getAttributeToIndexMap()
    {
        return attributeToIndexMap;
    }

    protected UnifiedSet getMonitoredAttributes()
    {
        return monitoredAttributes;
    }

    public void evictCollectedReferences()
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            if (this.primaryKeyIndex.needToEvictCollectedReferences())
            {
                this.readWriteLock.upgradeToWriteLock();

                if (this.primaryKeyIndex.evictCollectedReferences())
                {
                    for (int i = 1; i < indices.length; i++)
                    {
                        Index index = indices[i];
                        if (index != null)
                        {
                            index.evictCollectedReferences();
                        }
                    }
                }
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public void destroy()
    {
        for(Index index: indices)
        {
            index.destroy();
        }
    }

    @Override
    public boolean isDated()
    {
        return false;
    }
}
