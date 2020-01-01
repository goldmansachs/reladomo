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
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraDatedObjectFactory;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.cache.bean.TimestampArrayMutableBean;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDataStorage;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfOperation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.util.CooperativeCpuTaskFactory;
import com.gs.fw.common.mithra.util.CpuBoundTask;
import com.gs.fw.common.mithra.util.CpuTask;
import com.gs.fw.common.mithra.util.DoProcedure;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.FixedCountTaskFactory;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.ListBasedQueue;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MinExchange;
import com.gs.fw.common.mithra.util.MithraCompositeList;
import com.gs.fw.common.mithra.util.MithraCpuBoundThreadPool;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MithraTupleSet;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import com.gs.fw.common.mithra.util.ParallelIterator;
import com.gs.fw.common.mithra.util.ThreadChunkSize;
import com.gs.fw.common.mithra.util.TimestampPool;
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


public abstract class AbstractDatedCache extends AbstractCache implements ReferenceListener
{
    private static Logger logger = LoggerFactory.getLogger(AbstractDatedCache.class);

    private static final String NONUNIQUE_DATED_INDEX_NAME = " $3$ ";
    public static final byte REMOVED_VERSION = (byte) -50;

    private ConcurrentDatedObjectIndex uniqueConcurrentDatedObjectIndex;

    private int persistedState = DatedPersistenceState.PERSISTED;
    private final Attribute[] nonDatedPkAttributes;
    private final Attribute[] primaryKeyAttributes;
    private final Attribute[] immutableAttributes;
    private final AsOfAttribute[] asOfAttributes;
    private AsOfAttribute processingDate = null;
    private final HashStrategy nonDatedPkHashStrategy;
    private final MithraDatedObjectFactory factory;
    private final List indexToAttributesMap = new FastList();
    private final SemiUniqueDatedIndex semiUniqueDatedIndex;
    protected final IndexReference asOfProxyReference;

    private final ThreadLocal tempTimestamps = new ThreadLocal();
    private final DoProcedure removeMultipleProcedure;
    private MithraObjectPortal portal;
    private final long timeToLive;
    private final long relationshipTimeToLive;
    private final OffHeapDataStorage dataStorage;

    private final long currentDataOffset;

    private static final sun.misc.Unsafe UNSAFE = MithraUnsafe.getUnsafe();

    public AbstractDatedCache(Attribute[] nonDatedPkAttributes, AsOfAttribute[] asOfAttributes,
            MithraDatedObjectFactory factory, Attribute[] immutableAttributes, long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage)
    {
        super(2);
        currentDataOffset = MithraUnsafe.findCurrentDataOffset(asOfAttributes[0]);

        this.dataStorage = dataStorage;
        this.nonDatedPkAttributes = nonDatedPkAttributes;
        this.immutableAttributes = immutableAttributes;
        this.timeToLive = timeToLive;
        this.relationshipTimeToLive = relationshipTimeToLive;
        if (timeToLive > 0) CacheClock.register(timeToLive);
        if (relationshipTimeToLive > 0) CacheClock.register(relationshipTimeToLive);
        this.asOfAttributes = asOfAttributes;
        if (asOfAttributes.length > 1)
        {
            this.processingDate = asOfAttributes[1];
        }
        else if (asOfAttributes[0].isProcessingDate())
        {
            this.processingDate = asOfAttributes[0];
        }
        this.factory = factory;
        primaryKeyAttributes = new Attribute[nonDatedPkAttributes.length + asOfAttributes.length];
        System.arraycopy(nonDatedPkAttributes, 0, primaryKeyAttributes, 0, nonDatedPkAttributes.length);
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            primaryKeyAttributes[nonDatedPkAttributes.length + i] = asOfAttributes[i].getFromAttribute();
        }

        this.semiUniqueDatedIndex = this.createSemiUniqueDatedIndex(NONUNIQUE_DATED_INDEX_NAME,
                nonDatedPkAttributes, this.asOfAttributes, timeToLive, relationshipTimeToLive, this.dataStorage);
        this.nonDatedPkHashStrategy = this.semiUniqueDatedIndex.getNonDatedPkHashStrategy();
        this.indices[0] = new DatedDataIndex(this.semiUniqueDatedIndex);
        this.populateIndexAttributes(this.indices[0], 0);
        this.isIndexImmutable[0] = true;
        this.indexReferences[0] = new IndexReference(this, 1);

        this.indices[1] = new DatedSemiUniqueDataIndex(semiUniqueDatedIndex);
        this.populateIndexAttributes(this.indices[1], 1);
        this.isIndexImmutable[1] = true;
        this.indexReferences[1] = new IndexReference(this, 2);

        initConcurrentDatedObjectIndex();
        removeMultipleProcedure = new DoProcedure()
        {
            public void execute(Object object)
            {
                markOneDirty(object);
            }
        };
        MithraReferenceThread.getInstance().addListener(this);

        this.asOfProxyReference = new IndexReference(this, IndexReference.AS_OF_PROXY_INDEX_ID);
    }

    protected void setPersistedState(int persistedState)
    {
        this.persistedState = persistedState;
    }

    public long getRelationshipCacheTimeToLive()
    {
        return relationshipTimeToLive;
    }

    public long getCacheTimeToLive()
    {
        return timeToLive;
    }

    protected Attribute[] getNonDatedPkAttributes()
    {
        return nonDatedPkAttributes;
    }

    private void initConcurrentDatedObjectIndex()
    {
        this.uniqueConcurrentDatedObjectIndex = new ConcurrentDatedObjectIndex(this.semiUniqueDatedIndex.getNonDatedPkHashStrategy(), this.asOfAttributes, this.factory, 16);
    }

    protected SemiUniqueDatedIndex getSemiUniqueDatedIndex()
    {
        return semiUniqueDatedIndex;
    }

    public Attribute[] getPrimaryKeyAttributes()
    {
        return primaryKeyAttributes;
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return asOfAttributes;
    }

    protected Index[] getIndices()
    {
        return indices;
    }

    protected boolean isInactiveData(MithraDataObject data)
    {
        if (this.processingDate != null)
        {
            return (this.processingDate.getToAttribute().timestampValueOfAsLong(data) != this.processingDate.getInfinityDate().getTime());
        }
        return false;
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
                this.monitoredAttributes.add(attributes[i]);
                immutable = false;
            }
        }
        return immutable;
    }

    protected void populateIndexAttributes(Index index, int indexRef)
    {
        Extractor[] rawAttributes = (Extractor[]) index.getExtractors();
        Attribute[] attributes = new Attribute[rawAttributes.length + this.asOfAttributes.length];
        System.arraycopy(rawAttributes, 0, attributes, 0, rawAttributes.length);
        System.arraycopy(this.asOfAttributes, 0, attributes, rawAttributes.length, this.asOfAttributes.length);
        if (indexRef != this.indexToAttributesMap.size())
        {
            throw new RuntimeException("wacky index references");
        }
        this.indexToAttributesMap.add(attributes);
    }

    protected Timestamp[] getTempTimestamps()
    {
        Timestamp[] result = (Timestamp[]) this.tempTimestamps.get();
        if (result == null)
        {
            result = new Timestamp[this.asOfAttributes.length];
            this.tempTimestamps.set(result);
        }
        return result;
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

    protected abstract Index createIndex(String indexName, Extractor[] extractors, OffHeapDataStorage dataStorage);

    protected abstract SemiUniqueDatedIndex createSemiUniqueDatedIndex(String indexName, Extractor[] extractors, AsOfAttribute[] asOfAttributes, long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage);

    protected int addIndex(Index index, Extractor[] attributes)
    {
        int indexRef = this.addIndex(index);
        this.populateIndexAttributes(index, indexRef);
        isIndexImmutable[indexRef] = this.populateAttributeToIndexMap(attributes, index);
        return indexRef;
    }

    public int addIndex(String indexName, Extractor[] attributes)
    {
        int indexRef = -1;
        Index index = this.createIndex(indexName, attributes, dataStorage);
        if (index != null)
        {
            indexRef = this.addIndex(index);
            this.populateIndexAttributes(index, indexRef);
            this.populateAttributeToIndexMap(attributes, index);
        }
        return indexRef+1;
    }

    public int addTypedIndex(Extractor[] attributes, Class type, Class underlyingType)
    {
        int indexRef = -1;
        Index index = this.createIndex("", attributes, dataStorage);
        if (index != null)
        {
            TypedIndex typedIndex = new TypedIndex(index, type, underlyingType);
            indexRef = this.addIndex(typedIndex);
            this.populateIndexAttributes(typedIndex, indexRef);
            this.populateAttributeToIndexMap(attributes, typedIndex);
        }
        return indexRef+1;
    }

    public int addTypedUniqueIndex(Extractor[] attributes, Class type, Class underlyingType)
    {
        int indexRef = -1;
        SemiUniqueDatedIndex semiUniqueIndex = this.createSemiUniqueDatedIndex("", attributes, this.asOfAttributes, timeToLive, relationshipTimeToLive, dataStorage);
        if (semiUniqueIndex != null)
        {
            Index uniqueIndex = new DatedSemiUniqueDataIndex(semiUniqueIndex);
            TypedIndex typedIndex = new TypedIndex(uniqueIndex, type, underlyingType);
            indexRef = this.addIndex(typedIndex);
            this.populateIndexAttributes(typedIndex, indexRef);
            this.populateAttributeToIndexMap(attributes, typedIndex);
        }
        return indexRef+1;
    }

    public void clear()
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            this.semiUniqueDatedIndex.clear();
            for (int i = 2; i < this.indices.length; i++)
            {
                indices[i].clear();
            }
            if (dataStorage != null)
            {
                dataStorage.clear();
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public List getMany(int indexRef, MithraTupleSet dataHoldersTupleSet, Extractor[] extractors, boolean abortIfNotFound)
    {
        Timestamp[] asOfDates = new Timestamp[this.asOfAttributes.length];
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        this.readWriteLock.acquireReadLock();
        try
        {
            if (indexRef == this.asOfProxyReference.indexReference)
            {
                return getManyFromAsOfProxy(dataHoldersTupleSet, extractors, abortIfNotFound, asOfDates);
            }
            else
            {
                Index index = this.indices[indexRef - 1];
                boolean perData = isExtractorPerData(extractors, asOfDates);
                if (indexRef > 1 && index.isUnique())
                {
                    List results;
                    if (perData)
                    {
                        MithraFastList localResults = new MithraFastList(this.getAverageReturnSizeUnlocked((int) indexRef, (int) dataHoldersTupleSet.size()));
                        results = localResults;
                        if (getManyPerDataFromUnique(extractors, abortIfNotFound, asOfDates, tx, index, localResults, dataHoldersTupleSet.iterator()))
                        {
                            results = null;
                        }
                    }
                    else
                    {
                        DatedSemiUniqueDataIndex uniqueIndex = (DatedSemiUniqueDataIndex) index;
                        if (!abortIfNotFound && tx == null && dataHoldersTupleSet.size() > 1 && MithraCpuBoundThreadPool.isParallelizable(dataHoldersTupleSet.size()))
                        {
                            results = getManyFromUniqueParallel(extractors, asOfDates, dataHoldersTupleSet, uniqueIndex);
                        }
                        else
                        {
                            MithraFastList localResults = new MithraFastList(this.getAverageReturnSizeUnlocked((int) indexRef, (int) dataHoldersTupleSet.size()));
                            results = localResults;
                            if (getManyFromUniqueSequential(extractors, abortIfNotFound, asOfDates, tx, localResults, dataHoldersTupleSet.iterator(), uniqueIndex))
                            {
                                results = null;
                            }
                        }
                    }
                    return results;
                }
                else
                {
                    return getManyFromNonUnique(extractors, asOfDates, (IterableNonUniqueIndex) index, dataHoldersTupleSet, perData, tx, abortIfNotFound);
                }
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    private List getManyFromAsOfProxy(MithraTupleSet dataHoldersTupleSet, Extractor[] extractors, boolean abortIfNotFound, Timestamp[] asOfDates)
    {
        if (dataHoldersTupleSet.size() == 1)
        {
            Object dataHolder = dataHoldersTupleSet.getFirstDataHolder();
            populateAsOfDates(extractors, dataHolder, asOfDates);
            MatchAllAsOfDatesProcedure procedure =
                    new MatchAllAsOfDatesProcedure(extractors, 0, this.semiUniqueDatedIndex.getSemiUniqueSize(), asOfDates, this.asOfAttributes);
            this.semiUniqueDatedIndex.forAll(procedure);
            if (abortIfNotFound && procedure.getResult().isEmpty()) return null;
            return convertToBusinessObjectAndWrapInList(procedure.getResult(), extractors, asOfDates, false, true);
        }
        FullUniqueIndex results = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY, dataHoldersTupleSet.size());
        Iterator it = dataHoldersTupleSet.iterator();
        while(it.hasNext())
        {
            Object dataHolder = it.next();
            populateAsOfDates(extractors, dataHolder, asOfDates);
            MatchAllAsOfDatesProcedure procedure =
                    new MatchAllAsOfDatesProcedure(extractors, 0, this.semiUniqueDatedIndex.getSemiUniqueSize(), asOfDates, this.asOfAttributes);
            this.semiUniqueDatedIndex.forAll(procedure);
            if (abortIfNotFound && procedure.getResult().isEmpty()) return null;
            results.addAll(convertToBusinessObjectAndWrapInList(procedure.getResult(), extractors, asOfDates, false, true));
        }
        return results.getAll();
    }

    private List getManyFromNonUniqueParallel(final Extractor[] extractors, final Timestamp[] asOfDates, final IterableNonUniqueIndex index,
            final MithraTupleSet dataHolders, final boolean perData, final MithraTransaction tx)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        final ParallelIterator parallelIterator = dataHolders.parallelIterator(index.getAverageReturnSize());
        int threads = parallelIterator.getThreads();

        CpuBoundTask[] tasks = new CpuBoundTask[threads];
        final MithraFastList[] results = new MithraFastList[threads + 1];
        final int expectedSize = this.getAverageReturnSize(index, dataHolders.size()) / (threads + 1);
        results[threads] = new MithraFastList(0);
        final MinExchange minExchange = new MinExchange(results[threads], expectedSize);

        for(int i=0;i<threads;i++)
        {
            final int count = i;
            tasks[i] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    Timestamp[] localAsOfDates = asOfDates;
                    if (count > 0)
                    {
                        localAsOfDates = new Timestamp[asOfDates.length];
                    }
                    MatchAllAsOfDatesProcedureForMany procedure = new MatchAllAsOfDatesProcedureForMany(extractors, index.getExtractors().length,
                            localAsOfDates, asOfAttributes, perData, tx, false);
                    MithraFastList resultList = new MithraFastList(expectedSize);
                    results[count] = resultList;
                    procedure.setResult(resultList);
                    Iterator iterator = parallelIterator.makeOrRefillIterator(null);
                    while(iterator != null)
                    {
                        getManyFromNonUniqueSequential(extractors, localAsOfDates, index, iterator, perData, procedure);
                        procedure.setResult((MithraFastList) minExchange.exchange(procedure.result));
                        iterator = parallelIterator.makeOrRefillIterator(iterator);
                    }
                }
            };
        }
        new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();
        return new MithraCompositeList(results);
    }

    private MithraFastList getManyFromNonUniqueSequential(Extractor[] extractors, Timestamp[] asOfDates, IterableNonUniqueIndex index, Iterator dataHolders,
            boolean perData, MithraTransaction tx, boolean abortIfNotFound, boolean isLocked, int size)
    {
        MatchAllAsOfDatesProcedureForMany procedure = new MatchAllAsOfDatesProcedureForMany(extractors, index.getExtractors().length,
                asOfDates, this.asOfAttributes, perData, tx, isLocked);
        MithraFastList results = new MithraFastList(this.getAverageReturnSize(index, size));
        procedure.setResult(results);
        return getManyFromNonUniqueSequential(extractors, asOfDates, index, dataHolders, abortIfNotFound, procedure);
    }

    private MithraFastList getManyFromNonUniqueSequential(Extractor[] extractors, Timestamp[] asOfDates, IterableNonUniqueIndex index,
            Iterator dataHolders, boolean abortIfNotFound, MatchAllAsOfDatesProcedureForMany procedure)
    {
        while(dataHolders.hasNext())
        {
            int foundSoFar = procedure.result.size();
            Object dataHolder = dataHolders.next();
            populateAsOfDates(extractors, dataHolder, asOfDates);
            index.findAndExecute(dataHolder, extractors, procedure);
            if (abortIfNotFound && procedure.result.size() <= foundSoFar)
            {
                return null;
            }
        }
        return procedure.result;
    }

    private List getManyFromNonUnique(Extractor[] extractors, Timestamp[] asOfDates, IterableNonUniqueIndex index, MithraTupleSet dataHolders, boolean perData, MithraTransaction tx, boolean abortIfNotFound)
    {
        if (!abortIfNotFound && tx == null && dataHolders.size() > 1 && MithraCpuBoundThreadPool.isParallelizable(this.getAverageReturnSize(index, dataHolders.size())))
        {
            return getManyFromNonUniqueParallel(extractors, asOfDates, index, dataHolders, perData, tx);
        }
        else
        {
            return getManyFromNonUniqueSequential(extractors, asOfDates, index, dataHolders.iterator(), perData, tx, abortIfNotFound, true, dataHolders.size());
        }
    }

    private List getManyFromUniqueParallel(final Extractor[] extractors, final Timestamp[] asOfDates,
            final MithraTupleSet dataHolders, final DatedSemiUniqueDataIndex uniqueIndex)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        final ParallelIterator parallelIterator = dataHolders.parallelIterator(2);
        final int threads = parallelIterator.getThreads();
        CpuBoundTask[] tasks = new CpuBoundTask[threads];
        final MithraFastList[] results = new MithraFastList[threads];
        final int expectedSize = dataHolders.size()/threads;
        for(int i=0;i<threads;i++)
        {
            final int count = i;
            tasks[i] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    results[count] = new MithraFastList(expectedSize);
                    Iterator iterator = parallelIterator.makeOrRefillIterator(null);
                    Timestamp[] localAsOfDates = asOfDates;
                    if (count > 0)
                    {
                        localAsOfDates = new Timestamp[asOfAttributes.length];
                    }
                    while(iterator != null)
                    {
                        getManyFromUniqueSequential(extractors, false, localAsOfDates, null, results[count], iterator, uniqueIndex);
                        iterator = parallelIterator.makeOrRefillIterator(iterator);
                    }
                }
            };
        }
        new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();
        return new MithraCompositeList(results);
    }

    private boolean getManyFromUniqueSequential(Extractor[] extractors, boolean abortIfNotFound, Timestamp[] asOfDates,
            MithraTransaction tx, FastList results, Iterator dataHolders, DatedSemiUniqueDataIndex uniqueIndex)
    {
        CommonExtractorBasedHashingStrategy hashStrategy = uniqueIndex.getNonDatedPkHashStrategy();
        while(dataHolders.hasNext())
        {
            Object dataHolder = dataHolders.next();
            populateAsOfDates(extractors, dataHolder, asOfDates);
            MithraDataObject data = (MithraDataObject) uniqueIndex.getSemiUniqueAsOneWithDates(dataHolder,
                    extractors, asOfDates, hashStrategy.computeHashCode(dataHolder, extractors));
            if (data != null)
            {
                MithraDatedObject businessObject = this.getBusinessObjectFromData(data, asOfDates, getNonDatedPkHashCode(data), false, tx, true);
                if (businessObject == null)
                {
                    return true;
                }
                results.add(businessObject);
            }
            else
            {
                if (abortIfNotFound) return true;
            }
        }
        return false;
    }

    private boolean getManyPerDataFromUnique(Extractor[] extractors, boolean abortIfNotFound, Timestamp[] asOfDates, MithraTransaction tx, Index index, FastList results, Iterator dataHolders)
    {
        while(dataHolders.hasNext())
        {
            Object dataHolder = dataHolders.next();
            populateAsOfDates(extractors, dataHolder, asOfDates);
            MithraDataObject data = (MithraDataObject) index.get(dataHolder, extractors);
            if (data == null)
            {
                if (abortIfNotFound) return true;
            }
            else
            {
                this.extractTimestampsFromData(data, extractors, asOfDates);
                if (this.matchesAsOfDates(data, asOfDates))
                {
                    MithraDatedObject businessObject = this.getBusinessObjectFromData(data, asOfDates, getNonDatedPkHashCode(data), false, tx, true);
                    if (businessObject == null)
                    {
                        return true;
                    }
                    results.add(businessObject);
                }
                else
                {
                    if (abortIfNotFound) return true;
                }
            }
        }
        return false;
    }

    private void populateAsOfDates(Extractor[] extractors, Object dataHolder, Timestamp[] asOfDates)
    {
        int offset = extractors.length - this.asOfAttributes.length;
        for (int i = 0; i < asOfDates.length; i++)
        {
            TimestampExtractor asOfExtractor = (TimestampExtractor) extractors[offset + i];
            asOfDates[i] = TimestampPool.getInstance().getOrAddToCache(asOfExtractor.timestampValueOf(dataHolder), this.isFullCache());
        }
    }

    private void populateAsOfDates(List extractors, Object dataHolder, Timestamp[] asOfDates)
    {
        int offset = extractors.size() - this.asOfAttributes.length;
        for (int i = 0; i < asOfDates.length; i++)
        {
            TimestampExtractor asOfExtractor = (TimestampExtractor) extractors.get(offset + i);
            asOfDates[i] = TimestampPool.getInstance().getOrAddToCache(asOfExtractor.timestampValueOf(dataHolder), this.isFullCache());
        }
    }

    protected UnifiedSet getMonitoredAttributes()
    {
        return monitoredAttributes;
    }

    protected UnifiedMap getAttributeToIndexMap()
    {
        return attributeToIndexMap;
    }

    public int addUniqueIndex(String indexName, Extractor[] attributes)
    {
        int indexRef = -1;
        SemiUniqueDatedIndex semiUniqueIndex = this.createSemiUniqueDatedIndex(indexName, attributes, this.asOfAttributes, timeToLive, relationshipTimeToLive, dataStorage);
        if (semiUniqueIndex != null)
        {
            Index uniqueIndex = new DatedSemiUniqueDataIndex(semiUniqueIndex);
            indexRef = this.addIndex(uniqueIndex);
            this.populateIndexAttributes(uniqueIndex, indexRef);
            this.populateAttributeToIndexMap(attributes, uniqueIndex);
        }
        return indexRef+1;
    }

    public List getAll()
    {
        throw new MithraBusinessException(this.getMithraObjectPortal().getBusinessClassName() + ": getAll() not supported (there are infinitely many as of dates, therefore, infinite data!)");
    }

    @Override
    public void forAll(DoUntilProcedure procedure)
    {
        throw new RuntimeException("not supported");
    }

    public IndexReference getIndexRef(Attribute attribute)
    {
        if (this.asOfAttributes.length == 1 && attribute.equals(this.asOfAttributes[0]))
        {
            return this.asOfProxyReference;
        }
        return this.noIndexReference;
    }

    public Attribute[] getIndexAttributes(int indexReference)
    {
        if (indexReference == this.asOfProxyReference.indexReference)
        {
            return this.asOfAttributes;
        }
        return (Attribute[]) this.indexToAttributesMap.get(indexReference - 1);
    }

    public IndexReference getBestIndexReference(List attributes)
    {
        int bestReference = -2;
        if (hasAllAsOfAttributes(attributes))
        {
            bestReference = this.getBestIndexReferenceBasedOnAttributes(attributes);
            if (bestReference < 0)
            {
                return this.asOfProxyReference;
            }
        }
        if (bestReference >= 0 && bestReference < this.indices.length)
        {
            return this.getInitializedIndexReference(bestReference, this.semiUniqueDatedIndex);
        }
        return this.noIndexReference;
    }

    public boolean mapsToUniqueIndex(List attributes)
    {
        if (hasAllAsOfAttributes(attributes))
        {
            for (int i = 0; i < this.indices.length; i++)
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
        }
        return false;
    }

    private boolean hasAllAsOfAttributes(List attributes)
    {
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            boolean found = false;
            for (int j = 0; j < attributes.size() && !found; j++)
            {
                if (asOfAttributes[i].equals(attributes.get(j)))
                {
                    found = true;
                }
            }
            if (!found)
            {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean isSubset(List all, Extractor[] mustBeSubset)
    {
        if (mustBeSubset.length <= all.size() - this.asOfAttributes.length)
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
        if (indexReference == this.asOfProxyReference.indexReference)
        {
            return false;
        }
        return indices[indexReference - 1].isUnique();
    }

    @Override
    public boolean isInitialized(int indexReference)
    {
        if (indexReference == this.asOfProxyReference.indexReference)
        {
            return true;
        }
        return indices[indexReference - 1].isInitialized();
    }

    public boolean isUniqueAndImmutable(int indexReference)
    {
        if (indexReference == this.asOfProxyReference.indexReference)
        {
            return false;
        }
        return indices[indexReference - 1].isUnique() && isIndexImmutable[indexReference - 1].booleanValue();
    }

    public int getAverageReturnSize(int indexRef, int multiplier)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return this.getAverageReturnSizeUnlocked(indexRef, multiplier);
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
            int semiUniqueSize = this.semiUniqueDatedIndex.getSemiUniqueSize();
            if (indexRef == this.asOfProxyReference.indexReference)
            {
                return semiUniqueSize;
            }
            if (this.isPartialCache())
            {
                semiUniqueSize = 10 * multiplier;
            }
            long cacheSize = this.size();
            if (this.isPartialCache())
            {
                cacheSize = 10 * multiplier;
            }
            long indexReturnSize = this.indices[indexRef - 1].getMaxReturnSize(multiplier);
            long expectedSize = (long) (indexReturnSize * estimateOneDayCollapseFactor(semiUniqueSize, cacheSize, indexReturnSize == multiplier));
            return (int) Math.min(expectedSize, semiUniqueSize);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    private double estimateOneDayCollapseFactor(double semiUniqueSize, long cacheSize, boolean isUnique)
    {
        if (cacheSize == 0) return 0;
        if (isUnique) return 1;
        return semiUniqueSize / cacheSize;
    }

    private int getAverageReturnSizeUnlocked(int indexRef, int multiplier)
    {
        if (indexRef == this.asOfProxyReference.indexReference)
        {
            return this.semiUniqueDatedIndex.size();
        }
        return this.getAverageReturnSize(this.indices[indexRef-1], multiplier);
    }

    private int getAverageReturnSize(Index index, int multiplier)
    {
        int semiUniqueSize = this.semiUniqueDatedIndex.getSemiUniqueSize();
        if (this.isPartialCache())
        {
            semiUniqueSize = 10 * multiplier;
        }
        long cacheSize = this.size();
        if (this.isPartialCache())
        {
            cacheSize = 10 * multiplier;
        }
        int indexReturnSize = index.getAverageReturnSize();
        long expectedSize = (long)(((long) indexReturnSize) * multiplier * estimateOneDayCollapseFactor(semiUniqueSize, cacheSize, indexReturnSize == 1));
        return (int) Math.min(expectedSize, semiUniqueSize);
    }

    public void setMithraObjectPortal(MithraObjectPortal portal)
    {
        this.portal = portal;
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return portal;
    }

    public void remove(MithraObject object)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public Object put(MithraObject object)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void reindexForTransaction(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        throw new RuntimeException("not implemented. Not expected to be called.");
    }

    public void reindex(MithraObject object, MithraDataObject newData, Object optionalBehavior, MithraDataObject optionalOldData)
    {
        throw new RuntimeException("should never be called");
    }

    public void reindex(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        throw new RuntimeException("not implemented");
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

    public void removeIgnoringTransaction(MithraObject object)
    {
        this.remove(object);
    }

    public Object preparePut(MithraObject obj)
    {
        throw new RuntimeException("not implemented");
    }

    public void commitPreparedForIndex(Object index)
    {
        throw new RuntimeException("not implemented");
    }

    public void commitRemovedObject(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public void commitObject(MithraTransactionalObject mithraObject, MithraDataObject oldData)
    {
        throw new RuntimeException("not implemented");
    }

    public Object getObjectByPrimaryKey(MithraDataObject data, boolean evenIfDirty)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean enrollDatedObject(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        throw new RuntimeException("not implemented. Transactional subclass will override.");
    }

    public boolean enrollDatedObjectForDelete(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        throw new RuntimeException("not implemented. Transactional subclass will override.");
    }

    public TemporalContainer getOrCreateContainer(MithraDataObject mithraDataObject)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject getTransactionalDataFromData(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public void removeAll(List objects)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            for(int j=0;j<objects.size();j++)
            {
                MithraDatedTransactionalObject object = (MithraDatedTransactionalObject) objects.get(j);
                MithraDataObject dataObject = object.zGetTxDataForRead();

                this.uniqueConcurrentDatedObjectIndex.remove(object);
                dataObject = (MithraDataObject) this.semiUniqueDatedIndex.remove(dataObject);
                if (dataObject != null)
                {
                    for(int i=2;i<indices.length;i++)
                    {
                        indices[i].remove(dataObject);
                    }
                    dataObject.zSetDataVersion(REMOVED_VERSION);
                    releaseCacheData(dataObject);
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
            List toRemove = this.semiUniqueDatedIndex.removeAll(filter);
            for (int i = 2; i < indices.length; i++)
            {
                for(int r=0;r<toRemove.size();r++)
                {
                    indices[i].remove(toRemove.get(r));
                }
            }
            for(int r=0;r<toRemove.size();r++)
            {
                MithraDataObject dataObject = (MithraDataObject) toRemove.get(r);
                dataObject.zSetDataVersion(REMOVED_VERSION);
                releaseCacheData(dataObject);
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void removeUsingData(MithraDataObject object)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean markDirty(MithraDataObject object)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            return this.semiUniqueDatedIndex.removeAllIgnoringDate(object, removeMultipleProcedure);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public void markNonExistent(int indexReference, Collection<Object> parentObjects, List<Extractor> extractors, List<Extractor> extraExtractors, Operation extraOperation)
    {
        MithraFastList<MithraDataObject> toRefresh = null;
        if (this.isFullCache())
        {
            toRefresh = new MithraFastList();
        }
        try
        {
            this.readWriteLock.acquireWriteLock();
            Index index = indices[indexReference - 1];
            for(Iterator it = parentObjects.iterator(); it.hasNext(); )
            {
                Object parent = it.next();
                MithraDataObject data = (MithraDataObject) index.get(parent, extractors);
                if (data != null && matchesExtraExtractorsAndOp(data, parent, extraExtractors, extraOperation))
                {
                    this.semiUniqueDatedIndex.remove(data);
                    for(int i = 2;i<indices.length;i++)
                    {
                        indices[i].remove(data);
                    }
                    data.zSetDataVersion(REMOVED_VERSION);
                    if (toRefresh != null)
                    {
                        toRefresh.add(data);
                    }
                }
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
        if (toRefresh != null && toRefresh.size() > 0)
        {
            CacheRefresher cacheRefresher = new CacheRefresher(this.getMithraObjectPortal());
            cacheRefresher.refreshObjectsFromServer(toRefresh);
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
                out.writeInt(this.semiUniqueDatedIndex.size());
            }
            catch (IOException e)
            {
                throw new RuntimeException("serialization failed", e);
            }
            this.semiUniqueDatedIndex.forAll(new DoUntilProcedure()
            {
                public boolean execute(Object object)
                {
                    MithraDataObject obj = (MithraDataObject) object;
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

    private void archiveList(List<MithraDataObject> list, ObjectOutput out)
    {
        try
        {
            out.writeInt(list.size());
            for (MithraDataObject each : list)
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
            final List<MithraDataObject> list = FastList.newList(zLIST_CHUNK_SIZE);
            this.semiUniqueDatedIndex.forAll(new DoUntilProcedure()
            {
                public boolean execute(Object object)
                {
                    if (filterOfDatesToKeep.matches(object))
                    {
                        return false;
                    }
                    list.add((MithraDataObject) object);
                    if (list.size() >= zLIST_CHUNK_SIZE)
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

    protected void markOneDirty(Object existing)
    {
        MithraDataObject data = (MithraDataObject) existing;
        data.zSetDataVersion(REMOVED_VERSION);
        for (int i = 2; i < this.indices.length; i++)
        {
            indices[i].removeUsingUnderlying(data);
        }
        releaseCacheData(data);
    }

    public Object getObjectFromData(MithraDataObject data)
    {
        throw new RuntimeException("this cache only handles non-dated objects");
    }

    public void getManyObjectsFromData(Object[] dataArray, int length, boolean weak)
    {
        throw new RuntimeException("not implemented");
    }

    public Object getObjectFromDataWithoutCaching(MithraDataObject data)
    {
        throw new RuntimeException("this cache only handles non-dated objects");
    }

    public Object getObjectFromData(MithraDataObject data, Timestamp[] asOfDates)
    {
        return getObjectFromData(data, asOfDates, false);
    }

    protected Object getObjectFromData(MithraDataObject data, Timestamp[] asOfDates, boolean weak)
    {
        int nonPkHashCode = getNonDatedPkHashCode(data);
        MithraDataObject oldData;
        this.readWriteLock.acquireReadLock();
        try
        {
            oldData = (MithraDataObject) this.semiUniqueDatedIndex.getFromData(data, nonPkHashCode);
            boolean isNew = false;
            if (oldData == null)
            {
                if (this.getCacheLock().upgradeToWriteLock())
                {
                    oldData = (MithraDataObject) this.getSemiUniqueDatedIndex().getFromData(data, nonPkHashCode); // this is not redundant; it's now performed under a write lock
                }
                if (oldData == null)
                {
                    oldData = this.addToIndicies(data, nonPkHashCode);
                    isNew = true;
                }
            }
            if (!isNew)
            {
                boolean changed = oldData.changed(data);
                if (changed && this.readWriteLock.upgradeToWriteLock())
                {
                    // do it again under a write lock
                    changed = oldData.changed(data);
                }
                if (changed)
                {
                    this.reindexThenCopyOver(oldData, data);
                }
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
        return getBusinessObjectFromData(oldData, asOfDates, nonPkHashCode, weak, false);
    }

    public void getManyDatedObjectsFromData(Object[] dataArray, int length, ObjectWithMapperStack[] asOfOpWithStacks)
    {
        Timestamp[] asOfDates = this.getTempTimestamps();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        try
        {
            this.readWriteLock.acquireReadLock();
            for(int i=0;i<length;i++)
            {
                MithraDataObject data = (MithraDataObject) dataArray[i];
                for(int a=0;a<asOfOpWithStacks.length;a++)
                {
                    AsOfOperation asOfOperation = (AsOfOperation) asOfOpWithStacks[a].getObject();
                    asOfDates[a] = asOfOperation.inflateAsOfDate(data);
                }
                int nonPkHashCode = getNonDatedPkHashCode(data);
                MithraDataObject oldData = (MithraDataObject) this.semiUniqueDatedIndex.getFromData(data, nonPkHashCode);
                boolean isNew = false;
                if (oldData == null)
                {
                    if (this.readWriteLock.upgradeToWriteLock())
                    {
                        oldData = (MithraDataObject) this.getSemiUniqueDatedIndex().getFromData(data, nonPkHashCode); // this is not redundant; it's now performed under a write lock
                    }
                    if (oldData == null)
                    {
                        oldData = this.addToIndicies(data, nonPkHashCode);
                        isNew = true;
                    }
                }
                if (!isNew)
                {
                    boolean changed = oldData.changed(data);
                    if (changed && this.readWriteLock.upgradeToWriteLock())
                    {
                        // do it again under a write lock
                        changed = oldData.changed(data);
                    }
                    if (changed)
                    {
                        reindexThenCopyOver(oldData, data);
                    }
                }
                dataArray[i] = getBusinessObjectFromData(oldData, asOfDates, nonPkHashCode, false, tx, true);
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    protected void reindexThenCopyOver(MithraDataObject oldData, MithraDataObject data)
    {
        UnifiedSet affectedIndicies = null;
        if (this.monitoredAttributes.size() > 0)
        {
            for (Iterator it = this.monitoredAttributes.iterator(); it.hasNext();)
            {
                Attribute attribute = (Attribute) it.next();
                if (!attribute.valueEquals(oldData, data))
                {
                    if (affectedIndicies == null) affectedIndicies = new UnifiedSet();
                    affectedIndicies.addAll((List) this.attributeToIndexMap.get(attribute));
                }
            }
        }
        if (affectedIndicies != null)
        {
            reindexAffectedIndices(data, oldData, affectedIndicies);
        }
        else
        {
            oldData.copyNonPkAttributes(data);
        }
    }

    protected void reindexAffectedIndices(MithraDataObject data, MithraDataObject oldData, UnifiedSet affectedIndicies)
    {
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            Index index = (Index) it.next();
            index.remove(oldData);
        }
        oldData.copyNonPkAttributes(data);
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            Index index = (Index) it.next();
            index.put(oldData);
        }
    }

    public Object getObjectFromDataWithoutCaching(MithraDataObject data, Timestamp[] asOfDates)
    {
        return getObjectFromData(data, asOfDates, true);
    }

    protected MithraDatedObject getBusinessObjectFromData(MithraDataObject oldData,
            Timestamp[] asOfDates, int nonPkHashCode, boolean weak, MithraTransaction tx, boolean isLocked)
    {
        MithraDatedObject businessObject = (MithraDatedObject) this.uniqueConcurrentDatedObjectIndex.getFromDataOrPutIfAbsent(oldData, asOfDates, nonPkHashCode, weak);
        if (getCurrentData(businessObject) != oldData)
        {
            resetBusinessObjectData(oldData, businessObject);
        }
        return businessObject;
    }

    private MithraDataObject getCurrentData(MithraDatedObject businessObject)
    {
        return (MithraDataObject) UNSAFE.getObject(businessObject, currentDataOffset);
//        return businessObject.zGetCurrentData(); the above is the equivalent of this. it's faster because it's not (mega) polymorphic.
    }

    protected MithraDatedObject getBusinessObjectFromData(MithraDataObject oldData,
            Timestamp[] asOfDates, int nonPkHashCode, boolean weak, boolean isLocked)
    {
        MithraDatedObject businessObject = (MithraDatedObject) this.uniqueConcurrentDatedObjectIndex.getFromDataOrPutIfAbsent(oldData, asOfDates, nonPkHashCode, weak);
        if (getCurrentData(businessObject) != oldData)
        {
            resetBusinessObjectData(oldData, businessObject);
        }
        return businessObject;
    }

    private void resetBusinessObjectData(MithraDataObject oldData, MithraDatedObject businessObject)
    {
        businessObject.zSetCurrentData(oldData);
        businessObject.zSetNonTxPersistenceState(persistedState);
    }

    /* unsynchronized */
    protected MithraDataObject addToIndicies(MithraDataObject result, int nonDatedPkHashCode)
    {
        MithraDataObject removed = (MithraDataObject) this.semiUniqueDatedIndex.remove(result);
        MithraDataObject toPut;
        if (removed != null && removed != result)
        {
            for (int i = 2; i < indices.length; i++)
            {
                indices[i].remove(removed);
            }
            removed.copyNonPkAttributes(result);
            removed.zIncrementDataVersion();
            toPut = removed;
            result.zSetDataVersion(REMOVED_VERSION);
        }
        else
        {
            toPut = copyDataForCacheIfNotInTransaction(result);
        }
        this.semiUniqueDatedIndex.put(toPut, nonDatedPkHashCode);
        for (int i = 2; i < this.indices.length; i++)
        {
            indices[i].put(toPut);
        }
        return toPut;
    }

    protected void zSyncDataRemove(Object data)
    {
        this.semiUniqueDatedIndex.remove(data);
        for (int i = 2; i < indices.length; i++)
        {
            indices[i].remove(data);
        }
    }

    protected void zSyncDataAdd(Object data)
    {
        int nonPkHashCode = getNonDatedPkHashCode((MithraDataObject) data);
        this.semiUniqueDatedIndex.put(data, nonPkHashCode);
        for (int i = 2; i < this.indices.length; i++)
        {
            indices[i].put(data);
        }
    }

    protected MithraDataObject copyDataForCacheIgnoringTransaction(MithraDataObject dataObject)
    {
        return dataObject;
    }

    protected List copyDataForCacheIgnoringTransaction(List dataObjects)
    {
        return dataObjects;
    }

    protected MithraDataObject copyDataForCacheIfNotInTransaction(MithraDataObject result)
    {
         return result;
    }

    protected MithraDatedObjectFactory getFactory()
    {
        return factory;
    }

    protected ConcurrentDatedObjectIndex getUniqueConcurrentDatedObjectIndex()
    {
        return uniqueConcurrentDatedObjectIndex;
    }

    public static Logger getLogger()
    {
        return logger;
    }

    public List get(int indexRef, Object dataHolder, Extractor[] extractors, boolean parallelAllowed)
    {
        TimestampArrayMutableBean timestampBean = (TimestampArrayMutableBean) TimestampArrayMutableBean.POOLS[this.asOfAttributes.length - 1].getOrConstruct();
        try
        {
            Timestamp[] asOfDates = timestampBean.getArray();
            populateAsOfDates(extractors, dataHolder, asOfDates);
            if (indexRef == this.asOfProxyReference.indexReference)
            {
                return getFromProxyIndex(extractors, parallelAllowed, asOfDates);
            }
            this.readWriteLock.acquireReadLock();
            Object dataObjects;
            try
            {
                Index index = this.indices[indexRef - 1];
                dataObjects = index.get(dataHolder, extractors);
                if (dataObjects != null && !index.isUnique())
                {
                    int startIndex = index.getExtractors().length;
                    if (dataObjects instanceof List)
                    {
                        dataObjects = matchAsOfDatesForList(asOfDates, (List) dataObjects, extractors, startIndex);
                    }
                    else if (dataObjects instanceof FullUniqueIndex)
                    {
                        dataObjects = matchAsOfDatesForUniqueIndex(asOfDates, (FullUniqueIndex) dataObjects, extractors, startIndex, parallelAllowed);
                    }
                    else
                    {
                        if (!matchesAsOfAttributes(asOfDates, dataObjects, extractors, startIndex))
                        {
                            return ListFactory.EMPTY_LIST;
                        }
                    }
                }
            }
            finally
            {
                this.readWriteLock.release();
            }
            return convertToBusinessObjectAndWrapInList(dataObjects, extractors, asOfDates, false, false);
        }
        finally
        {
            timestampBean.release();
        }
    }

    private boolean matchesAsOfAttributes(Timestamp[] asOfDates, Object o, Extractor[] extractors, int startIndex)
    {
        for(int i=0;i<asOfAttributes.length;i++)
        {
            AsOfExtractor extractor = (AsOfExtractor) extractors[i+startIndex];
            if (!extractor.dataMatches(o, asOfDates[i], asOfAttributes[i]))
            {
                return false;
            }
        }
        return true;
    }

    private List matchAsOfDatesForUniqueIndex(Timestamp[] asOfDates, FullUniqueIndex fui, Extractor[] subExtractors, int extractorStartIndex, boolean parallelAllowed)
    {
        int size = fui.size();
        if (parallelAllowed && MithraCpuBoundThreadPool.isParallelizable(size))
        {
            ParallelMatchAllAsOfDatesProcedure procedure = new ParallelMatchAllAsOfDatesProcedure(subExtractors, extractorStartIndex, asOfDates, this.asOfAttributes);
            fui.forAllInParallel(procedure);
            return new MithraCompositeList(procedure.getResult());
        }
        else
        {
            MatchAllAsOfDatesProcedure procedure = new MatchAllAsOfDatesProcedure(subExtractors, extractorStartIndex, size, asOfDates, this.asOfAttributes);
            fui.forAll(procedure);
            return procedure.getResult();
        }
    }

    private List matchAsOfDatesForList(Timestamp[] asOfDates, List list, Extractor[] extractors, int startIndex)
    {
        // this is not parallelized because if an index keeps a list, it's guaranteed to be tiny
        FastList result = FastList.newList(list.size());
        for (int i = 0; i < list.size(); i++)
        {
            Object o = list.get(i);
            if (matchesAsOfAttributes(asOfDates, o, extractors, startIndex))
            {
                result.add(o);
            }
        }
        return result;
    }

    private List getFromProxyIndex(Extractor[] extractors, boolean parallelAllowed, Timestamp[] asOfDates)
    {
        this.readWriteLock.acquireReadLock();
        MatchAllAsOfDatesProcedure procedure;
        try
        {
            int size = this.semiUniqueDatedIndex.getSemiUniqueSize();
            if (parallelAllowed && MithraCpuBoundThreadPool.isParallelizable(size))
            {
                return getFromProxyIndexInParallel(extractors, asOfDates);
            }
            else
            {
                procedure =
                        new MatchAllAsOfDatesProcedure(extractors, 0, this.semiUniqueDatedIndex.getSemiUniqueSize(), asOfDates, this.asOfAttributes);
                this.semiUniqueDatedIndex.forAll(procedure);
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
        return convertToBusinessObjectAndWrapInList(procedure.getResult(), extractors, asOfDates, false, false);
    }

    private List getFromProxyIndexInParallel(final Extractor[] extractors, final Timestamp[] asOfDates)
    {
        ParallelMatchAndConvertAllAsOfDatesProcedure procedure =
                new ParallelMatchAndConvertAllAsOfDatesProcedure(extractors, asOfDates, this.asOfAttributes);
        this.semiUniqueDatedIndex.forAllInParallel(procedure);
        return new MithraCompositeList(procedure.getResult());
    }

    private boolean isExtractorPerData(List extractors, Timestamp[] asOfDates)
    {
        boolean localPerData = false;
        for (int i = 0; i < asOfDates.length; i++)
        {
            AsOfExtractor asOfExtractor = (AsOfExtractor) extractors.get(extractors.size() - this.asOfAttributes.length + i);
            if (asOfExtractor.matchesMoreThanOne())
            {
                localPerData = true;
            }
        }
        return localPerData;
    }

    private boolean isExtractorPerData(Extractor[] extractors, Timestamp[] asOfDates)
    {
        boolean localPerData = false;
        for (int i = 0; i < asOfDates.length; i++)
        {
            AsOfExtractor asOfExtractor = (AsOfExtractor) extractors[extractors.length - this.asOfAttributes.length + i];
            if (asOfExtractor.matchesMoreThanOne())
            {
                localPerData = true;
            }
        }
        return localPerData;
    }

    public Object getAsOne(Object dataHolder, List extractors)
    {
        throw new RuntimeException("not implemented");
    }

    public Object getAsOne(Object dataHolder, Extractor[] extractors)
    {
        throw new RuntimeException("not implemented");
    }

    public Object getAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int nonDatedHash = isOffHeap() ? relationshipHashStrategy.computeOffHeapHashCodeFromRelated(srcObject, srcData) : relationshipHashStrategy.computeHashCodeFromRelated(srcObject, srcData);
        this.readWriteLock.acquireReadLock();
        Object result;
        try
        {
            result = this.semiUniqueDatedIndex.getSemiUniqueAsOne(srcObject, srcData, relationshipHashStrategy, nonDatedHash, asOfDate0, asOfDate1);
            if (result instanceof List)
            {
                throw new MithraBusinessException("findOne returned more than one result!");
            }
            if (result == null) return null;
        }
        finally
        {
            this.readWriteLock.release();
        }
        Timestamp[] asOfDates = this.getTempTimestamps();
        asOfDates[0] = asOfDate0;
        if (this.asOfAttributes.length > 1)
        {
            asOfDates[1] = asOfDate1;
        }
        return getBusinessObjectFromData((MithraDataObject) result, asOfDates, nonDatedHash, false, false);
    }

    public Object getAsOneByIndex(int indexRef, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int nonDatedHash = isOffHeap() ? relationshipHashStrategy.computeOffHeapHashCodeFromRelated(srcObject, srcData) : relationshipHashStrategy.computeHashCodeFromRelated(srcObject, srcData);
        DatedSemiUniqueDataIndex index = (DatedSemiUniqueDataIndex) indices[indexRef - 1];
        this.readWriteLock.acquireReadLock();
        Object result;
        try
        {
            result = index.getSemiUniqueAsOne(srcObject, srcData, relationshipHashStrategy, nonDatedHash, asOfDate0, asOfDate1);
            if (result instanceof List)
            {
                throw new MithraBusinessException("findOne returned more than one result!");
            }
            if (result == null) return null;
        }
        finally
        {
            this.readWriteLock.release();
        }
        Timestamp[] asOfDates = this.getTempTimestamps();
        asOfDates[0] = asOfDate0;
        if (this.asOfAttributes.length > 1)
        {
            asOfDates[1] = asOfDate1;
        }
        return getBusinessObjectFromData((MithraDataObject) result, asOfDates, this.nonDatedPkHashStrategy.computeHashCode(result), false, false);
    }


    protected void extractTimestampsFromData(MithraDataObject data, List extractors, Timestamp[] asOfDates)
    {
        for (int i = 0; i < asOfDates.length; i++)
        {
            AsOfExtractor asOfExtractor = (AsOfExtractor) extractors.get(extractors.size() - this.asOfAttributes.length + i);
            if (asOfExtractor.matchesMoreThanOne())
            {
                asOfDates[i] = asOfExtractor.getDataSpecificValue(data);
            }
        }
    }

    protected void extractTimestampsFromData(MithraDataObject data, Extractor[] extractors, Timestamp[] asOfDates)
    {
        for (int i = 0; i < asOfDates.length; i++)
        {
            AsOfExtractor asOfExtractor = (AsOfExtractor) extractors[extractors.length - this.asOfAttributes.length + i];
            if (asOfExtractor.matchesMoreThanOne())
            {
                asOfDates[i] = asOfExtractor.getDataSpecificValue(data);
            }
        }
    }

    // if o is a list, we do the conversion in place
    protected List convertToBusinessObjectAndWrapInList(Object o, Extractor[] extractors, Timestamp[] asOfDates, boolean weak, boolean isLocked)
    {
        if (!(o instanceof List))
        {
            if (o == null)
            {
                return ListFactory.EMPTY_LIST;
            }
            MithraDataObject data = (MithraDataObject) o;
            this.extractTimestampsFromData(data, extractors, asOfDates);
            return ListFactory.create(this.getBusinessObjectFromData(data, asOfDates, getNonDatedPkHashCode(data), weak, isLocked));
        }
        else
        {
            List result = (List) o;
            boolean perData = isExtractorPerData(extractors, asOfDates);
            if (!perData && MithraCpuBoundThreadPool.isParallelizable(result.size()))
            {
                convertToBusinessObjectsInParallel(result, asOfDates, weak);
            }
            else
            {
                for (int i = 0; i < result.size(); i++)
                {
                    MithraDataObject data = (MithraDataObject) result.get(i);
                    if (perData) this.extractTimestampsFromData(data, extractors, asOfDates);
                    result.set(i, this.getBusinessObjectFromData(data, asOfDates, getNonDatedPkHashCode(data), weak, isLocked));
                }
            }
            return result;
        }
    }

    private void convertToBusinessObjectsInParallel(final List dataHolders, final Timestamp[] asOfDates, final boolean weak)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), dataHolders.size(), 1);
        final ListBasedQueue listBasedQueue = ListBasedQueue.createQueue(dataHolders, threadChunkSize.getChunkSize());
        CooperativeCpuTaskFactory taskFactory = new CooperativeCpuTaskFactory(pool,threadChunkSize.getThreads())
        {
            @Override
            protected CpuTask createCpuTask()
            {
                return new CpuTask()
                {
                    @Override
                    protected void execute()
                    {
                        List subList = listBasedQueue.borrow(null);
                        while(subList != null)
                        {
                            convertToBusinessObjectsSequential(subList, 0, subList.size(), asOfDates, weak, false);
                            subList = listBasedQueue.borrow(subList);
                        }
                    }
                };
            }
        };
        taskFactory.startAndWorkUntilFinished();
    }

    private void convertToBusinessObjectsSequential(List list, int start, int end, Timestamp[] asOfDates, boolean weak, boolean isLocked)
    {
        for (int i = start; i < end; i++)
        {
            MithraDataObject data = (MithraDataObject) list.get(i);
            list.set(i, this.getBusinessObjectFromData(data, asOfDates, getNonDatedPkHashCode(data), weak, isLocked));
        }
    }

    protected int getNonDatedPkHashCode(MithraDataObject data)
    {
        return this.nonDatedPkHashStrategy.computeHashCode(data);
    }

    protected boolean matchesAsOfDates(MithraDataObject data, Timestamp[] asOfDates)
    {
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            if (!asOfAttributes[i].dataMatches(data, asOfDates[i]))
            {
                return false;
            }
        }
        return true;
    }

    public List getNulls(int indexRef)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, Set indexValues)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, ByteArraySet indexValues)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, Object indexValue)
    {
        if (indexRef == this.asOfProxyReference.indexReference)
        {
            if (this.asOfAttributes.length > 1)
            {
                throw new MithraBusinessException("must specify both AsOfAttributes");
            }
            Timestamp[] asOfDates = new Timestamp[1];
            asOfDates[0] = (Timestamp) indexValue;
            MatchSingleAsOfDateProcedure procedure =
                    new MatchSingleAsOfDateProcedure(this.semiUniqueDatedIndex.getSemiUniqueSize(), asOfDates[0], this.asOfAttributes[0]);
            this.readWriteLock.acquireReadLock();
            try
            {
                this.semiUniqueDatedIndex.forAll(procedure);
            }
            finally
            {
                this.readWriteLock.release();
            }
            return convertToBusinessObjectAndWrapInList(procedure.getResult(), new Extractor[] {this.asOfAttributes[0]}, asOfDates, false, false);
        }
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, IntSet intSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, DoubleSet doubleSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, BooleanSet booleanSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, LongSet longSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, ByteSet byteSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, CharSet charSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, FloatSet floatSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    @Override
    public List get(int indexRef, ShortSet shortSetIndexValues)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, int indexValue)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, char indexValue)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, long indexValue)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, double indexValue)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, float indexValue)
    {
        throw new RuntimeException("not supported");
    }

    public List get(int indexRef, boolean indexValue)
    {
        throw new RuntimeException("not supported");
    }

    public boolean removeDatedData(MithraDataObject data)
    {
        boolean removedSomething = false;
        try
        {
            this.readWriteLock.acquireWriteLock();
            MithraDataObject removed = (MithraDataObject) this.semiUniqueDatedIndex.remove(data);
            if (removed != null)
            {
                data = removed;
                removedSomething = true;
            }
            for (int i = 2; i < indices.length; i++)
            {
                if (indices[i].remove(data) != null)
                {
                    removedSomething = true;
                }
            }
            releaseCacheData(removed);
        }
        finally
        {
            this.readWriteLock.release();
        }
        return removedSomething;
    }

    protected void releaseCacheData(MithraDataObject removed)
    {
        // do nothing. subclass to override
    }

    public List getDatedDataIgnoringDates(MithraDataObject data)
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            return this.semiUniqueDatedIndex.getFromDataForAllDatesAsList(data);
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void putDatedData(MithraDataObject data)
    {
        try
        {
            this.readWriteLock.acquireWriteLock();
            this.semiUniqueDatedIndex.put(data, this.getNonDatedPkHashCode(data));
            for (int i = 2; i < indices.length; i++)
            {
                indices[i].put(data);
            }
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public PrimaryKeyIndex getPrimayKeyIndexCopy()
    {
        try
        {
            this.readWriteLock.acquireReadLock();
            return this.semiUniqueDatedIndex.copy();
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public void updateCache(List newDataList, List updatedDataList, List deletedData)
    {
        this.readWriteLock.acquireWriteLock();
        try
        {
            if (deletedData.size() > 0)
            {
                for (int i = 0; i < deletedData.size(); i++)
                {
                    MithraDataObject oldData = (MithraDataObject) semiUniqueDatedIndex.removeUsingUnderlying(deletedData.get(i));
                    if (oldData != null)
                    {
                        for (int j = 2; j < indices.length; j++)
                        {
                            Index index = this.indices[j];
                            index.removeUsingUnderlying(oldData);
                        }
                        oldData.zSetDataVersion(REMOVED_VERSION);
                        this.releaseCacheData(oldData);
                    }
                }
            }
            //todo: parallelize
            if (this.semiUniqueDatedIndex.size() == 0 && MithraCpuBoundThreadPool.isParallelizable(newDataList.size()))
            {
                addAllToIndicesInParallel(newDataList);
            }
            else
            {
                ensureExtraCapacity(newDataList.size());
                for (int i = 0; i < newDataList.size(); i++)
                {
                    MithraDataObject data = (MithraDataObject) newDataList.get(i);
                    int nonDatedPkHash = getNonDatedPkHashCode(data);
                    MithraDataObject oldData = (MithraDataObject) this.semiUniqueDatedIndex.getFromData(data, nonDatedPkHash);
                    if (oldData == null)
                    {
                        data = this.addToIndiciesIgnoringTransaction(data);
                    }
                    else
                    {
                        updatedDataList.add(data);
                    }
                }
            }
            for (int i = 0; i < updatedDataList.size(); i++)
            {
                MithraDataObject data = (MithraDataObject) updatedDataList.get(i);
                MithraDataObject oldData = (MithraDataObject) this.semiUniqueDatedIndex.getFromData(data, this.getNonDatedPkHashCode(data));
                reindexThenCopyOver(oldData, data);
            }
            reportSpaceUsage();
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    protected void reportSpaceUsage()
    {

    }

    private void addAllToIndicesInParallel(final List newDataList)
    {
        final List copiedDataList = copyDataForCacheIgnoringTransaction(newDataList);
        CpuBoundTask[] tasks = new CpuBoundTask[this.indices.length - 1];
        tasks[0] = new CpuBoundTask()
        {
            @Override
            public void execute()
            {
                semiUniqueDatedIndex.ensureExtraCapacity(copiedDataList.size());
                for (int i = 0; i < copiedDataList.size(); i++)
                {
                    MithraDataObject data = (MithraDataObject) copiedDataList.get(i);
                    int nonDatedPkHash = getNonDatedPkHashCode(data);
                    semiUniqueDatedIndex.put(data, nonDatedPkHash);
                }
            }
        };
        for(int i=2; i < this.indices.length; i++)
        {
            final int count = i;
            tasks[i - 1] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    Index index = indices[count];
                    for (int i = 0; i < copiedDataList.size(); i++)
                    {
                        index.put(copiedDataList.get(i));
                    }
                }
            };
        }
        new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();
    }

    protected void ensureExtraCapacity(int size)
    {
        semiUniqueDatedIndex.ensureExtraCapacity(size);
        for(int i=2;i<indices.length;i++)
        {
            indices[i].ensureExtraCapacity(size);
        }
    }

    public void rollbackObject(MithraObject mithraObject)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject refreshOutsideTransaction(MithraDatedObject mithraObject, MithraDataObject data)
    {
        Timestamp[] asOfDates = new Timestamp[this.asOfAttributes.length];
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            asOfDates[i] = asOfAttributes[i].timestampValueOf(mithraObject);
        }
        MithraDataObject result = null;
        this.readWriteLock.acquireReadLock();
        try
        {
            result = (MithraDataObject) this.semiUniqueDatedIndex.getSemiUniqueFromData(data, asOfDates);
        }
        finally
        {
            this.readWriteLock.release();
        }
        if (result == null)
        {
            result = mithraObject.zRefreshWithLock(false);
            if (result != null)
            {
                this.readWriteLock.acquireWriteLock();
                try
                {
                    result = updateExistingDataIfAny(result, asOfDates, mithraObject);
                }
                finally
                {
                    this.readWriteLock.release();
                }
            }
        }
        return result;
    }

    public int size()
    {
        return this.semiUniqueDatedIndex.size();
    }

    protected MithraDataObject updateExistingDataIfAny(MithraDataObject data,
                                                       Timestamp[] asOfDates, MithraDatedObject mithraObject)
    {
        int nonDatedHashCode = this.getNonDatedPkHashCode(data);
        MithraDataObject oldData = (MithraDataObject) this.semiUniqueDatedIndex.getFromData(data, nonDatedHashCode);
        if (oldData == null)
        {
            if (this.readWriteLock.upgradeToWriteLock())
            {
                oldData = (MithraDataObject) this.semiUniqueDatedIndex.getFromData(data, nonDatedHashCode);
            }
        }
        if (oldData == null)
        {
            if (asOfDates == null && mithraObject == null)
            {
                List removed = this.getSemiUniqueDatedIndex().removeOldEntryForRange(data);
                if (removed != null)
                {
                    for (int i = 0; i < removed.size(); i++)
                    {
                        MithraDataObject removedData = (MithraDataObject) removed.get(i);
                        this.removeDatedDataIgnoringTransactionExcludingSemiUniqueIndex(removedData);
                        removedData.zSetDataVersion(REMOVED_VERSION);
                        this.releaseCacheData(removedData);
                    }
                }
            }
            else
            {
                if (asOfDates == null)
                {
                    AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
                    asOfDates = new Timestamp[asOfAttributes.length];
                    for (int i = 0; i < asOfAttributes.length; i++)
                    {
                        asOfDates[i] = asOfAttributes[i].timestampValueOf(mithraObject);
                    }
                }
                MithraDataObject removed = (MithraDataObject) this.getSemiUniqueDatedIndex().removeOldEntry(data, asOfDates);
                if (removed != null)
                {
                    this.removeDatedDataIgnoringTransactionExcludingSemiUniqueIndex(removed);
                    removed.zSetDataVersion(REMOVED_VERSION);
                    this.releaseCacheData(removed);
                }
            }
            data = this.addToIndiciesIgnoringTransaction(data);
        }
        else
        {
            boolean changed = oldData.changed(data);
            if (changed && this.readWriteLock.upgradeToWriteLock())
            {
                // do it again under a write lock
                changed = oldData.changed(data);
            }
            if (changed)
            {
                reindexThenCopyOver(oldData, data);
            }
            data = oldData;
        }
        return data;
    }

    protected MithraDataObject addToIndiciesIgnoringTransaction(MithraDataObject data)
    {
        return this.addToIndicies(data, this.getNonDatedPkHashCode(data));
    }

    protected void removeDatedDataIgnoringTransactionExcludingSemiUniqueIndex(MithraDataObject result)
    {
        for (int i = 2; i < this.indices.length; i++)
        {
            Index index = this.indices[i];
            index.remove(result);
        }
    }

    public void evictCollectedReferences()
    {
        if (this.uniqueConcurrentDatedObjectIndex.evictCollectedReferences())
        {
            this.readWriteLock.acquireReadLock();
            try
            {
                boolean needToEvict = false;
                for (int i = 0; !needToEvict && i < indices.length; i++)
                {
                    Index index = indices[i];
                    if (index != null)
                    {
                        needToEvict |= index.needToEvictCollectedReferences();
                    }
                }

                if (needToEvict)
                {
                    this.readWriteLock.upgradeToWriteLock();
                    for (int i = 0; i < indices.length; i++)
                    {
                        Index index = indices[i];
                        if (index != null)
                        {
                            index.evictCollectedReferences();
                        }
                    }
                }
            }
            finally
            {
                this.readWriteLock.release();
            }
        }
    }

    @Override
    public List<Object> collectMilestoningOverlaps()
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            SemiUniqueDatedIndex semiUniqueDatedIndex = this.getSemiUniqueDatedIndex();
            return semiUniqueDatedIndex.collectMilestoningOverlaps();

        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    @Override
    public int estimateQuerySize()
    {
        this.readWriteLock.acquireReadLock();
        try
        {
            int size = this.semiUniqueDatedIndex.getSemiUniqueSize();
            if (size < 10)
            {
                size = 10;
            }
            return size;
        }
        finally
        {
            this.readWriteLock.release();
        }
    }

    public class MatchAllAsOfDatesProcedureForMany implements DoUntilProcedure
    {
        private Extractor[] extractors;
        private int extratorStartIndex;
        private MithraFastList result;
        private Timestamp[] asOfDates;
        private AsOfAttribute[] asOfAttributes;
        private boolean perData;
        private MithraTransaction tx;
        private boolean isLocked;

        public MatchAllAsOfDatesProcedureForMany(Extractor[] extractors, int extratorStartIndex, Timestamp[] asOfDates, AsOfAttribute[] asOfAttributes, boolean perData, MithraTransaction tx, boolean locked)
        {
            this.extractors = extractors;
            this.extratorStartIndex = extratorStartIndex;
            this.asOfDates = asOfDates;
            this.asOfAttributes = asOfAttributes;
            this.perData = perData;
            this.tx = tx;
            this.isLocked = locked;
        }

        public FastList getResult()
        {
            return result;
        }

        public void setResult(MithraFastList result)
        {
            this.result = result;
        }

        public boolean execute(Object o)
        {
            int end = extractors.length - extratorStartIndex;
            for(int i=0;i< end;i++)
            {
                AsOfExtractor extractor = (AsOfExtractor) extractors[i + extratorStartIndex];
                if (!extractor.dataMatches(o, asOfDates[i], asOfAttributes[i]))
                {
                    return false;
                }
            }
            MithraDataObject data = (MithraDataObject) o;
            if (perData) extractTimestampsFromData(data, extractors, asOfDates);
            MithraDatedObject businessObject = getBusinessObjectFromData(data, asOfDates, getNonDatedPkHashCode(data), false, tx, isLocked);
            if (businessObject != null)
            {
                result.add(businessObject);
            }
            return false;
        }
    }

    protected class ParallelMatchAndConvertAllAsOfDatesProcedure implements ParallelProcedure
    {
        private AsOfExtractor[] extractors;
        private AsOfExtractor[] perDataExtractors;
        private MithraFastList[] results;
        private Timestamp[] asOfDates;
        private AsOfAttribute[] asOfAttributes;
        private boolean perData;
        private Timestamp[][] localAsOfDates;

        protected ParallelMatchAndConvertAllAsOfDatesProcedure(Extractor[] extractors, Timestamp[] asOfDates, AsOfAttribute[] asOfAttributes)
        {
            this.extractors = new AsOfExtractor[extractors.length];
            for(int i=0;i<extractors.length;i++)
            {
                this.extractors[i] = (AsOfExtractor) extractors[i];
            }
            this.asOfDates = asOfDates;
            this.asOfAttributes = asOfAttributes;
            for(AsOfExtractor e: this.extractors)
            {
                if (e.matchesMoreThanOne())
                {
                    perData = true;
                    break;
                }
            }
            if (perData)
            {
                perDataExtractors = new AsOfExtractor[this.extractors.length];
                for(int i=0;i<this.extractors.length;i++)
                {
                    AsOfExtractor e = this.extractors[i];
                    if (e.matchesMoreThanOne())
                    {
                        perDataExtractors[i] = e;
                    }
                }
            }
        }

        public MithraFastList[] getResult()
        {
            return results;
        }

        public void execute(Object o, int thread)
        {
            for(int i=0;i<extractors.length;i++)
            {
                if (!extractors[i].dataMatches(o, asOfDates[i], asOfAttributes[i]))
                {
                    return;
                }
            }
            MithraDataObject data = (MithraDataObject) o;
            Timestamp[] realAsOfDates =  asOfDates;
            if (perData)
            {
                realAsOfDates = localAsOfDates[thread];
                extractTimestampsFromData(data, perDataExtractors, realAsOfDates);
            }
            results[thread].add(getBusinessObjectFromData(data, realAsOfDates, getNonDatedPkHashCode(data), false, false));
        }

        public void setThreads(int threads, int expectedCallsPerChunk)
        {
            results = new MithraFastList[threads];
            for(int i=0;i< threads;i++)
            {
                results[i] = new MithraFastList(expectedCallsPerChunk);
            }
            if (perData)
            {
                localAsOfDates = new Timestamp[threads][];
                for(int i=0;i< threads;i++)
                {
                    localAsOfDates[i] = new Timestamp[asOfDates.length];
                    System.arraycopy(asOfDates, 0, localAsOfDates[i], 0, asOfDates.length);
                }
            }
        }

        protected void extractTimestampsFromData(MithraDataObject data, AsOfExtractor[] extractors, Timestamp[] asOfDates)
        {
            for (int i = 0; i < extractors.length; i++)
            {
                if (extractors[i] != null)
                {
                    asOfDates[i] = extractors[i].getDataSpecificValue(data);
                }
            }
        }
    }

    @Override
    public void destroy()
    {
        semiUniqueDatedIndex.destroy();
        for(int i=2;i<indices.length;i++)
        {
            indices[i].destroy();
            indices[i] = null;
        }
        if (dataStorage != null)
        {
            dataStorage.destroy();
        }
        MithraReferenceThread.getInstance().removeListener(this);
    }

    @Override
    public boolean isDated()
    {
        return true;
    }

    // for test purposes only
    private OffHeapDataStorage zGetDataStorage()
    {
        return this.dataStorage;
    }
}
