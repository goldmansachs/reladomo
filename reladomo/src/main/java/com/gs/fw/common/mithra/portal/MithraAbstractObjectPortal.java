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

package com.gs.fw.common.mithra.portal;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.aggregate.attribute.BeanAggregateAttribute;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.MappedAttribute;
import com.gs.fw.common.mithra.attribute.VersionAttribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.CacheClock;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.offheap.MasterCacheUplink;
import com.gs.fw.common.mithra.cache.offheap.OffHeapSyncableCache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.DelegatingList;
import com.gs.fw.common.mithra.list.NullPersistedRelation;
import com.gs.fw.common.mithra.list.NulledRelation;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.CompactUpdateCountOperation;
import com.gs.fw.common.mithra.querycache.QueryCache;
import com.gs.fw.common.mithra.tempobject.MithraTuplePersister;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import com.gs.fw.common.mithra.util.*;
import com.gs.reladomo.metadata.PrivateReladomoClassMetaData;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectStreamException;
import java.lang.ref.WeakReference;
import java.sql.Timestamp;
import java.util.*;



public abstract class MithraAbstractObjectPortal implements MithraObjectPortal
{
    static private Logger logger = LoggerFactory.getLogger(MithraAbstractObjectPortal.class.getName());

    private transient QueryCache queryCache;
    private transient Cache cache;
    private transient MithraObjectDeserializer objectFactory;
    private transient MithraObjectReader mithraObjectReader;
    private transient MithraTuplePersister mithraTuplePersister;
    private transient UpdateCountHolder classUpdateCountHolder;
    private transient RelatedFinder finder;
    private transient boolean disableCache = false;
    private transient boolean isTransactional = false;
    // used during deserialization
    private transient String finderClassName;
    private transient boolean useMultiUpdate = true;
    private transient RelatedFinder[] parentFinders;
    private transient RelatedFinder[] superClassFinders;
    private transient RelatedFinder[] subClassFinders;
    private transient MithraObjectPortal[] superClassPortals;
    private transient MithraObjectPortal[] subClassPortals;
    private transient String uniqueAlias;
    private transient int hierarchyDepth;
    private transient static final Object NO_GROUP_BY_KEY = new Object();
    private transient MithraPerformanceData performanceData = new MithraPerformanceData(this);
    private transient boolean isIndependent;
    private transient boolean isPureHome;
    private transient boolean isForTempObject;
    private transient String pureNotificationId = null;
    private transient volatile NullPersistedRelation lastPersistedRelation;
    private transient PersisterId persisterId;
    private transient volatile long latestRefreshTime;
    private transient UpdateDataChooser updateDataChooser;
    private transient Attribute[] optimisticAddressingAttributes;
    private transient Attribute[] addressingAttributes;
    private transient final ReladomoClassMetaData metaData;

    private static final int MAX_POOL_ARRAY_SIZE = 8;
    private static final int MAX_POOLED = 6;
    private transient UpdateCountHolder[][][] updateCountHolderPool = new UpdateCountHolder[MAX_POOL_ARRAY_SIZE][][];
    private transient int[][][] updateCountOriginalValues = new int[MAX_POOL_ARRAY_SIZE][][];
    private transient int poolCount = 0;
    private static final Cursor EMPTY_CURSOR = new EmptyCursor();
    private static int TRANSITIVE_THRESHOLD = 10000;

    protected MithraAbstractObjectPortal(MithraObjectDeserializer objectFactory, Cache cache,
                                         RelatedFinder finder, int relationshipCacheSize, int minQueriesToKeep,
                                         int hierarchyDepth, UpdateCountHolder classUpdateCountHolder,
                                         MithraObjectReader mithraObjectReader, MithraTuplePersister mithraTuplePersister, boolean isTransactional)
    {
        this.objectFactory = objectFactory;
        this.cache = cache;
        this.finder = finder;
        this.hierarchyDepth = hierarchyDepth;
        this.mithraObjectReader = mithraObjectReader;
        this.mithraTuplePersister = mithraTuplePersister;
        this.isTransactional = isTransactional;
        this.queryCache = new QueryCache(relationshipCacheSize, minQueriesToKeep, cache.getCacheTimeToLive(), cache.getRelationshipCacheTimeToLive(), cache.isFullCache());
        this.classUpdateCountHolder = classUpdateCountHolder;
        this.cache.setMithraObjectPortal(this);
        for (int i = 0; i < MAX_POOL_ARRAY_SIZE; i++)
        {
            updateCountHolderPool[i] = new UpdateCountHolder[MAX_POOLED][];
            updateCountOriginalValues[i] = new int[MAX_POOLED][];
        }
        this.metaData = ReladomoClassMetaData.fromFinder(finder);
    }

    protected MithraAbstractObjectPortal(RelatedFinder finder)
    {
        this.finder = finder;
        this.metaData = new PrivateReladomoClassMetaData(finder);
    }

    public static void setTransitiveThreshold(int transitiveThreshold)
    {
        MithraAbstractObjectPortal.TRANSITIVE_THRESHOLD = transitiveThreshold;
    }

    @Override
    public boolean isTransactional()
    {
        return isTransactional;
    }

    public PersisterId getPersisterId()
    {
        return persisterId;
    }

    public MithraObjectPersister getMithraObjectPersister()
    {
        return (MithraObjectPersister) this.mithraObjectReader;
    }

    public MithraTuplePersister getMithraTuplePersister()
    {
        return mithraTuplePersister;
    }

    public void setPersisterId(PersisterId persisterId)
    {
        this.persisterId = persisterId;
    }

    public boolean isForTempObject()
    {
        return isForTempObject;
    }

    public void setForTempObject(boolean forTempObject)
    {
        isForTempObject = forTempObject;
    }

    public boolean isPureHome()
    {
        return isPureHome;
    }

    public void setPureHome(boolean pureHome, String pureNotificationId)
    {
        this.isPureHome = pureHome;
        if (isPureHome)
        {
            this.pureNotificationId = pureNotificationId + ":" + MithraProcessInfo.getHostAddress();
        }
        else
        {
            this.pureNotificationId = pureNotificationId;
        }
    }

    public String getPureNotificationId()
    {
        return this.pureNotificationId;
    }

    public MithraObjectPortal getInitializedPortal()
    {
        return this;
    }

    public boolean isCacheDisabled()
    {
        return this.disableCache;
    }

    public void setParentFinders(RelatedFinder[] parentFinders)
    {
        this.parentFinders = parentFinders;
    }

    public boolean isParentFinder(RelatedFinder possibleParent)
    {
        if (parentFinders != null)
        {
            for (RelatedFinder finder : parentFinders)
            {
                if (finder == possibleParent) return true;
            }
        }
        return false;
    }

    public Object unwrapRelatedObject(Object from, Object related, Extractor[] fromExtractors, Extractor[] relatedExtractors)
    {
        if (related instanceof RelationshipReference)
        {
            return checkRelatedValues(from, ((RelationshipReference) related).get(), fromExtractors, relatedExtractors);
        }
        return checkRelatedValues(from, related, fromExtractors, relatedExtractors);
    }

    public Object wrapRelatedObject(Object result)
    {
        if (result == null)
        {
            NullPersistedRelation relation = lastPersistedRelation;
            if (relation != null && relation.isValid()) return relation;
            NullPersistedRelation npr = new NullPersistedRelation(this);
            this.lastPersistedRelation = npr;
            return npr;
        }
        if (this.isPartiallyCached())
        {
            if (cache.getRelationshipCacheTimeToLive() != 0)
            {
                return new TimedRelationshipReference(result, cache.getRelationshipCacheTimeToLive());
            }
            return new RelationshipReference(result);
        }
        return result;
    }

    public void prepareForMassPurge(List mithraObjects)
    {
        throw new RuntimeException("Not Implemented!");
    }

    public Object unwrapToManyRelatedObject(Object related)
    {
        if (related instanceof RelationshipReference)
        {
            related = ((RelationshipReference) related).get();
        }
        if (related == null) return null;
        return ((DelegatingList) related).zCloneForRelationship();
    }

    public boolean mapsToUniqueIndex(List attributes)
    {
        return this.cache.mapsToUniqueIndex(attributes);
    }

    public String getTableNameForQuery(SqlQuery sqlQuery, MapperStackImpl mapperStack, int currentSourceNumber, PersisterId persisterId)
    {
        return this.getDatabaseObject().getTableNameForQuery(sqlQuery, mapperStack, currentSourceNumber);
    }

    public UpdateCountHolder[] getPooledUpdateCountHolders(UpdateCountHolder[] updateCountHolders)
    {
        if (updateCountHolders.length > MAX_POOL_ARRAY_SIZE)
        {
            return updateCountHolders;
        }
        UpdateCountHolder[][] holders = updateCountHolderPool[updateCountHolders.length - 1];
        for (int i = 0; i < MAX_POOLED; i++)
        {
            UpdateCountHolder[] holder = holders[i];
            if (holder == null)
            {
                holders[i] = updateCountHolders;
                return updateCountHolders;
            }
            if (Arrays.equals(holder, updateCountHolders)) return holder;
        }
        int pos = poolCount++;
        if (pos >= MAX_POOLED)
        {
            pos = 0;
            poolCount = 0;
        }
        holders[pos] = updateCountHolders;
        return updateCountHolders;
    }

    public int[] getPooledIntegerArray(int[] originalValues)
    {
        if (originalValues.length > MAX_POOL_ARRAY_SIZE)
        {
            return originalValues;
        }
        int[][] holders = updateCountOriginalValues[originalValues.length - 1];
        for (int i = 0; i < MAX_POOLED; i++)
        {
            int[] holder = holders[i];
            if (holder == null)
            {
                holders[i] = originalValues;
                return originalValues;
            }
            if (Arrays.equals(holder, originalValues)) return holder;
        }
        int pos = poolCount++;
        if (pos >= MAX_POOLED)
        {
            pos = 0;
            poolCount = 0;
        }
        holders[pos] = originalValues;
        return originalValues;
    }

    public String getBusinessClassName()
    {
        String finderClassName = this.getFinderClassName();
        return finderClassName.substring(finderClassName.lastIndexOf('.') + 1, finderClassName.length() - "Finder".length());
    }

    @Override
    public synchronized void destroy()
    {
        this.cache.destroy();
        this.queryCache.destroy();
        this.queryCache = null;
        this.cache = null;
        this.objectFactory = null;
        this.mithraObjectReader = null;
        this.mithraTuplePersister = null;
        this.classUpdateCountHolder = null;
//        this.finder = null;
//        this.parentFinders = null;
//        this.superClassFinders = null;
//        subClassFinders = null;
        superClassPortals = null;
        subClassPortals = null;
        performanceData = null;
        lastPersistedRelation = null;
        persisterId = null;
        updateCountHolderPool = null;
        updateCountOriginalValues = null;
    }

    private Object checkRelatedValues(Object from, Object related, Extractor[] fromExtractors, Extractor[] relatedExtractors)
    {
        if (related != null)
        {
            if (((MithraObject) related).isDeletedOrMarkForDeletion()) return null;
            if (fromExtractors != null)
            {
                for (int i = 0; i < fromExtractors.length; i++)
                {
                    if (!fromExtractors[i].valueEquals(from, related, relatedExtractors[i])) return null;
                }
            }
        }
        return related;
    }

    public boolean isPartiallyCached()
    {
        return this.cache == null || this.cache.isPartialCache();
    }

    public boolean isFullyCached()
    {
        return this.cache != null && this.cache.isFullCache();
    }

    @Override
    public boolean isReplicated()
    {
        return this.cache instanceof OffHeapSyncableCache && ((OffHeapSyncableCache) this.cache).isReplicated();
    }

    public QueryCache getQueryCache()
    {
        return queryCache;
    }

    public Cache getCache()
    {
        return cache;
    }

    public MithraDatabaseObject getDatabaseObject()
    {
        return (MithraDatabaseObject) objectFactory;
    }

    public RelatedFinder getFinder()
    {
        return finder;
    }

    public UpdateCountHolder getPerClassUpdateCountHolder()
    {
        MithraObjectPortal[] superClassPortals = this.getSuperClassPortals();
        if (superClassPortals != null)
        {
            return superClassPortals[0].getPerClassUpdateCountHolder();
        }
        return this.classUpdateCountHolder;
    }

    public void incrementClassUpdateCount()
    {
        this.getPerClassUpdateCountHolder().incrementUpdateCount();
        lastPersistedRelation = null;
    }

    public CachedQuery zFindInMemory(Operation op, OrderBy orderby)
    {
        QueryCache queryCache = this.getQueryCache();
        CachedQuery result = queryCache.findByEquality(op, false);
        result = cloneOrAnalyzeAndFindInMemory(op, orderby, false, queryCache, result, null);
        return result;
    }

    public void setDisableCache(boolean disableCache)
    {
        this.disableCache = disableCache;
    }

    public void reloadCache()
    {
        if (this.getCache().isFullCache())
        {
            queryCache.clearCache();
            MithraObjectPortal[] superClassPortals = this.getSuperClassPortals();
            if (superClassPortals != null)
            {
                superClassPortals[0].reloadCache();
            }
            else
            {
                reloadFullCacheFromServer();
            }
            queryCache.clearCache();
            getPerClassUpdateCountHolder().incrementUpdateCount();
        }
        else
        {
            this.clearQueryCache();
        }
    }

    @Override
    public RenewedCacheStats renewCacheForOperation(Operation op)
    {
        return getMithraObjectReader().renewCacheForOperation(op);
    }

    protected void reloadFullCacheFromServer()
    {
        getMithraObjectReader().reloadFullCache();
    }

    public void loadCache()
    {
        if (this.getCache().isFullCache())
        {
            queryCache.clearCache();
            MithraObjectPortal[] superClassPortals = this.getSuperClassPortals();
            if (superClassPortals != null)
            {
                superClassPortals[0].loadCache();
            }
            else
            {
                loadFullCacheFromServer();
            }
            queryCache.clearCache();
            getPerClassUpdateCountHolder().incrementUpdateCount();
        }
        else
        {
            this.clearQueryCache();
        }
    }

    public MithraObjectReader getMithraObjectReader()
    {
        return mithraObjectReader;
    }

    protected void loadFullCacheFromServer()
    {
        getMithraObjectReader().loadFullCache();
    }

    public List zFindInMemoryWithoutAnalysis(Operation op, boolean isQueryCachable)
    {
        if (this.isForTempObject) return null;
        List resultList;
        CachedQuery cachedQuery = null;
        if (isQueryCachable)
        {
            QueryCache queryCache = this.getQueryCache();
            cachedQuery = queryCache.findByEquality(op);
        }
        if (cachedQuery == null || cachedQuery.wasDefaulted())
        {
            resultList = this.resolveOperationOnCache(op);
        }
        else
        {
            resultList = cachedQuery.getResult();
        }
        return resultList;
    }

    public Cursor findCursorFromServer(Operation op, Filter postLoadFilter, OrderBy orderby, int maxObjectsToRetrieve,
                                       boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        if (op.zIsNone())
        {
            return EMPTY_CURSOR;
        }
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            return findCursorFromServerWithoutRetry(op, postLoadFilter, orderby, maxObjectsToRetrieve, bypassCache, maxParallelDegree, forceImplicitJoin);
        }
        else
        {
            return findCursorFromServerWithRetry(op, postLoadFilter, orderby, maxObjectsToRetrieve, bypassCache, maxParallelDegree, forceImplicitJoin);
        }
    }

    private Cursor findCursorFromServerWithRetry(Operation op, Filter postLoadFilter, OrderBy orderby, int maxObjectsToRetrieve, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        int retriesLeft = MithraTransaction.DEFAULT_TRANSACTION_RETRIES;
        while (true)
        {
            try
            {
                return findCursorFromServerWithoutRetry(op, postLoadFilter, orderby, maxObjectsToRetrieve, bypassCache, maxParallelDegree, forceImplicitJoin);
            }
            catch (MithraBusinessException e)
            {
                retriesLeft = e.ifRetriableWaitElseThrow("find failed with retriable error. retrying.", retriesLeft, logger);
            }
        }
    }

    private Cursor findCursorFromServerWithoutRetry(Operation op, Filter postLoadFilter, OrderBy orderby, int maxObjectsToRetrieve, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        boolean bypassLocalCache = !this.isPureHome() && (this.disableCache || bypassCache);
        this.flushTransaction(op, bypassLocalCache);
        Cursor cursor = this.getMithraObjectReader().findCursor(new AnalyzedOperation(op, orderby), postLoadFilter, orderby, maxObjectsToRetrieve, bypassCache, maxParallelDegree, forceImplicitJoin);
        MithraManagerProvider.getMithraManager().incrementDatabaseRetrieveCount();
        return cursor;
    }

    public List find(Operation op, boolean bypassCache, boolean forceImplicitJoin)
    {
        return this.findAsCachedQuery(op, null, bypassCache, false, 0, forceImplicitJoin).getResult();
    }

    public List find(Operation op, boolean bypassCache)
    {
        return this.findAsCachedQuery(op, null, bypassCache, false, 0, false).getResult();
    }

    public int count(Operation op)
    {
        this.flushTransaction(op, true);
        return this.getMithraObjectReader().count(op);
    }

    public List computeFunction(Operation op, OrderBy orderby, String sqlExpression, ResultSetParser resultSetParser)
    {
        this.flushTransaction(op, true);
        return this.getMithraObjectReader().computeFunction(op, orderby, sqlExpression, resultSetParser);
    }

    public Object getAsOneFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        return wrapResultForFullCache(this.getCache().getAsOne(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1));
    }

    public Object getAsOneByIndexFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1, int indexRef)
    {
        return wrapResultForFullCache(this.getCache().getAsOneByIndex(indexRef, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1));
    }

    @Override
    public Object getAsOneFromCacheForFind(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        if (this.isCacheDisabled())
        {
            return null;
        }
        return getAsOneFromCache(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
    }

    @Override
    public Object getAsOneByIndexFromCacheForFind(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1, int indexRef)
    {
        if (this.isCacheDisabled())
        {
            return null;
        }
        return getAsOneByIndexFromCache(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1, indexRef);
    }

    private Object wrapResultForFullCache(Object result)
    {
        if (result == null && this.getCache().isFullCache()) result = NulledRelation.getInstance();
        return result;
    }

    private boolean requiresAnalysis(Operation op)
    {
        if (op instanceof CompactUpdateCountOperation)
        {
            if (!((CompactUpdateCountOperation) op).requiresAsOfEqualityCheck())
            {
                return false;
            }
        }
        return true;
    }

    private CachedQuery cloneOrAnalyzeAndFindInMemory(Operation op, OrderBy orderby, boolean forRelationship, QueryCache queryCache, CachedQuery result, AnalyzedOperation analyzedOperation)
    {
        if (result == null && requiresAnalysis(op))
        {
            analyzedOperation = analyzedOperation == null ? new AnalyzedOperation(op, orderby) : analyzedOperation;
            result = queryCache.findByEquality(analyzedOperation.getAnalyzedOperation(), forRelationship);
            if (result != null)
            {
                CachedQuery forOriginal = result.getCloneForEquivalentOperation(op, orderby);
                forOriginal.cacheQuery(forRelationship);
                result = forOriginal;
            }
        }
        if (result == null)
        {
            result = findInCache(op, analyzedOperation, orderby, forRelationship);
        }
        else
        {
            this.getPerformanceData().incrementQueryCacheHits();
            result = result.getCloneIfDifferentOrderBy(orderby);
        }
        if (result == null)
        {
            result = queryCache.findBySubQuery(op, analyzedOperation, orderby, forRelationship);
            if (result != null)
            {
                this.getPerformanceData().incrementSubQueryCacheHits();
            }
        }
        return result;
    }

    protected abstract CachedQuery findInCache(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship);

    protected void flushTransaction(Operation op, boolean bypassCache)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx != null)
        {
            tx.executeBufferedOperationsForOperation(op, bypassCache);
        }
    }

    public void clearQueryCache()
    {
        this.getQueryCache().clearCache();
        if (this.getCache().isPartialCache())
        {
            this.getCache().clear();
        }
        this.getPerClassUpdateCountHolder().incrementUpdateCount();
    }

    protected CachedQuery findInCacheForNoTransaction(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship)
    {
        CachedQuery emptyNewCachedQuery = new CachedQuery(op, orderby); // must create before executing the query to avoid a race condition against concurrent updates
        List resultList = resolveOperationOnCache(analyzedOperation != null ? analyzedOperation.getAnalyzedOperation() : op);
        return createAndCacheQuery(resultList, orderby, analyzedOperation, forRelationship, emptyNewCachedQuery);
    }

    protected List resolveOperationOnCache(Operation op)
    {
        List resultList;
        if (isOperationPartiallyCached(op))
        {
            resultList = op.applyOperationToPartialCache();
        }
        else
        {
            if (op.zEstimateReturnSize() > TRANSITIVE_THRESHOLD)
            {
                TransitivePropagator propagator = new TransitivePropagator(op);
                op = propagator.getOperation();
            }
            resultList = op.applyOperationToFullCache();
        }
        return resultList;
    }

    protected CachedQuery createAndCacheQuery(List resultList, OrderBy orderby, AnalyzedOperation analyzedOperation, boolean forRelationship, CachedQuery emptyNewCachedQuery)
    {
        CachedQuery result = null;
        if (resultList != null)
        {
            if (orderby != null && resultList.size() > 1) Collections.sort(resultList, orderby);
            result = emptyNewCachedQuery;
            result.setResult(resultList);
            boolean needsDefaulting = analyzedOperation != null && analyzedOperation.isAnalyzedOperationDifferent();
            if (needsDefaulting)
            {
                result.setWasDefaulted();
                CachedQuery cachedQuery2 = new CachedQuery(analyzedOperation.getAnalyzedOperation(), orderby, emptyNewCachedQuery);
                cachedQuery2.setResult(resultList);
                cachedQuery2.cacheQuery(forRelationship);
            }
            result.cacheQuery(forRelationship);
            this.getPerformanceData().incrementObjectCacheHits();
        }
        return result;
    }

    protected boolean isOperationPartiallyCached(Operation op)
    {
        boolean partiallyCached = this.isPartiallyCached();
        if (!partiallyCached && !(op instanceof CompactUpdateCountOperation))
        {
            UnifiedSet dependentPortals = new UnifiedSet(3);
            op.addDependentPortalsToSet(dependentPortals);
            if (dependentPortals.size() > 1)
            {
                Iterator it = dependentPortals.iterator();
                while (it.hasNext() && !partiallyCached)
                {
                    MithraObjectPortal depPortal = (MithraObjectPortal) it.next();
                    partiallyCached = depPortal.isPartiallyCached();
                }
            }
        }
        return partiallyCached;
    }

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship, int maxObjectsToRetrieve)
    {
        return this.findAsCachedQuery(op, orderby, bypassCache, forRelationship, maxObjectsToRetrieve, 1, false);
    }

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship, int maxObjectsToRetrieve, boolean forceImplicitJoin)
    {
        return this.findAsCachedQuery(op, orderby, bypassCache, forRelationship, maxObjectsToRetrieve, 1, forceImplicitJoin);
    }

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache,
                                         boolean forRelationship, int maxObjectsToRetrieve, int numOfParallelThreads, boolean forceImplicitJoin)
    {
        if ((bypassCache || this.disableCache) && op.zIsNone())
        {
            CachedQuery noneResult = new CachedQuery(op, orderby);
            noneResult.setResult(ListFactory.EMPTY_LIST);
            return noneResult;
        }
        boolean bypassLocalCache = !this.isPureHome() &&
                ((!forRelationship && this.disableCache &&
                        !(this.isTransactional && MithraManagerProvider.getMithraManager().isInTransaction())) || bypassCache);
        AnalyzedOperation analyzedOperation = null;
        CachedQuery result = null;
        if (!bypassLocalCache)
        {
            QueryCache queryCache = this.getQueryCache();
            result = queryCache.findByEquality(op, forRelationship);
            if (result == null && requiresAnalysis(op))
            {
                analyzedOperation = new AnalyzedOperation(op, orderby);
            }
            result = cloneOrAnalyzeAndFindInMemory(op, orderby, forRelationship, queryCache, result, analyzedOperation);
        }
        else
        {
            // to combine operands when bypassing cache
            op.hashCode();
        }
        if (result == null)
        {
            analyzedOperation = analyzedOperation == null ? new AnalyzedOperation(op, orderby) : analyzedOperation;
            this.flushTransaction(analyzedOperation.getAnalyzedOperation(), bypassLocalCache);
            result = findFromServer(analyzedOperation, orderby, bypassCache, forRelationship, maxObjectsToRetrieve, numOfParallelThreads, forceImplicitJoin);
            MithraManagerProvider.getMithraManager().incrementDatabaseRetrieveCount();
        }
        return result;
    }

    private Operation createOperationForInMemoryAggregation(Operation originalOperation, List<MithraGroupByAttribute> groupByAttributes)
    {
        for (MithraGroupByAttribute groupByAttribute : groupByAttributes)
        {
            if (groupByAttribute.getAttribute() instanceof MappedAttribute)
            {
                Mapper mapper = ((MappedAttribute) groupByAttribute.getAttribute()).getMapper();
                originalOperation = originalOperation.and(new MappedOperation(mapper, new All(mapper.getAnyRightAttribute())));
            }
        }
        return originalOperation;
    }


    public List findAggregatedBeanData(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap, Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation,
                                       com.gs.fw.finder.OrderBy orderBy, boolean bypassCache, Class bean)
    {
        List<MithraAggregateAttribute> aggregateAttributes = new FastList<com.gs.fw.common.mithra.MithraAggregateAttribute>(nameToAggregateAttributeMap.values());
        List<MithraGroupByAttribute> groupByAttributes = new FastList<MithraGroupByAttribute>(nameToGroupByAttributeMap.values());

        boolean bypassLocalCache = !this.isPureHome() &&
                ((this.disableCache &&
                        !(this.isTransactional && MithraManagerProvider.getMithraManager().isInTransaction())) || bypassCache);

        flushTransaction(op, bypassLocalCache);

        CachedQuery cachedQuery = findAggregateCachedQuery(op, groupByAttributes, aggregateAttributes, bypassLocalCache);

        List<Object> result;
        if (cachedQuery != null)
        {

            result = aggregateBeanInMemory(cachedQuery, nameToAggregateAttributeMap, nameToGroupByAttributeMap, havingOperation, bean);

        }
        else
        {
            result = findAggregatedBeanDataFromServer(op, nameToAggregateAttributeMap, nameToGroupByAttributeMap, havingOperation, bypassLocalCache, bean);

        }

        if (orderBy != null && result.size() > 1)
        {
            Collections.sort(result, orderBy);
        }

        return result;
    }

    @Override
    public synchronized boolean syncWithMasterCache(MasterCacheUplink masterCacheUplink)
    {
        if (this.cache == null)
        {
            return true;
        }
        return this.cache.syncWithMasterCache(masterCacheUplink);
    }

    private CachedQuery findAggregateCachedQuery(Operation op, List<MithraGroupByAttribute> groupByAttributes, List<MithraAggregateAttribute> aggregateAttributes, boolean bypassLocalCache)
    {
        CachedQuery result = null;
        if (!bypassLocalCache && hasNoToManyGroupBys(groupByAttributes) && this.findDeepRelationshipsInMemory(op, aggregateAttributes, groupByAttributes))
        {
            Operation aggregateOp = createOperationForInMemoryAggregation(op, groupByAttributes);
            QueryCache queryCache = this.getQueryCache();
            result = queryCache.findByEquality(aggregateOp, false);
            result = cloneOrAnalyzeAndFindInMemory(aggregateOp, null, false, queryCache, result, null);
        }
        return result;
    }

    public List<AggregateData> findAggregatedData(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                                  Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, com.gs.fw.finder.OrderBy orderBy, boolean bypassCache)
    {
        List<MithraAggregateAttribute> aggregateAttributes = new FastList<com.gs.fw.common.mithra.MithraAggregateAttribute>(nameToAggregateAttributeMap.values());
        List<MithraGroupByAttribute> groupByAttributes = new FastList<MithraGroupByAttribute>(nameToGroupByAttributeMap.values());

        boolean bypassLocalCache = !this.isPureHome() &&
                ((this.disableCache &&
                        !(this.isTransactional && MithraManagerProvider.getMithraManager().isInTransaction())) || bypassCache);

        flushTransaction(op, bypassLocalCache);

        CachedQuery cachedQuery = findAggregateCachedQuery(op, groupByAttributes, aggregateAttributes, bypassLocalCache);

        List<AggregateData> result;
        if (cachedQuery != null)
        {
            result = aggregateInMemory(cachedQuery, nameToAggregateAttributeMap, nameToGroupByAttributeMap, havingOperation);
        }
        else
        {
            result = findAggregatedBeanDataFromServer(op, nameToAggregateAttributeMap, nameToGroupByAttributeMap, havingOperation, bypassLocalCache, AggregateData.class);

        }

        if (orderBy != null && result.size() > 1)
        {
            Collections.sort(result, orderBy);
        }

        return result;
    }

    private boolean hasNoToManyGroupBys(List<MithraGroupByAttribute> groupByAttributes)
    {
        for (int i = 0; i < groupByAttributes.size(); i++)
        {
            Attribute attr = groupByAttributes.get(i).getAttribute();
            if (attr instanceof MappedAttribute)
            {
                if (((MappedAttribute) attr).getMapper().isToMany()) return false;
            }
        }
        return true;
    }

    private boolean findDeepRelationshipsInMemory(Operation op, List<com.gs.fw.common.mithra.MithraAggregateAttribute> aggregateAttributes, List groupByAttributes)
    {
        for (int i = 0; i < aggregateAttributes.size(); i++)
        {
            if (!aggregateAttributes.get(i).findDeepRelationshipInMemory(op)) return false;
        }
        for (int i = 0; i < groupByAttributes.size(); i++)
        {
            GroupByAttribute gba = (GroupByAttribute) groupByAttributes.get(i);
            if (!gba.findDeepRelationshipInMemory(op)) return false;
        }
        return true;
    }

    protected List findAggregatedBeanDataFromServer(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                                    Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, boolean bypassCache, Class bean)
    {
        int retriesLeft = MithraManagerProvider.getMithraManager().isInTransaction() ? 0 : MithraTransaction.DEFAULT_TRANSACTION_RETRIES;
        while (true)
        {
            try
            {
                return this.getMithraObjectReader().findAggregatedData(op, nameToAggregateAttributeMap,
                        nameToGroupByAttributeMap, havingOperation, bypassCache, bean);
            }
            catch (MithraBusinessException e)
            {
                retriesLeft = e.ifRetriableWaitElseThrow("find failed with retriable error. retrying.", retriesLeft, logger);
            }
        }
    }

    private List aggregateBeanInMemory(CachedQuery cachedQuery, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                       Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, Class bean)
    {
        Map<String, MithraAggregateAttribute> nameToOriginalAggregateAttributeMap = getAggregateMapForAggregateData(nameToAggregateAttributeMap);
        Map<String, MithraGroupByAttribute> nameToOriginalGroupByAttributeMap = getGroupByMapForAggregateData(nameToGroupByAttributeMap);

        List<AggregateData> aggregateDataList = this.aggregateInMemory(cachedQuery, nameToOriginalAggregateAttributeMap, nameToOriginalGroupByAttributeMap, havingOperation);

        return convertAggregateDataToBean(nameToAggregateAttributeMap, nameToGroupByAttributeMap, bean, aggregateDataList);
    }

    public static Map<String, MithraAggregateAttribute> getAggregateMapForAggregateData(Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap)
    {
        Map returnMap = new UnifiedMap<String, MithraAggregateAttribute>(nameToAggregateAttributeMap.size());
        Set<String> aggregateAttributeNames = nameToAggregateAttributeMap.keySet();
        for (String attributeName : aggregateAttributeNames)
        {
            MithraAggregateAttribute aggregateAttribute = nameToAggregateAttributeMap.get(attributeName);
            returnMap.put(attributeName, ((BeanAggregateAttribute) aggregateAttribute).getAggregateAttribute());

        }
        return returnMap;
    }

    public static Map<String, MithraGroupByAttribute> getGroupByMapForAggregateData(Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap)
    {
        Map returnMap = new UnifiedMap<String, MithraGroupByAttribute>(nameToGroupByAttributeMap.size());
        Set<String> groupByNames = nameToGroupByAttributeMap.keySet();
        for (String groupByName : groupByNames)
        {
            MithraGroupByAttribute groupByAttribute = nameToGroupByAttributeMap.get(groupByName);
            returnMap.put(groupByName, new GroupByAttribute(groupByAttribute.getAttribute()));

        }
        return returnMap;
    }


    public static List convertAggregateDataToBean(Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap, Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, Class bean, List<AggregateData> aggregateDataList)
    {
        List returnList = new FastList();
        List<String> groupByAttributeNamesList = new FastList<String>(nameToGroupByAttributeMap.size());
        List<MithraGroupByAttribute> groupByAttributeList = new FastList<MithraGroupByAttribute>(nameToGroupByAttributeMap.size());

        List<String> aggregateAttributeNameList = new FastList<String>(nameToAggregateAttributeMap.size());
        List<MithraAggregateAttribute> aggregateAttributeList = new FastList<MithraAggregateAttribute>(nameToAggregateAttributeMap.size());

        Set<String> groupByNames = nameToGroupByAttributeMap.keySet();
        for (String groupByName : groupByNames)
        {
            groupByAttributeNamesList.add(groupByName);
            groupByAttributeList.add(nameToGroupByAttributeMap.get(groupByName));
        }

        Set<String> aggregateAttributeNames = nameToAggregateAttributeMap.keySet();
        for (String aggregateAttributeName : aggregateAttributeNames)
        {
            aggregateAttributeNameList.add(aggregateAttributeName);
            aggregateAttributeList.add(nameToAggregateAttributeMap.get(aggregateAttributeName));
        }

        Object[] valueArray = new Object[1];

        for (int i = 0; i < aggregateDataList.size(); i++)
        {
            AggregateData data = aggregateDataList.get(i);
            Object beanInstance = getInstance(bean);

            for (int j = 0; j < groupByAttributeNamesList.size(); j++)
            {
                valueArray[0] = data.getAttributeAsObject(groupByAttributeNamesList.get(j));
                groupByAttributeList.get(j).setValue(beanInstance, valueArray);
            }
            for (int k = 0; k < aggregateAttributeNames.size(); k++)
            {
                valueArray[0] = data.getAttributeAsObject(aggregateAttributeNameList.get(k));
                aggregateAttributeList.get(k).setValue(beanInstance, valueArray);
            }
            returnList.add(beanInstance);
        }
        return returnList;
    }

    private static Object getInstance(Class bean)
    {
        try
        {
            return bean.newInstance();
        }
        catch (InstantiationException e)
        {
            throw new MithraBusinessException("Error instantiating class " + bean.getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke constructor of class " + bean.getName(), e);
        }
    }


    private List<AggregateData> aggregateInMemory(CachedQuery cachedQuery, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                                  Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation)
    {
        if (havingOperation != null)
        {
            havingOperation.zAddMissingAggregateAttributes(new UnifiedSet(nameToAggregateAttributeMap.values()), nameToAggregateAttributeMap);
        }

        AggregateDataConfig aggDataConfig = new AggregateDataConfig(nameToGroupByAttributeMap, nameToAggregateAttributeMap);

        List objectsToAggregate = cachedQuery.getResult();
        List<MithraAggregateAttribute> aggregateAttributes = aggDataConfig.getAggregateAttributes();
        int aggAttributeSize = aggregateAttributes.size();
        List<MithraGroupByAttribute> groupByAttributes = aggDataConfig.getGroupByAttributes();
        int groupBySize = groupByAttributes.size();

        int size = objectsToAggregate.size();
        if (size == 0 && !aggDataConfig.hasGroupBy())
        {
            // we have to supply null or default values in this case
            AggregateData result = new AggregateData(aggDataConfig);
            for (int j = 0; j < aggAttributeSize; j++)
            {
                result.setValueAt(j, aggregateAttributes.get(j).getDefaultValueForEmptyGroup());
            }
            return ListFactory.create(result);
        }
        Map<Object, AggregateData> aggregateMap = this.createAggregateResultMap(groupByAttributes);
        for (int i = 0; i < size; i++)
        {
            Object item = objectsToAggregate.get(i);
            AggregateData aggregateData;
            if (!aggDataConfig.hasGroupBy())
            {
                aggregateData = aggregateMap.get(NO_GROUP_BY_KEY);
            }
            else
            {
                aggregateData = aggregateMap.get(item);
            }

            if (aggregateData == null)
            {
                aggregateData = new AggregateData(aggDataConfig);

                for (int j = 0; j < groupBySize; j++)
                {
                    MithraGroupByAttribute groupByAttribute = groupByAttributes.get(j);
                    Object groupByValue = groupByAttribute.valueOf(item);

                    if (groupByValue == null)
                    {
                        groupByValue = groupByAttribute.getNullGroupByAttribute();
                        aggregateData.setValueAt(j, groupByValue);
                    }
                    else
                    {
                        groupByAttribute.populateAggregateDataValue(j, groupByValue, aggregateData);
                    }
                }
            }

            boolean aggregatedSomething = false;
            for (int j = 0; j < aggAttributeSize; j++)
            {
                MithraAggregateAttribute aggregateAttribute = aggregateAttributes.get(j);
                int position = groupBySize + j;
                Object aggregateSoFar = aggregateData.getValueAt(position);
                Object aggregatedResult = aggregateAttribute.aggregate(aggregateSoFar, item);
                if (((Nullable) aggregatedResult).isInitialized())
                {
                    aggregatedSomething = true;
                    aggregateData.setValueAt(position, aggregatedResult);
                }
            }

            if (aggregatedSomething || aggregateAttributes.isEmpty())
            {
                if (!aggDataConfig.hasGroupBy())
                {
                    aggregateMap.put(NO_GROUP_BY_KEY, aggregateData);
                }
                else
                {
                    aggregateMap.put(item, aggregateData);
                }
            }
        }

        MithraFastList<AggregateData> result = new MithraFastList<AggregateData>(aggregateMap.values());

        if (havingOperation != null)
        {
            Map<com.gs.fw.common.mithra.MithraAggregateAttribute, String> attributeToNameMap =
                    this.createAggregateAttributeToNameMap(nameToAggregateAttributeMap);
            for (int i = 0; i < result.size(); )
            {
                if (!havingOperation.zMatches(result.get(i), attributeToNameMap))
                {
                    result.removeByReplacingFromEnd(i);
                }
                else
                {
                    i++;
                }
            }
        }
        return result;
    }

    private Map<MithraAggregateAttribute, String> createAggregateAttributeToNameMap(Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap)
    {
        Map<com.gs.fw.common.mithra.MithraAggregateAttribute, String> aggregateAttributeToNameMap = new UnifiedMap<MithraAggregateAttribute, String>(nameToAggregateAttributeMap.size());
        for (Map.Entry<String, MithraAggregateAttribute> entry : nameToAggregateAttributeMap.entrySet())
        {
            aggregateAttributeToNameMap.put(entry.getValue(), entry.getKey());
        }
        return aggregateAttributeToNameMap;
    }

    private Map<Object, AggregateData> createAggregateResultMap(List<MithraGroupByAttribute> groupByAttributes)
    {
        Map<Object, AggregateData> map;
        if (groupByAttributes.isEmpty())
        {
            map = new UnifiedMap();
        }
        else
        {
            map = new UnifiedMapWithHashingStrategy<Object, AggregateData>(this.createHashingStrategyForAggregation(groupByAttributes));
        }
        return map;
    }

    private HashingStrategy createHashingStrategyForAggregation(List groupByattributes)
    {
        Extractor[] extractors = new Extractor[groupByattributes.size()];
        for (int i = 0; i < extractors.length; i++)
        {
            extractors[i] = ((GroupByAttribute) groupByattributes.get(i)).getAttribute();
        }
        return ExtractorBasedHashStrategy.create(extractors);
    }

    protected CachedQuery findFromServer(AnalyzedOperation analyzedOperation, OrderBy orderby,
                                         boolean bypassCache, boolean forRelationship, int maxObjectsToRetrieve, int numOfParallelThreads, boolean forceImplicitJoin)
    {
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            return findFromServerForTransaction(analyzedOperation, orderby, forRelationship, maxObjectsToRetrieve, bypassCache, forceImplicitJoin);
        }
        else
        {
            return findFromServerForNoTransaction(analyzedOperation, orderby, forRelationship, maxObjectsToRetrieve, numOfParallelThreads, bypassCache, forceImplicitJoin);
        }
    }

    protected CachedQuery findFromServerForNoTransaction(AnalyzedOperation analyzedOperation, OrderBy orderby,
                                                         boolean forRelationship, int maxObjectsToRetrieve, int numOfParallelThreads, boolean bypassCache, boolean forceImplicitJoin)
    {
        int retriesLeft = MithraTransaction.DEFAULT_TRANSACTION_RETRIES;
        while (true)
        {
            try
            {
                return this.getMithraObjectReader().find(analyzedOperation, orderby, forRelationship, maxObjectsToRetrieve, numOfParallelThreads, bypassCache, forceImplicitJoin);
            }
            catch (MithraBusinessException e)
            {
                retriesLeft = e.ifRetriableWaitElseThrow("find failed with retriable error. retrying.", retriesLeft, logger);
            }
        }
    }

    protected CachedQuery findFromServerForTransaction(AnalyzedOperation analyzedOperation, OrderBy orderby,
                                                       boolean forRelationship, int maxObjectsToRetrieve, boolean bypassCache, boolean forceImplicitJoin)
    {
        CachedQuery result;
        result = this.getMithraObjectReader().find(analyzedOperation, orderby, forRelationship, maxObjectsToRetrieve, 1, bypassCache, forceImplicitJoin);
        return result;
    }

    protected String getFinderClassName()
    {
        return this.finder.getFinderClassName();
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException
    {
        out.writeObject(getFinderClassName());
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException
    {
        this.finderClassName = (String) in.readObject();
    }


    public Object readResolve() throws ObjectStreamException
    {
        try
        {
            return ReflectionMethodCache.getZeroArgMethod(Class.forName(this.finderClassName), "getMithraObjectPortal").invoke(null, (Object[]) null);
        }
        catch (Exception e)
        {
            InvalidClassException invalidClassException = new InvalidClassException("could not find object portal for " + this.finderClassName);
            invalidClassException.initCause(e);
            throw invalidClassException;
        }
    }

    public MithraObjectDeserializer getMithraObjectDeserializer()
    {
        return objectFactory;
    }

    public MithraObjectFactory getMithraObjectFactory()
    {
        return (MithraObjectFactory) this.objectFactory;
    }

    public MithraDatedObjectFactory getMithraDatedObjectFactory()
    {
        return (MithraDatedObjectFactory) this.objectFactory;
    }

    public Map extractDatabaseIdentifiers(Operation op)
    {
        return this.getMithraObjectReader().extractDatabaseIdentifiers(op);
    }

    public Map extractDatabaseIdentifiers(Set sourceAttributeValueSet)
    {
        return this.getMithraObjectReader().extractDatabaseIdentifiers(sourceAttributeValueSet);
    }

    public void registerForNotification(String subject)
    {
        if (this.isForTempObject()) return;
        MithraManagerProvider.getMithraManager().getNotificationEventManager().registerForNotification(subject, this);
    }

    public void registerForApplicationNotification(String subject, MithraApplicationNotificationListener listener,
                                                   List mithraObjectList, Operation operation)
    {
        if (this.isForTempObject()) return;
        MithraManagerProvider.getMithraManager().getNotificationEventManager().
                registerForApplicationNotification(subject, listener, this.getFinder(), mithraObjectList, operation);
    }

    public void registerForApplicationClassLevelNotification(MithraApplicationClassLevelNotificationListener listener)
    {
        if (this.getFinder().getSourceAttribute() != null)
        {
            throw new MithraBusinessException("This method can only be called for classes without a source attribute");
        }

        Set sourceAttributeValueSet = new HashSet();
        sourceAttributeValueSet.add(null);

        registerForApplicationClassLevelNotification(sourceAttributeValueSet, listener);
    }

    public void registerForApplicationClassLevelNotification(Set sourceAttributeValueSet, MithraApplicationClassLevelNotificationListener listener)
    {
        if (sourceAttributeValueSet == null || sourceAttributeValueSet.isEmpty())
        {
            throw new MithraBusinessException("Source attribute value set is mandatory");
        }
        if (this.getFinder().getSourceAttribute() == null && (sourceAttributeValueSet.size() != 1 || !sourceAttributeValueSet.contains(null)))
        {
            throw new MithraBusinessException("The source attribute value set must contain a single null value because this class does not have a source attribute");
        }

        if (this.isForTempObject()) return;

        Collection<String> databaseIdentifiers = this.extractDatabaseIdentifiers(sourceAttributeValueSet).values();
        for (String databaseIdentifier : databaseIdentifiers)
        {
            MithraManagerProvider.getMithraManager().getNotificationEventManager().
                    registerForApplicationClassLevelNotification(databaseIdentifier, listener, this.getFinder());
        }
    }

    public MithraDataObject refresh(MithraDataObject data, boolean lockInDatabase)
            throws MithraDatabaseException
    {
        if (this.isPureHome())
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) this.getCache().getObjectByPrimaryKey(data, true);
            if (obj == null) return null;
            return obj.zGetTxDataForRead();
        }
        else
        {
            MithraDataObject newData = this.getMithraObjectReader().refresh(data, lockInDatabase);
            MithraManagerProvider.getMithraManager().incrementDatabaseRetrieveCount();
            return newData;
        }
    }

    public MithraDataObject refreshDatedObject(MithraDatedObject mithraDatedObject, boolean lockInDatabase)
            throws MithraDatabaseException
    {
        MithraDataObject newData = this.getMithraObjectReader().refreshDatedObject(mithraDatedObject, lockInDatabase);
        MithraManagerProvider.getMithraManager().incrementDatabaseRetrieveCount();
        return newData;
    }

    public boolean useMultiUpdate()
    {
        return this.useMultiUpdate;
    }

    public void setUseMultiUpdate(boolean useMultiUpdate)
    {
        this.useMultiUpdate = useMultiUpdate;
    }

    public int getHierarchyDepth()
    {
        return hierarchyDepth;
    }

    public void setSuperClassFinders(RelatedFinder[] superClassFinders)
    {
        this.superClassFinders = superClassFinders;
    }

    public void setSubClassFinders(RelatedFinder[] subClassFinders)
    {
        this.subClassFinders = subClassFinders;
    }

    public MithraObjectPortal[] getSuperClassPortals()
    {
        if (this.superClassFinders != null && this.superClassPortals == null)
        {
            MithraObjectPortal[] portals = new MithraObjectPortal[this.superClassFinders.length];
            for (int i = 0; i < superClassFinders.length; i++)
            {
                portals[i] = superClassFinders[i].getMithraObjectPortal();
            }
            this.superClassPortals = portals;
        }
        return this.superClassPortals;
    }

    public MithraObjectPortal[] getJoinedSubClassPortals()
    {
        if (this.subClassFinders != null && this.subClassPortals == null)
        {
            MithraObjectPortal[] portals = new MithraObjectPortal[this.subClassFinders.length];
            for (int i = 0; i < subClassFinders.length; i++)
            {
                portals[i] = subClassFinders[i].getMithraObjectPortal();
            }
            this.subClassPortals = portals;
        }
        return this.subClassPortals;
    }

    public String getUniqueAlias()
    {
        return this.uniqueAlias;
    }

    public void setUniqueAlias(String uniqueAlias)
    {
        this.uniqueAlias = uniqueAlias;
    }

    public void appendJoinToSuper(StringBuilder buffer, String defaultDatabaseAlias)
    {
        RelatedFinder superFinder = superClassFinders[0];
        Attribute[] superPkAttributes = superFinder.getPrimaryKeyAttributes();
        Attribute[] pkAttributes = ((PrivateReladomoClassMetaData)this.getClassMetaData()).getCachedPrimaryKeyAttributes();
        MithraObjectPortal[] superPortals = getSuperClassPortals();
        String superAlias = superPortals[0].getUniqueAlias();
        if (superAlias == null) superAlias = "";
        buffer.append(defaultDatabaseAlias).append(superAlias).append('.').append(superPkAttributes[0].getColumnName()).append("=");
        buffer.append(defaultDatabaseAlias).append(this.uniqueAlias).append('.').append(pkAttributes[0].getColumnName());
        for (int i = 1; i < pkAttributes.length; i++)
        {
            buffer.append(" AND ");
            buffer.append(defaultDatabaseAlias).append(superAlias).append('.').append(superPkAttributes[i].getColumnName()).append("=");
            buffer.append(defaultDatabaseAlias).append(this.uniqueAlias).append('.').append(pkAttributes[i].getColumnName());
        }
    }

    public MithraPerformanceData getPerformanceData()
    {
        return this.performanceData;
    }

    public void setIndependent(boolean independent)
    {
        isIndependent = independent;
    }

    public boolean isIndependent()
    {
        return isIndependent;
    }

    protected CachedQuery findInMemoryForAnalyzed(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, CachedQuery result, QueryCache queryCache)
    {
        if (result == null && analyzedOperation.isAnalyzedOperationDifferent())
        {
            result = queryCache.findByEquality(analyzedOperation.getAnalyzedOperation(), forRelationship);
            if (result != null)
            {
                CachedQuery forOriginal = result.getCloneForEquivalentOperation(analyzedOperation.getOriginalOperation(), orderby);
                forOriginal.cacheQuery(forRelationship);
                result = forOriginal;
            }
        }
        return result;
    }

    private static class RelationshipReference extends WeakReference
    {
        private RelationshipReference(Object referent)
        {
            super(referent);
        }
    }

    private static class TimedRelationshipReference extends RelationshipReference
    {
        private long creationTime;
        private long timeToLive;

        private TimedRelationshipReference(Object ref, long timeToLive)
        {
            super(ref);
            this.creationTime = CacheClock.getTime();
            this.timeToLive = timeToLive;
        }

        public Object get()
        {
            if (creationTime + timeToLive > CacheClock.getTime()) return super.get();
            return null;
        }
    }

    private static class EmptyCursor implements Cursor
    {
        public void close()
        {
            // nothing to do
        }

        public boolean hasNext()
        {
            return false;
        }

        public Object next()
        {
            throw new RuntimeException("should not get here");
        }

        public void remove()
        {
            throw new RuntimeException("should not get here");
        }
    }

    public long getLatestRefreshTime()
    {
        return latestRefreshTime;
    }

    public void setLatestRefreshTime(long latestRefreshTime)
    {
        this.latestRefreshTime = latestRefreshTime;
    }

    @Override
    public Attribute[] zGetAddressingAttributes()
    {
        if (this.getTxParticipationMode().isOptimisticLocking())
        {
            computeOptimisticAddressingAttributes();
            return this.optimisticAddressingAttributes;
        }
        else
        {
            computeAddressingAttributes();
            return this.addressingAttributes;
        }
    }

    @Override
    public MithraDataObject zChooseDataForMultiupdate(MithraTransactionalObject obj)
    {
        if (updateDataChooser == null)
        {
            if (finder.getAsOfAttributes() == null)
            {
                updateDataChooser = NonDatedUpdateDataChooser.getInstance();
            }
            else
            {
                updateDataChooser = DatedUpdateDataChooser.getInstance();
            }
        }
        return updateDataChooser.chooseDataForMultiUpdate(obj);

    }

    protected void computeAddressingAttributes()
    {
        if (this.addressingAttributes == null)
        {
            MithraFastList<Attribute> addressingList = createFirstAddressingAttributes();

            this.addressingAttributes = addressingList.toArray(new Attribute[addressingList.size()]);
        }
    }

    private MithraFastList<Attribute> createFirstAddressingAttributes()
    {
        Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        MithraFastList<Attribute> addressingList = new MithraFastList(primaryKeyAttributes.length + 3);
        for (int i = 0; i < primaryKeyAttributes.length; i++)
        {
            if (!primaryKeyAttributes[i].isSourceAttribute())
            {
                addressingList.add(primaryKeyAttributes[i]);
            }
        }
        if (asOfAttributes != null)
        {
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                addressingList.add(asOfAttributes[i].getToAttribute());
            }
        }
        return addressingList;
    }

    protected void computeOptimisticAddressingAttributes()
    {
        if (this.optimisticAddressingAttributes == null)
        {
            MithraFastList<Attribute> addressingAttributes = createFirstAddressingAttributes();
            VersionAttribute versionAttribute = this.getFinder().getVersionAttribute();
            if (versionAttribute != null)
            {
                addressingAttributes.add(((Attribute)versionAttribute));
            }
            Attribute optimisticAttribute = getClassMetaData().getOptimisticKeyFromAsOfAttributes();
            if (optimisticAttribute != null) addressingAttributes.add(optimisticAttribute);
            this.optimisticAddressingAttributes = addressingAttributes.toArray(new Attribute[addressingAttributes.size()]);
        }
    }

    @Override
    public ReladomoClassMetaData getClassMetaData()
    {
        return this.metaData;
    }
}
