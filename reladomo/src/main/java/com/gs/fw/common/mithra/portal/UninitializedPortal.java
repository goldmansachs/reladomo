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

package com.gs.fw.common.mithra.portal;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.offheap.MasterCacheUplink;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.QueryCache;
import com.gs.fw.common.mithra.tempobject.MithraTuplePersister;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import com.gs.fw.common.mithra.util.PersisterId;
import com.gs.fw.common.mithra.util.RenewedCacheStats;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;



public class UninitializedPortal implements MithraObjectPortal
{

    private String className;
    private MithraObjectPortal initialized;

    public UninitializedPortal(String className)
    {
        this.className = className;
    }

    private synchronized MithraObjectPortal initializeNow(String msg)
    {
        if (initialized == null)
        {
            MithraObjectPortal portal = MithraManagerProvider.getMithraManager().initializePortal(this.className);
            initialized = portal;
            if (portal == null)
            {
                throw new MithraConfigurationException("Could not " + msg + " for " + className + ". did you forget to add it to the configuration XML?");
            }
        }
        return initialized;
    }

    public void appendJoinToSuper(StringBuilder buffer, String defaultDatabaseAlias)
    {
        initializeNow("appendJoinToSuper").appendJoinToSuper(buffer, defaultDatabaseAlias);
    }

    public void clearQueryCache()
    {
        initializeNow("clearQueryCache").clearQueryCache();
    }

    public void clearTxParticipationMode(MithraTransaction tx)
    {
        initializeNow("clearTxParticipationMode").clearTxParticipationMode(tx);
    }

    public List computeFunction(Operation op, OrderBy orderby, String sqlExpression, ResultSetParser resultSetParser)
    {
        return initializeNow("computeFunction").computeFunction(op, orderby, sqlExpression, resultSetParser);
    }

    public int count(Operation op)
    {
        return initializeNow("count").count(op);
    }

    public Map extractDatabaseIdentifiers(Operation op)
    {
        return initializeNow("extractDatabaseIdentifiers").extractDatabaseIdentifiers(op);
    }

    public Map extractDatabaseIdentifiers(Set sourceAttributeValueSet)
    {
        return initializeNow("extractDatabaseIdentifiers").extractDatabaseIdentifiers(sourceAttributeValueSet);
    }

    public List find(Operation op, boolean bypassCache, boolean forceImplicitJoin)
    {
        return initializeNow("find").find(op, bypassCache, forceImplicitJoin);
    }

    public List find(Operation op, boolean bypassCache)
    {
        return initializeNow("find").find(op, bypassCache);
    }

    public List<AggregateData> findAggregatedData(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                                  Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, com.gs.fw.finder.OrderBy orderBy, boolean bypassCache)
    {
        return initializeNow("findAggregatedData").findAggregatedData(op, nameToAggregateAttributeMap, nameToGroupByAttributeMap, havingOperation, orderBy, bypassCache);
    }

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship, int maxObjectsToRetrieve)
    {
        return initializeNow("findAsCachedQuery").findAsCachedQuery(op, orderby, bypassCache, forRelationship, maxObjectsToRetrieve);
    }

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship, int maxObjectsToRetrieve, boolean forceImplicitJoin)
    {
        return initializeNow("findAsCachedQuery").findAsCachedQuery(op, orderby, bypassCache, forRelationship, maxObjectsToRetrieve, 1, forceImplicitJoin);
    }

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship, int maxObjectsToRetrieve, int numOfParallelThreads, boolean forceImplicitJoin)
    {
        return initializeNow("findAsCachedQuery").findAsCachedQuery(op, orderby, bypassCache, forRelationship, maxObjectsToRetrieve, numOfParallelThreads, forceImplicitJoin);
    }

    public Cursor findCursorFromServer(Operation op, Filter postLoadFilter, OrderBy orderby, int maxObjectsToRetrieve, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        return initializeNow("findCursorFromServer").findCursorFromServer(op, postLoadFilter, orderby, maxObjectsToRetrieve, bypassCache, maxParallelDegree, forceImplicitJoin);
    }

    public List findForMassDeleteInMemory(Operation op, MithraTransaction tx)
    {
        return initializeNow("findForMassDeleteInMemory").findForMassDeleteInMemory(op, tx);
    }

    public Object getAsOneFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        return initializeNow("getAsOneFromCache").getAsOneFromCache(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
    }

    public Object getAsOneByIndexFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1, int indexRef)
    {
        return initializeNow("getAsOneByIndexFromCache").getAsOneByIndexFromCache(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1, indexRef);
    }

    @Override
    public Object getAsOneFromCacheForFind(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        return initializeNow("getAsOneFromCacheForFind").getAsOneFromCacheForFind(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
    }

    @Override
    public Object getAsOneByIndexFromCacheForFind(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1, int indexRef)
    {
        return initializeNow("getAsOneByIndexFromCacheForFind").getAsOneByIndexFromCacheForFind(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1, indexRef);
    }

    public Cache getCache()
    {
        return initializeNow("getCache").getCache();
    }

    public MithraDatabaseObject getDatabaseObject()
    {
        return initializeNow("getDatabaseObject").getDatabaseObject();
    }

    public RelatedFinder getFinder()
    {
        return initializeNow("getFinder").getFinder();
    }

    public int getHierarchyDepth()
    {
        return initializeNow("getHierarchyDepth").getHierarchyDepth();
    }

    public MithraObjectPortal[] getJoinedSubClassPortals()
    {
        return initializeNow("getJoinedSubClassPortals").getJoinedSubClassPortals();
    }

    public MithraDatedObjectFactory getMithraDatedObjectFactory()
    {
        return initializeNow("getMithraDatedObjectFactory").getMithraDatedObjectFactory();
    }

    public MithraObjectDeserializer getMithraObjectDeserializer()
    {
        return initializeNow("getMithraObjectDeserializer").getMithraObjectDeserializer();
    }

    public MithraObjectFactory getMithraObjectFactory()
    {
        return initializeNow("getMithraObjectFactory").getMithraObjectFactory();
    }

    public MithraObjectPersister getMithraObjectPersister()
    {
        return initializeNow("getMithraObjectPersister").getMithraObjectPersister();
    }

    public UpdateCountHolder getPerClassUpdateCountHolder()
    {
        return initializeNow("getPerClassUpdateCountHolder").getPerClassUpdateCountHolder();
    }

    public MithraPerformanceData getPerformanceData()
    {
        return initializeNow("getPerformanceData").getPerformanceData();
    }

    public QueryCache getQueryCache()
    {
        return initializeNow("getQueryCache").getQueryCache();
    }

    public MithraObjectPortal[] getSuperClassPortals()
    {
        return initializeNow("getSuperClassPortals").getSuperClassPortals();
    }

    public TxParticipationMode getTxParticipationMode()
    {
        return initializeNow("getTxParticipationMode").getTxParticipationMode();
    }

    public TxParticipationMode getTxParticipationMode(MithraTransaction tx)
    {
        return initializeNow("getTxParticipationMode").getTxParticipationMode(tx);
    }

    public String getUniqueAlias()
    {
        return initializeNow("getUniqueAlias").getUniqueAlias();
    }

    public void incrementClassUpdateCount()
    {
        initializeNow("incrementClassUpdateCount").incrementClassUpdateCount();
    }

    public boolean isIndependent()
    {
        return initializeNow("isIndependent").isIndependent();
    }

    public boolean isPartiallyCached()
    {
        return initializeNow("isPartiallyCached").isPartiallyCached();
    }

    public boolean isFullyCached()
    {
        return initializeNow("isFullyCached").isFullyCached();
    }

    @Override
    public boolean isReplicated()
    {
        return initializeNow("isReplicated").isReplicated();
    }

    public PersisterId getPersisterId()
    {
        return initializeNow("getPersisterId").getPersisterId();
    }

    public void setPersisterId(PersisterId persisterId)
    {
        initializeNow("setPersisterId").setPersisterId(persisterId);
    }

    public boolean isForTempObject()
    {
        return initializeNow("isForTempObject").isForTempObject();
    }

    public void setForTempObject(boolean forTempObject)
    {
        initializeNow("setForTempObject").setForTempObject(forTempObject);
    }

    public boolean isPureHome()
    {
        return initializeNow("isPureHome").isPureHome();
    }

    public void loadCache()
    {
        initializeNow("loadCache").loadCache();
    }

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        initializeNow("prepareForMassDelete").prepareForMassDelete(op, forceImplicitJoin);
    }

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin)
    {
        initializeNow("prepareForMassPurge").prepareForMassPurge(op, forceImplicitJoin);
    }

    public MithraDataObject refresh(MithraDataObject data, boolean lockInDatabase)
            throws MithraDatabaseException
    {
        return initializeNow("refresh").refresh(data, lockInDatabase);
    }

    public MithraDataObject refreshDatedObject(MithraDatedObject mithraDatedObject, boolean lockInDatabase)
            throws MithraDatabaseException
    {
        return initializeNow("refreshDatedObject").refreshDatedObject(mithraDatedObject, lockInDatabase);
    }

    public void registerForApplicationNotification(String subject, MithraApplicationNotificationListener listener, List mithraObjectList, Operation operation)
    {
        initializeNow("registerForApplicationNotification").registerForApplicationNotification(subject, listener, mithraObjectList, operation);
    }

    public void registerForApplicationClassLevelNotification(MithraApplicationClassLevelNotificationListener listener)
    {
        initializeNow("registerForApplicationClassLevelNotification").registerForApplicationClassLevelNotification(listener);
    }

    public void registerForApplicationClassLevelNotification(Set sourceAttributeValueSet, MithraApplicationClassLevelNotificationListener listener)
    {
        initializeNow("registerForApplicationClassLevelNotification").registerForApplicationClassLevelNotification(sourceAttributeValueSet, listener);
    }

    public void registerForNotification(String subject)
    {
        initializeNow("registerForNotification").registerForNotification(subject);
    }

    public void reloadCache()
    {
        initializeNow("reloadCache").reloadCache();
    }

    @Override
    public RenewedCacheStats renewCacheForOperation(Operation op)
    {
        return initializeNow("renewCacheForOperation").renewCacheForOperation(op);
    }

    public void setDefaultTxParticipationMode(TxParticipationMode mode)
    {
        initializeNow("setDefaultTxParticipationMode").setDefaultTxParticipationMode(mode);
    }

    @Override
    public boolean syncWithMasterCache(MasterCacheUplink masterCacheUplink)
    {
        return initializeNow("syncWithMasterCache").syncWithMasterCache(masterCacheUplink);
    }

    public List findAggregatedBeanData(Operation operation, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap, Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, com.gs.fw.finder.OrderBy orderBy, boolean bypassCache, Class bean)
    {
        return initializeNow("findAggregatedBeanData").findAggregatedBeanData(operation, nameToAggregateAttributeMap, nameToGroupByAttributeMap, havingOperation, orderBy, bypassCache, bean);
    }

    @Override
    public long getLatestRefreshTime()
    {
        return 0;
    }

    @Override
    public void setLatestRefreshTime(long time)
    {
    }

    public void setDisableCache(boolean disableCache)
    {
        initializeNow("setDisableCache").setDisableCache(disableCache);
    }

    public MithraTuplePersister getMithraTuplePersister()
    {
        return initializeNow("getMithraTuplePersister").getMithraTuplePersister();
    }

    public void setIndependent(boolean independent)
    {
        initializeNow("setIndependent").setIndependent(independent);
    }

    public void setPureHome(boolean pureHome, String pureNotificationId)
    {
        initializeNow("setPureHome").setPureHome(pureHome, pureNotificationId);
    }

    public String getPureNotificationId()
    {
        return initializeNow("getPureNotificationId").getPureNotificationId();
    }

    public MithraObjectPortal getInitializedPortal()
    {
        return initializeNow("get portal");
    }

    public boolean isCacheDisabled()
    {
        return initializeNow("is cache disabled").isCacheDisabled();
    }

    public void setParentFinders(RelatedFinder[] parentFinders)
    {
        initializeNow("setParentFinders").setParentFinders(parentFinders);
    }

    public boolean isParentFinder(RelatedFinder possibleParent)
    {
        return initializeNow("isParentFinder").isParentFinder(possibleParent);
    }

    public void setTxParticipationMode(TxParticipationMode txParticipationMode, MithraTransaction tx)
    {
        initializeNow("setTxParticipationMode").setTxParticipationMode(txParticipationMode, tx);
    }

    public void setUseMultiUpdate(boolean useMultiUpdate)
    {
        initializeNow("setUseMultiUpdate").setUseMultiUpdate(useMultiUpdate);
    }

    public boolean useMultiUpdate()
    {
        return initializeNow("useMultiUpdate").useMultiUpdate();
    }

    public CachedQuery zFindInMemory(Operation op, OrderBy orderby)
    {
        return initializeNow("zFindInMemory").zFindInMemory(op, orderby);
    }

    public List zFindInMemoryWithoutAnalysis(Operation op, boolean isQueryCachable)
    {
        return initializeNow("zFindInMemoryWithoutAnalysis").zFindInMemoryWithoutAnalysis(op, isQueryCachable);
    }

    public Object unwrapRelatedObject(Object from, Object related, Extractor[] fromExtractors, Extractor[] relatedExtractors)
    {
        return initializeNow("unwrapRelatedObject").unwrapRelatedObject(from, related, fromExtractors, relatedExtractors);
    }

    public Object wrapRelatedObject(Object result)
    {
        return initializeNow("wrapRelatedObject").wrapRelatedObject(result);
    }

    public Object unwrapToManyRelatedObject(Object related)
    {
        return initializeNow("unwrapToManyRelatedObject").unwrapToManyRelatedObject(related);
    }

    public boolean mapsToUniqueIndex(List attributes)
    {
        return initializeNow("mapsToUniqueIndex").mapsToUniqueIndex(attributes);
    }

    public String getTableNameForQuery(SqlQuery sqlQuery, MapperStackImpl mapperStack, int currentSourceNumber, PersisterId persisterId)
    {
        return initializeNow("getTableNameForQuery").getTableNameForQuery(sqlQuery, mapperStack, currentSourceNumber, persisterId);
    }

    public UpdateCountHolder[] getPooledUpdateCountHolders(UpdateCountHolder[] updateCountHolders)
    {
        return initializeNow("getPooledUpdateCountHolders").getPooledUpdateCountHolders(updateCountHolders);
    }

    public int[] getPooledIntegerArray(int[] originalValues)
    {
        return initializeNow("getPooledIntegerArray").getPooledIntegerArray(originalValues);
    }

    public String getBusinessClassName()
    {
        return this.className.substring(this.className.lastIndexOf('.'));
    }


    @Override
    public void destroy()
    {
        //nothing to do
    }

    public void prepareForMassPurge(List mithraObjects)
    {
        initializeNow("prepareForMassPurge").prepareForMassPurge(mithraObjects);
    }
}