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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.attribute.Attribute;
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
import com.gs.reladomo.metadata.ReladomoClassMetaData;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;



public interface MithraObjectPortal
{

    public boolean isPartiallyCached();

    public boolean isFullyCached();

    public boolean isReplicated();

    public Cache getCache();

    public QueryCache getQueryCache();

    public RelatedFinder getFinder();

    public MithraDatabaseObject getDatabaseObject();

    public UpdateCountHolder getPerClassUpdateCountHolder();

    public void incrementClassUpdateCount();

    public void clearQueryCache();

    public void setDisableCache(boolean disableCache);

    public MithraTuplePersister getMithraTuplePersister();

    public MithraObjectPersister getMithraObjectPersister();

    public MithraObjectFactory getMithraObjectFactory();

    public MithraDatedObjectFactory getMithraDatedObjectFactory();

    public TxParticipationMode getTxParticipationMode(MithraTransaction tx);

    public TxParticipationMode getTxParticipationMode();

    public void clearTxParticipationMode(MithraTransaction tx);

    public void setTxParticipationMode(TxParticipationMode txParticipationMode, MithraTransaction tx);

    public void reloadCache();

    public RenewedCacheStats renewCacheForOperation(Operation op);

    public void loadCache();

    public List find(Operation op, boolean bypassCache);

    public List find(Operation op, boolean bypassCache, boolean forceImplicitJoin);

    public Cursor findCursorFromServer(Operation op, Filter postLoadOperation, OrderBy orderby, int maxObjectsToRetrieve, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin);

    public int count(Operation op);

    public List computeFunction(Operation op, OrderBy orderby, String sqlExpression,
                                ResultSetParser resultSetParser);

    public Object getAsOneFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1);

    public Object getAsOneByIndexFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1, int indexRef);

    public Object getAsOneFromCacheForFind(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1);

    public Object getAsOneByIndexFromCacheForFind(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1, int indexRef);

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship,
                                         int maxObjectsToRetrieve);

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship,
                                         int maxObjectsToRetrieve, boolean forceImplicitJoin);

    public CachedQuery findAsCachedQuery(Operation op, OrderBy orderby, boolean bypassCache, boolean forRelationship,
                                         int maxObjectsToRetrieve, int numOfParallelThreads, boolean forceImplicitJoin);

    public CachedQuery zFindInMemory(Operation op, OrderBy orderby);

    public List zFindInMemoryWithoutAnalysis(Operation op, boolean isQueryCachable);

    public List findForMassDeleteInMemory(Operation op, MithraTransaction tx);

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin);

    public void prepareForMassPurge(List mithraObjects);


    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin);

    public void registerForNotification(String subject);

    public void registerForApplicationNotification(String subject, MithraApplicationNotificationListener listener,
                                                   List mithraObjectList, Operation operation);

    public void registerForApplicationClassLevelNotification(MithraApplicationClassLevelNotificationListener listener);

    public void registerForApplicationClassLevelNotification(Set sourceAttributeValueSet, MithraApplicationClassLevelNotificationListener listener);

    public Map extractDatabaseIdentifiers(Operation op);

    public Map extractDatabaseIdentifiers(Set sourceAttributeValueSet);

    public MithraDataObject refresh(MithraDataObject data, boolean lockInDatabase) throws MithraDatabaseException;

    public MithraDataObject refreshDatedObject(MithraDatedObject mithraDatedObject, boolean lockInDatabase) throws MithraDatabaseException;

    public boolean useMultiUpdate();

    public void setUseMultiUpdate(boolean useMultiUpdate);

    public void setDefaultTxParticipationMode(TxParticipationMode mode);

    public List<AggregateData> findAggregatedData(Operation op, Map<String, MithraAggregateAttribute> aggregateAttribute,
                                                  Map<String, MithraGroupByAttribute> groupByAttribute, HavingOperation havingOperation, com.gs.fw.finder.OrderBy orderBy, boolean bypassCache);

    public MithraObjectPortal[] getSuperClassPortals();

    public MithraObjectPortal[] getJoinedSubClassPortals();

    public String getUniqueAlias();

    public void appendJoinToSuper(StringBuilder buffer, String defaultDatabaseAlias);

    public int getHierarchyDepth();

    public MithraPerformanceData getPerformanceData();

    public boolean isIndependent();

    public void setIndependent(boolean independent);

    public MithraObjectDeserializer getMithraObjectDeserializer();

    public PersisterId getPersisterId();

    public void setPersisterId(PersisterId persisterId);

    public boolean isForTempObject();

    public void setForTempObject(boolean forTempObject);

    public boolean isPureHome();

    public void setPureHome(boolean pureHome, String pureNotificationId);

    public String getPureNotificationId();

    public MithraObjectPortal getInitializedPortal();

    public boolean isCacheDisabled();

    public void setParentFinders(RelatedFinder[] parentFinders);

    public boolean isParentFinder(RelatedFinder possibleParent);

    public Object unwrapRelatedObject(Object from, Object related, Extractor[] fromExtractors, Extractor[] relatedExtractors);

    public Object wrapRelatedObject(Object result);

    public Object unwrapToManyRelatedObject(Object related);

    public boolean mapsToUniqueIndex(List attributes);

    public String getTableNameForQuery(SqlQuery sqlQuery, MapperStackImpl mapperStack, int currentSourceNumber, PersisterId persisterId);

    public UpdateCountHolder[] getPooledUpdateCountHolders(UpdateCountHolder[] updateCountHolders);

    public int[] getPooledIntegerArray(int[] originalValues);

    public String getBusinessClassName();

    public void destroy();

    public List findAggregatedBeanData(Operation operation, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                       Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, com.gs.fw.finder.OrderBy orderBy, boolean bypassCache, Class bean);

    public boolean syncWithMasterCache(MasterCacheUplink masterCacheUplink);

    public long getLatestRefreshTime();

    public void setLatestRefreshTime(long time);

    public Attribute[] zGetAddressingAttributes();

    public MithraDataObject zChooseDataForMultiupdate(MithraTransactionalObject obj);

    public ReladomoClassMetaData getClassMetaData();

}