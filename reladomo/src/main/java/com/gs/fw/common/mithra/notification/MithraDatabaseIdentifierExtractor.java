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

package com.gs.fw.common.mithra.notification;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SourceAttributeType;
import com.gs.fw.common.mithra.connectionmanager.IntSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.finder.ExplicitJoinClause;
import com.gs.fw.common.mithra.finder.ImplicitJoinClause;
import com.gs.fw.common.mithra.finder.JoinClause;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.MapperStack;
import com.gs.fw.common.mithra.finder.MapperStackImpl;
import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SourceOperation;
import com.gs.fw.common.mithra.util.InternalList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;



public class MithraDatabaseIdentifierExtractor implements MapperStack
{

    private int tableNumber = 1;
    private static final String tablePrefix = "t";
    public static final String DEFAULT_DATABASE_ALIAS = tablePrefix + "0";
    private Map sourceOperationMap;
    private Map sourceOperationMapCopy;
    private Map equalitySourceOperationMap;
    private Map mapperStackToJoinClauseMap;
    private Map databaseIdentifierMap;
    private MapperStackImpl mapperStackImpl = new MapperStackImpl();
    private InternalList joinClauses = null;
    private String aliasPrefix = "t";
    private boolean useExplicitJoins = true;

    private  final JoinClause DEFAULT_EXPLICIT_JOIN_CLAUSE = new ExplicitJoinClause(null, 0).makeReadOnly();
    private  final JoinClause DEFAULT_IMPLICIT_JOIN_CLAUSE = new ImplicitJoinClause(null, 0);
    private boolean copySourceOnSet;

    public MithraDatabaseIdentifierExtractor()
    {
    }

    public MithraDatabaseIdentifierExtractor(String aliasPrefix)
    {
        this.aliasPrefix = aliasPrefix;
    }

    public boolean isUseExplicitJoins()
    {
        return useExplicitJoins;
    }

    public void setUseExplicitJoins(boolean useExplicitJoins)
    {
        this.useExplicitJoins = useExplicitJoins;
    }

    public Map extractDatabaseIdentifierMap(RelatedFinder finder, Set sourceAttributeValueSet)
    {
        this.databaseIdentifierMap = new UnifiedMap();
        for(Iterator it = sourceAttributeValueSet.iterator(); it.hasNext();)
        {
           this.createDatabaseIdentifier(finder.getMithraObjectPortal(), it.next());
        }
        return databaseIdentifierMap;
    }

    public void restoreMapperStack(ObjectWithMapperStack objectWithMapperStack)
    {
        this.mapperStackImpl = (MapperStackImpl) objectWithMapperStack.getMapperStack().clone();
    }

    public void restoreMapperStack(MapperStackImpl mapperStack)
    {
        this.mapperStackImpl = mapperStack;
    }

    public Map extractDatabaseIdentifierMap(Operation op)
    {
        this.databaseIdentifierMap = new UnifiedMap();
        registerOperations(op);
        this.processSourceAttributeEqualities();
        int count = this.getSourceAttributeValueCount();

        for(int i = 0; i < count; i++)
        {
            MithraObjectPortal emptyKeyPortal;
            MapperStackImpl emptyKey = MapperStackImpl.EMPTY_MAPPER_STACK_IMPL;
            SourceOperation sourceOperation = this.getSourceOperation(emptyKey);
            if(sourceOperation == null)
            {
                emptyKeyPortal = op.getResultObjectPortal();
                createDatabaseIdentifier(emptyKeyPortal);
            }
            else
            {
                emptyKeyPortal = sourceOperation.getAttribute().getOwnerPortal();
                createDatabaseIdentifier(emptyKey, emptyKeyPortal, i);
            }

            if(mapperStackToJoinClauseMap != null)
            {

                for(Iterator it = mapperStackToJoinClauseMap.keySet().iterator(); it.hasNext();)
                {
                    MapperStackImpl key = (MapperStackImpl)it.next();
                    if(key.getMapperStack().size() > 0)
                    {
                        SourceOperation relatedSourceOperation = this.getSourceOperation(key);
                        MithraObjectPortal portal = key.getLastMapper().getFromPortal();
                        if(relatedSourceOperation == null)
                        {
                              createDatabaseIdentifier(portal);
                        }
                        else
                        {
                             createDatabaseIdentifier(key, portal, i);
                        }
                    }
                }
            }
        }
        return databaseIdentifierMap;
    }

    public void registerOperations(Operation op)
    {
        op.registerOperation(this, true);
    }

    public void registerRelatedAttributeEquality(Attribute attribute)
    {
        if (this.mapperStackToJoinClauseMap != null && !this.mapperStackImpl.isEmpty() && !this.mapperStackImpl.isAtContainerBoundry())
        {
            JoinClause joinClause = (JoinClause) this.mapperStackToJoinClauseMap.get(this.mapperStackImpl);
            joinClause.addAttribute(attribute);
        }
    }

    public void registerRelatedAttributeEqualityFromMapper(Attribute attribute)
    {
        if (this.mapperStackToJoinClauseMap != null && !this.mapperStackImpl.isEmpty() && !this.mapperStackImpl.isAtContainerBoundry())
        {
            JoinClause joinClause = (JoinClause) this.mapperStackToJoinClauseMap.get(this.mapperStackImpl);
            joinClause.addAttributeFromMapper(attribute);
        }
    }

    private void createDatabaseIdentifier(MithraObjectPortal portal)
    {
        this.createDatabaseIdentifier(portal, null);
    }

    private void createDatabaseIdentifier(MithraObjectPortal portal, Object sourceAttributeValue)
    {
        if (portal.isForTempObject()) return;
        RelatedFinder finder = portal.getFinder();

        String databaseIdentifier = this.getDatabaseIdentifier(portal, sourceAttributeValue);

        DatabaseIdentifierKey dbidKey = new DatabaseIdentifierKey(sourceAttributeValue, finder);
        databaseIdentifierMap.put(dbidKey, databaseIdentifier);
    }

    private void createDatabaseIdentifier(MapperStackImpl mapper, MithraObjectPortal portal, int queryNumber)
    {
        if (portal.isForTempObject()) return;
        RelatedFinder finder = portal.getFinder();

        Object sourceAttributeValue = this.getSourceAttributeValueForSelectedObject(mapper, queryNumber);
        String databaseIdentifier = this.getDatabaseIdentifier(portal, sourceAttributeValue);

        DatabaseIdentifierKey dbidKey = new DatabaseIdentifierKey(sourceAttributeValue, finder);
        databaseIdentifierMap.put(dbidKey, databaseIdentifier);
    }

    private String getDatabaseIdentifier(MithraObjectPortal portal, Object sourceAttributeValue)
    {
        String databaseIdentifier = portal.getPureNotificationId();
        if (databaseIdentifier != null) return databaseIdentifier;
        Object connectionManager = portal.getDatabaseObject().getConnectionManager();
        SourceAttributeType sourceAttributeType = portal.getFinder().getSourceAttributeType();
        if(sourceAttributeValue == null)
        {
            databaseIdentifier = ((SourcelessConnectionManager)connectionManager).getDatabaseIdentifier();
        }
        else if(sourceAttributeType.isStringSourceAttribute())
        {
            databaseIdentifier = ((ObjectSourceConnectionManager)connectionManager).getDatabaseIdentifier(sourceAttributeValue);
        }
        else
        {
            databaseIdentifier = ((IntSourceConnectionManager)connectionManager).getDatabaseIdentifier(((Integer)sourceAttributeValue).intValue());
        }
        return databaseIdentifier;
    }

    private Object getSourceAttributeValueForSelectedObject(MapperStackImpl mapper, int queryNumber)
    {
        return this.getSourceAttributeValue(mapper, queryNumber);
        //return this.getSourceAttributeValue(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL, queryNumber);
    }


    private Object getSourceAttributeValue(MapperStackImpl mapperStack, int queryNumber)
    {
        SourceOperation so = getSourceOperation(mapperStack);
        if (so == null)
        {
            throw new MithraBusinessException("could not find source attribute");
            //throw new MithraBusinessException("could not find source attribute for " + this.getLastMapperClassName(mapperStack));
        }
        return so.getSourceAttributeValue(null, queryNumber, mapperStack.isEmpty() || so == getSourceOperation(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL));
    }

    public SourceOperation getSourceOperation(MapperStackImpl mapperStack)
    {
        processSourceAttributeEqualities();
        return (SourceOperation) this.getSourceOperationMap().get(mapperStack);
    }

    private void processSourceAttributeEqualities()
    {
        if (this.equalitySourceOperationMap != null)
        {
            ArrayList mapperStacksToProcess = new ArrayList(this.getSourceOperationMap().keySet());
            for (int i = 0; i < mapperStacksToProcess.size(); i++)
            {
                MapperStackImpl mapperStack = (MapperStackImpl) mapperStacksToProcess.get(i);
                processAliasEquality(mapperStack, mapperStacksToProcess);
            }
            this.equalitySourceOperationMap = null;
        }
    }

    public boolean hasEqualitySourceOperations()
    {
        return this.equalitySourceOperationMap != null && !this.equalitySourceOperationMap.isEmpty();
    }

    protected int getSourceAttributeValueCount()
    {
        SourceOperation so = getSourceOperation(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL);
        if (so == null) return 1;
        return so.getSourceAttributeValueCount();
    }

    private void processAliasEquality(MapperStackImpl mapperStack, ArrayList aliasesToProcess)
    {
        ArrayList equalities = (ArrayList) this.equalitySourceOperationMap.get(mapperStack);
        if (equalities != null)
        {
            for (int i = 0; i < equalities.size(); i++)
            {
                MapperStackImpl rightMapperStack = (MapperStackImpl) equalities.get(i);
                SourceOperation op = (SourceOperation) this.getSourceOperationMap().get(mapperStack);
                this.setSourceOperation(rightMapperStack, op);
                if (rightMapperStack.isAtContainerBoundry())
                {
                    MapperStackImpl copy = (MapperStackImpl) rightMapperStack.clone();
                    while(copy.isAtContainerBoundry()) copy.popMapperContainer();
                    this.setSourceOperation(copy, op);
                }
                if (!aliasesToProcess.contains(rightMapperStack)) aliasesToProcess.add(rightMapperStack);
            }
        }
    }

    public void setEqualitySourceOperation()
    {
        MapperStackImpl leftList = (MapperStackImpl) this.mapperStackImpl.clone();
        leftList.popMapper();
        while (leftList.isAtContainerBoundry()) leftList.popMapperContainer();
        MapperStackImpl rightList = (MapperStackImpl) this.mapperStackImpl.clone();

        addEqualitySourceOperation(leftList, rightList);
        addEqualitySourceOperation(rightList, leftList);
    }

    private void addEqualitySourceOperation(MapperStackImpl left, MapperStackImpl right)
    {
        ArrayList existing = (ArrayList) this.getEqualitySourceOperationMap().get(left);
        if (existing == null)
        {
            existing = new ArrayList(4);
            this.getEqualitySourceOperationMap().put(left, existing);
        }
        existing.add(right);
    }

    public boolean isMappedAlready(Mapper mapper)
    {
        if (this.mapperStackToJoinClauseMap != null)
        {
            MapperStackImpl copy = (MapperStackImpl) this.mapperStackImpl.clone();
            copy.pushMapper(mapper);
            JoinClause joinClause = (JoinClause) this.mapperStackToJoinClauseMap.get(copy);
            return joinClause != null && joinClause.isMapped();
        }
        return false;
    }

    private Map getOrCreateMapperStackToDatabaseAliasMap()
    {
        if (mapperStackToJoinClauseMap == null) mapperStackToJoinClauseMap = new UnifiedMap(3);
        return mapperStackToJoinClauseMap;
    }

    public Map getRawMapperStackToJoinClauseMap()
    {
        return mapperStackToJoinClauseMap;
    }

    public void reset()
    {
        this.copySourceOnSet = true;
        if (this.sourceOperationMapCopy != null)
        {
            this.sourceOperationMap = this.sourceOperationMapCopy;
            this.sourceOperationMapCopy = null;
        }
        if (this.joinClauses != null)
        {
            for(int i=0;i<this.joinClauses.size();i++ )
            {
                JoinClause jc = (JoinClause) this.joinClauses.get(i);
                jc.reset();
            }
        }
        if (this.databaseIdentifierMap != null)
        {
            this.databaseIdentifierMap.clear();
        }
    }

    public void setSourceOperation(SourceOperation op)
    {
        if (copySourceOnSet && this.sourceOperationMap != null && this.sourceOperationMapCopy == null)
        {
            this.sourceOperationMapCopy = new UnifiedMap(this.sourceOperationMap);
        }
        SourceOperation so = (SourceOperation) this.getSourceOperationMap().get(this.mapperStackImpl);
        if (so == null)
        {
            this.getSourceOperationMap().put(this.mapperStackImpl.clone(), op);
            if (this.mapperStackImpl.isAtContainerBoundry())
            {
                MapperStackImpl copy = (MapperStackImpl) this.mapperStackImpl.clone();
                while(copy.isAtContainerBoundry()) copy.popMapperContainer();
                this.getSourceOperationMap().put(copy, op);
            }
        }
        else
        {
            if (!so.equals(op)) throw new MithraBusinessException("can't have multiple source id operations");
        }
    }

    protected void setSourceOperation(MapperStackImpl mapperStack, SourceOperation op)
    {
        SourceOperation so = (SourceOperation) this.getSourceOperationMap().get(mapperStack);
        if (so == null)
        {
            this.getSourceOperationMap().put(mapperStack, op);
        }
        else
        {
            if (!so.isSameSourceOperation(op)) throw new MithraBusinessException("can't have multiple source id operations");
        }
    }

    private Map getSourceOperationMap()
    {
        if (sourceOperationMap == null) sourceOperationMap = new UnifiedMap(3);
        return sourceOperationMap;
    }

    public Map getRawSourceOperationMap()
    {
        return sourceOperationMap;
    }

    private Map getEqualitySourceOperationMap()
    {
        if (equalitySourceOperationMap == null) equalitySourceOperationMap = new UnifiedMap(8);
        return equalitySourceOperationMap;
    }

    public void pushMapper(Mapper mapper)
    {
        pushMapperAndGetJoinClause(mapper);
    }

    public JoinClause pushMapperAndGetJoinClause(Mapper mapper)
    {
        JoinClause parentClause = this.getCurrentJoinClause();
        this.mapperStackImpl.pushMapper(mapper);
        Map toDatabaseAliasMap = this.getOrCreateMapperStackToDatabaseAliasMap();
        JoinClause joinClause = (JoinClause) toDatabaseAliasMap.get(this.mapperStackImpl);
        if (joinClause == null)
        {
            MapperStackImpl immutableMapperStack = (MapperStackImpl) this.mapperStackImpl.clone();
            joinClause = createJoinClause(mapper.getFromPortal(), aliasPrefix, immutableMapperStack, parentClause);
            tableNumber++;
            toDatabaseAliasMap.put(immutableMapperStack, joinClause);
        }
        joinClause.setMapped(true);
        return joinClause;
    }

    private JoinClause createJoinClause(MithraObjectPortal portal, String tableAliasPrefix, MapperStackImpl immutableMapperStack, JoinClause parentClause)
    {
        JoinClause joinClause = useExplicitJoins ?
                new ExplicitJoinClause(portal, tableNumber, tableAliasPrefix, immutableMapperStack, parentClause) :
                new ImplicitJoinClause(portal, tableNumber, tableAliasPrefix, immutableMapperStack);
        if (this.joinClauses == null)
        {
            joinClauses = new InternalList(4);
        }
        this.joinClauses.add(joinClause);
        return joinClause;
    }

    public InternalList getJoinClauses()
    {
        return joinClauses;
    }

    public Mapper popMapper()
    {
        return mapperStackImpl.popMapper();
    }

    public void pushMapperContainer(Object mapper)
    {
        Map toJoinClauseMap = this.getOrCreateMapperStackToDatabaseAliasMap();
        JoinClause joinClause = useExplicitJoins ? DEFAULT_EXPLICIT_JOIN_CLAUSE : DEFAULT_IMPLICIT_JOIN_CLAUSE;
        if (!mapperStackImpl.isEmpty())
        {
            joinClause = (JoinClause) toJoinClauseMap.get(this.mapperStackImpl);
        }
        mapperStackImpl.pushMapperContainer(mapper);
        toJoinClauseMap.put(this.mapperStackImpl.clone(), joinClause);
    }

    public void popMapperContainer()
    {
        mapperStackImpl.popMapperContainer();
    }

    public ObjectWithMapperStack constructWithMapperStack(Object o)
    {
        return mapperStackImpl.constructWithMapperStack(o);
    }

    public ObjectWithMapperStack constructWithMapperStackWithoutLastMapper(Object o)
    {
        return mapperStackImpl.constructWithMapperStackWithoutLastMapper(o);
    }

    public MapperStackImpl getCurrentMapperList()
    {
        if (this.mapperStackImpl == null)
        {
            return MapperStackImpl.EMPTY_MAPPER_STACK_IMPL;
        }
        return this.mapperStackImpl;
    }

    public void clearMapperStack()
    {
        this.mapperStackImpl.clear();
    }

    public boolean isEmpty()
    {
        return this.mapperStackImpl.isFullyEmpty();
    }

    public JoinClause getCurrentJoinClause()
    {
        if (mapperStackToJoinClauseMap == null) return null;
        return ((JoinClause)this.mapperStackToJoinClauseMap.get(this.mapperStackImpl));
    }

    public JoinClause getJoinCaluseFor(MapperStackImpl mapperStack)
    {
        if (mapperStackToJoinClauseMap == null) return null;
        return ((JoinClause)this.mapperStackToJoinClauseMap.get(mapperStack));
    }

    public void computeJoinClauseAggregation()
    {
        if (this.joinClauses != null)
        {
            for(int i=0;i<this.joinClauses.size();i++)
            {
                ((JoinClause)this.joinClauses.get(i)).computeAggregationForTree();
            }
        }
    }

    public static class DatabaseIdentifierKey
    {
        private Object sourceAttributeValue;
        private RelatedFinder finder;

        public DatabaseIdentifierKey(Object sourceAttributeValue, RelatedFinder finder)
        {
            this.sourceAttributeValue = sourceAttributeValue;
            this.finder = finder;
        }

        public Object getSourceAttributeValue()
        {
            return sourceAttributeValue;
        }

        public RelatedFinder getFinder()
        {
            return finder;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final DatabaseIdentifierKey that = (DatabaseIdentifierKey) o;

            if (!finder.getClass().getName().equals(that.finder.getClass().getName())) return false;
            if (sourceAttributeValue != null ? !sourceAttributeValue.equals(that.sourceAttributeValue) : that.sourceAttributeValue != null) return false;

            return true;
        }

        public int hashCode()
        {
            int result;
            result = (sourceAttributeValue != null ? sourceAttributeValue.hashCode() : 0);
            result = 29 * result + finder.hashCode();
            return result;
        }
    }
}