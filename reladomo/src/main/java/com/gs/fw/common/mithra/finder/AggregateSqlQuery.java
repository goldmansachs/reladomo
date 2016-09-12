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

package com.gs.fw.common.mithra.finder;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.finder.asofop.AsOfOperation;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.MappedAttribute;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


public class AggregateSqlQuery extends SqlQuery
{

    private List<com.gs.fw.common.mithra.MithraAggregateAttribute> aggregateAttributes;
    private List<MithraGroupByAttribute> groupByAttributes;
    private com.gs.fw.common.mithra.HavingOperation havingOperation;
    protected MithraDatabaseIdentifierExtractor aggrIdExtractor = new MithraDatabaseIdentifierExtractor("a");
    private boolean doneWithWhereClause;
    private AsOfEqualityChecker asOfEqualityChecker;
    private List aggAsOfAttributeWithMapperStackList;
    private StringBuilder havingClause = null;
    public AggregateSqlQuery(Operation op, List<com.gs.fw.common.mithra.MithraAggregateAttribute> aggregateAttributes,
                             List<MithraGroupByAttribute> groupByAttributes, HavingOperation havingOperation, OrderBy orderBy)
    {
        super(op, orderBy, false);
        this.aggregateAttributes = aggregateAttributes;
        this.groupByAttributes = groupByAttributes;
        this.havingOperation = havingOperation;

        MithraDatabaseIdentifierExtractor superIdExtractor = super.getIdExtractor();
        if (superIdExtractor.getRawSourceOperationMap() != null)
        {
            SourceOperation mainObjectSourceOp = superIdExtractor.getSourceOperation(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL);
            if (mainObjectSourceOp != null)
            {
                this.aggrIdExtractor.setSourceOperation(mainObjectSourceOp);
            }
        }

        Operation aggOps = NoOperation.instance();

        for(int i = 0 ; i < aggregateAttributes.size();i++)
        {
            com.gs.fw.common.mithra.MithraAggregateAttribute attr = aggregateAttributes.get(i);
            Operation attrMappedOp = attr.createMappedOperation();
            aggOps = aggOps.and(attrMappedOp);
        }

        for(int i = 0; i < groupByAttributes.size(); i++)
        {
            MithraGroupByAttribute attr = groupByAttributes.get(i);
            Attribute normalAttr = attr.getAttribute();
            if (normalAttr instanceof MappedAttribute)
            {
                Mapper mapper = ((MappedAttribute) normalAttr).getMapper();
                aggOps = aggOps.and(new MappedOperation(mapper, new All(mapper.getAnyRightAttribute())));
            }
        }

        if(havingOperation != null)
        {
            Operation havingattributeMappedOperation = havingOperation.zCreateMappedOperation();
            aggOps = aggOps.and(havingattributeMappedOperation);
        }
        if (aggOps != NoOperation.instance())
        {
            this.aggrIdExtractor.registerOperations(aggOps);
            AsOfAttribute[] asOfAttributes = op.getResultObjectPortal().getFinder().getAsOfAttributes();
            if (asOfAttributes != null)
            {
                for(AsOfAttribute attr: asOfAttributes)
                {
                    ObjectWithMapperStack asOfOpStack = this.getAnalyzedOperation().getAsOfOperationForTopLevel(attr);
                    AsOfOperation asOfOp = (AsOfOperation) asOfOpStack.getObject();
                    AtomicOperation newAsOfOp = asOfOp.createAsOfOperationCopy(attr, op);
                    if (newAsOfOp == null)
                    {
                        throw new RuntimeException("can't join with non-standard as-operation "+asOfOp.getClass().getName()+" "+attr.getAttributeName());
                    }
                    aggOps = aggOps.and((Operation)newAsOfOp);
                }
            }
            this.asOfEqualityChecker = new AsOfEqualityChecker(aggOps, null);
        }
    }

    @Override
    protected MithraDatabaseIdentifierExtractor getCurrentIdExtractor()
    {
        if(!this.isDoneWithWhereClause())
        {
            return super.getIdExtractor();
        }
        else
        {
            return this.aggrIdExtractor;
        }
    }

    @Override
    public int prepareQueryForSource(int sourceNumber, DatabaseType dt, TimeZone timeZone)
    {
        this.setDoneWithWhereClause(false);
        int queries = super.prepareQueryForSource(sourceNumber, dt, timeZone);
        this.setDoneWithWhereClause(true);
        this.aggrIdExtractor.reset();

        if (asOfEqualityChecker != null)
        {
            ObjectWithMapperStack[] mapperStacks = asOfEqualityChecker.getAllAsOfAttributesWithMapperStack();
            aggAsOfAttributeWithMapperStackList = new FastList(mapperStacks.length);
            for(ObjectWithMapperStack s: mapperStacks)
            {
                aggAsOfAttributeWithMapperStackList.add(s);
            }
            for(int i=0;i< aggAsOfAttributeWithMapperStackList.size();i++)
            {
                ObjectWithMapperStack asOfAttributeWithMapperStack = (ObjectWithMapperStack) aggAsOfAttributeWithMapperStackList.get(i);
                ObjectWithMapperStack asOfOperationStack = asOfEqualityChecker.getAsOfOperation(asOfAttributeWithMapperStack);
                if (asOfOperationStack == null)
                {
                    throw new MithraBusinessException("could not determine as of date for " + asOfAttributeWithMapperStack.getObject().toString());
                }
                AsOfOperation asOfOperation = (AsOfOperation) asOfOperationStack.getObject();
            }
        }


        for(int i = 0 ; i < aggregateAttributes.size();i++)
        {
            com.gs.fw.common.mithra.MithraAggregateAttribute attr = aggregateAttributes.get(i);
            attr.generateMapperSql(this);
        }

        for(int i = 0; i < groupByAttributes.size(); i++)
        {
            MithraGroupByAttribute attr = groupByAttributes.get(i);
            attr.getAttribute().generateMapperSql(this);
        }

        if(havingOperation != null)
        {
            if (havingClause != null)
            {
                havingClause.setLength(0);
                havingClause.append(" having ");
            }
            havingOperation.zGenerateMapperSql(this);
            havingOperation.zGenerateSql(this);
        }
        return queries;
    }

    public void addAsOfAttributeSql()
    {
        if(!this.isDoneWithWhereClause())
        {
            super.addAsOfAttributeSql();
        }
        else
        {
            if (this.aggAsOfAttributeWithMapperStackList != null)
            {
                MapperStackImpl currentMapperStack = this.aggrIdExtractor.getCurrentMapperList();
                for(int i=0;i<this.aggAsOfAttributeWithMapperStackList.size();)
                {
                    ObjectWithMapperStack asOfAttributeWithMapperStack = (ObjectWithMapperStack) aggAsOfAttributeWithMapperStackList.get(i);
                    ObjectWithMapperStack asOfOperationStack = asOfEqualityChecker.getAsOfOperation(asOfAttributeWithMapperStack);
                    if (asOfOperationStack == null)
                    {
                        throw new MithraBusinessException("could not determine as of date for " + asOfAttributeWithMapperStack.getObject().getClass().getName());
                    }
                    if (currentMapperStack.equals(asOfAttributeWithMapperStack.getMapperStack()))
                    {
                        aggAsOfAttributeWithMapperStackList.remove(i);
                        AsOfOperation asOfOperation = (AsOfOperation) asOfOperationStack.getObject();
                        if (asOfOperation.addsToAsOfOperationWhereClause(asOfAttributeWithMapperStack, asOfOperationStack))
                        {
                            asOfOperation.generateSql(this, asOfAttributeWithMapperStack, asOfOperationStack);
                            this.aggrIdExtractor.restoreMapperStack(currentMapperStack);
                        }
                    }
                    else
                    {
                        i++;
                    }
                }
            }
        }
    }

    public String getFromClauseAsString()
    {
        this.setDoneWithWhereClause(false);
        StringBuilder fromClause = new StringBuilder(super.getFromClauseAsString());
        this.setDoneWithWhereClause(true);
        this.fillStringBufferWithTableNames(fromClause);
        return fromClause.toString();
    }

    private void fillStringBufferWithTableNames(StringBuilder result)
    {
        InternalList joinClauses = aggrIdExtractor.getJoinClauses();
        if (joinClauses != null)
        {
            for(int i=0;i<joinClauses.size();i++)
            {
                JoinClause joinClause = (JoinClause) joinClauses.get(i);
                joinClause.appendJoinsToFromClause(this, result, null, null);
            }
        }
    }

    public String getDatabaseAlias(MithraObjectPortal objectPortal)
    {
        if (this.aggrIdExtractor.getCurrentMapperList().isEmpty())
        {
           return super.getDatabaseAlias(objectPortal);
        }
        else
        {
            Mapper lastMapper = this.aggrIdExtractor.getCurrentMapperList().getLastMapper();
            if (objectPortal != lastMapper.getFromPortal())
            {
                throw new RuntimeException("unexpected operation on object "+objectPortal.getFinder().getClass().getName());
            }
            return ((JoinClause) this.aggrIdExtractor.getRawMapperStackToJoinClauseMap().get(this.aggrIdExtractor.getCurrentMapperList())).getTableAlias();
        }
    }

    protected boolean isDoneWithWhereClause()
    {
        return doneWithWhereClause;
    }

    protected void setDoneWithWhereClause(boolean doneWithWhereClause)
    {
        this.doneWithWhereClause = doneWithWhereClause;
    }

    public void appendHavingClause(String s)
    {
        if (this.havingClause == null)
        {
            this.havingClause = new StringBuilder(" having ");
        }

        this.havingClause.append(s);
    }

    public String getHavingClause()
    {
        return havingClause.toString();
    }

    protected boolean maySplit(SetBasedAtomicOperation potentiallyLargeClause)
    {
        return false;
    }

    protected int setJoinParameters(PreparedStatement ps, int count) throws SQLException
    {
        count = super.setJoinParameters(ps, count);
        InternalList joinClauses = aggrIdExtractor.getJoinClauses();
        if (joinClauses != null)
        {
            for(int i=0;i<joinClauses.size();i++)
            {
                JoinClause joinClause = (JoinClause) joinClauses.get(i);
                if (joinClause.isTopLevel())
                {
                    count = joinClause.setJoinSqlParameters(ps, count, this);
                }
            }
        }
        return count;
    }

}