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
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TemporalAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.asofop.AsOfOperation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.InternalList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;



public class SqlQuery implements MapperStack
{

    private static final Logger logger = LoggerFactory.getLogger(SqlQuery.class);
    public static final String DEFAULT_DATABASE_ALIAS = MithraDatabaseIdentifierExtractor.DEFAULT_DATABASE_ALIAS;
    private static final int CHARS_PER_TABLE_NAME = 20;

    private WhereClause whereClause = new WhereClause(this);
    private StringBuilder orderByClause = null;

//    private InternalList sqlParameterSetters = new InternalList(3);

    private InternalList setOperationWhereClausePositionList;
    private SetBasedOpAndPosition largeInClause;
    private MithraDatabaseIdentifierExtractor idExtractor = new MithraDatabaseIdentifierExtractor();

    private AnalyzedOperation analyzedOperation;
    private OrderBy orderby;
    private int clauseCount;
    private int totalInClauseParameters = 0;
    private int numberOfChunksPerIn = 0;
    private int numberOfUnions = 1;
    private int numberOfQueries = 0;
    private String firstWhereClause;
    private String lastWhereClause;
    private int currentUnionNumber = 0;
    private boolean useDatabaseAliasInSqlQuery = true;
    private List asOfAttributeWithMapperStackList;
    private boolean forceServerSideOrderBy = false;
    private DatabaseType databaseType;
    private boolean preparedOnce = false;
    private int currentSourceNumber;
    private int maxUnionCount = -1;
    private int tempTableNumber = 0;

    private TimeZone timeZone = null;
    private int currentQueryNumber;
    private int finalUnionNumber = 1;
    private boolean notExists = false;
    private boolean disableTempTableJoin;
    private InternalList tupleTempContextList;
    private UnifiedSet subSelectedInClauses;
    private String extraSelectColumns;
    private boolean isParallel = false;
    private UnifiedMap<String, String> derivedColumnSubstitutionMap;

    public SqlQuery(Operation op, OrderBy orderBy, boolean forceImplicitJoin)
    {
        this(new AnalyzedOperation(op), orderBy, forceImplicitJoin);
    }

    public SqlQuery(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forceExplicitJoin)
    {
        this.analyzedOperation = analyzedOperation;
        this.orderby = orderby;
        this.idExtractor.setUseExplicitJoins(!forceExplicitJoin && !analyzedOperation.getOriginalOperation().zHazTriangleJoins());
        this.idExtractor.registerOperations(this.analyzedOperation.getAnalyzedOperation());
        this.idExtractor.computeJoinClauseAggregation();
        if (orderby != null)
        {
            orderby.registerSourceOperation(this.idExtractor);
        }
    }

    protected MithraDatabaseIdentifierExtractor getCurrentIdExtractor()
    {
        return this.idExtractor;
    }

    public void prepareForQuery(int currentQueryNumber) throws SQLException
    {
        this.currentQueryNumber = currentQueryNumber;
        if (currentQueryNumber == numberOfQueries - 1)
        {
            this.numberOfUnions = this.finalUnionNumber;
        }
    }

    public void setMaxUnionCount(int maxUnionCount)
    {
        this.maxUnionCount = maxUnionCount;
    }

    public AnalyzedOperation getAnalyzedOperation()
    {
        return analyzedOperation;
    }

    public DatabaseType getDatabaseType()
    {
        return databaseType;
    }

    public int prepareQueryForSource(int sourceNumber, DatabaseType dt, TimeZone timeZone)
    {
        return prepareQueryForSource(sourceNumber, dt, timeZone, false);
    }

    public boolean isParallel()
    {
        return this.isParallel;
    }

    public int prepareQueryForSource(int sourceNumber, DatabaseType dt, TimeZone timeZone, boolean isParallel)
    {
        this.currentSourceNumber = sourceNumber;
        this.timeZone = timeZone;
        this.isParallel = isParallel;
        getCurrentIdExtractor().reset();
        this.databaseType = dt;
        this.whereClause.clear();
        if (this.derivedColumnSubstitutionMap != null)
        {
            this.derivedColumnSubstitutionMap.clear();
        }
        totalInClauseParameters = 0;
        largeInClause = null;
        numberOfChunksPerIn = 0;
        numberOfQueries = 0;
        numberOfUnions = 1;
        finalUnionNumber = 1;
        this.currentQueryNumber = 0;
        firstWhereClause = null;
        lastWhereClause = null;
        currentUnionNumber = 0;
        if (orderByClause != null)
        {
            orderByClause.setLength(0);
        }
        if (setOperationWhereClausePositionList != null)
        {
            setOperationWhereClausePositionList.clear();
        }
        prepareQuery();
        return this.getNumberOfQueries();
    }

    private String safeToString()
    {
        try
        {
            return this.analyzedOperation.getOriginalOperation().toString();
        }
        catch(Throwable t)
        {
            // for badly constructed operations, even toString can fail.
            return "<unprintable operation>";
        }
    }

    private void prepareQuery()
    {
        Operation op = this.analyzedOperation.getAnalyzedOperation();
        boolean foundAsOfAttributes = analyzedOperation.hasAsOfAttributes();
        if (!preparedOnce)
        {
            clauseCount = op.getClauseCount(this);
            if (foundAsOfAttributes)
            {
                ObjectWithMapperStack[] mapperStacks = analyzedOperation.getAllAsOfAttributes();
                asOfAttributeWithMapperStackList = new FastList(mapperStacks.length);
                for(ObjectWithMapperStack s: mapperStacks)
                {
                    asOfAttributeWithMapperStackList.add(s);
                }
                for(int i=0;i<asOfAttributeWithMapperStackList.size();i++)
                {
                    ObjectWithMapperStack asOfAttributeWithMapperStack = (ObjectWithMapperStack) asOfAttributeWithMapperStackList.get(i);
                    ObjectWithMapperStack asOfOperationStack = analyzedOperation.getAsOfOperation(asOfAttributeWithMapperStack);
                    if (asOfOperationStack == null && ((TemporalAttribute)asOfAttributeWithMapperStack.getObject()).isAsOfAttribute())
                    {
                        throw new MithraBusinessException("could not determine as of date for " + asOfAttributeWithMapperStack.getObject().toString()+" in operation "+safeToString());
                    }
                }
            }
        }
        boolean queryHasOrderBy = orderby != null && (orderby.mustUseServerSideOrderBy() || this.forceServerSideOrderBy);

		op.generateSql(this);

        if (!getCurrentIdExtractor().isEmpty()) throw new RuntimeException("op.generateSql mappers not popped properly! "+safeToString());

		if (queryHasOrderBy )
		{
			orderby.generateMapperSql(this);//this must happen here for the mapper sql to be inserted at the right place
            if (!getCurrentIdExtractor().isEmpty()) throw new RuntimeException("orderby.generateMapperSql mappers not popped properly! "+safeToString());
		}

        if (foundAsOfAttributes)
        {
            for (int i=0;i<asOfAttributeWithMapperStackList.size();i++)
            {
                ObjectWithMapperStack asOfAttributeWithMapperStack = (ObjectWithMapperStack) asOfAttributeWithMapperStackList.get(i);
                ObjectWithMapperStack asOfOperationStack = analyzedOperation.getAsOfOperation(asOfAttributeWithMapperStack);
                AsOfOperation asOfOperation = (AsOfOperation) asOfOperationStack.getObject();
                if (asOfOperation.addsToAsOfOperationWhereClause(asOfAttributeWithMapperStack, asOfOperationStack))
                {
                    asOfOperation.generateSql(this, asOfAttributeWithMapperStack, asOfOperationStack);
                    getCurrentIdExtractor().clearMapperStack();
                }
            }
        }
        if (queryHasOrderBy)
        {
            this.orderby.generateSql(this);
            if (!getCurrentIdExtractor().isEmpty()) throw new RuntimeException("orderby.generateSql mappers not popped properly! "+safeToString());
        }
        preparedOnce = true;
    }

    public void setForceServerSideOrderBy(boolean forceServerSideOrderBy)
    {
        this.forceServerSideOrderBy = forceServerSideOrderBy;
    }

    public void restoreMapperStack(ObjectWithMapperStack objectWithMapperStack)
    {
        getCurrentIdExtractor().restoreMapperStack(objectWithMapperStack);
    }

	public void setUseDatabaseAliasInSqlQuery(boolean useDatabaseAliasInSqlQuery)
    {
        this.useDatabaseAliasInSqlQuery = useDatabaseAliasInSqlQuery;
    }

    public String getDatabaseAlias(MithraObjectPortal objectPortal)
    {
        if (!this.useDatabaseAliasInSqlQuery) return null;
        Operation op = this.analyzedOperation.getAnalyzedOperation();
        if (getCurrentIdExtractor().getCurrentMapperList().isEmpty())
        {
            if (objectPortal == op.getResultObjectPortal())
            {
                return DEFAULT_DATABASE_ALIAS;
            }
            else
            {
                throw new RuntimeException("unexpected top level operation on object "+objectPortal.getFinder().getClass().getName()+" in operation "+safeToString());
            }
        }
        else
        {
            Mapper lastMapper = getCurrentIdExtractor().getCurrentMapperList().getLastMapper();
            if (objectPortal != lastMapper.getFromPortal())
            {
                throw new RuntimeException("unexpected operation on object "+objectPortal.getFinder().getClass().getName()+" in operation "+safeToString());
            }
            return ((JoinClause) getCurrentIdExtractor().getRawMapperStackToJoinClauseMap().get(getCurrentIdExtractor().getCurrentMapperList())).getTableAlias();
        }
    }

    private WhereClause getActiveWhereClause()
    {
        WhereClause result = null;
        JoinClause joinClause = getCurrentIdExtractor().getCurrentJoinClause();
        if (joinClause != null && joinClause.getWhereClause() != null)
        {
            result = joinClause.getWhereClause();
        }
        if (result == null) result = this.whereClause;
        return result;
    }

    public void beginBracket()
    {
        this.getActiveWhereClause().beginBracket();
    }

    public boolean endBracket()
    {
        return this.getActiveWhereClause().endBracket();
    }

    public void beginAnd()
    {
        this.getActiveWhereClause().beginAnd();
    }

    public void endAnd()
    {
        this.getActiveWhereClause().endAnd();
    }

    public void beginOr()
    {
        this.getActiveWhereClause().beginOr();
    }

    public void endOr()
    {
        this.getActiveWhereClause().endOr();
    }

    public int getWhereClauseLength()
    {
        return this.getActiveWhereClause().length();
    }

    public void appendWhereClause(CharSequence clause)
    {
        this.getActiveWhereClause().appendWithSpace(clause);
    }

    public void addSqlParameterSetter(SqlParameterSetter sqlParameterSetter)
    {
        this.getActiveWhereClause().addSqlParameterSetter(sqlParameterSetter);
    }

    public String getWhereClauseAsString(int unionNumber)
    {
        if (firstWhereClause == null)
        {
            return whereClause.toString();
        }
        else
        {
            if (unionNumber == this.numberOfUnions - 1 && this.currentQueryNumber == this.numberOfQueries - 1) return this.lastWhereClause;
            return this.firstWhereClause;
        }
    }

    public String getTableName(MithraObjectPortal objectPortal, MapperStackImpl mapperStack)
    {
        return objectPortal.getTableNameForQuery(this, mapperStack, this.currentSourceNumber,
                this.analyzedOperation.getOriginalOperation().getResultObjectPortal().getPersisterId());
    }

    protected void appendLockMode(StringBuilder buffer, MithraObjectPortal portal, String withLock, String withoutLock)
    {
        if (withLock != null)
        {
            buffer.append(' ');
            if (portal.getTxParticipationMode().mustLockOnRead())
            {
                buffer.append(withLock);
            }
            else
            {
                buffer.append(withoutLock);
            }
        }
    }

    private void appendSelectedTableFromClause(StringBuilder buffer, String withLock, String withoutLock)
    {
        Operation op = this.analyzedOperation.getAnalyzedOperation();
        MithraObjectPortal portal = op.getResultObjectPortal();
        MithraObjectPortal[] superClasses = portal.getSuperClassPortals();
        if (superClasses != null)
        {
            buffer.append(this.getTableName(superClasses[0], MapperStackImpl.EMPTY_MAPPER_STACK_IMPL));
            if (this.useDatabaseAliasInSqlQuery)
            {
                buffer.append(' ').append(DEFAULT_DATABASE_ALIAS);
            }
            this.appendLockMode(buffer, superClasses[0], withLock, withoutLock);
            for(int i=1;i<superClasses.length;i++)
            {
                joinWithSuper(buffer, " JOIN ", MapperStackImpl.EMPTY_MAPPER_STACK_IMPL, DEFAULT_DATABASE_ALIAS, superClasses[i], withLock, withoutLock);
            }
            joinWithSuper(buffer, " JOIN ", MapperStackImpl.EMPTY_MAPPER_STACK_IMPL, DEFAULT_DATABASE_ALIAS, portal, withLock, withoutLock);
        }
        else
        {
            buffer.append(this.getTableName(portal, MapperStackImpl.EMPTY_MAPPER_STACK_IMPL));
            if (this.useDatabaseAliasInSqlQuery)
            {
                buffer.append(' ').append(DEFAULT_DATABASE_ALIAS);
            }
            this.appendLockMode(buffer, portal, withLock, withoutLock);
        }
        MithraObjectPortal[] joinedSubclasses = portal.getJoinedSubClassPortals();
        if (joinedSubclasses != null)
        {
            for(int i=0;i<joinedSubclasses.length;i++)
            {
                joinWithSuper(buffer, " LEFT JOIN ", MapperStackImpl.EMPTY_MAPPER_STACK_IMPL, DEFAULT_DATABASE_ALIAS,
                        joinedSubclasses[i], withLock,  withoutLock);
            }
        }
    }

    protected void joinWithSuper(StringBuilder buffer, String join, MapperStackImpl stack, String tableAlias,
            MithraObjectPortal portal, String withLock, String withoutLock)
    {
        buffer.append(join);
        buffer.append(this.getTableName(portal, stack));
        if (this.useDatabaseAliasInSqlQuery)
        {
            buffer.append(' ').append(tableAlias).append(portal.getUniqueAlias());
        }
        this.appendLockMode(buffer, portal, withLock, withoutLock);
        buffer.append(" ON ");
        portal.appendJoinToSuper(buffer, tableAlias);
    }

    public void appendFromClauseWithPerTableLocking(StringBuilder buffer, String withLock, String withoutLock)
    {
        appendSelectedTableFromClause(buffer, withLock, withoutLock);
        if (getIdExtractor().getRawMapperStackToJoinClauseMap() != null)
        {
            fillStringBufferWithTableNames(buffer, withLock, withoutLock);
        }
        this.whereClause.appendTempTableFromClause(this, buffer);
    }

    private void fillStringBufferWithTableNames(StringBuilder result, String withLock, String withoutLock)
    {
        InternalList joinClauses = getIdExtractor().getJoinClauses();
        if (joinClauses != null)
        {
            for(int i=0;i<joinClauses.size();i++)
            {
                JoinClause joinClause = (JoinClause) joinClauses.get(i);
                joinClause.appendJoinsToFromClause(this, result, withLock, withoutLock);
            }
        }
    }

    public String getFromClauseAsString()
    {
        Operation op = this.analyzedOperation.getAnalyzedOperation();
        Map mapperStackToDatabaseAliasMap = getIdExtractor().getRawMapperStackToJoinClauseMap();
        if (mapperStackToDatabaseAliasMap != null)
        {
            StringBuilder fromClause = new StringBuilder(CHARS_PER_TABLE_NAME * (mapperStackToDatabaseAliasMap.size() + 1));
            appendSelectedTableFromClause(fromClause, null, null);
            fillStringBufferWithTableNames(fromClause, null, null);
            this.whereClause.appendTempTableFromClause(this, fromClause);
            return fromClause.toString();
        }
        else
        {
            //todo: fix this for joined subclass
            StringBuilder fromClause = new StringBuilder(this.getTableName(op.getResultObjectPortal(), MapperStackImpl.EMPTY_MAPPER_STACK_IMPL));
            if (this.useDatabaseAliasInSqlQuery)
            {
                fromClause.append(' ').append(DEFAULT_DATABASE_ALIAS);
            }
            this.whereClause.appendTempTableFromClause(this, fromClause);
            return fromClause.toString();
        }
    }

    public void appendFromClause(StringBuilder builder)
    {
        appendSelectedTableFromClause(builder, null, null);
        if (getIdExtractor().getRawMapperStackToJoinClauseMap() != null)
        {
            fillStringBufferWithTableNames(builder, null, null);
        }
        this.whereClause.appendTempTableFromClause(this, builder);
    }

    public boolean isMappedAlready(Mapper mapper)
    {
        return getCurrentIdExtractor().isMappedAlready(mapper);
    }

    private String getLastMapperClassName(MapperStackImpl mapperStack)
    {
        if (mapperStack.isEmpty())
        {
            return this.analyzedOperation.getOriginalOperation().getResultObjectPortal().getFinder().getClass().getName();
        }
        else
        {
            Mapper lastMapper = mapperStack.getLastMapper();
            return lastMapper.getFromPortal().getFinder().getClass().getName();
        }
    }

    public Object getSourceAttributeValueForCurrentSource()
    {
        return this.getSourceAttributeValue(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL, this.currentSourceNumber);
    }

    public int getCurrentSourceNumber()
    {
        return currentSourceNumber;
    }

    public Object getSourceAttributeValueForSelectedObject(int queryNumber)
    {
        return this.getSourceAttributeValue(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL, queryNumber);
    }

    public Object getSourceAttributeValue(MapperStackImpl mapperStack, int sourceNumber)
    {
        SourceOperation so = getSourceOperation(mapperStack);
        if (so == null)
        {
            throw new MithraBusinessException("could not find source attribute for " + this.getLastMapperClassName(mapperStack)+" in operation "+safeToString());
        }
        return so.getSourceAttributeValue(this, sourceNumber, mapperStack.isEmpty() || so == getSourceOperation(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL));
    }

    public void setSourceOperation(SourceOperation op)
    {
        getIdExtractor().setSourceOperation(op);
    }

    public void registerRelatedAttributeEquality(Attribute attribute)
    {
        getIdExtractor().registerRelatedAttributeEquality(attribute);
    }

    protected SourceOperation getSourceOperation(MapperStackImpl mapperStack)
    {
        return getCurrentIdExtractor().getSourceOperation(mapperStack);
    }

    protected int getSourceAttributeValueCount()
    {
        SourceOperation so = getSourceOperation(MapperStackImpl.EMPTY_MAPPER_STACK_IMPL);
        if (so == null) return 1;
        return so.getSourceAttributeValueCount();
    }

    public void setStatementParameters(PreparedStatement ps) throws SQLException
    {
        int count = 1;
        for (int u = 0; u < this.numberOfUnions; u++)
        {
            this.currentUnionNumber = u + currentQueryNumber * getMaxUnionCount();
            count = setJoinParameters(ps, count);
            count = this.whereClause.setSqlParameters(this, ps, count, this);
        }
    }

    protected int setJoinParameters(PreparedStatement ps, int count) throws SQLException
    {
        InternalList joinClauses = getIdExtractor().getJoinClauses();
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

    public int getCurrentUnionNumber()
    {
        return this.currentUnionNumber;
    }

    public int getNumberOfSources()
    {
        if (getIdExtractor().getRawSourceOperationMap() == null) return 1;
        return this.getSourceAttributeValueCount();
    }

    public void addOrderBy(String orderByColumnName, boolean ascending)
    {
        if (this.orderByClause == null)
        {
            this.orderByClause = new StringBuilder();
        }
        else
        {
            this.orderByClause.append(", ");
        }
        JoinClause joinClause = this.getCurrentIdExtractor().getCurrentJoinClause();
        if (joinClause != null)
        {
            orderByColumnName = joinClause.getFullyQualifiedOrderByColumnNameFor(orderByColumnName);
            if (this.extraSelectColumns == null)
            {
                this.extraSelectColumns = ","+orderByColumnName;
            }
            else
            {
                this.extraSelectColumns = extraSelectColumns +","+orderByColumnName;
            }
        }
        this.orderByClause.append(orderByColumnName);
        if (!ascending) this.orderByClause.append(" desc");
    }

    public String getOrderByClause()
    {
        if (this.orderByClause == null) return "";
        return this.orderByClause.toString();
    }

    public boolean mayNeedToSplit()
    {
        return clauseCount > this.databaseType.getMaxSearchableArguments();
    }

    public int getNumberOfUnions()
    {
        return this.numberOfUnions;
    }

    public boolean hasChunkedUnions(SetBasedAtomicOperation setBasedAtomicOperation)
    {
        return this.largeInClause != null && this.largeInClause.getOp().equals(setBasedAtomicOperation);
    }

    protected int getNumberOfQueries()
    {
        this.numberOfQueries = 1;
        if (!mayNeedToSplit() || this.setOperationWhereClausePositionList == null)
        {
            return this.numberOfQueries;
        }
        int dbMaxClauses = this.databaseType.getMaxClauses();
        int dbMaxSearchableArgs = this.databaseType.getMaxSearchableArguments();
        int maxClauseCount = dbMaxClauses;
        if (clauseCount - totalInClauseParameters > maxClauseCount)
        {
            logger.warn("Query has too many parameters and cannot be broken up. Max database params: "+
                    maxClauseCount+". Query has "+clauseCount+" parameters, of which "+totalInClauseParameters+" are large in-clauses.");
        }
        this.setOperationWhereClausePositionList.sort();
        if (dbMaxClauses > dbMaxSearchableArgs)
        {
            int largestInClauseParamCount = ((SetBasedOpAndPosition)this.setOperationWhereClausePositionList.get(0)).getOp().getSetSize();
            int minClauseCount = clauseCount - largestInClauseParamCount + (largestInClauseParamCount > 0 ? 10 : 0);
            if (minClauseCount < dbMaxSearchableArgs)
            {
                maxClauseCount = dbMaxSearchableArgs;
            }
        }
        int countSoFar = fillInSetBasedOperations(maxClauseCount);
        if (largeInClause == null)
        {
            return this.numberOfQueries;
        }
        fillInSplitInClause(maxClauseCount, countSoFar);
        return this.numberOfQueries;
    }

    private void fillInSplitInClause(int maxClauseCount, int countSoFar)
    {
        int totalLeft = maxClauseCount - countSoFar;
        int totalNeeded = largeInClause.getOp().getSetSize();
        numberOfChunksPerIn = totalNeeded / totalLeft;
        if (totalNeeded % totalLeft > 0) numberOfChunksPerIn++;
        this.numberOfUnions = numberOfChunksPerIn;
        this.finalUnionNumber = numberOfChunksPerIn;
        int maxUnionCount = getMaxUnionCount();
        if (this.numberOfUnions > maxUnionCount)
        {
            this.numberOfQueries = (this.numberOfUnions/ maxUnionCount);
            this.finalUnionNumber = this.numberOfUnions % maxUnionCount;
            if (finalUnionNumber > 0)
            {
                this.numberOfQueries++;
            }
            else
            {
                finalUnionNumber = maxUnionCount;
            }
            this.numberOfUnions = maxUnionCount;
        }
        fillSplitWhereClause(largeInClause);
    }

    private int fillInSetBasedOperations(int maxClauseCount)
    {
        int countSoFar = clauseCount - totalInClauseParameters;
        int totalInClauseLeft = totalInClauseParameters;
        int fillRestIndex = -1;
        for(int i=0;i<setOperationWhereClausePositionList.size();i++)
        {
            SetBasedOpAndPosition opAndPosition = (SetBasedOpAndPosition) setOperationWhereClausePositionList.get(i);
            if (countSoFar + totalInClauseLeft > maxClauseCount)
            {
                int setSize = opAndPosition.getOp().getSetSize();
                if ((opAndPosition.isInsideAggregateJoin() || setSize > this.databaseType.getUseTempTableThreshold()))
                {
                    replaceSetBasedWithTemp(opAndPosition);
                    totalInClauseLeft -= setSize;
                }
                else
                {
                    // need to decide between using a temp table and in-clause splitting
                    if (countSoFar + totalInClauseLeft - setSize + 10 < maxClauseCount
                            && maySplit(opAndPosition.getOp()) && largeInClause == null && !opAndPosition.isInExistsOrOrClause())
                    {
                        // split
                        largeInClause = opAndPosition;
                        fillRestIndex = i+1;
                        break;
                    }
                    else
                    {
                        // use temp table
                        replaceSetBasedWithTemp(opAndPosition);
                        totalInClauseLeft -= setSize;
                    }
                }
            }
            else
            {
                fillRestIndex = i;
                break;
            }
        }
        countSoFar = fillRestOfSetBasedOperations(countSoFar, fillRestIndex);
        return countSoFar;
    }

    private int fillRestOfSetBasedOperations(int countSoFar, int fillRestIndex)
    {
        if (fillRestIndex >= 0)
        {
            for(int i=fillRestIndex;i<setOperationWhereClausePositionList.size();i++)
            {
                SetBasedOpAndPosition opAndPosition = (SetBasedOpAndPosition) setOperationWhereClausePositionList.get(i);
                int setSize = opAndPosition.getOp().getSetSize();
                countSoFar += setSize;
                // now fix the where string
                insertSubSelectForSetBasedOp(opAndPosition, createQuestionMarks(setSize));
            }
        }
        return countSoFar;
    }

    public int addTempContext(TupleTempContext tupleTempContext)
    {
        if (tupleTempContextList == null) tupleTempContextList = new InternalList(4);
        tupleTempContextList.add(tupleTempContext);
        return tempTableNumber++;
    }

    private void replaceSetBasedWithTemp(SetBasedOpAndPosition largeOpAndPosition)
    {
        SetBasedAtomicOperation largeOp = largeOpAndPosition.getOp();
        TupleTempContext tempContext = createTempContext(largeOp);
        Object source = null;
        MithraObjectPortal resultPortal = this.getAnalyzedOperation().getOriginalOperation().getResultObjectPortal();
        if (resultPortal.getFinder().getSourceAttribute() != null)
        {
            source = this.getSourceAttributeValueForCurrentSource();
        }
        if (largeOpAndPosition.allowsInClauseTempJoinReplacement(this))
        {
            MapperStackImpl currentMapperList = this.getCurrentIdExtractor().getCurrentMapperList();
            largeOpAndPosition.restoreMapper(this);
            largeOp.generateTupleTempContextJoinSql(this, tempContext, source, resultPortal.getPersisterId(), largeOpAndPosition.getPosition(), largeOpAndPosition.isInOrClause());
            this.getCurrentIdExtractor().restoreMapperStack(currentMapperList);
        }
        else
        {
            String sql = largeOp.getSubSelectStringForTupleTempContext(tempContext,
                    source, resultPortal.getPersisterId());
            insertSubSelectForSetBasedOp(largeOpAndPosition, sql);
        }
    }

    private boolean allowsInClauseTempJoinReplacement(JoinClause joinClause)
    {
        if (disableTempTableJoin) return false;
        if (joinClause == null)
        {
            return this.getCurrentIdExtractor().isUseExplicitJoins();
        }
        else
        {
            return joinClause.allowsInClauseTempJoinReplacement();
        }
    }

    private TupleTempContext createTempContext(SetBasedAtomicOperation largeOp)
    {
        if (tupleTempContextList == null) tupleTempContextList = new InternalList(4);
        if (subSelectedInClauses == null) subSelectedInClauses = new UnifiedSet(4);
        subSelectedInClauses.add(largeOp);
        TupleTempContext tempContext = largeOp.createTempContextAndInsert(this);
        tupleTempContextList.add(tempContext);
        return tempContext;
    }

    public void addTupleTempContextJoin(String tempTableName, boolean innerJoin, String onClause, String textToReplace, String newText, int position)
    {
        TempTableJoin tempTableJoin = new TempTableJoin(tempTableName, innerJoin, onClause);
        WhereClause activeWhereClause = this.getActiveWhereClause();
        activeWhereClause.addTempTableJoin(tempTableJoin);
        int shifted = activeWhereClause.replaceTextWithAnd(position + 1 - textToReplace.length(), textToReplace.length(), newText);
        for (int j = 0; j < setOperationWhereClausePositionList.size(); j++)
        {
            ((SetBasedOpAndPosition) setOperationWhereClausePositionList.get(j)).incrementPosition(position, shifted, activeWhereClause);
        }
    }

    private void insertSubSelectForSetBasedOp(SetBasedOpAndPosition opAndPosition, String sql)
    {
        opAndPosition.insertSql(sql, this);
        for (int j = 0; j < setOperationWhereClausePositionList.size(); j++)
        {
            ((SetBasedOpAndPosition) setOperationWhereClausePositionList.get(j)).incrementPosition(sql.length(), opAndPosition);
        }
    }

    protected boolean maySplit(SetBasedAtomicOperation potentiallyLargeClause)
    {
        return potentiallyLargeClause.maySplit();
    }

    private int getMaxUnionCount()
    {
        if (this.maxUnionCount >=0 ) return this.maxUnionCount;
        return this.databaseType.getMaxUnionCount();
    }

    private void fillSplitWhereClause(SetBasedOpAndPosition opAndPosition)
    {
        SetBasedAtomicOperation setBasedAtomicOperation = opAndPosition.getOp();
        int position = opAndPosition.getPosition();
        int numberOfQuestions = setBasedAtomicOperation.getSetSize() / numberOfChunksPerIn;
        StringBuilder firstWhereBuffer = new StringBuilder(this.whereClause.length() + numberOfQuestions * 2);
        firstWhereBuffer.append(this.whereClause);
        // now fix the where string
        if ((setBasedAtomicOperation.getSetSize() % numberOfChunksPerIn) > 0) numberOfQuestions++;
        String bunchOfQuestionMarks = createQuestionMarks(numberOfQuestions);
        firstWhereBuffer.insert(position, bunchOfQuestionMarks);
        numberOfQuestions = setBasedAtomicOperation.getSetSize() - ((numberOfChunksPerIn - 1) * numberOfQuestions);
        bunchOfQuestionMarks = createQuestionMarks(numberOfQuestions);
        StringBuilder lastWhereBuffer = new StringBuilder(this.whereClause.length() + numberOfQuestions * 2);
        lastWhereBuffer.append(this.whereClause);
        lastWhereBuffer.insert(position, bunchOfQuestionMarks);
        this.firstWhereClause = firstWhereBuffer.toString();
        this.lastWhereClause = lastWhereBuffer.toString();
    }

    private String createQuestionMarks(int numberOfQuestions)
    {
        int questionLength = (numberOfQuestions - 1) * 2 + 1;
        StringBuilder bunchOfQuestionMarks = new StringBuilder(questionLength);
        bunchOfQuestionMarks.append('?');
        for (int k = 1; k < numberOfQuestions; k++)
        {
            bunchOfQuestionMarks.append(",?");
        }
        return bunchOfQuestionMarks.toString();
    }

    public int getNumberOfChunksPerIn()
    {
        return numberOfChunksPerIn;
    }

    public void setSetBasedClausePosition(SetBasedAtomicOperation setBasedAtomicOperation)
    {
        if (this.setOperationWhereClausePositionList == null)
        {
            this.setOperationWhereClausePositionList = new InternalList(4);
        }
        JoinClause joinClause = getCurrentIdExtractor().getCurrentJoinClause();
        boolean belongsToInner = false;
        WhereClause activeWhereClause = this.whereClause;
        if (joinClause != null && joinClause.getWhereClause() != null)
        {
            belongsToInner = joinClause.belongsToInnerSql(this);
            activeWhereClause = joinClause.getWhereClause();
        }
        int position = activeWhereClause.length() - 1;
        SetBasedOpAndPosition opAndPos = new SetBasedOpAndPosition(setBasedAtomicOperation, position,
                belongsToInner, activeWhereClause.isInOrClause(), joinClause);
        this.setOperationWhereClausePositionList.add(opAndPos);
        int setSize = setBasedAtomicOperation.getSetSize();
        this.totalInClauseParameters += setSize;
    }

    protected WhereClause getWhereClause()
    {
        return whereClause;
    }

    public void setNotExistsForNextOperation()
    {
        this.notExists = true;
    }

    public void pushMapper(Mapper mapper)
    {
        JoinClause clause = getCurrentIdExtractor().pushMapperAndGetJoinClause(mapper);
        clause.generateSql(this, this.notExists);
        this.notExists = false;
    }

    public Mapper popMapper()
    {
        return getCurrentIdExtractor().popMapper();
    }

    public Mapper temporarilyPopMapper()
    {
        return getCurrentIdExtractor().popMapper();
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void pushMapperContainer(Object mapper)
    {
        getCurrentIdExtractor().pushMapperContainer(mapper);
    }

    public void popMapperContainer()
    {
        getCurrentIdExtractor().popMapperContainer();
    }

    public ObjectWithMapperStack constructWithMapperStack(Object o)
    {
        return getCurrentIdExtractor().constructWithMapperStack(o);
    }

    public ObjectWithMapperStack constructWithMapperStackWithoutLastMapper(Object o)
    {
        return getCurrentIdExtractor().constructWithMapperStackWithoutLastMapper(o);
    }

    public MapperStackImpl getCurrentMapperList()
    {
        return getCurrentIdExtractor().getCurrentMapperList();
    }

    public void clearMapperStack()
    {
        getCurrentIdExtractor().clearMapperStack();
    }

    public void addAsOfAttributeSql()
    {
        if (this.asOfAttributeWithMapperStackList != null)
        {
            MapperStackImpl currentMapperStack = getCurrentIdExtractor().getCurrentMapperList();
            for(int i=0;i<this.asOfAttributeWithMapperStackList.size();)
            {
                ObjectWithMapperStack asOfAttributeWithMapperStack = (ObjectWithMapperStack) asOfAttributeWithMapperStackList.get(i);
                ObjectWithMapperStack asOfOperationStack = analyzedOperation.getAsOfOperation(asOfAttributeWithMapperStack);
                if (asOfOperationStack == null)
                {
                    throw new MithraBusinessException("could not determine as of date for " + asOfAttributeWithMapperStack.getObject().getClass().getName()+" in operation "+safeToString());
                }
                if (currentMapperStack.equals(asOfAttributeWithMapperStack.getMapperStack()))
                {
                    asOfAttributeWithMapperStackList.remove(i);
                    AsOfOperation asOfOperation = (AsOfOperation) asOfOperationStack.getObject();
                    if (asOfOperation.addsToAsOfOperationWhereClause(asOfAttributeWithMapperStack, asOfOperationStack))
                    {
                        asOfOperation.generateSql(this, asOfAttributeWithMapperStack, asOfOperationStack);
                        getCurrentIdExtractor().restoreMapperStack(currentMapperStack);
                    }
                }
                else
                {
                    i++;
                }
            }
        }
    }

    public boolean isSubSelectInstead(SetBasedAtomicOperation operation)
    {
        return this.subSelectedInClauses != null && subSelectedInClauses.contains(operation);
    }

    public void setDisableTempTableJoin(boolean disable)
    {
        this.disableTempTableJoin = disable;
    }

    public void cleanTempForSource(int sourceNum, DatabaseType dt)
    {
        if (tupleTempContextList != null)
        {
            for(int i=0;i<tupleTempContextList.size();i++)
            {
                try
                {
                    ((TupleTempContext) tupleTempContextList.get(i)).destroy();
                }
                catch (Exception e)
                {
                    logger.error("Could not destroy temp context", e); // we have to continue here, otherwise, we leave other temp contexts lying around
                }
            }
            tupleTempContextList.clear();
            if (subSelectedInClauses != null) subSelectedInClauses.clear();
        }
    }

    public void generateJoinSql(String fullyQualifiedLeftColumnName, String fullyQualifiedRightHandColumn, String operator)
    {
        JoinClause joinClause = getCurrentIdExtractor().getCurrentJoinClause();
        if (joinClause == null)
        {
            throw new RuntimeException("internal error: join without join clause?"+" in operation "+safeToString());
        }
        else
        {
            joinClause.generateJoinSql(this, fullyQualifiedLeftColumnName, fullyQualifiedRightHandColumn, operator);
        }
    }

    public void generateAsOfJoinSql(MapperStackImpl mapperStack, String fullyQualifiedLeftColumnName,
            String fullyQualifiedRightHandColumn, String operator)
    {
        JoinClause joinClause = getCurrentIdExtractor().getCurrentJoinClause();
        if (joinClause == null)
        {
            throw new RuntimeException("internal error: join without join clause?"+" in operation "+safeToString());
        }
        else
        {
            joinClause.generateAsOfJoinSql(this, mapperStack, fullyQualifiedLeftColumnName, fullyQualifiedRightHandColumn, operator);
        }
    }

    public void generateMixedJoinSql(MapperStackImpl mapperStack, String fullyQualifiedLeftColumnName, String fullyQualifiedFromColumn,
            String fullyQualifiedToColumn, SqlParameterSetter parameterSetter)
    {
        JoinClause joinClause = getCurrentIdExtractor().getCurrentJoinClause();
        if (joinClause == null)
        {
            throw new RuntimeException("internal error: join without join clause?"+" in operation "+safeToString());
        }
        else
        {
            joinClause.generateMixedJoinSql(mapperStack, getCurrentIdExtractor().getJoinCaluseFor(mapperStack), fullyQualifiedLeftColumnName, fullyQualifiedFromColumn, fullyQualifiedToColumn, parameterSetter, false, this);
        }
    }

    public void generateReverseMixedJoinSql(MapperStackImpl mapperStack, String fullyQualifiedLeftColumnName, String fullyQualifiedFromColumn,
            String fullyQualifiedToColumn, SqlParameterSetter parameterSetter)
    {
        JoinClause joinClause = getCurrentIdExtractor().getCurrentJoinClause();
        if (joinClause == null)
        {
            throw new RuntimeException("internal error: join without join clause?"+" in operation "+safeToString());
        }
        else
        {
            joinClause.generateMixedJoinSql(mapperStack, getCurrentIdExtractor().getJoinCaluseFor(mapperStack), fullyQualifiedLeftColumnName, fullyQualifiedFromColumn, fullyQualifiedToColumn, parameterSetter, true, this);
        }
    }

    public String getExtraColumns()
    {
        return extraSelectColumns == null ? "" : extraSelectColumns;
    }

    public void addExtraJoinColumn(String fullyQualifiedColumnName)
    {
        fullyQualifiedColumnName = this.getCurrentIdExtractor().getCurrentJoinClause().getFullyQualifiedColumnNameFor(fullyQualifiedColumnName);
        if (this.extraSelectColumns == null)
        {
            this.extraSelectColumns = ","+fullyQualifiedColumnName;
        }
        else
        {
            this.extraSelectColumns = extraSelectColumns +","+fullyQualifiedColumnName;
        }
    }

    public boolean requiresDistinct()
    {
        return !this.idExtractor.isUseExplicitJoins();
    }

    public boolean requiresUnionWithoutAll()
    {
        return requiresDistinct();
    }

    public boolean requiresInMemoryDistinct()
    {
        return this.numberOfQueries > 1 && requiresDistinct();
    }

    public TupleTempContext getMultiInTempContext(MultiInOperation multiInOperation)
    {
        return this.analyzedOperation.getAsOfEqualityChecker().getOrCreateMultiInTempContext(multiInOperation);
    }

    public void registerTempTupleMapper(Mapper mapper)
    {
        mapper.registerOperation(this.getIdExtractor(), true);
        this.getCurrentIdExtractor().getCurrentJoinClause().setMapped(false);
        mapper.popMappers(this);
    }

    public void addDerivedColumnSubstitution(String fullyQualifiedColumn, String derivedColumnName, WhereClause whereClause)
    {
        if (this.derivedColumnSubstitutionMap == null)
        {
            this.derivedColumnSubstitutionMap = UnifiedMap.newMap();
        }
        if (!this.derivedColumnSubstitutionMap.containsKey(fullyQualifiedColumn))
        {
            this.derivedColumnSubstitutionMap.put(fullyQualifiedColumn, derivedColumnName);
        }
        whereClause.addReachableColumn(fullyQualifiedColumn);
    }

    public String getColumnNameWithDerivedTableSubstitution(String fullyQualifiedColumnName, WhereClause whereClause)
    {
        if (this.derivedColumnSubstitutionMap == null || whereClause.isColumnReachable(fullyQualifiedColumnName))
        {
            return fullyQualifiedColumnName;
        }
        String subst = this.derivedColumnSubstitutionMap.get(fullyQualifiedColumnName);
        return subst == null ? fullyQualifiedColumnName : subst;
    }

    private static class SetBasedOpAndPosition implements Comparable
    {
        private boolean inOrClause;
        private boolean inExistsClause;
        private SetBasedAtomicOperation op;
        private int position;
        private JoinClause joinClause;

        private SetBasedOpAndPosition(SetBasedAtomicOperation op, int position, boolean inExistsClause, boolean inOrClause, JoinClause joinClause)
        {
            this.op = op;
            this.position = position;
            this.inExistsClause = inExistsClause;
            this.inOrClause = inOrClause;
            this.joinClause = joinClause;
        }

        public int getPosition()
        {
            return position;
        }

        public boolean isInOrClause()
        {
            return inOrClause;
        }

        public boolean isInExistsOrOrClause()
        {
            return inExistsClause || inOrClause;
        }

        public SetBasedAtomicOperation getOp()
        {
            return op;
        }

        public int compareTo(Object o)
        {
            // reverse sort
            return ((SetBasedOpAndPosition)o).op.getSetSize() - this.op.getSetSize();
        }

        public void incrementPosition(int inc, SetBasedOpAndPosition filledOp)
        {
            if (this.hasSameWhereClause(filledOp) && this.position > filledOp.getPosition())
            {
                this.position += inc;
            }
        }

        public void incrementPosition(int position, int inc, WhereClause activeWhereClause)
        {
            if (this.hasSameWhereClause(activeWhereClause) && this.position > position)
            {
                this.position += inc;
            }
        }

        private boolean hasSameWhereClause(WhereClause activeWhereClause)
        {
            if (this.joinClause != null && this.joinClause.getWhereClause() != null)
            {
                return this.joinClause.getWhereClause() == activeWhereClause;
            }
            return activeWhereClause.getOwner() instanceof SqlQuery;
        }

        private boolean hasSameWhereClause(SetBasedOpAndPosition setBasedOpAndPosition)
        {
            JoinClause otherJoinClause = setBasedOpAndPosition.joinClause;
            return hasSameWhereClause(otherJoinClause);
        }

        private boolean hasSameWhereClause(JoinClause otherJoinClause)
        {
            if (this.joinClause == otherJoinClause) // also takes care of null
            {
                return true;
            }
            if (this.joinClause != null)
            {
                return this.joinClause.hasSameWhereClause(otherJoinClause);
            }
            return otherJoinClause.hasSameWhereClause(this.joinClause);
        }

        public void insertSql(String sql, SqlQuery query)
        {
            if (this.joinClause == null || this.joinClause.getWhereClause() == null)
            {
                query.insertWhereWithoutShift(this.position, sql);
            }
            else
            {
                this.joinClause.insertWhereSql(this.position, sql);
            }
        }

        public boolean isInsideAggregateJoin()
        {
            return this.joinClause != null && this.joinClause.isAggregateJoin();
        }

        public boolean allowsInClauseTempJoinReplacement(SqlQuery query)
        {
            return query.allowsInClauseTempJoinReplacement(this.joinClause);
        }

        public void restoreMapper(SqlQuery sqlQuery)
        {
            if (this.joinClause != null)
            {
                this.joinClause.restoreMapper(sqlQuery);
            }
        }
    }

    protected void insertWhereWithoutShift(int position, String sql)
    {
        this.whereClause.insert(position, sql);
    }

    protected Map getRawMapperStackToJoinClauseMap()
    {
        return getCurrentIdExtractor().getRawMapperStackToJoinClauseMap();
    }

    protected MithraDatabaseIdentifierExtractor getIdExtractor()
    {
        return this.idExtractor;
    }
}
