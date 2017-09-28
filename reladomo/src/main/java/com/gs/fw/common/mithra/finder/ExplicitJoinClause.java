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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;

import com.gs.collections.impl.list.mutable.FastList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public class ExplicitJoinClause implements JoinClause, WhereClause.WhereClauseOwner
{
    private boolean isContainerOnly = true;
    private MithraObjectPortal portal;
    private int tableNumber;
    private List<Attribute> attributes;
    private List<Attribute> attributesFromMappers;
    private List<ExplicitJoinClause> children;
    private boolean isMapped = true;
    private String tableAliasPrefix = "t";
    private ExplicitJoinClause parent;
    private WhereClause whereClause;
    private List<SqlJoin> sqlJoins = FastList.newList(4);
    private List<SqlJoin> whereClauseSqlJoins;
    private List<String> extraSelectAttributesWithGroupBy;
    private List<String> extraSelectAttributesWithoutGroupBy;
    private List<MixedSqlJoin> mixedSqlJoins;
    private List<MixedSqlJoin> outOfScopeMixedSqlJoins;
    private MapperStackImpl mapperStackImpl;
    private boolean prepared = false;
    private boolean mustAggregate;
    private boolean mustLeftJoin;
    private boolean notExists;
    private boolean mustDistinct;
    private boolean readOnly = false;

    public ExplicitJoinClause(MithraObjectPortal portal, int tableNumber)
    {
        this.portal = portal;
        this.tableNumber = tableNumber;
    }

    public ExplicitJoinClause makeReadOnly()
    {
        this.readOnly = true;
        return this;
    }

    public ExplicitJoinClause(MithraObjectPortal portal, int tableNumber, String tableAliasPrefix,
            MapperStackImpl immutableMapperStack, JoinClause incomingParent)
    {
        this(portal, tableNumber);
        this.tableAliasPrefix = tableAliasPrefix;
        this.mapperStackImpl = immutableMapperStack;
        ExplicitJoinClause parent = (ExplicitJoinClause) incomingParent;
        while(parent != null && parent.isContainerOnly)
        {
            parent = parent.parent;
        }
        this.parent = parent;
        if (this.parent != null)
        {
            this.parent.addChild(this);
        }
        this.isContainerOnly = false;
    }

    private int getTableNumber()
    {
        return tableNumber;
    }

    public void addAttributeFromMapper(Attribute attr)
    {
        if (this.attributes == null)
        {
            this.attributesFromMappers = new FastList<Attribute>(4);
        }
        this.attributesFromMappers.add(attr);
        this.addAttribute(attr);
    }

    public void addAttribute(Attribute attr)
    {
        if (this.attributes == null)
        {
            this.attributes = new FastList<Attribute>(4);
        }
        this.attributes.add(attr);
    }

    public boolean belongsToInnerSql(SqlQuery query)
    {
        return this.whereClause.getOwner() != query;
    }

    private boolean isPartOfAggregation()
    {
        return this.mustAggregate || (parent != null && parent.isPartOfAggregation());
    }

    public void generateSql(SqlQuery query, boolean notExists)
    {
        if (this.prepared) return;
        this.prepared = true;
        WhereClause parentWhereClause = parent == null ? query.getWhereClause() : parent.getWhereClause();
        this.mustLeftJoin = notExists || parentWhereClause.isInOrClause() || hasLeftJoinedParent();
        this.notExists = notExists;
        this.mustDistinct = parentWhereClause.isInOrClause() || !notExists;

        if (mustAggregate || mustLeftJoin)
        {
            this.whereClause = new WhereClause(this);
        }
        else
        {
            this.whereClause = getAggregateOrQueryWhereClause(query);
        }

        if (mustAggregate)
        {
            registerColumnSubstitutes(query);
        }

        if (mustLeftJoin && mustAggregate)
        {
            addParentWhereClause(query, "d"+this.getTableNumber()+".c0 is "+(notExists ? "" : "not ")+"null");
        }
    }

    private boolean isAggregationCandidate()
    {
        if (isDefaultJoin() && this.isNotUnique())
        {
            return true;
        }
        if (this.children != null && this.children.size() > 1)
        {
            int count = 0;
            for(int i=0;i<children.size() && count < 2;i++)
            {
                ExplicitJoinClause child = children.get(i);
                if (child.isAggregationCandidate())
                {
                    count++;
                }
            }
            return count >= 2;
        }
        return false;
    }

    private boolean hasLeftJoinedParent()
    {
        ExplicitJoinClause p = this.parent;
        while(p != null)
        {
            if (p.mustAggregate) return false;
            if (p.mustLeftJoin) return true;
            p = p.parent;
        }
        return false;
    }

    private void addChild(ExplicitJoinClause joinClause)
    {
        if (this.children == null)
        {
            this.children = FastList.newList(4);
        }
        this.children.add(joinClause);
    }

    private void addParentWhereClause(SqlQuery query, String sql)
    {
        WhereClause wc = getAggregateOrQueryWhereClause(query);
        wc.beginAnd();
        wc.appendWithSpace(sql);
        wc.endAnd();
    }

    private WhereClause getAggregateOrQueryWhereClause(SqlQuery query)
    {
        WhereClause wc = null;
        ExplicitJoinClause p = this.parent;

        while (p != null && wc == null)
        {
            if (p.isOwnedNonAggregateWhereClause())
            {
                p = p.parent;
            }
            else
            {
                wc = p.whereClause;
            }
        }

        if (wc == null)
        {
            wc = query.getWhereClause();
        }
        return wc;
    }

    private boolean isOwnedNonAggregateWhereClause()
    {
        return this.whereClause.getOwner() == this && !this.mustAggregate;
    }

    public boolean mustUseExistsForMapperOnly(SqlQuery query)
    {
        return this.belongsToInnerSql(query) || isNotUniqueForMapper();

    }

    public boolean allowsInClauseTempJoinReplacement()
    {
        if (this.portal == null) return false; // portal is only null for DEFAULT container clause, which will only get here for an or-clause.
        Object owner = this.whereClause.getOwner();
        if (owner == this)
        {
            return !(this.mustLeftJoin && !this.mustAggregate);
        }
        if (owner instanceof JoinClause)
        {
            return ((JoinClause)owner).allowsInClauseTempJoinReplacement();
        }
        return true;
    }

    public void restoreMapper(SqlQuery sqlQuery)
    {
        sqlQuery.getCurrentIdExtractor().restoreMapperStack(this.mapperStackImpl);
    }

    @Override
    public void computeAggregationForTree()
    {
        if (this.parent == null)
        {
            //we're the root of the tree, so we'll compute the aggregation
            this.computeSelfAndChildAggregation();
        }
    }

    private void computeSelfAndChildAggregation()
    {
        this.mustAggregate = isAggregationCandidate() && (this.parent == null || !this.parent.isPartOfAggregation());
        if (this.children != null)
        {
            for (int i = 0; i < children.size(); i++)
            {
                ExplicitJoinClause child = this.children.get(i);
                child.computeSelfAndChildAggregation();
            }
        }
    }

    private boolean isNotUnique()
    {
        return (this.attributes == null || !this.portal.mapsToUniqueIndex(this.attributes));
    }

    private boolean isNotUniqueForMapper()
    {
        return (this.attributesFromMappers == null || !this.portal.mapsToUniqueIndex(this.attributesFromMappers));
    }

    public String getTableAlias()
    {
        return tableAliasPrefix+tableNumber;
    }

    public boolean isMapped()
    {
        return isMapped;
    }

    public void setMapped(boolean mapped)
    {
        isMapped = mapped;
    }

    private boolean isDefaultJoin()
    {
        return this.tableAliasPrefix.equals("t");
    }

    public void reset()
    {
        this.setMapped(false);
        this.sqlJoins.clear();
        if (this.whereClauseSqlJoins != null)
        {
            this.whereClauseSqlJoins.clear();
        }
        if (this.mixedSqlJoins != null)
        {
            this.mixedSqlJoins.clear();
        }
        if (this.whereClause != null)
        {
            this.whereClause.clear();
        }
        if (this.extraSelectAttributesWithGroupBy != null)
        {
            this.extraSelectAttributesWithGroupBy.clear();
        }
        if (this.extraSelectAttributesWithoutGroupBy != null)
        {
            this.extraSelectAttributesWithoutGroupBy.clear();
        }
    }

    public WhereClause getWhereClause()
    {
        return whereClause;
    }

    @Override
    public WhereClause getParentWhereClause(SqlQuery sqlQuery)
    {
        ExplicitJoinClause p = this.parent;
        while (p != null)
        {
            if (p.getWhereClause() == null)
            {
                p = p.parent;
            }
            else
            {
                return p.getWhereClause();
            }
        }
        return sqlQuery.getWhereClause();
    }

    public void appendJoinsToFromClause(SqlQuery query, StringBuilder fromClause, String withLock, String withoutLock)
    {
        if (this.parent == null)
        {
            createAndAppendJoinClause(query, fromClause, withLock, withoutLock);
        }
    }

    private void createAndAppendJoinClause(SqlQuery query, StringBuilder fromClause, String withLock, String withoutLock)
    {
        if (this.outOfScopeMixedSqlJoins != null)
        {
            for(int i=0;i<outOfScopeMixedSqlJoins.size();i++)
            {
                MixedSqlJoin mixedSqlJoin = outOfScopeMixedSqlJoins.get(i);
                StringBuilder builder = new StringBuilder(32);
                mixedSqlJoin.appendInnerOnClauseWithSubstitution(builder, query, this.whereClause);
                this.whereClause.beginAnd();
                this.whereClause.appendWithSpace(builder);
                this.whereClause.endAnd();
                this.whereClause.addSqlParameterSetter(mixedSqlJoin.parameterSetter);
            }
        }
        if (this.whereClauseSqlJoins != null)
        {
            for(int i=0;i<this.whereClauseSqlJoins.size();i++)
            {
                SqlJoin sqlJoin = this.whereClauseSqlJoins.get(i);
                this.whereClause.beginAnd();
                this.whereClause.appendWithSpace(query.getColumnNameWithDerivedTableSubstitution(sqlJoin.fullyQualifiedLeftColumnName, this.whereClause))
                        .appendWithSpace(sqlJoin.operator).appendWithSpace(sqlJoin.fullyQualifiedRightHandColumn);
                this.whereClause.endAnd();
            }
        }
        fromClause.append(' ').append(this.mustLeftJoin ? "left" : "inner").append(" join ");
        if (this.mustAggregate)
        {
            fromClause.append(" (select ");
            if (hasExtraSelectAttributesWithoutGroupBy())
            {
                fromClause.append(createAggregateSelectAttributes());
            }
            else
            {
                if (mustDistinct) fromClause.append("distinct ");
                fromClause.append(createDistinctSelectAttributes());
            }
            fromClause.append(" from ");
        }
        appendTableAndSuperClasses(query, fromClause, withLock, withoutLock);

        if (this.mustAggregate)
        {
            appendChildJoins(query, fromClause, withLock, withoutLock);
            if (haveOwnWhereClause())
            {
                fromClause.append(" where ").append(this.whereClause);
            }
            if (hasExtraSelectAttributesWithoutGroupBy())
            {
                fromClause.append(" group by ").append(createAggregateGroupByAttributes());
            }
            if (query.getDatabaseType().supportsAsKeywordForTableAliases())
            {
                fromClause.append(") as ").append("d").append(this.getTableNumber());
            }
            else
            {
                fromClause.append(") ").append("d").append(this.getTableNumber());
            }

            fromClause.append(" on ").append(createAggregateOnClause());
        }
        else
        {
            fromClause.append(" on ").append(createPlainOnClause());
            if (haveOwnWhereClause())
            {
                fromClause.append(" and ").append(this.whereClause);
            }
            appendChildJoins(query, fromClause, withLock, withoutLock);
        }
    }

    private boolean haveOwnWhereClause()
    {
        return this.whereClause.getOwner() == this && this.whereClause.length() > 0;
    }

    private void appendChildJoins(SqlQuery query, StringBuilder fromClause, String withLock, String withoutLock)
    {
        if (this.children != null)
        {
            for(int i=0;i<this.children.size();i++)
            {
                this.children.get(i).createAndAppendJoinClause(query, fromClause, withLock, withoutLock);
            }
        }
        this.whereClause.appendTempTableFromClause(this, fromClause);
    }

    private void appendTableAndSuperClasses(SqlQuery query, StringBuilder fromClause, String withLock, String withoutLock)
    {
        MithraObjectPortal[] superClasses = portal.getSuperClassPortals();
        if (superClasses != null)
        {
            fromClause.append(query.getTableName(superClasses[0], mapperStackImpl));
            String tableAlias = "t"+this.getTableNumber();
            fromClause.append(' ').append(tableAlias);
            query.appendLockMode(fromClause, superClasses[0], withLock, withoutLock);
            for(int i=1;i<superClasses.length;i++)
            {
                query.joinWithSuper(fromClause, " JOIN ", mapperStackImpl, tableAlias, superClasses[i], withLock, withoutLock);
            }
            query.joinWithSuper(fromClause, " JOIN ", mapperStackImpl, tableAlias, portal, withLock, withoutLock);
        }
        else
        {
            fromClause.append(query.getTableName(portal, this.mapperStackImpl));
            fromClause.append(' ').append(this.getTableAlias());
            query.appendLockMode(fromClause, this.portal, withLock, withoutLock);
        }
    }

    private StringBuilder createAggregateOnClause()
    {
        StringBuilder builder = new StringBuilder(this.sqlJoins.size() * 23);
        boolean appended = false;
        if (this.mixedSqlJoins != null)
        {
            for(int i=0;i<mixedSqlJoins.size();i++)
            {
                MixedSqlJoin mixedSqlJoin = mixedSqlJoins.get(i);
                if (appended) builder.append(" and ");
                appended = true;
                mixedSqlJoin.appendOnClause(builder, i, this.getTableNumber());
            }
        }
        for(int i=0;i<this.sqlJoins.size();i++)
        {
            SqlJoin sqlJoin = sqlJoins.get(i);
            if (appended) builder.append(" and ");
            appended = true;
            builder.append(sqlJoin.fullyQualifiedLeftColumnName).append(' ').append(sqlJoin.operator);
            builder.append(" d").append(this.getTableNumber()).append(".c").append(i);
        }
        return builder;
    }

    private StringBuilder createPlainOnClause()
    {
        StringBuilder builder = new StringBuilder(this.sqlJoins.size() * 30);
        boolean appended = false;
        if (this.mixedSqlJoins != null)
        {
            for(int i=0;i<mixedSqlJoins.size();i++)
            {
                MixedSqlJoin mixedSqlJoin = mixedSqlJoins.get(i);
                if (appended) builder.append(" and ");
                appended = true;
                mixedSqlJoin.appendInnerOnClause(builder);
            }
        }
        for(int i=0;i<this.sqlJoins.size();i++)
        {
            SqlJoin sqlJoin = sqlJoins.get(i);
            if (appended) builder.append(" and ");
            appended = true;
            builder.append(sqlJoin.fullyQualifiedLeftColumnName).append(' ').append(sqlJoin.operator).append(' ');
            builder.append(sqlJoin.fullyQualifiedRightHandColumn);
        }
        return builder;
    }

    private boolean hasExtraSelectAttributesWithoutGroupBy()
    {
        return this.extraSelectAttributesWithoutGroupBy != null;
    }

    private StringBuilder createDistinctSelectAttributes()
    {
        StringBuilder builder = new StringBuilder(this.sqlJoins.size() * 18);
        appendAggregateSelectAttributesFromJoins(builder);
        appendExtraSelectAttributesWithGroupBy(builder);
        return builder;
    }

    private StringBuilder createAggregateSelectAttributes()
    {
        StringBuilder builder = new StringBuilder(this.sqlJoins.size() * 18);
        appendAggregateSelectAttributesFromJoins(builder);
        appendExtraSelectAttributesWithGroupBy(builder);
        appendExtraSelectAttributesWithoutGroupBy(builder);
        return builder;
    }

    private void appendExtraSelectAttributesWithoutGroupBy(StringBuilder builder)
    {
        if (this.extraSelectAttributesWithoutGroupBy != null)
        {
            for(int i=0;i< extraSelectAttributesWithoutGroupBy.size();i++)
            {
                builder.append(", ").append(extraSelectAttributesWithoutGroupBy.get(i)).append(" r").append(i);
            }
        }
    }

    private void appendExtraSelectAttributesWithGroupBy(StringBuilder builder)
    {
        if (this.extraSelectAttributesWithGroupBy != null)
        {
            for(int i=0;i< extraSelectAttributesWithGroupBy.size();i++)
            {
                builder.append(", ").append(extraSelectAttributesWithGroupBy.get(i)).append(" s").append(i);
            }
        }
    }

    private void appendAggregateSelectAttributesFromJoins(StringBuilder builder)
    {
        boolean appended = false;
        if (this.mixedSqlJoins != null)
        {
            for(int i=0;i<mixedSqlJoins.size();i++)
            {
                MixedSqlJoin mixedSqlJoin = mixedSqlJoins.get(i);
                if (appended) builder.append(',');
                appended = true;
                mixedSqlJoin.appendLeftJoinAttributes(builder, i);
            }
        }
        for(int i=0;i<this.sqlJoins.size();i++)
        {
            SqlJoin sqlJoin = sqlJoins.get(i);
            if (appended) builder.append(',');
            appended = true;
            builder.append(sqlJoin.fullyQualifiedRightHandColumn).append(" c").append(i);
        }
    }

    private StringBuilder createAggregateGroupByAttributes()
    {
        StringBuilder builder = new StringBuilder(this.sqlJoins.size() * 15);
        boolean appended = false;
        if (this.mixedSqlJoins != null)
        {
            for(int i=0;i<mixedSqlJoins.size();i++)
            {
                MixedSqlJoin mixedSqlJoin = mixedSqlJoins.get(i);
                if (appended) builder.append(',');
                appended = true;
                mixedSqlJoin.appendGroupByAttributes(builder);
            }
        }
        for(int i=0;i<this.sqlJoins.size();i++)
        {
            SqlJoin sqlJoin = sqlJoins.get(i);
            if (appended) builder.append(',');
            appended = true;
            builder.append(sqlJoin.fullyQualifiedRightHandColumn);
        }
        if (this.extraSelectAttributesWithGroupBy != null)
        {
            for(int i=0;i< extraSelectAttributesWithGroupBy.size();i++)
            {
                builder.append(", ").append(extraSelectAttributesWithGroupBy.get(i));
            }
        }
        return builder;
    }

    public int setJoinSqlParameters(PreparedStatement ps, int count, SqlQuery query) throws SQLException
    {
        if (!mustAggregate)
        {
            count = this.setMixedSqlJoinParameters(ps, count, query);
            count = this.getWhereClause().setSqlParameters(query, ps, count, this);
        }
        if (this.children != null)
        {
            for(int i=0;i<this.children.size();i++)
            {
                count = this.children.get(i).setJoinSqlParameters(ps, count, query);
            }
        }
        if (mustAggregate)
        {
            count = this.getWhereClause().setSqlParameters(query, ps, count, this);
            count = this.setMixedSqlJoinParameters(ps, count, query);
        }
        return count;
    }

    private int setMixedSqlJoinParameters(PreparedStatement ps, int count, SqlQuery query)
            throws SQLException
    {
        if (this.mixedSqlJoins != null)
        {
            for(int i=0;i<mixedSqlJoins.size();i++)
            {
                MixedSqlJoin mixedSqlJoin = mixedSqlJoins.get(i);
                count += mixedSqlJoin.parameterSetter.setSqlParameters(ps, count, query);
            }
        }
        return count;
    }

    private Object getTopLevelAppender()
    {
        return this.getWhereClause().getOwner();
    }

    public boolean hasSameWhereClause(JoinClause clause)
    {
        if (clause == null) return appendViaQuery();
        ExplicitJoinClause joinClause = (ExplicitJoinClause) clause;
        return joinClause == this
                || (joinClause.getTopLevelAppender() ==  this.getTopLevelAppender());
    }

    private boolean appendViaQuery()
    {
        if (this.getWhereClause() == null)
        {
            // This logic handles the use case of MithraDatabaseIdentifierExtractor.DEFAULT_EXPLICIT_JOIN_CLAUSE,
            // It is not a real join clause and this is flagged by isContainerOnly == true.
            // Genuine join clauses will not satisfy this condition.
            return isContainerOnly;
        }
        return this.getWhereClause().getOwner() instanceof SqlQuery;
    }

    public boolean isAggregateJoin()
    {
        return this.mustAggregate || (this.parent != null && this.parent.isAggregateJoin());
    }

    public void insertWhereSql(int position, String sql)
    {
        this.getWhereClause().insert(position, sql);
    }

    private void registerColumnSubstitutes(SqlQuery query)
    {
        for(int i=0;i<sqlJoins.size();i++)
        {
            SqlJoin sqlJoin = sqlJoins.get(i);
            query.addDerivedColumnSubstitution(sqlJoin.fullyQualifiedRightHandColumn, "d" + this.getTableNumber() + ".c" + i, this.whereClause);
        }
        if (this.mixedSqlJoins != null)
        {
            for (int i=0;i<mixedSqlJoins.size();i++)
            {
                MixedSqlJoin mixedSqlJoin = mixedSqlJoins.get(i);
                query.addDerivedColumnSubstitution(mixedSqlJoin.fullyQualifiedLeftColumnName, "d" + this.getTableNumber() + ".cf" + i, this.whereClause);
            }
        }
    }

    public void generateMixedJoinSql(MapperStackImpl mapperStack, JoinClause otherJoinClause, String fullyQualifiedLeftColumnName,
            String fullyQualifiedFromColumn, String fullyQualifiedToColumn, SqlParameterSetter parameterSetter, boolean reversed, SqlQuery query)
    {
        if (isInScope(mapperStack))
        {
            MixedSqlJoin mixedSqlJoin = new MixedSqlJoin(fullyQualifiedLeftColumnName, fullyQualifiedFromColumn, fullyQualifiedToColumn, parameterSetter, reversed);
            if (isAccessible(mapperStack))
            {
                if (this.mixedSqlJoins == null) this.mixedSqlJoins = FastList.newList(4);
                this.mixedSqlJoins.add(mixedSqlJoin);
                if (this.mustAggregate && this.whereClause != null)
                {
                    query.addDerivedColumnSubstitution(fullyQualifiedLeftColumnName, "d" + this.getTableNumber() + ".cf" + (this.mixedSqlJoins.size() - 1), this.whereClause);
                }
            }
            else
            {
                if (otherJoinClause instanceof ExplicitJoinClause)
                {
                    ExplicitJoinClause other = (ExplicitJoinClause) otherJoinClause;
                    if (other.getTableNumber() > this.getTableNumber())
                    {
                        other.addOutOfScopeMixedSqlJoin(mixedSqlJoin);
                    }
                    else
                    {
                        this.addOutOfScopeMixedSqlJoin(mixedSqlJoin);
                    }
                }
                else
                {
                    addOutOfScopeMixedSqlJoin(mixedSqlJoin);
                }
            }
        }
        else
        {
            if (mustAggregate)
            {
                if (this.extraSelectAttributesWithGroupBy == null)
                {
                    this.extraSelectAttributesWithGroupBy = FastList.newList();
                }
                if (reversed)
                {
                    this.extraSelectAttributesWithGroupBy.add(fullyQualifiedLeftColumnName);
                    fullyQualifiedLeftColumnName = "d"+this.getTableNumber()+".s"+(this.extraSelectAttributesWithGroupBy.size() - 1);
                }
                else
                {
                    this.extraSelectAttributesWithGroupBy.add(fullyQualifiedFromColumn);
                    fullyQualifiedFromColumn = "d"+this.getTableNumber()+".s"+(this.extraSelectAttributesWithGroupBy.size() - 1);
                    this.extraSelectAttributesWithGroupBy.add(fullyQualifiedToColumn);
                    fullyQualifiedToColumn = "d"+this.getTableNumber()+".s"+(this.extraSelectAttributesWithGroupBy.size() - 1);
                }
            }
            this.parent.generateMixedJoinSql(mapperStack, otherJoinClause, fullyQualifiedLeftColumnName, fullyQualifiedFromColumn,
                    fullyQualifiedToColumn, parameterSetter, reversed, query);
        }
    }

    private void addOutOfScopeMixedSqlJoin(MixedSqlJoin mixedSqlJoin)
    {
        if (this.outOfScopeMixedSqlJoins == null) this.outOfScopeMixedSqlJoins = FastList.newList(2);
        this.outOfScopeMixedSqlJoins.add(mixedSqlJoin);
    }

    public void generateJoinSql(SqlQuery query, String fullyQualifiedLeftColumnName, String fullyQualifiedRightHandColumn, String operator)
    {
        SqlJoin sqlJoin = new SqlJoin(fullyQualifiedLeftColumnName, fullyQualifiedRightHandColumn, operator);
        this.sqlJoins.add(sqlJoin);
        if (sqlJoins.size() == 1 && this.mustLeftJoin && !this.mustAggregate)
        {
            addParentWhereClause(query, fullyQualifiedRightHandColumn+" is "+(notExists ? "" : "not ")+"null");
        }
        if (this.mustAggregate && this.whereClause != null)
        {
            query.addDerivedColumnSubstitution(fullyQualifiedRightHandColumn, "d" + this.getTableNumber() + ".c" + (sqlJoins.size() - 1), this.whereClause);
        }
    }

    public void generateAsOfJoinSql(SqlQuery query, MapperStackImpl mapperStack, String fullyQualifiedLeftColumnName,
            String fullyQualifiedRightHandColumn, String operator)
    {
        // enable this for testing
//        if (this.readOnly)
//        {
//            throw new RuntimeException("read only");
//        }
        if (isInScope(mapperStack))
        {
            if (isAccessible(mapperStack))
            {
                generateJoinSql(query, fullyQualifiedLeftColumnName, fullyQualifiedRightHandColumn, operator);
            }
            else
            {
                if (mustAggregate)
                {
                    SqlJoin sqlJoin = new SqlJoin(fullyQualifiedLeftColumnName, fullyQualifiedRightHandColumn, operator);
                    if (this.whereClauseSqlJoins == null)
                    {
                        this.whereClauseSqlJoins = FastList.newList(4);
                    }
                    this.whereClauseSqlJoins.add(sqlJoin);
                }
                else
                {
                    this.parent.generateAsOfJoinSql(query, mapperStack, fullyQualifiedLeftColumnName, fullyQualifiedRightHandColumn, operator);
                }
            }
        }
        else
        {
            if (mustAggregate)
            {
                if (this.extraSelectAttributesWithGroupBy == null)
                {
                    this.extraSelectAttributesWithGroupBy = FastList.newList();
                }
                this.extraSelectAttributesWithGroupBy.add(fullyQualifiedRightHandColumn);
                fullyQualifiedRightHandColumn = "d"+this.getTableNumber()+".s"+(this.extraSelectAttributesWithGroupBy.size() - 1);
            }
            this.parent.generateAsOfJoinSql(query, mapperStack, fullyQualifiedLeftColumnName, fullyQualifiedRightHandColumn, operator);
        }
    }

    private boolean isAccessible(MapperStackImpl mapperStack)
    {
        if (this.mapperStackImpl == null)
        {
            return mapperStack == null || mapperStack.isFullyEmpty();
        }
        return this.mapperStackImpl.isGreaterThanEqualsWithEqualityCheck(mapperStack);
    }

    private boolean isInScope(MapperStackImpl mapperStack)
    {
        if (parent == null) return true;
        if (mapperStack.equals(parent.mapperStackImpl)) return true; // most common case where the join is to the immediate parent
        ExplicitJoinClause aggregateParent = this.parent;
        while(aggregateParent != null && !aggregateParent.mustAggregate)
        {
            aggregateParent = aggregateParent.parent;
        }
        return aggregateParent == null || aggregateParent.mapperStackImpl.compareTo(mapperStack) <= 0;
    }

    public boolean isTopLevel()
    {
        return this.parent == null;
    }

    private boolean hasNoAggregateParent()
    {
        return parent == null || !parent.isAggregateJoin();
    }

    public String getFullyQualifiedColumnNameFor(String fullyQualifiedColumnName)
    {
        if (this.isAggregateJoin())
        {
            if (mustAggregate)
            {
                fullyQualifiedColumnName = this.getFullyQualifiedColumnNameFromJoins(fullyQualifiedColumnName);
            }
            if (hasNoAggregateParent())
            {
                return fullyQualifiedColumnName;
            }
            else
            {
                // we have to now pass this all the way up to the query through one or more aggregate parents!!
                return parent.addToSelectAttributes(fullyQualifiedColumnName);
            }
        }
        else
        {
            return fullyQualifiedColumnName;
        }
    }

    private String addToSelectAttributes(String fullyQualifiedColumnName)
    {
        if (mustAggregate)
        {
            if (extraSelectAttributesWithGroupBy == null) extraSelectAttributesWithGroupBy = FastList.newList();
            extraSelectAttributesWithGroupBy.add(fullyQualifiedColumnName);
            fullyQualifiedColumnName = "d"+this.getTableNumber()+".s"+(extraSelectAttributesWithGroupBy.size() - 1);
        }
        if (hasNoAggregateParent())
        {
            return fullyQualifiedColumnName;
        }
        else
        {
            return parent.addToSelectAttributes(fullyQualifiedColumnName);
        }
    }

    private String addToUngroupedSelectAttributes(String fullyQualifiedColumnName)
    {
        if (mustAggregate)
        {
            if (extraSelectAttributesWithoutGroupBy == null) extraSelectAttributesWithoutGroupBy = FastList.newList();
            extraSelectAttributesWithoutGroupBy.add("min("+fullyQualifiedColumnName+")");
            fullyQualifiedColumnName = "d"+this.getTableNumber()+".r"+(extraSelectAttributesWithoutGroupBy.size() - 1);
        }
        if (hasNoAggregateParent())
        {
            return fullyQualifiedColumnName;
        }
        else
        {
            return parent.addToUngroupedSelectAttributes(fullyQualifiedColumnName);
        }
    }

    private String getFullyQualifiedColumnNameFromJoins(String fullyQualifiedColumnName)
    {
        // simple case, just find the column number:
        for(int i=0;i<this.sqlJoins.size();i++)
        {
            if (sqlJoins.get(i).fullyQualifiedRightHandColumn.equals(fullyQualifiedColumnName))
            {
                return "d"+this.getTableNumber()+".c"+i;
            }
        }
        if (this.mixedSqlJoins != null)
        {
            for(int i=0;i<this.mixedSqlJoins.size();i++)
            {
                if (mixedSqlJoins.get(i).fullyQualifiedLeftColumnName.equals(fullyQualifiedColumnName))
                {
                    return "d"+this.getTableNumber()+".cf"+i;
                }
            }
        }
        return null;
    }

    public String getFullyQualifiedOrderByColumnNameFor(String orderByColumnName)
    {
        if (this.isAggregateJoin())
        {
            if (mustAggregate)
            {
                String joinedColumnName = this.getFullyQualifiedColumnNameFromJoins(orderByColumnName);
                if (joinedColumnName == null)
                {
                    if (this.extraSelectAttributesWithoutGroupBy == null)
                    {
                        this.extraSelectAttributesWithoutGroupBy = FastList.newList();
                    }
                    this.extraSelectAttributesWithoutGroupBy.add("min("+orderByColumnName+")");
                    orderByColumnName = "d"+this.getTableNumber()+".r"+(extraSelectAttributesWithoutGroupBy.size() - 1);
                }
                else
                {
                    orderByColumnName = joinedColumnName;
                }
            }
            if (hasNoAggregateParent())
            {
                return orderByColumnName;
            }
            else
            {
                // we have to now pass this all the way up to the query through one or more aggregate parents!!
                return parent.addToUngroupedSelectAttributes(orderByColumnName);
            }
        }
        else
        {
            return orderByColumnName;
        }
    }



    private static class SqlJoin
    {
        private String fullyQualifiedLeftColumnName;
        private String fullyQualifiedRightHandColumn;
        private String operator;

        private SqlJoin(String fullyQualifiedLeftColumnName, String fullyQualifiedRightHandColumn, String operator)
        {
            this.fullyQualifiedLeftColumnName = fullyQualifiedLeftColumnName;
            this.fullyQualifiedRightHandColumn = fullyQualifiedRightHandColumn;
            this.operator = operator;
        }
    }

    private static class MixedSqlJoin
    {
        private String fullyQualifiedLeftColumnName;
        private String fullyQualifiedFromColumn;
        private String fullyQualifiedToColumn;
        private SqlParameterSetter parameterSetter;
        private boolean reversed;

        private MixedSqlJoin(String fullyQualifiedLeftColumnName, String fullyQualifiedFromColumn, String fullyQualifiedToColumn, SqlParameterSetter parameterSetter, boolean reversed)
        {
            this.fullyQualifiedLeftColumnName = fullyQualifiedLeftColumnName;
            this.fullyQualifiedFromColumn = fullyQualifiedFromColumn;
            this.fullyQualifiedToColumn = fullyQualifiedToColumn;
            this.parameterSetter = parameterSetter;
            this.reversed = reversed;
        }

        private void appendInnerOnClauseWithSubstitution(StringBuilder builder, SqlQuery query, WhereClause whereClause)
        {
            appendInnerOnClause(builder, query.getColumnNameWithDerivedTableSubstitution(this.fullyQualifiedLeftColumnName, whereClause));
        }

        private void appendInnerOnClause(StringBuilder builder)
        {
            appendInnerOnClause(builder, this.fullyQualifiedLeftColumnName);
        }

        private void appendInnerOnClause(StringBuilder builder, String left)
        {
            builder.append("((");
            builder.append(left).append(" >= ");
            builder.append(this.fullyQualifiedFromColumn).append(" and ") ;
            builder.append(left).append(" < ");
            builder.append(this.fullyQualifiedToColumn).append(") or (");
            builder.append(this.fullyQualifiedToColumn).append(" = ? and ");
            builder.append(left).append(" = ? ))");
        }

        private void appendOnClause(StringBuilder builder, int joinNumber, int tableNumber)
        {
            if (reversed)
            {
                builder.append("((");
                builder.append("d").append(tableNumber).append(".cf").append(joinNumber).append(" >= ");
                builder.append(this.fullyQualifiedFromColumn).append(" and ") ;
                builder.append("d").append(tableNumber).append(".cf").append(joinNumber).append(" < ");
                builder.append(this.fullyQualifiedToColumn).append(") or (");
                builder.append(this.fullyQualifiedToColumn).append(" = ? and ");
                builder.append("d").append(tableNumber).append(".cf").append(joinNumber).append(" = ? ))");
            }
            else
            {
                builder.append("((");
                builder.append(this.fullyQualifiedLeftColumnName).append(" >= ");
                builder.append("d").append(tableNumber).append(".cf").append(joinNumber).append(" and ") ;
                builder.append(this.fullyQualifiedLeftColumnName).append(" < ");
                builder.append("d").append(tableNumber).append(".ct").append(joinNumber).append(") or (");
                builder.append("d").append(tableNumber).append(".ct").append(joinNumber).append(" = ? and ");
                builder.append(this.fullyQualifiedLeftColumnName).append(" = ? ))");
            }
        }

        private void appendLeftJoinAttributes(StringBuilder builder, int joinNumber)
        {
            if (reversed)
            {
                builder.append(this.fullyQualifiedLeftColumnName).append(" cf").append(joinNumber);
            }
            else
            {
                builder.append(this.fullyQualifiedFromColumn).append(" cf").append(joinNumber).append(", ");
                builder.append(this.fullyQualifiedToColumn).append(" ct").append(joinNumber);
            }
        }

        private void appendGroupByAttributes(StringBuilder builder)
        {
            if (reversed)
            {
                builder.append(this.fullyQualifiedLeftColumnName);
            }
            else
            {
                builder.append(this.fullyQualifiedFromColumn).append(", ");
                builder.append(this.fullyQualifiedToColumn);
            }
        }
    }
}