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

import java.sql.PreparedStatement;
import java.sql.SQLException;


public class ImplicitJoinClause implements JoinClause
{
    private final int tableNumber;
    private String tableAliasPrefix = "t";
    private boolean isMapped = true;
    private WhereClause whereClause;
    private final MithraObjectPortal portal;
    private MapperStackImpl mapperStackImpl;

    public ImplicitJoinClause(MithraObjectPortal portal, int tableNumber)
    {
        this.tableNumber = tableNumber;
        this.portal = portal;
    }

    public ImplicitJoinClause(MithraObjectPortal portal, int tableNumber, String tableAliasPrefix,
            MapperStackImpl immutableMapperStack)
    {
        this(portal, tableNumber);
        this.mapperStackImpl = immutableMapperStack;
        this.tableAliasPrefix = tableAliasPrefix;
    }

    public void addAttributeFromMapper(Attribute attr)
    {
        //nothing to do
    }

    public void addAttribute(Attribute attr)
    {
        //nothing to do
    }

    public boolean belongsToInnerSql(SqlQuery query)
    {
        return false;
    }

    public void generateSql(SqlQuery query, boolean notExists)
    {
        if (notExists || query.getWhereClause().isInOrClause())
        {
            throw new RuntimeException("triangle joins and notExists/or are not supported");
        }
        this.whereClause = query.getWhereClause();
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

    public void reset()
    {
        this.setMapped(false);
    }

    public WhereClause getWhereClause()
    {
        return this.whereClause;
    }

    private int getTableNumber()
    {
        return tableNumber;
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

    public void appendJoinsToFromClause(SqlQuery query, StringBuilder fromClause, String withLock, String withoutLock)
    {
        fromClause.append(", ");
        appendTableAndSuperClasses(query, fromClause, withLock, withoutLock);
    }

    public int setJoinSqlParameters(PreparedStatement ps, int count, SqlQuery query) throws SQLException
    {
        //nothing to do
        return count;
    }

    public boolean hasSameWhereClause(JoinClause joinClause)
    {
        return true;
    }

    public boolean isAggregateJoin()
    {
        return false;
    }

    public void insertWhereSql(int position, String sql)
    {
        this.getWhereClause().insert(position, sql);
    }

    public void generateMixedJoinSql(MapperStackImpl mapperStack, JoinClause otherJoinClause, String fullyQualifiedLeftColumnName,
            String fullyQualifiedFromColumn, String fullyQualifiedToColumn, SqlParameterSetter parameterSetter, boolean reversed, SqlQuery query)
    {
        if (reversed)
        {
            this.whereClause.append("((");
            this.whereClause.append(fullyQualifiedLeftColumnName).append(" >= ");
            this.whereClause.append(fullyQualifiedFromColumn).append(" and ") ;
            this.whereClause.append(fullyQualifiedLeftColumnName).append(" < ");
            this.whereClause.append(fullyQualifiedToColumn).append(") or (");
            this.whereClause.append(fullyQualifiedToColumn).append(" = ? and ");
            this.whereClause.append(fullyQualifiedLeftColumnName).append(" = ? ))");
        }
        else
        {
            this.whereClause.append("((");
            this.whereClause.append(fullyQualifiedLeftColumnName).append(" >= ");
            this.whereClause.append(fullyQualifiedFromColumn).append(" and ") ;
            this.whereClause.append(fullyQualifiedLeftColumnName).append(" < ");
            this.whereClause.append(fullyQualifiedToColumn).append(") or (");
            this.whereClause.append(fullyQualifiedToColumn).append(" = ? and ");
            this.whereClause.append(fullyQualifiedLeftColumnName).append(" = ? ))");
        }
        this.whereClause.addSqlParameterSetter(parameterSetter);
    }

    public void generateJoinSql(SqlQuery query, String fullyQualifiedLeftColumnName, String fullyQualifiedRightHandColumn, String operator)
    {
        boolean insertedAnd = query.beginAnd();
        query.appendWhereClause(fullyQualifiedLeftColumnName);
        query.appendWhereClause(operator);
        query.appendWhereClause(fullyQualifiedRightHandColumn);
        query.endAnd(insertedAnd);
    }

    public void generateAsOfJoinSql(SqlQuery query, MapperStackImpl mapperStack, String fullyQualifiedLeftColumnName, String fullyQualifiedRightHandColumn, String operator)
    {
        boolean insertedAnd = query.beginAnd();
        query.appendWhereClause(fullyQualifiedLeftColumnName);
        query.appendWhereClause(operator);
        query.appendWhereClause(fullyQualifiedRightHandColumn);
        query.endAnd(insertedAnd);
    }

    public boolean isTopLevel()
    {
        return true;
    }

    public String getFullyQualifiedColumnNameFor(String fullyQualifiedColumnName)
    {
        return fullyQualifiedColumnName;
    }

    public String getFullyQualifiedOrderByColumnNameFor(String orderByColumnName)
    {
        return orderByColumnName;
    }

    public boolean mustUseExistsForMapperOnly(SqlQuery query)
    {
        return false;
    }

    public boolean allowsInClauseTempJoinReplacement()
    {
        return false;
    }

    public void restoreMapper(SqlQuery sqlQuery)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void computeAggregationForTree()
    {
        //nothing to do
    }
}
