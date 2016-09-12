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

import com.gs.fw.common.mithra.attribute.Attribute;

import java.sql.PreparedStatement;
import java.sql.SQLException;


public interface JoinClause
{
    void addAttributeFromMapper(Attribute attr);

    void addAttribute(Attribute attr);

    boolean belongsToInnerSql(SqlQuery query);

    void generateSql(SqlQuery query, boolean notExists);

    String getTableAlias();

    boolean isMapped();

    void setMapped(boolean mapped);

    void reset();

    WhereClause getWhereClause();

    void appendJoinsToFromClause(SqlQuery query, StringBuilder fromClause, String withLock, String withoutLock);

    int setJoinSqlParameters(PreparedStatement ps, int count, SqlQuery query) throws SQLException;

    boolean hasSameWhereClause(JoinClause joinClause);

    boolean isAggregateJoin();

    void insertWhereSql(int position, String sql);

    void generateMixedJoinSql(MapperStackImpl mapperStack, JoinClause otherJoinClause, String fullyQualifiedLeftColumnName,
            String fullyQualifiedFromColumn, String fullyQualifiedToColumn, SqlParameterSetter parameterSetter, boolean reversed, SqlQuery query);

    void generateJoinSql(SqlQuery query, String fullyQualifiedLeftColumnName, String fullyQualifiedRightHandColumn, String operator);

    void generateAsOfJoinSql(SqlQuery query, MapperStackImpl mapperStack, String fullyQualifiedLeftColumnName,
                    String fullyQualifiedRightHandColumn, String operator);

    boolean isTopLevel();

    String getFullyQualifiedColumnNameFor(String fullyQualifiedColumnName);

    String getFullyQualifiedOrderByColumnNameFor(String orderByColumnName);

    boolean mustUseExistsForMapperOnly(SqlQuery query);

    boolean allowsInClauseTempJoinReplacement();

    public void restoreMapper(SqlQuery sqlQuery);

    public void computeAggregationForTree();
}
