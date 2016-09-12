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

import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.Map;
import java.util.Set;
import java.io.Serializable;



public interface HavingOperation extends Serializable
{

    public HavingOperation and(HavingOperation op);

    public HavingOperation or(HavingOperation op);

    public void zGenerateSql(AggregateSqlQuery query);

    public MithraObjectPortal getResultObjectPortal();

    public void zAddMissingAggregateAttributes(Set<MithraAggregateAttribute> aggregateAttributes, Map<String, MithraAggregateAttribute> nameToAttributeMap);

    public boolean zMatches(AggregateData aggregateData, Map<com.gs.fw.common.mithra.MithraAggregateAttribute, String> attributeToNameMap);

    public void zGenerateMapperSql(AggregateSqlQuery aggregateSqlQuery);

    public Operation zCreateMappedOperation();
}
