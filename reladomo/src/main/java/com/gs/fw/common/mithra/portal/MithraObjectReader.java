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
import com.gs.fw.common.mithra.cacheloader.CacheLoaderMonitor;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.ResultSetParser;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Pair;
import com.gs.fw.common.mithra.util.RenewedCacheStats;

import java.util.List;
import java.util.Map;
import java.util.Set;


public interface MithraObjectReader
{
    public CachedQuery find(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, int rowcount, int numberOfThreads, boolean bypassCache, boolean forceImplicitJoin);

    public Cursor findCursor(AnalyzedOperation analyzedOperation, Filter postLoadFilter, OrderBy orderby, int rowcount, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin);

    public int count(Operation op);

    public List computeFunction(Operation op, OrderBy orderby, String sqlExpression,
                                ResultSetParser resultSetParser);

    public MithraDataObject refresh(MithraDataObject data, boolean lockInDatabase) throws MithraDatabaseException;

    public MithraDataObject refreshDatedObject(MithraDatedObject mithraDatedObject, boolean lockInDatabase) throws MithraDatabaseException;

    public List findAggregatedData(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                   Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, boolean bypassCache, Class bean);

    public void loadFullCache();

    public void reloadFullCache();

    public RenewedCacheStats renewCacheForOperation(Operation op);

    public Map extractDatabaseIdentifiers(Operation op);

    public Map extractDatabaseIdentifiers(Set sourceAttributeValueSet);
}
