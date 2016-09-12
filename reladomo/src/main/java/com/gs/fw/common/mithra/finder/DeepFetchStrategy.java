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

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.util.List;
import java.util.Set;


public abstract class DeepFetchStrategy
{

    public abstract List deepFetch(DeepFetchNode node, boolean bypassCache, boolean forceImplicitJoin);

    public abstract DeepFetchResult deepFetchFirstLinkInMemory(DeepFetchNode node);

    public abstract List deepFetchAdhocUsingTempContext(DeepFetchNode node, TupleTempContext tempContext, Object parentPrototype, List immediateParentList);

    public List finishAdhocDeepFetch(DeepFetchNode node, DeepFetchResult resultSoFar)
    {
        throw new RuntimeException("should not get here");
    }

    public abstract List deepFetchAdhocUsingInClause(DeepFetchNode node, Attribute singleAttribute, List parentList);

    public boolean canFinishAdhocDeepFetchResult()
    {
        return false;
    }
}
