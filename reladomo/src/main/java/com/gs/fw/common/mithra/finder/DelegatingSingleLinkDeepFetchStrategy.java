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
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.util.List;


public class DelegatingSingleLinkDeepFetchStrategy extends SingleLinkDeepFetchStrategy
{

    private final SingleLinkDeepFetchStrategy delegated;

    public DelegatingSingleLinkDeepFetchStrategy(Mapper mapper, OrderBy orderBy, Mapper alternateMapper, int chainPosition)
    {
        super(mapper, orderBy, alternateMapper, chainPosition);
        SingleLinkDeepFetchStrategy newDelegated;
        if (isResolvableInCache)
        {
            newDelegated = new SimpleToOneDeepFetchStrategy(this.mapper, this.orderBy, this.getAlternateMapper(), this.getChainPosition());
        }
        else
        {
            newDelegated = new SimpleToManyDeepFetchStrategy(this.mapper, this.orderBy, this.getAlternateMapper(), this.getChainPosition());
        }
        delegated = newDelegated;
    }

    public List deepFetch(DeepFetchNode node, boolean bypassCache, boolean forceImplicitJoin)
    {
        return this.delegated.deepFetch(node, bypassCache, forceImplicitJoin);
    }

    @Override
    public DeepFetchResult deepFetchFirstLinkInMemory(DeepFetchNode node)
    {
        return this.delegated.deepFetchFirstLinkInMemory(node);
    }

    @Override
    public List deepFetchAdhocUsingTempContext(DeepFetchNode node, TupleTempContext tempContext, Object parentPrototype, List immediateParentList)
    {
        return this.delegated.deepFetchAdhocUsingTempContext(node, tempContext, parentPrototype, immediateParentList);
    }

    @Override
    public List deepFetchAdhocUsingInClause(DeepFetchNode node, Attribute singleAttribute, List parentList)
    {
        return this.delegated.deepFetchAdhocUsingInClause(node, singleAttribute, parentList);
    }
}
