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
import com.gs.fw.common.mithra.util.InternalList;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.util.ListFactory;

import java.util.List;


public class ChainedDeepFetchStrategy extends DeepFetchStrategy
{

    private final InternalList chainedStrategies;

    public ChainedDeepFetchStrategy(Mapper mapper, OrderBy orderBy)
    {
        List<Mapper> unChainedMappers = mapper.getUnChainedMappers();
        chainedStrategies = new InternalList(unChainedMappers.size());
        Mapper parent = mapper.getParentMapper();
        Mapper grandParent = mapper.getParentMapper();
        Mapper alternateMapper = null;
        for(int i=0;i<unChainedMappers.size();i++)
        {
            Mapper curMapper = unChainedMappers.get(i);
            Mapper nextParent = curMapper;
            if (parent != null)
            {
                nextParent = parent.link(curMapper);
            }
            if (i == 1)
            {
                alternateMapper = new ChainedMapper(unChainedMappers.get(0), unChainedMappers.get(1));
            }
            else if (i > 1)
            {
                alternateMapper = new ChainedMapper(alternateMapper, unChainedMappers.get(i));
            }
            Mapper alternateMapperWithParent = alternateMapper;
            if (grandParent != null && alternateMapperWithParent != null)
            {
                alternateMapperWithParent = grandParent.link(alternateMapper);
            }
            DelegatingSingleLinkDeepFetchStrategy delegatingStrategy = new DelegatingSingleLinkDeepFetchStrategy(nextParent, i == unChainedMappers.size() - 1 ? orderBy : null, alternateMapperWithParent, i);
            chainedStrategies.add(delegatingStrategy);
            parent = nextParent;
        }
    }

    public List deepFetch(DeepFetchNode node, boolean bypassCache, boolean forceImplicitJoin)
    {
        //todo: make sure allocation doesn't happen twice!!!
        node.allocatedChainedResults(chainedStrategies.size());
        FastList result = new FastList(chainedStrategies.size());
        for(int i=0;i<chainedStrategies.size();i++)
        {
            List list = ((DeepFetchStrategy) chainedStrategies.get(i)).deepFetch(node, bypassCache, forceImplicitJoin);
            if (list == null)
            {
                node.setResolvedList(ListFactory.EMPTY_LIST, i);
            }
            else
            {
                result.add(list);
            }
        }
        return result;
    }

    @Override
    public DeepFetchResult deepFetchFirstLinkInMemory(DeepFetchNode node)
    {
        return DeepFetchResult.incompleteResult();
    }

    @Override
    public List deepFetchAdhocUsingTempContext(DeepFetchNode node, TupleTempContext tempContext, Object parentPrototype, List immediateParentList)
    {
        //todo: make sure allocation doesn't happen twice!!!
        node.allocatedChainedResults(chainedStrategies.size());
        FastList result = new FastList(chainedStrategies.size());
        for(int i=0;i<chainedStrategies.size();i++)
        {
            List list = ((DeepFetchStrategy) chainedStrategies.get(i)).deepFetchAdhocUsingTempContext(node, tempContext, parentPrototype, immediateParentList);
            if (list == null)
            {
                node.setResolvedList(ListFactory.EMPTY_LIST, i);
            }
            else
            {
                result.add(list);
            }
        }
        return result;
    }

    @Override
    public List deepFetchAdhocUsingInClause(DeepFetchNode node, Attribute singleAttribute, List parentList)
    {
        throw new RuntimeException("should not get here");
    }
}
