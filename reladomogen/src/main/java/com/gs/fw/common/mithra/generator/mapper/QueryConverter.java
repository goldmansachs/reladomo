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

package com.gs.fw.common.mithra.generator.mapper;

import com.gs.fw.common.mithra.generator.RelationshipAttribute;

import java.util.List;


public class QueryConverter
{

    private RelationshipAttribute relationshipAttribute;
    private JoinNode treeRootWithoutSource;
    private JoinNode sourceNode;
    private List<JoinNode> danglingNodesAtEnd;

    public QueryConverter(RelationshipAttribute relationshipAttribute, JoinNode sourceNode, JoinNode treeRootWithoutSource, List<JoinNode> danglingNodesAtEnd)
    {
        this.relationshipAttribute = relationshipAttribute;
        this.sourceNode = sourceNode;
        this.treeRootWithoutSource = treeRootWithoutSource;
        this.danglingNodesAtEnd = danglingNodesAtEnd;
    }

    public boolean hasCondition()
    {
        return !sourceNode.getFurtherJoins().isEmpty() || sourceNode.getParentJoin().hasRightFilters();
    }

    public String getCondition()
    {
        //todo
        return "FIXME";
    }

    public String getOperation()
    {
        String opStr = constructOperationWithoutMapper();
        if (hasMapperFragment())
        {
            opStr = "new MappedOperation("+relationshipAttribute.getFromObject().getFinderClassName()+".zGet"+
                    relationshipAttribute.getMapperFragmentName()+"MapperFragment("+getMapperFragmentParameterVariables()+"), "+opStr+")";
        }
        return opStr;
    }

    public String getMapperFragmentParameterVariables()
    {
        if (relationshipAttribute.hasParameters())
        {
            return treeRootWithoutSource.getUsedParameterVariables(this.relationshipAttribute);
        }
        return "";
    }

    private String constructOperationWithoutMapper()
    {
        String operation = sourceNode.getParentJoin().constructOperationFromRight(this.hasDangleMapper());
        if (this.hasDangleMapper())
        {
            operation = operation + ".and(new MappedOperation("+relationshipAttribute.getFromObject().getFinderClassName()+".zGet"+relationshipAttribute.getMapperPartialName()+"DangleMapper("+
                    relationshipAttribute.getParameterVariables()+"), "
                    +this.danglingNodesAtEnd.get(0).getParentJoin().getRight().getFinderClassName()+".all()))";
        }
        return operation;
    }

    public boolean hasMapperFragment()
    {
        return !treeRootWithoutSource.isEndNode();
    }

    public String constructMapperFragment()
    {
        return treeRootWithoutSource.constructReverseMapperCommon(relationshipAttribute.getName(), false, null);
    }

    public boolean hasMapperFragmentParameters()
    {
        return getMapperFragmentParameterVariables().length() > 0;
    }

    public String getMapperFragmentParameters()
    {
        if (relationshipAttribute.hasParameters())
        {
            return treeRootWithoutSource.getUsedParameters(this.relationshipAttribute);
        }
        return "";
    }

    public boolean hasDangleMapper()
    {
        return !this.danglingNodesAtEnd.isEmpty();
    }

    public String constructDangleMapper()
    {
        return this.danglingNodesAtEnd.get(0).getParentJoin().constructMapper("dangleMapper") + "\n"+ "return dangleMapper;";
    }

    public boolean isExtractorBasedMultiEquality()
    {
        return this.sourceNode.getParentJoin().isExtractorBasedMultiEquality(this.hasDangleMapper());
    }

    public String getRelationshipMultiExtractorConstructor()
    {
        return this.sourceNode.getParentJoin().getRelationshipMultiExtractorConstructor();
    }

    public String getFindByUniqueLookupParameters()
    {
        return this.sourceNode.getParentJoin().getFindByUniqueLookupParameters();
    }
}
