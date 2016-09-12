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

import java.util.*;

import com.gs.fw.common.mithra.generator.*;
import com.gs.fw.common.mithra.generator.queryparser.*;
import com.gs.fw.common.mithra.generator.util.*;


public class JoinNode
{
    private MithraObjectTypeWrapper left;
    private Join parentJoin;
    private List<JoinNode> furtherJoins = new ArrayList(2);
    private boolean isJoinedToThis;
    private boolean isEndNode;

    public JoinNode(MithraObjectTypeWrapper left, boolean joinedToThis)
    {
        this.left = left;
        isJoinedToThis = joinedToThis;
    }

    public JoinNode(MithraObjectTypeWrapper left, boolean joinedToThis, Join parentJoin)
    {
        this.left = left;
        isJoinedToThis = joinedToThis;
        this.parentJoin = parentJoin;
    }

    public boolean isJoinedToThis()
    {
        return isJoinedToThis;
    }

    public JoinNode addRight(MithraObjectTypeWrapper right, RelationshipAttribute relationshipAttribute)
    {
        JoinNode result = new JoinNode(right, false, new Join(this.left, right, this.isJoinedToThis, relationshipAttribute));
        this.furtherJoins.add(result);
        return result;
    }

    public List<JoinNode> getFurtherJoins()
    {
        return furtherJoins;
    }

    public MithraObjectTypeWrapper getLeft()
    {
        return left;
    }

    public void addJoin(JoinNode current)
    {
        this.furtherJoins.add(current);
    }

    public Join getParentJoin()
    {
        return parentJoin;
    }

    public void reorder(MithraObjectTypeWrapper last)
    {
        determineEndNode(last);
        for(int i=0;i<furtherJoins.size();i++)
        {
            JoinNode child = furtherJoins.get(i);
            child.reorder(last);
        }
        if (this.furtherJoins.size() < 2) return;
        for(int i=0;i<furtherJoins.size();i++)
        {
            JoinNode child = furtherJoins.get(i);
            if (child.terminatesIn(last))
            {
                JoinNode first = furtherJoins.get(0);
                furtherJoins.set(0, child);
                furtherJoins.set(i, first);
                return;
            }
        }
    }

    private void determineEndNode(MithraObjectTypeWrapper last)
    {
        if (this.isJoinedToThis) return;
        if (this.left.getClassName().equals(last.getClassName()))
        {
            this.isEndNode = true;
        }
        if (this.furtherJoins.size() == 0)
        {
            this.isEndNode = true;
        }
    }

    private boolean terminatesIn(MithraObjectTypeWrapper last)
    {
        if (this.left.getClassName().equals(last.getClassName()))
        {
            return true;
        }
        for(int i=0;i<furtherJoins.size();i++)
        {
            JoinNode child = furtherJoins.get(i);
            if (child.terminatesIn(last)) return true;
        }
        return false;
    }

    public void addIndices()
    {
        if (this.parentJoin != null) this.parentJoin.addIndices();
        for(int i=0;i<furtherJoins.size();i++)
        {
            furtherJoins.get(i).addIndices();
        }
    }

    public void addConstantSets()
    {
        if (this.parentJoin != null) this.parentJoin.addConstantSets();
        for(int i=0;i<furtherJoins.size();i++)
        {
            furtherJoins.get(i).addConstantSets();
        }
    }

    public void addJoinsToConstantPool()
    {
        if (this.parentJoin != null) this.parentJoin.addJoinsToConstantPool();
        for(int i=0;i<furtherJoins.size();i++)
        {
            furtherJoins.get(i).addJoinsToConstantPool();
        }
    }

    public void addOperationsToConstantPool()
    {
        if (this.parentJoin != null) this.parentJoin.addOperationsToConstantPool();
        for(int i=0;i<furtherJoins.size();i++)
        {
            furtherJoins.get(i).addOperationsToConstantPool();
        }
    }

    public void autoAddSourceAndAsOfAttributeJoins()
    {
        if (this.parentJoin != null) this.parentJoin.autoAddSourceAndAsOfAttributeJoins();
        for(int i=0;i<furtherJoins.size();i++)
        {
            furtherJoins.get(i).autoAddSourceAndAsOfAttributeJoins();
        }
    }

    public boolean addJoinExpression(ASTRelationalExpression node)
    {
        if (this.parentJoin != null)
        {
            if (this.parentJoin.addJoin(node)) return true;
        }
        for(int i=0;i<furtherJoins.size();i++)
        {
            if (furtherJoins.get(i).addJoinExpression(node)) return true;
        }
        return false;
    }

    public boolean addConstraintExpression(ASTRelationalExpression node)
    {
        if (this.parentJoin != null)
        {
            if (this.parentJoin.addConstraint(node)) return true;
        }
        for(int i=0;i<furtherJoins.size();i++)
        {
            if (furtherJoins.get(i).addConstraintExpression(node)) return true;
        }
        return false;
    }

    public boolean addOrConstraintExpression(MithraObjectTypeWrapper constrainedClass, ASTOrExpression node, boolean belongsToThis)
    {
        if (this.parentJoin != null)
        {
            if (this.parentJoin.addOrConstraint(constrainedClass, node, belongsToThis)) return true;
        }
        for(int i=0;i<furtherJoins.size();i++)
        {
            if (furtherJoins.get(i).addOrConstraintExpression(constrainedClass, node, belongsToThis)) return true;
        }
        return false;
    }

    private String buildMappedOperationForBranch(int start, String variable)
    {
        String result = "";
        for(int i=start;i<furtherJoins.size();i++)
        {
            if (i > start)
            {
                result += ".and(";
            }
            result += "new MappedOperation("+variable+"b"+i+"Mapper, ";
            result += "new All("+variable+"b"+i+"Mapper.getAnyRightAttribute()))";
            if (i > start)
            {
                result += ")";
            }
        }
        return result;
    }

    public String constructReverseMapperCommon(String variableName, boolean pure, String name)
    {
        String variable = StringUtility.firstLetterToLower(variableName);
        StringBuilder builder = new StringBuilder();
        this.constructReverseMapper(variable, pure, builder);
        if (name != null)
        {
            builder.append(variable).append("Mapper.setName(\"").append(name).append("\");\n");
            return builder.append("return ").append(variable).append("Mapper;").toString();
        }
        else
        {
            String assignment = "Mapper " + variable + "Mapper =";
            int start = builder.lastIndexOf(assignment);
            builder.replace(start, start +(assignment.length()), "return");
            return builder.toString();
        }
    }

    private void constructReverseMapper(String variable, boolean pure, StringBuilder builder)
    {
        if (this.isEndNode)
        {
            if (furtherJoins.size() == 0)
            {
                builder.append(pure ? this.parentJoin.constructPureMapper(variable+"Mapper") : this.parentJoin.constructMapper(variable+"Mapper")).append('\n');
            }
            else
            {
                builder.append(pure ? this.parentJoin.constructPureMapper(variable+"pMapper") : this.parentJoin.constructMapper(variable+"pMapper")).append('\n');
                for(int i=0;i<furtherJoins.size();i++)
                {
                    this.furtherJoins.get(i).constructReverseMapper(variable+'b'+i, pure, builder);
                }
                builder.append("Mapper ").append(variable).append("Mapper = ");
                builder.append("new FilteredMapper(").append(variable).append("pMapper, null, ");
                builder.append(buildMappedOperationForBranch(0, variable)).append(");\n");
            }
        }
        else
        {
            if (parentJoin == null)
            {
                if (this.furtherJoins.size() == 1)
                {
                    this.furtherJoins.get(0).constructReverseMapper(variable, pure, builder);
                }
                else
                {
                    for(int i=0;i<furtherJoins.size();i++)
                    {
                        this.furtherJoins.get(i).constructReverseMapper(variable+'b'+i, pure, builder);
                    }
                    builder.append("Mapper ").append(variable).append("Mapper = ");
                    builder.append("new FilteredMapper(").append(variable).append("b0Mapper, ");
                    builder.append(buildMappedOperationForBranch(1, variable)).append(", null);\n");
                }
            }
            else
            {
                builder.append(pure ? this.parentJoin.constructPureMapper(variable+"c0Mapper") : this.parentJoin.constructMapper(variable+"c0Mapper")).append('\n');
                if (this.furtherJoins.size() == 1)
                {
                    this.furtherJoins.get(0).constructReverseMapper(variable+"c1", pure, builder);
                }
                else
                {
                    for(int i=0;i<furtherJoins.size();i++)
                    {
                        this.furtherJoins.get(i).constructReverseMapper(variable+'b'+i, pure, builder);
                    }
                    builder.append("Mapper ").append(variable).append("c1Mapper = ");
                    builder.append("new FilteredMapper(").append(variable).append("b0Mapper, ");
                    builder.append(buildMappedOperationForBranch(1, variable)).append(", null);\n");
                }
                builder.append("Mapper ").append(variable).append("Mapper = ").append("new ChainedMapper(");
                builder.append(variable).append("c0Mapper").append(',').append(variable).append("c1Mapper");
                builder.append(");\n");
            }
        }
    }

    /*
    an inverted tree inverts the core relationship path, but leaves the dangling paths alone.
    Legend: S = start, E = end, D = dangling (existence) clause, M = middle join (e.g. many-to-many)

      S                 E
     /   becomes       /
    E                 S

        S                 E
       /   becomes       /
      M                 M
     /                 /
    E                 S

      S                 E                                                               S
     /   becomes       / \  Note: the initial tree in memory is really setup as:       /
    E                 S   D       but E has the isEndNode flag set on it              E
     \                                                                               /
      D                                                                             D

      S                   E
     / \   becomes       /     Note: again, the result tree has isEndNode set on the new S node. 
    E   D               S
                         \
                          D

        S                 E
       /   becomes       /
      M                 M
     / \               / \
    E   D             S   D
     */
    public JoinNode invert()
    {
        List<JoinNode> toInvert = new ArrayList();
        JoinNode cur = this;
        while(!cur.isEndNode)
        {
            toInvert.add(cur);
            cur = cur.getFurtherJoins().get(0);
        }
        // cur is now the ultimate end node
        JoinNode inverted = new JoinNode(cur.left, true);
        inverted.furtherJoins.addAll(cur.furtherJoins);
        JoinNode curInverted = inverted;
        for(int i=toInvert.size() - 1; i >= 0; i--)
        {
            JoinNode nextToInvert = toInvert.get(i);
            JoinNode nextInverted = new JoinNode(nextToInvert.left, false);
            if (nextToInvert.furtherJoins.size() > 1)
            {
                nextInverted.furtherJoins.addAll(nextToInvert.furtherJoins.subList(1, nextToInvert.furtherJoins.size()));
            }
            nextInverted.parentJoin = cur.parentJoin.getReverseJoin();
            curInverted.furtherJoins.add(0, nextInverted);
            curInverted = nextInverted;
            cur = nextToInvert;
        }
        curInverted.isEndNode = true;
        return inverted;
    }

    public List getMainJoins()
    {
        ArrayList result = new ArrayList();
        JoinNode node = this.furtherJoins.get(0);
        while(!node.isEndNode)
        {
            result.add(node.parentJoin);
            node = node.furtherJoins.get(0);
        }
        result.add(node.parentJoin);
        result.remove(0);
        return result;
    }

    public QueryConverter createQueryConverter(RelationshipAttribute relationshipAttribute)
    {
        //note, we assume this is the inverted version of the tree.
        JoinNode result = new JoinNode(this.left, true);
        JoinNode original = this;
        JoinNode converted = result;
        while(!original.getFurtherJoins().get(0).isEndNode)
        {
            List<JoinNode> originalFurtherJoins = original.getFurtherJoins();
            JoinNode nextOriginal = originalFurtherJoins.get(0);
            JoinNode nextConverted = new JoinNode(nextOriginal.left, false, nextOriginal.parentJoin);
            converted.furtherJoins.add(nextConverted);
            if (originalFurtherJoins.size() > 1)
            {
                converted.furtherJoins.addAll(originalFurtherJoins.subList(1, originalFurtherJoins.size()));
            }
            converted = nextConverted;
            original = nextOriginal;
        }
        converted.isEndNode = true;
        List<JoinNode> originalFurtherJoins = original.getFurtherJoins();
        List<JoinNode> danglingNodeAtEnd = Collections.emptyList();
        if (originalFurtherJoins.size() > 1)
        {
            danglingNodeAtEnd = originalFurtherJoins.subList(1, originalFurtherJoins.size());
        }
        return new QueryConverter(relationshipAttribute, originalFurtherJoins.get(0), result, danglingNodeAtEnd);
    }

    public boolean isEndNode()
    {
        return isEndNode;
    }

    public String getUsedParameterVariables(RelationshipAttribute attr)
    {
        int count = attr.getParameterCount();
        String result = "";
        for(int i=0;i<count;i++)
        {
            String param = attr.getParameterVariableAt(i);
            if (this.parentJoin != null && this.parentJoin.usesParameter(param))
            {
                if (result.length() > 0) result += ", ";
                result += param;
                break;
            }
            for(int c = 0; c < this.furtherJoins.size(); c++)
            {
                if (furtherJoins.get(c).usesParameter(param))
                {
                    if (result.length() > 0) result += ", ";
                    result += param;
                    break;
                }
            }
        }
        return result;
    }

    public String getUsedParameters(RelationshipAttribute attr)
    {
        int count = attr.getParameterCount();
        String result = "";
        for(int i=0;i<count;i++)
        {
            String param = attr.getParameterVariableAt(i);
            if (this.parentJoin != null && this.parentJoin.usesParameter(param))
            {
                if (result.length() > 0) result += ", ";
                result += attr.getParameterTypeAt(i) + " "+param;
                break;
            }
            for(int c = 0; c < this.furtherJoins.size(); c++)
            {
                if (furtherJoins.get(c).usesParameter(param))
                {
                    if (result.length() > 0) result += ", ";
                    result += attr.getParameterTypeAt(i) + " "+param;
                    break;
                }
            }
        }
        return result;
    }

    private boolean usesParameter(String param)
    {
        if (this.parentJoin != null && this.parentJoin.usesParameter(param))
        {
            return true;
        }
        for(int c = 0; c < this.furtherJoins.size(); c++)
        {
            if (furtherJoins.get(c).usesParameter(param))
            {
                return true;
            }
        }
        return false;
    }
}
