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

import com.gs.fw.common.mithra.generator.AbstractAttribute;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.RelationshipAttribute;
import com.gs.fw.common.mithra.generator.queryparser.ASTAttributeName;
import com.gs.fw.common.mithra.generator.queryparser.ASTRelationalExpression;
import com.gs.fw.common.mithra.generator.queryparser.MithraQLVisitorAdapter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Stack;


public class JoinOrderQueryConversionVisitor extends MithraQLVisitorAdapter
{

    private RelationshipAttribute relationshipAttribute;
    private HashSet joins = new HashSet();

    public JoinOrderQueryConversionVisitor(RelationshipAttribute relationshipAttribute)
    {
        this.relationshipAttribute = relationshipAttribute;
        this.relationshipAttribute.getParsedQuery().childrenPolymorphicAccept(this, null);
    }

    public JoinNode getJoinTree()
    {
        JoinNode root = null;
        HashSet<String> alreadyProcessed = new HashSet<String>();
        Stack<JoinNode> toFollow = new Stack<JoinNode>();
        root = createFirstJoinNode(root, alreadyProcessed, toFollow);
        while(!toFollow.isEmpty() && !this.joins.isEmpty())
        {
            JoinNode currentRoot = toFollow.pop();
            for(Iterator it = this.joins.iterator(); it.hasNext(); )
            {
                Join join = (Join) it.next();
                if (join.hasObject(currentRoot.getLeft()))
                {
                    it.remove();
                    join.alignToLeft(currentRoot.getLeft());
                    if (alreadyProcessed.contains(join.getRight().getClassName()))
                    {
                        throw new RuntimeException("Triangluar relationship not supported, in relationship "+relationshipAttribute.getName()+" in object "+
                                relationshipAttribute.getFromObject().getClassName()+" joining "+join.getLeft().getClassName()+" and "+
                                join.getRight().getClassName());
                    }
                    toFollow.push(currentRoot.addRight(join.getRight(), relationshipAttribute));
                    alreadyProcessed.add(join.getRight().getClassName());
                }
            }
        }
        root.reorder(relationshipAttribute.getRelatedObject());
        return root;
    }

    private JoinNode createFirstJoinNode(JoinNode root, HashSet<String> alreadyProcessed, Stack<JoinNode> toFollow)
    {
        for(Iterator it = this.joins.iterator(); it.hasNext(); )
        {
            Join join = (Join) it.next();
            if (join.isJoinedToThis)
            {
                it.remove();
                join.alignToLeft(this.relationshipAttribute.getFromObject());
                if (root == null)
                {
                    root = new JoinNode(this.relationshipAttribute.getFromObject(), true);
                }
                toFollow.push(root.addRight(join.getRight(), relationshipAttribute));
                alreadyProcessed.add(join.getRight().getClassName());
            }
        }
        return root;
    }

    private void addJoin(ASTRelationalExpression node)
    {
        AbstractAttribute leftAttribute = node.getLeft().getAttribute();
        AbstractAttribute rightAttribute = ((ASTAttributeName) node.getRight()).getAttribute();
        Join join = new Join(leftAttribute.getOwner(), rightAttribute.getOwner(), node.involvesThis());
        this.joins.add(join);
    }

    public Object visit(ASTRelationalExpression node, Object data)
    {
        if (node.isJoin())
        {
            addJoin(node);
        }
        return null;
    }

    private static class Join
    {
        private MithraObjectTypeWrapper left;
        private MithraObjectTypeWrapper right;
        private boolean isJoinedToThis;

        public Join(MithraObjectTypeWrapper left, MithraObjectTypeWrapper right, boolean joinedToThis)
        {
            this.left = left;
            this.right = right;
            isJoinedToThis = joinedToThis;
        }

        public boolean equals(Object obj)
        {
            Join other = (Join) obj;

            return (other.isJoinedToThis == this.isJoinedToThis &&
                    ((other.left.getClassName().equals(this.left.getClassName()) && other.right.getClassName().equals(this.right.getClassName())) ||
                    ((other.right.getClassName().equals(this.left.getClassName()) && other.left.getClassName().equals(this.right.getClassName())))));
        }

        public int hashCode()
        {
            return this.left.getClassName().hashCode() ^ this.right.getClassName().hashCode();
        }

        public void alignToLeft(MithraObjectTypeWrapper fromObject)
        {
            if (!left.getClassName().equals(fromObject.getClassName()))
            {
                MithraObjectTypeWrapper tmp = left;
                left = right;
                right = tmp;
            }
        }

        public boolean hasObject(MithraObjectTypeWrapper lastRight)
        {
            return this.left.getClassName().equals(lastRight.getClassName()) ||
                    this.right.getClassName().equals(lastRight.getClassName());
        }

        public MithraObjectTypeWrapper getLeft()
        {
            return left;
        }

        public MithraObjectTypeWrapper getRight()
        {
            return right;
        }
    }

}
