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
import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.Cardinality;
import com.gs.fw.common.mithra.generator.Index;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.RelationshipAttribute;
import com.gs.fw.common.mithra.generator.queryparser.ASTAndExpression;
import com.gs.fw.common.mithra.generator.queryparser.ASTAttributeName;
import com.gs.fw.common.mithra.generator.queryparser.ASTOrExpression;
import com.gs.fw.common.mithra.generator.queryparser.ASTRelationalExpression;
import com.gs.fw.common.mithra.generator.queryparser.MithraQLVisitorAdapter;
import com.gs.fw.common.mithra.generator.queryparser.SimpleNode;
import java.util.List;
import java.util.Map;


public class RelationshipConversionVisitor extends MithraQLVisitorAdapter
{

    private RelationshipAttribute relationshipAttribute;
    private JoinNode joinTreeRoot;
    private JoinNode invertedJoinTreeRoot;
    private QueryConverter queryConverter;

    public Join getJoinedToThis()
    {
        return joinTreeRoot.getFurtherJoins().get(0).getParentJoin();
    }

    private Join getJoinedToThisWithNoFurtherJoinsCheck()
    {
        checkFurtherJoinsDontExist();
        return this.getJoinedToThis();
    }

    public RelationshipConversionVisitor(RelationshipAttribute relationshipAttribute)
    {
        this.relationshipAttribute = relationshipAttribute;
        JoinOrderQueryConversionVisitor orderVisitor = new JoinOrderQueryConversionVisitor(relationshipAttribute);
        this.joinTreeRoot = orderVisitor.getJoinTree();
        this.relationshipAttribute.getParsedQuery().jjtAccept(this, null);
        this.joinTreeRoot.autoAddSourceAndAsOfAttributeJoins();
        this.invertedJoinTreeRoot = this.joinTreeRoot.invert();
        this.queryConverter = this.invertedJoinTreeRoot.createQueryConverter(this.relationshipAttribute);
    }

    public List getFurtherJoins()
    {
        return this.joinTreeRoot.getMainJoins();
    }

    public boolean isSingleAttributeJoin()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().isSingleAttributeJoinIgnoringRightSourceAttribute();
    }

    public boolean isNextObjectInJoinRelatedObject()
    {
        return this.getJoinedToThis().getRight().getClassName().equals(this.relationshipAttribute.getRelatedObject().getClassName());
    }

    private void checkNextJoinIsRelatedObject()
    {
        if (!isNextObjectInJoinRelatedObject())
        {
            throw new RuntimeException("shouldn't get here");
        }
    }

    private void checkFurtherJoinsDontExist()
    {
        if (joinTreeRoot.getFurtherJoins().size() > 1 || joinTreeRoot.getFurtherJoins().get(0).getFurtherJoins().size() > 0)
        {
            throw new RuntimeException("shouldn't get here");
        }
    }

    public boolean isSingleAttributeJoinIgnoringAsOfAttributes()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().isSingleAttributeJoinIgnoringRightSourceAndAsOfAttribute();
    }

    public Attribute[] getAttributesForInClauseEval()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getAttributesForInClauseEval();
    }

    public Attribute[] getAsOfAttributesForSingleCheck()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getAsOfAttributesForSingleCheck();
    }

    public Map getAsOfAttributesMap()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getAsOfAttributesMap();
    }

    public Attribute[] getLeftJoinAttributes()
    {
        return getJoinedToThisWithNoFurtherJoinsCheck().getLeftJoinAttributes();
    }

    public MithraObjectTypeWrapper getLeft()
    {
        return getJoinedToThis().getLeft();
    }

    public MithraObjectTypeWrapper getRight()
    {
        return getJoinedToThis().getRight();
    }

    public Attribute[] getAttributesToSetOnRelatedObject()
    {
        checkNextJoinIsRelatedObject();
        return getJoinedToThis().getAttributesToSetOnRelatedObject();
    }

    public void addIndicies(Cardinality cardinality)
    {
        this.joinTreeRoot.addIndices();
        addJoinsToConstantPool();
        addOperationsToConstantPool();
        addConstantSets();
    }

    private void addConstantSets()
    {
        this.joinTreeRoot.addConstantSets();
    }

    private void addJoinsToConstantPool()
    {
        this.joinTreeRoot.addJoinsToConstantPool();
    }

    private void addOperationsToConstantPool()
    {
        this.joinTreeRoot.addOperationsToConstantPool();
    }

    public String constructReverseMapper()
    {
        return this.joinTreeRoot.constructReverseMapperCommon(relationshipAttribute.getName(), false, relationshipAttribute.getName());
    }

    public String constructPureReverseMapper()
    {
        return this.joinTreeRoot.constructReverseMapperCommon(relationshipAttribute.getName(), true, relationshipAttribute.getName());
    }

    public String constructMapper()
    {
        String reverseName = relationshipAttribute.getReverseName();
        if (reverseName == null)
        {
            reverseName = relationshipAttribute.getName()+"_Reverse";
        }
        return invertedJoinTreeRoot.constructReverseMapperCommon(relationshipAttribute.getName(), false,
                reverseName);
    }

    private void addJoin(ASTRelationalExpression node)
    {
        joinTreeRoot.addJoinExpression(node);
    }

    private void addConstraint(ASTRelationalExpression node)
    {
        joinTreeRoot.addConstraintExpression(node);
    }

    protected void visitChildren(SimpleNode node, Object data)
    {
        if (node.jjtGetNumChildren() > 0)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
        }
    }

    public Object visit(ASTOrExpression node, Object data)
    {
        // an "or" clause is only allowed as part of a constant expression
        // it must all belong to the same object
        OrConstraintVisitor orConstraintVisitor = new OrConstraintVisitor();
        node.childrenPolymorphicAccept(orConstraintVisitor, null);
        this.joinTreeRoot.addOrConstraintExpression(orConstraintVisitor.getConstrainedClass(), node, orConstraintVisitor.isBelongsToThis());
        return data;
    }

    public Object visit(ASTAndExpression node, Object data)
    {
        this.visitChildren(node, data);
        return data;
    }

    public Object visit(ASTRelationalExpression node, Object data)
    {
        if (node.isJoin())
        {
            addJoin(node);
        }
        else
        {
            this.addConstraint(node);
        }
        return null;
    }

    public Attribute getAttributeToGetForSetOnRelatedObject(int index)
    {
        checkNextJoinIsRelatedObject();
        return getJoinedToThis().getAttributeToGetForSetOnRelatedObject(index);
    }

    public boolean isByPrimaryKey()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().isByPrimaryKey();
    }

    public Index getUniqueMatchingIndex()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getUniqueMatchingIndex();
    }

    public String getAsOfAttributesDataMatchesConditions()
    {
        return getJoinedToThisWithNoFurtherJoinsCheck().getAsOfAttributesDataMatchesConditions();
    }

    public String getEqualsConditions()
    {
        return getJoinedToThisWithNoFurtherJoinsCheck().getEqualsConditions();
    }

    public boolean requiresSrcObjectForEquals()
    {
        return getJoinedToThisWithNoFurtherJoinsCheck().requiresSrcObjectForEquals();
    }

    public boolean requiresSrcDataForEquals()
    {
        return getJoinedToThisWithNoFurtherJoinsCheck().requiresSrcDataForEquals();
    }

    public boolean requiresSrcObjectForHashCode()
    {
        return getJoinedToThisWithNoFurtherJoinsCheck().requiresSrcObjectForHashCode();
    }

    public boolean requiresSrcDataForHashCode()
    {
        return getJoinedToThisWithNoFurtherJoinsCheck().requiresSrcDataForHashCode();
    }

    public String getHashCodeComputationForPk(boolean offHeap)
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getHashCodeComputationForPk(offHeap);
    }

    public String getHashCodeComputation(AbstractAttribute[] attributes, boolean offHeap)
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getHashCodeComputation(attributes, offHeap);
    }

    public boolean hasDifferentOffHeapHash(AbstractAttribute[] attributes)
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().hasDifferentOffHeapHash(attributes);
    }

    public boolean hasDifferentOffHeapHashForPk()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().hasDifferentOffHeapHashForPk();
    }

    public String getCacheLookUpParameters()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getCacheLookUpParameters();
    }

    public boolean needsParameterOperationForPrimaryKey(String parameter)
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().needsParameterOperationForPrimaryKey(parameter);
    }

    public boolean needsParametersOperationForPrimaryKey()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().needsParametersOperationForPrimaryKey();
    }

    public boolean needsParameterOperationForUniqueIndex(String parameter)
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().needsParameterOperationForUniqueIndex(parameter);
    }

    public boolean needsParametersOperationForUniqueIndex()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().needsParametersOperationForUniqueIndex();
    }

    public boolean needsDefaultAsOfDatesOperation()
    {
        return this.getJoinedToThis().needsDefaultAsOfDatesOperation();
    }

    public String getDefaultAsOfDatesOperation()
    {
        return this.getJoinedToThis().getDefaultAsOfDatesOperation();
    }

    public boolean dependsOnFromAsOfAttributes()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().dependsOnFromAsOfAttributes();
    }

    public boolean needsDirectRefExtractors()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().needsDirectRefExtractors();
    }

    public String getFromDirectRefExtractors()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getFromDirectRefExtractors();
    }

    public String getToDirectRefExtractors()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getToDirectRefExtractors();
    }

    public String getOperationExpression()
    {
        return queryConverter.getOperation();
    }

    public boolean hasMapperFragment()
    {
        return this.queryConverter.hasMapperFragment();
    }

    public boolean hasMapperFragmentParameters()
    {
        return this.queryConverter.hasMapperFragmentParameters();
    }

    public String getMapperFragmentParameterVariables()
    {
        return this.queryConverter.getMapperFragmentParameterVariables();
    }

    public String getMapperFragmentParameters()
    {
        return this.queryConverter.getMapperFragmentParameters();
    }

    public String constructMapperFragment()
    {
        return this.queryConverter.constructMapperFragment();
    }

    public boolean hasDangleMapper()
    {
        return this.queryConverter.hasDangleMapper();
    }

    public String constructDangleMapper()
    {
        return this.queryConverter.constructDangleMapper();
    }

    public boolean isExtractorBasedMultiEquality()
    {
        return this.queryConverter.isExtractorBasedMultiEquality();
    }

    public String getRelationshipMultiExtractorConstructor()
    {
        return this.queryConverter.getRelationshipMultiExtractorConstructor();
    }

    public boolean requiresOverSpecifiedParameterCheck()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().requiresOverSpecifiedParameterCheck();
    }

    public String getOverSpecificationCheck()
    {
        return this.getJoinedToThisWithNoFurtherJoinsCheck().getOverSpecificationCheck();
    }

    public String getFindByUniqueLookupParameters()
    {
        return this.queryConverter.getFindByUniqueLookupParameters();
    }

    private static class OrConstraintVisitor extends MithraQLVisitorAdapter
    {
        private boolean gotSomething = false;
        private boolean belongsToThis = false;
        private MithraObjectTypeWrapper constrainedClass;

        public boolean isBelongsToThis()
        {
            return belongsToThis;
        }

        public MithraObjectTypeWrapper getConstrainedClass()
        {
            return constrainedClass;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            if (node.isJoin())
            {
                throw new RuntimeException("An 'or' clause can only be used in constant expressions, not joins");
            }
            validate(node.involvesThis(), node.getLeft());
            return data;
        }

        private void validate(boolean nodeInvolvesThis, ASTAttributeName attribute)
        {
            if (gotSomething)
            {
                if (belongsToThis != nodeInvolvesThis)
                {
                    throw new RuntimeException("Inside an 'or' clause, either everything must belong to 'this' or not");
                }
                if (!attribute.getOwner().getClassName().equals(constrainedClass.getClassName()))
                {
                    throw new RuntimeException("Inside an 'or' clause, all clauses must reference the same object");
                }
            }
            else
            {
                this.gotSomething = true;
                this.belongsToThis = nodeInvolvesThis;
                this.constrainedClass = attribute.getOwner();
            }
        }
    }
}