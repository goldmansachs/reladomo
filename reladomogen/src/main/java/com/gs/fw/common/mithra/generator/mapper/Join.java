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
import com.gs.fw.common.mithra.generator.AsOfAttribute;
import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.Index;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.RelationshipAttribute;
import com.gs.fw.common.mithra.generator.SourceAttribute;
import com.gs.fw.common.mithra.generator.queryparser.ASTAndExpression;
import com.gs.fw.common.mithra.generator.queryparser.ASTAttributeName;
import com.gs.fw.common.mithra.generator.queryparser.ASTInLiteral;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.queryparser.ASTOrExpression;
import com.gs.fw.common.mithra.generator.queryparser.ASTRelationalExpression;
import com.gs.fw.common.mithra.generator.queryparser.MithraQLVisitorAdapter;
import com.gs.fw.common.mithra.generator.queryparser.Operator;
import com.gs.fw.common.mithra.generator.queryparser.SimpleNode;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.*;


public class Join
{

    private MithraObjectTypeWrapper left;
    private MithraObjectTypeWrapper right;
    private boolean isJoinedToThis;
    private SimpleNode leftFilters;
    private SimpleNode rightFilters;
    private List<ASTRelationalExpression> joins = new ArrayList<ASTRelationalExpression>();
    private List<String> filterLiterals;
    private RelationshipAttribute relationshipAttribute;

    public Join(MithraObjectTypeWrapper left, MithraObjectTypeWrapper right, boolean joinedToThis, RelationshipAttribute relationshipAttribute)
    {
        this.left = left;
        this.right = right;
        isJoinedToThis = joinedToThis;
        this.relationshipAttribute = relationshipAttribute;
    }

    public void addIndices()
    {
        List leftAttributes = getLeftRelationshipAttributes();
        List rightAttributes = getRightRelationshipAttributes();

        if (leftFilters != null)
        {
            FilterIndexVisitor visitor = new FilterIndexVisitor();
            leftFilters.jjtAccept(visitor, null);
            leftAttributes.addAll(visitor.getAttributes());
        }
        if (rightFilters != null)
        {
            FilterIndexVisitor visitor = new FilterIndexVisitor();
            rightFilters.jjtAccept(visitor, null);
            rightAttributes.addAll(visitor.getAttributes());
        }
        left.addIndex(leftAttributes, right);
        right.addIndex(rightAttributes, left);
    }

    public List<AbstractAttribute> getLeftRelationshipAttributes()
    {
        List<AbstractAttribute> leftAttributes = new ArrayList<AbstractAttribute>();

        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            leftAttributes.add(exp.getLeft().getAttribute());
        }

        return leftAttributes;
    }

    public Attribute[] getLeftRelationshipAttributesAsArray()
    {
        List leftAttributes = getLeftRelationshipAttributes();
        Attribute[] attributeArray = new Attribute[leftAttributes.size()];

        for (int i = 0; i < attributeArray.length; i++)
        {
            attributeArray[i] = (Attribute) leftAttributes.get(i);
        }

        return attributeArray;
    }

    public List<AbstractAttribute> getRightRelationshipAttributes()
    {
        List<AbstractAttribute> rightAttributes = new ArrayList<AbstractAttribute>();

        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            rightAttributes.add(((ASTAttributeName) exp.getRight()).getAttribute());
        }

        return rightAttributes;
    }

    public Attribute[] getRightRelationshipAttributesAsArray()
    {
        List rightAttributes = getRightRelationshipAttributes();
        Attribute[] attributeArray = new Attribute[rightAttributes.size()];

        for (int i = 0; i < attributeArray.length; i++)
        {
            attributeArray[i] = (Attribute) rightAttributes.get(i);
        }

        return attributeArray;
    }

    public void autoAddSourceAndAsOfAttributeJoins()
    {
        this.autoAddSourceAttributeJoin();
        this.autoAddAsOfAttributeJoins();
    }

    public void autoAddSourceAttributeJoin()
    {
        if (right.hasSourceAttribute() && left.hasSourceAttribute())
        {
            SourceAttribute rightSource = right.getSourceAttribute();
            SourceAttribute leftSource = left.getSourceAttribute();
            if (leftSource.getType().equals(rightSource.getType()) && leftSource.getName().equals(rightSource.getName()))
            {
                if (hasNoOperationForRightAttribute(rightSource))
                {
                    this.joins.add(new ASTRelationalExpression(leftSource, rightSource, this.isJoinedToThis));
                }
            }
        }
    }

    public void autoAddAsOfAttributeJoins()
    {
        if (right.hasAsOfAttributes() && left.hasAsOfAttributes())
        {
            AsOfAttribute[] rightAsOfAttributes = right.getAsOfAttributes();
            for(int i=0;i<rightAsOfAttributes.length;i++)
            {
                if (hasNoOperationForRightAttribute(rightAsOfAttributes[i]))
                {
                    AsOfAttribute leftAsOfAttribute = left.getCompatibleAsOfAttribute(rightAsOfAttributes[i]);
                    if (leftAsOfAttribute != null)
                    {
                        this.joins.add(new ASTRelationalExpression(leftAsOfAttribute, rightAsOfAttributes[i], this.isJoinedToThis));
                    }

                }
            }
        }
    }

    public MithraObjectTypeWrapper getLeft()
    {
        return left;
    }

    public MithraObjectTypeWrapper getRight()
    {
        return right;
    }

    public String constructMapper(String variable, String finalExpression)
    {
        if (this.leftFilters != null || this.rightFilters != null)
        {
            return this.constructFilteredMapper(variable, finalExpression);
        }
        return constructPureMapper(variable, finalExpression);
    }

    public String constructMapper(String variable)
    {
        return this.constructMapper(variable, "Mapper " + variable + " = ");
    }

    public String constructPureMapper(String variable)
    {
        return this.constructPureMapper(variable, "Mapper " + variable + " = ");
    }

    public String constructPureMapper(String variable, String finalExpression)
    {
        if (this.joins.size() > 1)
        {
            return this.constructMultiEqualityMapper(variable, finalExpression);
        }
        return this.constructEqualityMapper(finalExpression);
    }

    private String constructMultiEqualityMapper(String variable, String finalExpression)
    {
        String result = "InternalList "+variable+"MapperList = new InternalList("+this.joins.size()+"); \n";
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            result += variable+"MapperList.add("+this.getJoinExpression(exp)+"); \n";
        }
        result += finalExpression + " new MultiEqualityMapper("+variable+"MapperList);";
        return result;
    }

    private String constructFilteredMapper(String variable, String finalExpression)
    {
        String result = constructPureMapper(variable + "InnerMapper", "Mapper " + variable + "InnerMapper = ");
        result += "\n"+finalExpression + " new FilteredMapper("+variable+"InnerMapper, "+
                constructFilters(leftFilters)+", "+constructFilters(rightFilters)+");";
        return result;
    }

    private String constructEqualityMapper(String finalExpression)
    {
        ASTRelationalExpression join = this.joins.get(0);
        return finalExpression + getJoinExpression(join)+";";
    }

    private String constructFilters(SimpleNode filters)
    {
        if (filters == null)
        {
            return "null";
        }
        FilterVisitor filterVisitor = new FilterVisitor();
        filters.jjtAccept(filterVisitor, null);
        return filterVisitor.getResult();
    }

    private int countEqualityFilters(SimpleNode filters)
    {
        if (filters == null)
        {
            return 0;
        }
        EqualityFilterCountVisitor filterVisitor = new EqualityFilterCountVisitor();
        filters.jjtAccept(filterVisitor, null);
        return filterVisitor.getResult();
    }

    private String getJoinExpression(ASTRelationalExpression join)
    {
        String result = null;
        MithraObjectTypeWrapper owner = join.getLeft().getOwner();
        int index = owner.getJoinIndexFromConstantPool(join);
        if (index < 0)
        {
            owner = ((ASTAttributeName)join.getRight()).getOwner();
            index = owner.getJoinIndexFromConstantPool(join);
        }
        if (index >=0)
        {
            result = owner.getFinderClassName()+".zGetConstantJoin("+index+")";
        }
        if (result == null)
        {
            result = join.getJoinExpression();
        }
        return result;
    }

    private boolean hasNoOperationForRightAttribute(AbstractAttribute rightSource)
    {
        boolean found = false;
        for(int i=0; !found && i < joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            found = ((ASTAttributeName)exp.getRight()).getAttribute().getName().equals(rightSource.getName());
        }
        if (!found)
        {
            found = hasFilterForAttribute(rightFilters, rightSource);
        }
        return !found;
    }

    private boolean hasNoOperationForLeftAttribute(AbstractAttribute leftSource)
    {
        boolean found = false;
        for(int i=0; !found && i < joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            found = exp.getLeft().getAttribute().getName().equals(leftSource.getName());
        }
        if (!found)
        {
            found = hasFilterForAttribute(leftFilters, leftSource);
        }
        return !found;
    }

    private boolean hasFilterForAttribute(SimpleNode rightFilters, AbstractAttribute rightSource)
    {
        boolean found = false;
        if (rightFilters != null)
        {
            HasAttributeVisitor hasAttributeVisitor = new HasAttributeVisitor(rightSource);
            rightFilters.childrenPolymorphicAccept(hasAttributeVisitor, null);
            found = hasAttributeVisitor.isFound();
        }
        return found;
    }

    public boolean addJoin(ASTRelationalExpression join)
    {
        boolean added = false;
        ASTAttributeName rightAttribute = (ASTAttributeName) join.getRight();
        if (this.isJoinedToThis == join.involvesThis())
        {
            if (this.isJoinedToThis)
            {
                added = this.hasObject(rightAttribute.getOwner());
            }
            else
            {
                ASTAttributeName leftAttribute = join.getLeft();
                if (this.hasObject(leftAttribute.getOwner()) && this.hasObject(rightAttribute.getOwner()))
                {
                    added = true;
                    join.reAlignForOwner(left);
                }
            }
        }
        if (added)
        {
            this.joins.add(join);
        }
        return added;
    }

    private void addLeftFilter(SimpleNode constraint)
    {
        if (this.leftFilters == null)
        {
            this.leftFilters = constraint;
        }
        else
        {
            this.leftFilters = this.combine(this.leftFilters, constraint);
        }
    }

    private void addRightFilter(SimpleNode constraint)
    {
        if (this.rightFilters == null)
        {
            this.rightFilters = constraint;
        }
        else
        {
            this.rightFilters = this.combine(this.rightFilters, constraint);
        }
    }

    private SimpleNode combine(SimpleNode filters, SimpleNode constraint)
    {
        if (filters instanceof ASTAndExpression)
        {
            ASTAndExpression and = (ASTAndExpression) filters;
            and.jjtAddChild(constraint, and.jjtGetNumChildren());
            return and;
        }
        else
        {
            ASTAndExpression result = new ASTAndExpression(0);
            result.jjtAddChild(filters, 0);
            result.jjtAddChild(constraint, 1);
            return result;
        }
    }

    public boolean addConstraint(ASTRelationalExpression constraint)
    {
        boolean added = false;
        if (this.isJoinedToThis)
        {
            if (constraint.getLeft().belongsToThis())
            {
                this.addLeftFilter(constraint);
                added = true;
            }
            else if (constraint.getLeft().getOwner().equals(this.right))
            {
                this.addRightFilter(constraint);
                added = true;
            }
        }
        else
        {
            if (constraint.getLeft().getOwner().equals(this.left))
            {
                this.addLeftFilter(constraint);
                added = true;
            }
            else if (constraint.getLeft().getOwner().equals(this.right))
            {
                this.addRightFilter(constraint);
                added = true;
            }
        }
        return added;
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

    public boolean hasObject(MithraObjectTypeWrapper lastRight)
    {
        return this.left.getClassName().equals(lastRight.getClassName()) ||
                this.right.getClassName().equals(lastRight.getClassName());
    }

    public boolean addOrConstraint(MithraObjectTypeWrapper constrainedClass, ASTOrExpression node, boolean nodeBelongsToThis)
    {
        boolean added = false;
        if (this.isJoinedToThis)
        {
            if (nodeBelongsToThis)
            {
                this.addLeftFilter(node);
                added = true;
            }
            else if (constrainedClass.getClassName().equals(this.right.getClassName()))
            {
                this.addRightFilter(node);
                added = true;
            }
        }
        else
        {
            if (this.left.getClassName().equals(constrainedClass.getClassName()))
            {
                this.addLeftFilter(node);
                added = true;
            }
            else if (this.right.getClassName().equals(constrainedClass.getClassName()))
            {
                this.addRightFilter(node);
                added = true;
            }
        }
        return added;
    }

    public void addJoinsToConstantPool()
    {
        for(int i=0;i < joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            MithraObjectTypeWrapper toAdd = this.left.chooseForRelationshipAdd(right);
            toAdd.addJoinToConstantPool(exp);
            ASTRelationalExpression reverse = getReverseJoin(exp);
            toAdd.addJoinToConstantPool(reverse);
        }
    }

    private ASTRelationalExpression getReverseJoin(ASTRelationalExpression exp)
    {
        return new ASTRelationalExpression(((ASTAttributeName) exp.getRight()).getAttribute(),
                        exp.getLeft().getAttribute(), false);
    }

    public void addOperationsToConstantPool()
    {
        if (leftFilters != null)
        {
            addOperationToConstantPool(left, leftFilters);
        }
        if (rightFilters != null)
        {
            addOperationToConstantPool(right, rightFilters);
        }
    }

    private void addOperationToConstantPool(MithraObjectTypeWrapper objectTypeWrapper, SimpleNode filters)
    {
        ConstantPoolAdderVisitor visitor = new ConstantPoolAdderVisitor(objectTypeWrapper);
        filters.childrenPolymorphicAccept(visitor, null);
    }

    public boolean isSingleAttributeJoinIgnoringRightSourceAttribute()
    {
        if (this.joins.size() > 2)
        {
            return false;
        }
        int count = 0;
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            SimpleNode right = exp.getRight();
            if (!exp.getOperator().isEqual())
            {
                return false;
            }
            ASTAttributeName rightAstAttribute = (ASTAttributeName) right;
            if (!rightAstAttribute.isSourceAttribute())
            {
                count++;
            }
        }
        return count <= 1;
    }

    public boolean isSingleAttributeJoinIgnoringRightSourceAndAsOfAttribute()
    {
        if (this.joins.size() > 4)
        {
            return false;
        }
        int count = 0;
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            SimpleNode right = exp.getRight();
            if (!exp.getOperator().isEqual())
            {
                return false;
            }
            ASTAttributeName rightAstAttribute = (ASTAttributeName) right;
            if (!(rightAstAttribute.isAsOfAttribute() || rightAstAttribute.isSourceAttribute()))
            {
                count++;
            }
        }
        return count <= 1;
    }

    private Attribute[] getUniqueAttributes(Collection attributeSet)
    {
        HashSet names = new HashSet();
        ArrayList list = new ArrayList();
        for(Iterator it = attributeSet.iterator(); it.hasNext(); )
        {
            Attribute attribute = (Attribute) it.next();
            if (!names.contains(attribute.getName()))
            {
                names.add(attribute.getName());
                list.add(attribute);
            }
        }
        Attribute[] result = new Attribute[list.size()];
        if (list.size() > 0)
        {
            list.toArray(result);
        }
        return result;
    }

    public Attribute[] getLeftJoinAttributes()
    {
        HashSet set = new HashSet();
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            if (!exp.getOperator().isEqual())
            {
                throw new RuntimeException("not implemented");
            }
            ASTAttributeName left = exp.getLeft();
            AbstractAttribute attribute = left.getAttribute();
            set.add(attribute);
        }
        return getUniqueAttributes(set);
    }

    public Attribute[] getAttributesForInClauseEval()
    {
        HashSet set = new HashSet();
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            SimpleNode right = exp.getRight();
            if (!exp.getOperator().isEqual())
            {
                throw new RuntimeException("not implemented");
            }
            ASTAttributeName rightAstAttribute = (ASTAttributeName) right;
            if (!(rightAstAttribute.isAsOfAttribute() || rightAstAttribute.isSourceAttribute()))
            {
                ASTAttributeName left = exp.getLeft();
                AbstractAttribute attribute = left.getAttribute();
                set.add(attribute);
            }
        }
        return getUniqueAttributes(set);
    }

    public Attribute[] getAsOfAttributesForSingleCheck()
    {
        HashSet set = new HashSet();
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            SimpleNode right = exp.getRight();
            if (!exp.getOperator().isEqual())
            {
                throw new RuntimeException("not implemented");
            }
            ASTAttributeName rightAstAttribute = (ASTAttributeName) right;
            if (rightAstAttribute.isAsOfAttribute())
            {
                ASTAttributeName left = exp.getLeft();
                AbstractAttribute attribute = left.getAttribute();
                set.add(attribute);
            }
        }
        return getUniqueAttributes(set);
    }

    public Map getAsOfAttributesMap()
    {
        HashMap result = new HashMap();
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            SimpleNode right = exp.getRight();
            if (!exp.getOperator().isEqual())
            {
                throw new RuntimeException("not implemented");
            }
            ASTAttributeName rightAstAttribute = (ASTAttributeName) right;
            if (rightAstAttribute.isAsOfAttribute())
            {
                ASTAttributeName left = exp.getLeft();
                AbstractAttribute attribute = left.getAttribute();
                result.put(rightAstAttribute.getAttribute(), attribute);
            }
        }
        return result;
    }

    public Attribute[] getAttributesToSetOnRelatedObject()
    {
        Attribute[] result = new Attribute[joins.size()];
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            if (!exp.getOperator().isEqual())
            {
                throw new RuntimeException("not implemented");
            }
            ASTAttributeName right = (ASTAttributeName) exp.getRight();
            result[i] = (Attribute) right.getAttribute();
        }
        return result;
    }

    public Attribute getAttributeToGetForSetOnRelatedObject(int index)
    {
        ASTRelationalExpression exp = joins.get(index);
        ASTAttributeName left = exp.getLeft();
        return (Attribute) left.getAttribute();
    }

    public Index getUniqueMatchingIndex()
    {
        List indices = this.right.getIndices();
        for(int i=0;i<indices.size();i++)
        {
            Index index = (Index) indices.get(i);
            if (index.isUnique())
            {
                if (matchesAttributes(index.getAttributes(), this.getRightAttributesForMatching()))
                {
                    return index;
                }
            }
        }
        return null;
    }

    public boolean isByPrimaryKey()
    {
        HashSet<String> rightAttributes = getRightAttributesForMatching();
        Attribute[] primaryKeyAttributes = this.right.getPrimaryKeyAttributes();
        return matchesAttributes(primaryKeyAttributes, rightAttributes);
    }

    private HashSet<String> getRightAttributesForMatching()
    {
        HashSet<String> rightAttributes = new HashSet();
        for(int i=0;i<joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            rightAttributes.add(((ASTAttributeName)exp.getRight()).getAttribute().getName());
        }
        if (rightFilters != null)
        {
            ByPrimaryKeyVisitor visitor = new ByPrimaryKeyVisitor();
            rightFilters.jjtAccept(visitor, null);
            for(int i=0;i<visitor.getAttributes().size();i++)
            {
                rightAttributes.add(((AbstractAttribute)visitor.getAttributes().get(i)).getName());
            }
        }
        // add default as of attributes
        if (right.hasAsOfAttributes())
        {
            AsOfAttribute[] rightAsOfAttributes = right.getAsOfAttributes();
            for(int i=0;i<rightAsOfAttributes.length;i++)
            {
                if (!rightAttributes.contains(rightAsOfAttributes[i].getName()))
                {
                    String defaultDateExpression = rightAsOfAttributes[i].getDefaultDateExpression();
                    if (defaultDateExpression != null && !defaultDateExpression.equals("null"))
                    {
                        rightAttributes.add(rightAsOfAttributes[i].getName());
                    }
                }
            }
        }
        return rightAttributes;
    }

    private boolean matchesAttributes(AbstractAttribute[] indexAttributes, HashSet<String> rightAttributes)
    {
        HashSet<String> indexAttributeSet = new HashSet<String>();
        for(AbstractAttribute attr: indexAttributes)
        {
            indexAttributeSet.add(attr.getName());
        }
        SourceAttribute sourceAttribute = this.right.getSourceAttribute();
        if (sourceAttribute != null)
        {
            indexAttributeSet.add(sourceAttribute.getName());
        }
        if (this.right.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = this.right.getAsOfAttributes();
            for(int i=0;i<asOfAttributes.length;i++)
            {
                indexAttributeSet.add(asOfAttributes[i].getName());
            }
        }
        if (rightAttributes.size() == indexAttributeSet.size())
        {
            for(int i=0;i<indexAttributes.length;i++)
            {
                rightAttributes.remove(indexAttributes[i].getName());
            }
            if (sourceAttribute != null)
            {
                rightAttributes.remove(sourceAttribute.getName());
            }
            if (this.right.hasAsOfAttributes())
            {
                AsOfAttribute[] asOfAttributes = this.right.getAsOfAttributes();
                for(int i=0;i<asOfAttributes.length;i++)
                {
                    rightAttributes.remove(asOfAttributes[i].getName());
                }
            }
            return rightAttributes.isEmpty();
        }
        return false;
    }

    public String getAsOfAttributesDataMatchesConditions()
    {
        StringBuilder result = new StringBuilder();
        AsOfAttribute[] asOfAttributes = right.getAsOfAttributes();
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            addAnd(result).append(asOfAttributes[i].getName()+".dataMatches(_castedTargetData, _asOfDate"+i+ ')');
        }
        return result.toString();
    }

    public boolean requiresSrcObjectForEquals()
    {
        if (!this.left.hasData()) return true;
        for(int j=0;j<this.joins.size();j++)
        {
            ASTRelationalExpression exp = joins.get(j);
            if (exp.getLeft().isAsOfAttribute()) return true;
        }
        return false;
    }

    public boolean requiresSrcDataForEquals()
    {
        if (!this.left.hasData()) return false;
        for(int j=0;j<this.joins.size();j++)
        {
            ASTRelationalExpression exp = joins.get(j);
            if (!exp.getLeft().isAsOfAttribute()) return true;
        }
        return false;
    }

    public boolean requiresSrcObjectForHashCode()
    {
        if (!this.left.hasData()) return true;
        for(int j=0;j<this.joins.size();j++)
        {
            ASTRelationalExpression exp = joins.get(j);
            AbstractAttribute rightAttribute = ((ASTAttributeName) exp.getRight()).getAttribute();
            if (!rightAttribute.isAsOfAttribute() && exp.getLeft().isAsOfAttribute()) return true;
        }
        return false;
    }

    public boolean requiresSrcDataForHashCode()
    {
        if (!this.left.hasData()) return false;
        for(int j=0;j<this.joins.size();j++)
        {
            ASTRelationalExpression exp = joins.get(j);
            if (!exp.getLeft().isAsOfAttribute()) return true;
        }
        return false;
    }

    public String getEqualsConditions()
    {
        StringBuilder result = new StringBuilder();
        for (int j = 0; j < this.joins.size() ; j++)
        {
            ASTRelationalExpression exp = joins.get(j);
            AbstractAttribute leftAttr = exp.getLeft().getAttribute();
            AbstractAttribute rightAttr = ((ASTAttributeName) exp.getRight()).getAttribute();
            if (!rightAttr.isAsOfAttribute())
            {
                if (leftAttr.isNullable() && rightAttr.isNullable())
                {
                    addAnd(result).append(
                        "_castedSrc"+getDataOrObject(leftAttr)+ '.' +leftAttr.getNullGetter()
                        + " == _castedTargetData."+rightAttr.getNullGetter()
                        + " && (_castedSrc"+getDataOrObject(leftAttr)+ '.' +leftAttr.getNullGetter()+ " || "
                                + getRelationshipAttributeEquals(leftAttr, rightAttr)
                            + ')'
                    );
                }
                else
                {
                    if (leftAttr.isNullable())
                    {
                        addAnd(result).append("!_castedSrc"+getDataOrObject(leftAttr)+ '.' +leftAttr.getNullGetter());
                    }
                    if (rightAttr.isNullable())
                    {
                        addAnd(result).append("!_castedTargetData."+rightAttr.getNullGetter());
                    }
                    addAnd(result).append(getRelationshipAttributeEquals(leftAttr, rightAttr));
                }
            }
        }
        if (this.rightFilters != null)
        {
            RightFilterEqualityMapperVisitor filters = new RightFilterEqualityMapperVisitor();
            String filterResult = (String) this.rightFilters.jjtAccept(filters, null);
            if (filterResult.length() > 0)
            {
                addAnd(result).append(filterResult);
            }
        }
        return result.toString();
    }

    private String getRelationshipAttributeEquals(AbstractAttribute leftAttr, AbstractAttribute rightAttr)
    {
        String toAppend = "";
        if (leftAttr.isArray())
        {
            toAppend += "Arrays.equals(_castedSrc"+getDataOrObject(leftAttr)+"."+leftAttr.getGetter()+"(), ";
            toAppend += "_castedTargetData."+rightAttr.getGetter()+"())";
        }
        else
        {
            toAppend += "_castedSrc"+getDataOrObject(leftAttr)+"."+leftAttr.getGetter()+"()";
            if (leftAttr.isPrimitive())
            {
                toAppend += " == ";
            }
            else
            {
                if (!leftAttr.isNullable()) // extra check, even if it's not nullable, it could be uniniatilized
                {
                    toAppend += "!= null && _castedSrc"+getDataOrObject(leftAttr)+"."+leftAttr.getGetter()+"()";
                }
                toAppend += ".equals(";
            }
            toAppend += "_castedTargetData."+rightAttr.getGetter()+"()";
            if (!leftAttr.isPrimitive())
            {
                toAppend += ")";
            }
        }
        return toAppend;
    }

    public String getHashCodeComputationForPk(boolean offHeap)
    {
        Attribute[] primaryKeyAttributes = this.right.getPrimaryKeyAttributes();
        return getHashCodeComputation(primaryKeyAttributes, offHeap);
    }

    public String getHashCodeComputation(AbstractAttribute[] uniqueAttributes, boolean offHeap)
    {
        HashMap<String, ASTRelationalExpression> rightToLeft = getRightToLeftAttributeToExpressionMap();
        String result = null;
        boolean sourceAdded = false;
        for(int i=0;i<uniqueAttributes.length;i++)
        {
            AbstractAttribute uniqueAttribute = uniqueAttributes[i];
            if (uniqueAttribute.isSourceAttribute())
            {
                sourceAdded = true;
            }
            result = addHashCodeExpression(rightToLeft, result, uniqueAttribute, offHeap);
        }
        SourceAttribute sourceAttribute = this.right.getSourceAttribute();
        if (sourceAttribute != null && !sourceAdded)
        {
            result = addHashCodeExpression(rightToLeft, result, sourceAttribute, offHeap);
        }
        result = "return "+result+";";
        return result;
    }

    public boolean hasDifferentOffHeapHashForPk()
    {
        return hasDifferentOffHeapHash(right.getPrimaryKeyAttributes());
    }
    public boolean hasDifferentOffHeapHash(AbstractAttribute[] attributes)
    {
        for(int i=0;i<attributes.length;i++)
        {
            if (attributes[i].isStringAttribute())
            {
                return true;
            }
        }
        SourceAttribute sourceAttribute = this.right.getSourceAttribute();
        return sourceAttribute != null && sourceAttribute.isStringSourceAttribute();
    }

    private String getDataOrObject(AbstractAttribute attr)
    {
        String result = "Data";
        if (attr.isAsOfAttribute() || !attr.getOwner().hasData())
        {
            result = "Object";
        }
        return result;
    }

    private String addHashCodeExpression(HashMap<String, ASTRelationalExpression> rightToLeft, String result, AbstractAttribute uniqueAttribute, boolean offHeap)
    {
        boolean combine = false;
        String hashMethod = "hash";
        if (uniqueAttribute.isStringAttribute() && offHeap)
        {
            hashMethod = "offHeapHash";
        }
        if (result != null)
        {
            combine = true;
            result = "HashUtil.combineHashes("+result+",HashUtil."+hashMethod+"(";
        }
        else
        {
            result = "HashUtil."+hashMethod+"(";
        }
        ASTRelationalExpression exp = rightToLeft.get(uniqueAttribute.getName());
        if (exp.isJoin())
        {
            AbstractAttribute attribute = exp.getLeft().getAttribute();
            String getterMethod = attribute.getGetter();
            if (offHeap && attribute.isStringAttribute() && attribute.getOwner().hasOffHeap() && "Data".equals(getDataOrObject(attribute)))
            {
                getterMethod = "zGet"+ StringUtility.firstLetterToUpper(attribute.getName())+"AsInt";
            }
            result += "_castedSrc"+getDataOrObject(attribute)+".";
            result += getterMethod+"()";
            if (attribute.isNullablePrimitive())
            {
                result += ", _castedSrc"+getDataOrObject(attribute)+"."+attribute.getNullGetter();
            }
        }
        else
        {
            result += exp.getNonInLiteral(exp.getRight());
        }
        result += ")";
        if (combine) result += ")";
        return result;
    }

    private HashMap<String, ASTRelationalExpression> getRightToLeftAttributeToExpressionMap()
    {
        HashMap<String, ASTRelationalExpression> rightToLeft = new HashMap<String, ASTRelationalExpression>();

        for(int j=0;j<this.joins.size();j++)
        {
            ASTRelationalExpression exp = joins.get(j);
            String rightName = ((ASTAttributeName)exp.getRight()).getAttribute().getName();
            rightToLeft.put(rightName, exp);
        }
        if (this.rightFilters != null)
        {
            this.rightFilters.jjtAccept(new PrimaryKeyMapperVisitor(rightToLeft), null);
        }
        return rightToLeft;
    }

    public String getCacheLookUpParameters()
    {
        String result = "";
        if (right.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = right.getAsOfAttributes();
            for(int i=0;i<asOfAttributes.length;i++)
            {
                boolean found = false;
                result += ",";
                for(int j=0;j< this.joins.size();j++)
                {
                    ASTRelationalExpression exp = joins.get(j);
                    String rightName = ((ASTAttributeName)exp.getRight()).getAttribute().getName();
                    if (rightName.equals(asOfAttributes[i].getName()))
                    {
                        AbstractAttribute left = exp.getLeft().getAttribute();
                        if (left.isAsOfAttribute() || left.getOwner().isReadOnly())
                        {
                            result += "this.";
                        }
                        else
                        {
                            result += "_data.";
                        }
                        result += left.getGetter() + "()";
                        found = true;
                    }
                }
                if (!found)
                {
                    ConstantFinderVisitor constantFinderVisitor = new ConstantFinderVisitor(asOfAttributes[i].getName());
                    String toAppend = null;
                    if (rightFilters != null)
                    {
                        this.rightFilters.jjtAccept(constantFinderVisitor, null);
                        ASTRelationalExpression constant = constantFinderVisitor.getResult();
                        if (constant != null)
                        {
                            ASTLiteral literal = (ASTLiteral) constant.getRight();
                            toAppend = literal.getValue();
                    }
                    }
                    if (toAppend == null && !asOfAttributes[i].getDefaultDateExpression().equals("null"))
                    {
                        toAppend = asOfAttributes[i].getDefaultDateExpression();
                    }
                    if (toAppend == null)
                    {
                        throw new RuntimeException("could not determine literal in "+rightFilters.getFinderString());
                    }
                    result += toAppend;
                }
            }
            if (asOfAttributes.length == 1)
            {
                result += ", null";
            }
        }
        else
        {
            result = ", null, null";
        }
        return result;
    }

    public void addConstantSets()
    {
        for(int i=0;i < joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            if (!exp.isJoin())
            {
                getConstantExpression(exp);
            }
        }
        if (leftFilters != null)
        {
            addConstantSets(left.chooseForRelationshipAdd(this.right), leftFilters);
        }
        if (rightFilters != null)
        {
            addConstantSets(right.chooseForRelationshipAdd(this.left), rightFilters);
        }
    }

    private void addConstantSets(MithraObjectTypeWrapper objectTypeWrapper, SimpleNode filters)
    {
        ConstantSetAdderVisitor visitor = new ConstantSetAdderVisitor(objectTypeWrapper);
        filters.childrenPolymorphicAccept(visitor, null);
    }

    public boolean needsParameterOperationForPrimaryKey(String parameter)
    {
        List<String> parameterVariableList = Collections.singletonList(parameter);
        Attribute[] uniqueAttributes = this.right.getPrimaryKeyAttributes();
        return needsParameterListFromUniqueIndex(parameterVariableList, uniqueAttributes);
    }

    public boolean needsParametersOperationForPrimaryKey()
    {
        List<String> parameterVariableList = this.relationshipAttribute.getParameterVariableList();
        if (parameterVariableList == null || parameterVariableList.size() == 0) return false;
        Attribute[] uniqueAttributes = this.right.getPrimaryKeyAttributes();
        return needsParameterListFromUniqueIndex(parameterVariableList, uniqueAttributes);
    }

    private boolean needsParameterListFromUniqueIndex(List<String> parameterVariableList, AbstractAttribute[] uniqueAttributes)
    {
        boolean result = false;
        HashMap<String, ASTRelationalExpression> rightToLeft = getRightToLeftAttributeToExpressionMap();
        for(int i=0;i<uniqueAttributes.length;i++)
        {
            ASTRelationalExpression exp = rightToLeft.get(uniqueAttributes[i].getName());
            if (!exp.isJoin() && exp.isRightHandJavaLiteral())
            {
                String literal = ((ASTLiteral) exp.getRight()).getValue().trim();
                if (isParameterUsed(parameterVariableList, literal))
                {
                    result = true;
                    break;
                }
            }
        }
        SourceAttribute sourceAttribute = this.right.getSourceAttribute();
        if (sourceAttribute != null)
        {
            ASTRelationalExpression exp = rightToLeft.get(sourceAttribute.getName());
            if (!exp.isJoin() && exp.isRightHandJavaLiteral())
            {
                String literal = ((ASTLiteral) exp.getRight()).getValue().trim();
                if (isParameterUsed(parameterVariableList, literal))
                {
                    result = true;
                }
            }
        }
        return result;
    }

    private String findCorrespondingParameter(String literal)
    {
        if (literal == null) return null;
        return findCorrespondingParameter(relationshipAttribute.getParameterVariableList(), literal);
    }

    private String findCorrespondingParameter(List<String> parameterVariableList, String literal)
    {
        if (parameterVariableList == null) return null;
        int index = parameterVariableList.indexOf(literal);
        if (index >= 0) return parameterVariableList.get(index);
        StringTokenizer strTok = new StringTokenizer(literal, " .()-=+*/[]:'\"&|!^%\n");
        while(strTok.hasMoreTokens())
        {
            index = parameterVariableList.indexOf(strTok.nextToken());
            if (index >= 0) return parameterVariableList.get(index);
        }
        return null;
    }

    private boolean isParameterUsed(List<String> parameterVariableList, String literal)
    {
        if (literal == null) return false;
        return findCorrespondingParameter(parameterVariableList, literal) != null;
    }

    public boolean usesParameter(String parameterVariable)
    {
        List<String> literals = this.getFilterLiterals();
        if (literals.size() == 0) return false;
        if (literals.contains(parameterVariable)) return true;
        for(int i=0;i<literals.size();i++)
        {
            String literal = literals.get(i);
            StringTokenizer strTok = new StringTokenizer(literal, " .()-=+*/[]:'\"&|!^%\n");
            while(strTok.hasMoreTokens())
            {
                if (parameterVariable.equals(strTok.nextToken())) return true;
            }
        }
        return false;
    }

    private List<String> getFilterLiterals()
    {
        if (filterLiterals == null)
        {
            filterLiterals = new ArrayList<String>();
            if (this.leftFilters != null)
            {
                LiteralCollectorVisitor collectorVisitor = new LiteralCollectorVisitor();
                this.leftFilters.jjtAccept(collectorVisitor, null);
                filterLiterals.addAll(collectorVisitor.getResult());
            }
            if (this.rightFilters != null)
            {
                LiteralCollectorVisitor collectorVisitor = new LiteralCollectorVisitor();
                this.rightFilters.jjtAccept(collectorVisitor, null);
                filterLiterals.addAll(collectorVisitor.getResult());
            }
        }
        return filterLiterals;
    }

    public boolean needsParameterOperationForUniqueIndex(String parameter)
    {
        List<String> parameterVariableList = Collections.singletonList(parameter);
        return needsParameterListFromUniqueIndex(parameterVariableList, getUniqueMatchingIndex().getAttributes());
    }

    public boolean needsParametersOperationForUniqueIndex()
    {
        List<String> parameterVariableList = this.relationshipAttribute.getParameterVariableList();
        if (parameterVariableList == null || parameterVariableList.size() == 0) return false;
        return needsParameterListFromUniqueIndex(parameterVariableList, getUniqueMatchingIndex().getAttributes());
    }

    public boolean needsDefaultAsOfDatesOperation()
    {
        if (right.hasAsOfAttributes())
        {
            AsOfAttribute[] rightAsOfAttributes = right.getAsOfAttributes();
            for(int i=0;i<rightAsOfAttributes.length;i++)
            {
                if (hasNoOperationForRightAttribute(rightAsOfAttributes[i]))
                {
                    String defaultDateExpression = rightAsOfAttributes[i].getDefaultDateExpression();
                    if (defaultDateExpression != null && !defaultDateExpression.equals("null"))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isMissingAsOfDatesOperationAndDefaults()
    {
        if (left.hasAsOfAttributes())
        {
            AsOfAttribute[] rightAsOfAttributes = left.getAsOfAttributes();
            for(int i=0;i<rightAsOfAttributes.length;i++)
            {
                if (hasNoOperationForLeftAttribute(rightAsOfAttributes[i]))
                {
                    String defaultDateExpression = rightAsOfAttributes[i].getDefaultDateExpression();
                    if (defaultDateExpression == null || defaultDateExpression.equals("null"))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public String getDefaultAsOfDatesOperation()
    {
        String result = null;
        if (right.hasAsOfAttributes())
        {
            AsOfAttribute[] rightAsOfAttributes = right.getAsOfAttributes();
            for(int i=0;i<rightAsOfAttributes.length;i++)
            {
                if (hasNoOperationForRightAttribute(rightAsOfAttributes[i]))
                {
                    String defaultDateExpression = rightAsOfAttributes[i].getDefaultDateExpression();
                    if (defaultDateExpression != null && !defaultDateExpression.equals("null"))
                    {
                        String op = right.getFinderClassName() + "." + rightAsOfAttributes[i].getName() + "().eq(" + defaultDateExpression + ")";
                        if (result == null)
                        {
                            result = op;
                        }
                        else
                        {
                            result = result+".and("+op+")";
                        }
                    }
                }
            }
        }
        return result;
    }

    public boolean dependsOnFromAsOfAttributes()
    {
        if (left.hasAsOfAttributes())
        {
            for(int i=0;i<this.joins.size();i++)
            {
                ASTRelationalExpression exp = joins.get(i);
                AbstractAttribute leftAttr = exp.getLeft().getAttribute();
                if (leftAttr.isAsOfAttribute())
                {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean needsDirectRefExtractors()
    {
        HashMap<String, ASTRelationalExpression> rightToLeft = getRightToLeftAttributeToExpressionMap();
        for(Iterator<ASTRelationalExpression> it = rightToLeft.values().iterator(); it.hasNext();)
        {
            ASTRelationalExpression exp = it.next();
            AbstractAttribute attr;
            if (exp.isJoin())
            {
                attr = ((ASTAttributeName) exp.getRight()).getAttribute();
            }
            else
            {
                attr = exp.getLeft().getAttribute();
            }
            if (needsDirectRefCheck(attr))
            {
                return true;
            }
        }
        return false;
    }

    private boolean needsDirectRefCheck(AbstractAttribute attr)
    {
        return !attr.isAsOfAttribute() && !attr.isPrimaryKey() && !attr.isSourceAttribute();
    }

    public String getFromDirectRefExtractors()
    {
        String result = "";
        for(int i=0;i<this.joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            AbstractAttribute attr = ((ASTAttributeName) exp.getRight()).getAttribute();
            if (needsDirectRefCheck(attr))
            {
                if (result.length() > 0)
                {
                    result += ", ";
                }
                result += this.left.getFinderClassName()+"."+exp.getLeft().getAttribute().getName()+"()";
            }
        }
        if (this.rightFilters != null)
        {
            List<ASTRelationalExpression> rightExpressions = new ArrayList<ASTRelationalExpression>();
            this.rightFilters.jjtAccept(new RightAttributeMapperVisitor(rightExpressions), null);
            for(int i=0;i<rightExpressions.size();i++)
            {
                ASTRelationalExpression exp = rightExpressions.get(i);
                AbstractAttribute attr = exp.getLeft().getAttribute();
                if (needsDirectRefCheck(attr))
                {
                    if (result.length() > 0)
                    {
                        result += ", ";
                    }
                    result += "((OperationWithParameterExtractor)"+ getConstantExpression(exp) +").getParameterExtractor()";
                }
            }
        }
        return result;
    }

    private String getConstantExpression(ASTRelationalExpression exp)
    {
        return exp.getConstantExpression(this.left.chooseForRelationshipAdd(this.right));
    }

    public String getToDirectRefExtractors()
    {
        String result = "";
        for(int i=0;i<this.joins.size();i++)
        {
            ASTRelationalExpression exp = joins.get(i);
            AbstractAttribute attr = ((ASTAttributeName) exp.getRight()).getAttribute();
            if (needsDirectRefCheck(attr))
            {
                if (result.length() > 0)
                {
                    result += ", ";
                }
                result += this.right.getFinderClassName()+"."+attr.getName()+"()";
            }
        }
        if (this.rightFilters != null)
        {
            List<ASTRelationalExpression> rightExpressions = new ArrayList<ASTRelationalExpression>();
            this.rightFilters.jjtAccept(new RightAttributeMapperVisitor(rightExpressions), null);
            for(int i=0;i<rightExpressions.size();i++)
            {
                ASTRelationalExpression exp = rightExpressions.get(i);
                AbstractAttribute attr = exp.getLeft().getAttribute();
                if (needsDirectRefCheck(attr))
                {
                    if (result.length() > 0)
                    {
                        result += ", ";
                    }
                    result += this.right.getFinderClassName()+"."+attr.getName()+"()";
                }
            }
        }
        return result;
    }

    public Join getReverseJoin()
    {
        Join result = new Join(this.right, this.left, false, this.relationshipAttribute);
        result.rightFilters = this.leftFilters;
        result.leftFilters = this.rightFilters;
        for(int i=0;i<this.joins.size();i++)
        {
            result.joins.add(this.getReverseJoin(this.joins.get(i)));
        }
        return result;
    }

    public boolean hasRightFilters()
    {
        return this.rightFilters != null;
    }

    public String constructOperationFromRight(boolean hasDangleMapper)
    {
        if (this.isExtractorBasedMultiEquality(hasDangleMapper))
        {
            return constructOperationFromRightForExtractorBased();
        }
        return constructPlainOperationFromRight();
    }

    private String constructPlainOperationFromRight()
    {
        String result = constructOperation(joins.get(0));
        for(int i=1;i<joins.size();i++)
        {
            result = result+".and("+constructOperation(joins.get(i))+")";
        }
        if (leftFilters != null)
        {
            result = result+".and("+constructFilters(leftFilters)+")";
        }
        return result;
    }

    private String constructOperationFromRightForExtractorBased()
    {
        return "new RelationshipMultiEqualityOperation("+this.relationshipAttribute.getFromObject().getFinderClassName()+"."+this.relationshipAttribute.getName()+"().zGetRelationshipMultiExtractor(), this)";
    }

    private String constructOperation(ASTRelationalExpression exp)
    {
        AbstractAttribute leftAttr = exp.getLeft().getAttribute();
        String result = leftAttr.getOwner().getFinderClassName()+"."+leftAttr.getName()+"()"+".eq(";
        AbstractAttribute rightAttr = ((ASTAttributeName) exp.getRight()).getAttribute();
        if (rightAttr.isAsOfAttribute())
        {
            result += "this.";
        }
        else
        {
            result += "_data.";
        }
        result += rightAttr.getGetter()+"()";
        result += ")";
        return result;
    }

    public boolean isExtractorBasedMultiEquality(boolean hasDangleMapper)
    {
        if (hasDangleMapper || this.relationshipAttribute.hasParameters() || !this.relationshipAttribute.dependsOnlyOnFromToObjects() || isMissingAsOfDatesOperationAndDefaults())
        {
            return false;
        }
        int expressionSize = this.joins.size();
        if (leftFilters != null)
        {
            int filters = countEqualityFilters(leftFilters);
            if (filters < 0)
            {
                return false;
            }
            expressionSize += filters;
        }
        return expressionSize > 1;
    }

    public String getRelationshipMultiExtractorConstructor()
    {
        List<ASTRelationalExpression> sortedJoins = getIndexOrderedJoins(this.relationshipAttribute.getRelatedObject().getPkAndAllIndices());
        String result = "RelationshipMultiExtractor.withLeftAttributes(\n";
        for(int i=0;i<sortedJoins.size();i++)
        {
            if (i > 0)
            {
                result += ",\n";
            }
            ASTRelationalExpression exp = sortedJoins.get(i);
            ASTAttributeName attribute = exp.getLeft();
            result += attribute.getAttribute().getOwner().getFinderClassName()+"."+ attribute.getAttribute().getName()+"()";
        }
        result += ").withExtractors(\n";
        for(int i=0;i<sortedJoins.size();i++)
        {
            if (i > 0)
            {
                result += ",\n";
            }
            ASTRelationalExpression exp = sortedJoins.get(i);
            if (exp.isJoin())
            {
                AbstractAttribute joinAttr = ((ASTAttributeName)exp.getRight()).getAttribute();
                result += joinAttr.getOwner().getFinderClassName() + "." + joinAttr.getName() + "()";
            }
            else
            {
                result += "((OperationWithParameterExtractor)";
                result += createConstantOperation(exp);
                result += ").getParameterExtractor()";
            }
        }
        result += ")";
        return result;
    }

    private List<ASTRelationalExpression> getIndexOrderedJoins(List<Index> indices)
    {
        List<ASTRelationalExpression> sortedJoins = new ArrayList<ASTRelationalExpression>(this.joins);
        if (leftFilters != null)
        {
            EqualityAndFilterVisitor visitor = new EqualityAndFilterVisitor();
            leftFilters.jjtAccept(visitor, null);
            sortedJoins.addAll(visitor.getResult());
        }
        AsOfAttribute[] asOfAttributes = this.relationshipAttribute.getRelatedObject().getAsOfAttributes();
        List<ASTRelationalExpression> sortedAsOfAttributeJoins = new ArrayList<ASTRelationalExpression>();

        if (asOfAttributes.length > 0)
        {
            for(AsOfAttribute asOfAttribute: asOfAttributes)
            {
                boolean found = false;
                for(int i=0;i<sortedJoins.size();i++)
                {
                    ASTRelationalExpression exp = sortedJoins.get(i);
                    AbstractAttribute attribute = exp.getLeft().getAttribute();
                    if (attribute.equals(asOfAttribute))
                    {
                        sortedJoins.remove(i);
                        sortedAsOfAttributeJoins.add(exp);
                        i--;
                        found = true;
                    }
                }
                if (!found)
                {
                    ASTRelationalExpression exp = new ASTRelationalExpression(asOfAttribute, new ASTLiteral(asOfAttribute.getDefaultDateExpression()), false);
                    sortedAsOfAttributeJoins.add(exp);
                }
            }
        }
        for(Index index: indices)
        {
            AbstractAttribute[] indexAttributes = index.getAttributes();
            if (indexAttributes.length == sortedJoins.size())
            {
                Set<String> indexAttributeNames = new HashSet<String>();
                for(AbstractAttribute attr: index.getAttributes())
                {
                    indexAttributeNames.add(attr.getName());
                }
                boolean found = true;
                for(ASTRelationalExpression exp: sortedJoins)
                {
                    ASTAttributeName attribute = exp.getLeft();
                    if (!indexAttributeNames.contains(attribute.getAttribute().getName()))
                    {
                        found = false;
                        break;
                    }
                }
                if (found)
                {
                    Collections.sort(sortedJoins, new ByAttributeOrderComparator(index.getAttributes()));
                    break;
                }
            }
        }
        sortedJoins.addAll(sortedAsOfAttributeJoins);
        return sortedJoins;
    }

    public boolean requiresOverSpecifiedParameterCheck()
    {
        if (this.rightFilters != null)
        {
            List<ASTRelationalExpression> rightExpressions = new ArrayList<ASTRelationalExpression>();
            this.rightFilters.jjtAccept(new RightAttributeMapperVisitor(rightExpressions), null);
            for(int i=0;i<rightExpressions.size();i++)
            {
                ASTRelationalExpression exp = rightExpressions.get(i);
                AbstractAttribute rightAttr = exp.getLeft().getAttribute();
                if (!rightAttr.isAsOfAttribute())
                {
                    Operator op = exp.getOperator();
                    if (!op.isIsNull())
                    {
                        String right = op.isUnary() ? null : exp.getLiteralRightHand(Join.this.left.chooseForRelationshipAdd(Join.this.right));
                        String correspondingParameter = findCorrespondingParameter(right);

                        if (!(correspondingParameter == null || ((isByPrimaryKey() && needsParameterOperationForPrimaryKey(correspondingParameter))
                                || (getUniqueMatchingIndex() != null && needsParameterOperationForUniqueIndex(correspondingParameter)))))
                        {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public String getOverSpecificationCheck()
    {
        RightFilterOverSpecificationVisitor filters = new RightFilterOverSpecificationVisitor();
        return (String) this.rightFilters.jjtAccept(filters, null);
    }

    public String getFindByUniqueLookupParameters()
    {
        List<ASTRelationalExpression> sortedJoins = getIndexOrderedJoins(this.relationshipAttribute.getRelatedObject().getPkAndUniqueIndices());
        String result = "";
        for(int i=0;i<sortedJoins.size();i++)
        {
            if (i > 0)
            {
                result += ",\n";
            }
            ASTRelationalExpression exp = sortedJoins.get(i);
            if (exp.isJoin())
            {
                AbstractAttribute right = ((ASTAttributeName) exp.getRight()).getAttribute();
                if (right.isAsOfAttribute() || right.getOwner().isReadOnly())
                {
                    result += "this.";
                }
                else
                {
                    result += "_data.";
                }
                result += right.getGetter() + "()";
            }
            else
            {
                MithraObjectTypeWrapper expressionHolder = this.left.chooseForRelationshipAdd(Join.this.right);
                result += exp.getLiteralRightHand(expressionHolder);
            }
        }
        return result;
    }

    private static class ByAttributeOrderComparator implements Comparator<ASTRelationalExpression>
    {
        private Map<String, Integer> orderMap = new HashMap<String, Integer>();


        public ByAttributeOrderComparator(AbstractAttribute[] attributes)
        {
            for(int i=0;i<attributes.length;i++)
            {
                orderMap.put(attributes[i].getName(), i);
            }
        }

        @Override
        public int compare(ASTRelationalExpression first, ASTRelationalExpression second)
        {
            return orderMap.get(first.getLeft().getAttribute().getName()) - orderMap.get(second.getLeft().getAttribute().getName());
        }
    }
    
    private static class ConstantSetAdderVisitor extends MithraQLVisitorAdapter
    {
        private MithraObjectTypeWrapper wrapper;

        public ConstantSetAdderVisitor(MithraObjectTypeWrapper wrapper)
        {
            this.wrapper = wrapper;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            if (!node.isJoin())
            {
                node.getConstantExpression(this.wrapper);
            }
            return data;
        }
    }

    private static class ConstantPoolAdderVisitor extends MithraQLVisitorAdapter
    {
        private MithraObjectTypeWrapper wrapper;

        public ConstantPoolAdderVisitor(MithraObjectTypeWrapper wrapper)
        {
            this.wrapper = wrapper;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            this.wrapper.addConstantOperationToPool(node);
            return data;
        }
    }

    private static class HasAttributeVisitor extends MithraQLVisitorAdapter
    {
        private boolean found = false;
        private AbstractAttribute attr;

        public HasAttributeVisitor(AbstractAttribute attr)
        {
            this.attr = attr;
        }

        public boolean isFound()
        {
            return found;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            if (node.getLeft().getAttribute().getName().equals(attr.getName()))
            {
                found = true;
            }
            return data;
        }
    }

    private class FilterVisitor extends MithraQLVisitorAdapter
    {
        String result = "";

        public Object visit(ASTOrExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
                if (i > 0)
                {
                    result += ")";
                }
                if (i < node.jjtGetNumChildren() - 1)
                {
                    result += ".or(";
                }
            }
            return data;
        }

        public Object visit(ASTAndExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
                if (i > 0)
                {
                    result += ")";
                }
                if (i < node.jjtGetNumChildren() - 1)
                {
                    result += ".and(";
                }
            }
            return data;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            result += createConstantOperation(node);
            return data;
        }

        public String getResult()
        {
            return result;
        }
    }

    private String createConstantOperation(ASTRelationalExpression node)
    {
        String result;
        MithraObjectTypeWrapper owner = node.getLeft().getOwner();
        int index = owner.getConstantOperationIndexFromPool(node);
        if (index >= 0)
        {
            result = owner.getFinderClassName()+".zGetConstantOperation("+index+")";
        }
        else
        {
            MithraObjectTypeWrapper expressionHolder = Join.this.left.chooseForRelationshipAdd(Join.this.right);
            result = node.getConstantExpression(owner.chooseForRelationshipAdd(expressionHolder));
        }
        return result;
    }

    private class EqualityFilterCountVisitor extends MithraQLVisitorAdapter
    {
        int result = 0;

        public Object visit(ASTOrExpression node, Object data)
        {
            result = -1;
            return data;
        }

        public Object visit(ASTAndExpression node, Object data)
        {
            if (result >= 0)
            {
                for (int i = 0; i < node.jjtGetNumChildren() && result >= 0; ++i)
                {
                    node.jjtGetChild(i).jjtAccept(this, data);
                }
            }
            return data;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            if (result < 0)
            {
                return data;
            }
            if (!node.getOperator().isEqual())
            {
                result = -1;
                return data;
            }
            result ++;
            return data;
        }

        public int getResult()
        {
            return result;
        }
    }

    private class EqualityAndFilterVisitor extends MithraQLVisitorAdapter
    {
        private List<ASTRelationalExpression> result = new ArrayList<ASTRelationalExpression>();

        public Object visit(ASTAndExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            result.add(node);
            return data;
        }

        public List<ASTRelationalExpression> getResult()
        {
            return result;
        }
    }

    private static class LiteralCollectorVisitor extends MithraQLVisitorAdapter
    {
        private List<String> result = new ArrayList<String>();

        @Override
        public Object visit(ASTOrExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        @Override
        public Object visit(ASTAndExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        @Override
        public Object visit(ASTRelationalExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        @Override
        public Object visit(ASTInLiteral node, Object data)
        {
            result.add(node.getValue());
            return data;
        }

        @Override
        public Object visit(ASTLiteral node, Object data)
        {
            result.add(node.getValue());
            return data;
        }

        public List<String> getResult()
        {
            return result;
        }
    }

    private static class PrimaryKeyMapperVisitor extends MithraQLVisitorAdapter
    {
        protected ArrayList attributes = new ArrayList();
        private Map<String, ASTRelationalExpression> map;

        public PrimaryKeyMapperVisitor(Map<String, ASTRelationalExpression> map)
        {
            this.map = map;
        }

        public Object visit(ASTAndExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            map.put(node.getLeft().getAttribute().getName(), node);
            return data;
        }

        public ArrayList getAttributes()
        {
            return attributes;
        }
    }

    private class RightFilterEqualityMapperVisitor extends MithraQLVisitorAdapter
    {
        @Override
        public Object visit(ASTAndExpression node, Object data)
        {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                String localResult = (String) node.jjtGetChild(i).jjtAccept(this, data);
                if (localResult.length() > 0)
                {
                    addAnd(result).append(localResult);
                }
            }
            return result.length() == 0 ? "" : "(" + result + ")";
        }

        @Override
        public Object visit(ASTOrExpression node, Object data)
        {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                String localResult = (String) node.jjtGetChild(i).jjtAccept(this, data);
                if (localResult.length() > 0)
                {
                    addOr(result).append(localResult);
                }
            }
            return result.length() == 0 ? "" : "(" + result + ")";
        }

        @Override
        public Object visit(ASTRelationalExpression node, Object data)
        {
            StringBuilder result = new StringBuilder();
            AbstractAttribute rightAttr = node.getLeft().getAttribute();
            if (!rightAttr.isAsOfAttribute())
            {
                Operator op = node.getOperator();
                if (op.isIsNull())
                {
                    addAnd(result);
                    result.append("_castedTargetData.").append(rightAttr.getNullGetter());
                }
                else
                {
                    String right = op.isUnary() ? null : node.getLiteralRightHand(Join.this.left.chooseForRelationshipAdd(Join.this.right));
                    String correspondingParameter = findCorrespondingParameter(right);

                    if (correspondingParameter == null || ((isByPrimaryKey() && needsParameterOperationForPrimaryKey(correspondingParameter))
                            || (getUniqueMatchingIndex() != null && needsParameterOperationForUniqueIndex(correspondingParameter))))
                    {
                        addAnd(result);
                        result.append("((");
                        if (rightAttr.isNullable())
                        {
                            result.append("!_castedTargetData.").append(rightAttr.getNullGetter());
                            addAnd(result);
                        }
                        String left = "_castedTargetData."+rightAttr.getGetter()+"()";

                        if (rightAttr.isPrimitive())
                        {
                            result.append(op.getPrimitiveExpression(left, right));
                        }
                        else
                        {
                            result.append(op.getNonPrimitiveExpression(left, right));
                        }
                        result.append(')');

                        if (isParameterUsed(relationshipAttribute.getParameterVariableList(), right)
                                && rightAttr.isNullable() && !rightAttr.isPrimitive())
                        {
                            addOr(result);

                            result.append("(_castedTargetData.").append(rightAttr.getNullGetter());

                            addAnd(result);
                            result.append(right).append(" == null");

                            result.append(')');
                        }
                        result.append(')');
                    }
                }
            }
            return result.toString();
        }
    }

    private class RightFilterOverSpecificationVisitor extends MithraQLVisitorAdapter
    {
        @Override
        public Object visit(ASTAndExpression node, Object data)
        {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                String localResult = (String) node.jjtGetChild(i).jjtAccept(this, data);
                if (localResult.length() > 0)
                {
                    addAnd(result).append(localResult);
                }
            }
            return result.length() == 0 ? "" : "(" + result + ")";
        }

        @Override
        public Object visit(ASTOrExpression node, Object data)
        {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                String localResult = (String) node.jjtGetChild(i).jjtAccept(this, data);
                if (localResult.length() > 0)
                {
                    addOr(result).append(localResult);
                }
            }
            return result.length() == 0 ? "" : "(" + result + ")";
        }

        @Override
        public Object visit(ASTRelationalExpression node, Object data)
        {
            StringBuilder result = new StringBuilder();
            AbstractAttribute rightAttr = node.getLeft().getAttribute();
            if (!rightAttr.isAsOfAttribute())
            {
                Operator op = node.getOperator();
                if (!op.isIsNull())
                {
                    String right = op.isUnary() ? null : node.getLiteralRightHand(Join.this.left.chooseForRelationshipAdd(Join.this.right));
                    String correspondingParameter = findCorrespondingParameter(right);

                    if (!(correspondingParameter == null || ((isByPrimaryKey() && needsParameterOperationForPrimaryKey(correspondingParameter))
                            || (getUniqueMatchingIndex() != null && needsParameterOperationForUniqueIndex(correspondingParameter)))))
                    {
                        addAnd(result);
                        result.append("((");
                        if (rightAttr.isNullable())
                        {
                            result.append("!_result.").append(rightAttr.getNullGetter());
                            addAnd(result);
                        }
                        String left = "_result."+rightAttr.getGetter()+"()";

                        if (rightAttr.isPrimitive())
                        {
                            result.append(op.getPrimitiveExpression(left, right));
                        }
                        else
                        {
                            result.append(op.getNonPrimitiveExpression(left, right));
                        }
                        result.append(')');

                        if (isParameterUsed(relationshipAttribute.getParameterVariableList(), right)
                                && rightAttr.isNullable() && !rightAttr.isPrimitive())
                        {
                            addOr(result);

                            result.append("(_result.").append(rightAttr.getNullGetter());

                            addAnd(result);
                            result.append(right).append(" == null");

                            result.append(')');
                        }
                        result.append(')');
                    }
                }
            }
            return result.toString();
        }
    }

    private static class RightAttributeMapperVisitor extends MithraQLVisitorAdapter
    {
        private List<ASTRelationalExpression> list;

        public RightAttributeMapperVisitor(List<ASTRelationalExpression> list)
        {
            this.list = list;
        }

        public Object visit(ASTAndExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            list.add(node);
            return data;
        }
    }

    private static class FilterIndexVisitor extends MithraQLVisitorAdapter
    {
        protected ArrayList attributes = new ArrayList();
        protected boolean hasOr = false;

        public Object visit(ASTOrExpression node, Object data)
        {
            hasOr = true;
            attributes.clear();
            return data;
        }

        public Object visit(ASTAndExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            if (!hasOr)
            {
                if (node.getOperator().isIn() || node.getOperator().isEqual() || node.getOperator().isEqualsEdgePoint())
                {
                    attributes.add(node.getLeft().getAttribute());
                }
                else
                {
                    this.hasOr = true;
                    attributes.clear();
                }
            }
            return data;
        }

        public ArrayList getAttributes()
        {
            return attributes;
        }
    }

    private static class ByPrimaryKeyVisitor extends FilterIndexVisitor
    {
        public Object visit(ASTRelationalExpression node, Object data)
        {
            if (!hasOr)
            {
                if (node.getOperator().isEqual() || node.getOperator().isEqualsEdgePoint())
                {
                    attributes.add(node.getLeft().getAttribute());
                }
                else
                {
                    this.hasOr = true;
                    attributes.clear();
                }
            }
            return data;
        }
    }

    private static class ConstantFinderVisitor extends MithraQLVisitorAdapter
    {
        private String attributeToFind;
        private ASTRelationalExpression result;

        public ConstantFinderVisitor(String attributeToFind)
        {
            this.attributeToFind = attributeToFind;
        }

        public Object visit(ASTAndExpression node, Object data)
        {
            for (int i = 0; i < node.jjtGetNumChildren(); ++i)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
            }
            return data;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            if (node.getLeft().getAttribute().getName().equals(attributeToFind))
            {
                result = node;
            }
            return data;
        }

        public ASTRelationalExpression getResult()
        {
            return result;
        }
    }

    private StringBuilder addAnd(StringBuilder result)
    {
        if (result.length() > 0)
        {
            result.append(" && ");
        }
        return result;
    }

    private StringBuilder addOr(StringBuilder result)
    {
        if (result.length() > 0)
        {
            result.append(" || ");
        }
        return result;
    }
}