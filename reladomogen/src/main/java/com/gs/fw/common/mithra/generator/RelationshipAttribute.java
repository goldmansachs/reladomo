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

package com.gs.fw.common.mithra.generator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.gs.fw.common.mithra.generator.mapper.RelationshipConversionVisitor;
import com.gs.fw.common.mithra.generator.metamodel.ForeignKeyType;
import com.gs.fw.common.mithra.generator.metamodel.RelationshipType;
import com.gs.fw.common.mithra.generator.queryparser.ASTAndExpression;
import com.gs.fw.common.mithra.generator.queryparser.ASTAttributeName;
import com.gs.fw.common.mithra.generator.queryparser.ASTInLiteral;
import com.gs.fw.common.mithra.generator.queryparser.ASTInOperator;
import com.gs.fw.common.mithra.generator.queryparser.ASTIsNullClause;
import com.gs.fw.common.mithra.generator.queryparser.ASTLiteral;
import com.gs.fw.common.mithra.generator.queryparser.ASTOrExpression;
import com.gs.fw.common.mithra.generator.queryparser.ASTRelationalExpression;
import com.gs.fw.common.mithra.generator.queryparser.ASTRelationalOperator;
import com.gs.fw.common.mithra.generator.queryparser.ASTequalsEdgePointClause;
import com.gs.fw.common.mithra.generator.queryparser.MithraQLVisitorAdapter;
import com.gs.fw.common.mithra.generator.queryparser.Node;
import com.gs.fw.common.mithra.generator.queryparser.SimpleNode;
import com.gs.fw.common.mithra.generator.util.StringUtility;


public class RelationshipAttribute implements CommonAttribute
{
    private String name;
    private String reverseName;
    private String query;
    private String orderBy;
    private String parameters;
    private boolean isRelatedDependent;
    private boolean hasParentContainer;
    private boolean isReverseRelationship = false;
    private ForeignKeyType foreignKeyType;
    private Cardinality cardinality;
    private MithraObjectTypeWrapper fromObject;
    private MithraObjectTypeWrapper relatedObject;
    private SimpleNode parsedQuery;
    private SimpleNode reverseParsedQuery;
    private List<OrderByAttribute> orderByAttributes = new ArrayList<OrderByAttribute>();
	private RelationshipType xmlRelationshipType;
    private String parameterVariables;
    private List<String> parameterVariableList;
    private List<String> parameterTypeList;
    private List<String> nonAsOfAttributeParameterVariableList;
    private List<String> nonAsOfAttributeParameterTypeList;
    private RelationshipConversionVisitor mapperVisitor;
    private boolean directReference = false;
    private int positionInObjectArray = -1;
    private boolean inhereted;

    public RelationshipAttribute(MithraObjectTypeWrapper from, MithraObjectTypeWrapper to, RelationshipType relationshipType)
    {
        this.fromObject = from;
        this.relatedObject = to;
        this.name = relationshipType.getName();
        this.query = relationshipType.value();
        this.orderBy = relationshipType.getOrderBy();
        this.reverseName = relationshipType.getReverseRelationshipName();
        this.isRelatedDependent = relationshipType.isRelatedIsDependent();
        this.cardinality = Cardinality.getByName(relationshipType.getCardinality().value());
        this.parameters = relationshipType.getParameters();
		this.xmlRelationshipType = relationshipType;
        this.foreignKeyType = relationshipType.getForeignKey();
        this.directReference = relationshipType.isDirectReference();
    }

    private RelationshipAttribute(String name, String reverseName, String query, String orderBy,
                                 boolean relatedDependent,
                                 Cardinality cardinality, MithraObjectTypeWrapper fromObject,
                                 MithraObjectTypeWrapper toObject, SimpleNode parsedQuery, String parameters,
            ForeignKeyType foreignKeyType, boolean directReference, RelationshipType relationshipType)
    {
        if (query == null)
        {
            throw new RuntimeException("null query");
        }
        this.name = name;
        this.reverseName = reverseName;
        this.query = query;
        this.orderBy = orderBy;
        this.hasParentContainer = relatedDependent;
        this.isReverseRelationship = true;
        this.cardinality = cardinality;
        this.parameters = parameters;
        this.fromObject = fromObject;
        this.relatedObject = toObject;
        this.setParsedQueryNode(parsedQuery);
        this.foreignKeyType = foreignKeyType;
        this.directReference = directReference;
        if (fromObject.disableForeignKeys()) this.foreignKeyType = ForeignKeyType.FALSE;
        this.xmlRelationshipType = relationshipType;
    }

    public ForeignKeyType getForeignKeyType()
    {
        return this.foreignKeyType;
    }

    public int getPositionInObjectArray()
    {
        return this.positionInObjectArray;
    }

    public void setPositionInObjectArray(int positionInObjectArray)
    {
        this.positionInObjectArray = positionInObjectArray;
    }

    public SimpleNode getParsedQuery()
    {
        return this.parsedQuery;
    }

    protected boolean hasLeafNodes(SimpleNode node)
    {
        if (node == null)
        {
            node = this.parsedQuery;
        }

        int size = node.jjtGetNumChildren();
        if (size > 1)
        {
            for (int i = 0; i < size; i++)
            {
                if (this.hasLeafNodes((SimpleNode) node.jjtGetChild(i)))
                {
                    return true;
                }
            }
        }
        else
        {
            if (node instanceof ASTLiteral || node instanceof ASTInLiteral)
            {
                SimpleNode parent = (SimpleNode)node.jjtGetParent();
                Node firstNode;
                if (parent.jjtGetChild(2) instanceof ASTLiteral || parent.jjtGetChild(2) instanceof ASTInLiteral)
                {
                    firstNode = parent.jjtGetChild(0);
                }
                else
                {
                    firstNode = parent.jjtGetChild(2);
                }
                if (firstNode instanceof ASTAttributeName && parent.jjtGetNumChildren() == 3 && firstNode.toString().startsWith("this.") )
                {
                    return true;
                }
            }
            else if (node instanceof ASTIsNullClause)
            {
                SimpleNode parent = (SimpleNode)node.jjtGetParent();

                Node firstNode = parent.jjtGetChild(0);
                if (firstNode instanceof ASTAttributeName && firstNode.toString().startsWith("this.") )
                {
                    return true;
                }
            }
        }
        return false;
    }

    public RelationshipConversionVisitor getMapperVisitor()
    {
        if (this.mapperVisitor == null)
        {
            this.mapperVisitor = new RelationshipConversionVisitor(this);
        }
        return this.mapperVisitor;
    }

    public String constructReverseMapper()
    {
        return this.getMapperVisitor().constructReverseMapper();
    }

    public String constructPureReverseMapper()
    {
        return this.getMapperVisitor().constructPureReverseMapper();
    }

    public String constructMapper()
    {
        RelationshipConversionVisitor mapperVisitor = new RelationshipConversionVisitor(this);
        return mapperVisitor.constructMapper();
    }

    public boolean hasDangleMapper()
    {
        return this.getMapperVisitor().hasDangleMapper();
    }

    public String constructDangleMapper()
    {
        return this.getMapperVisitor().constructDangleMapper();
    }

    public String getFilterExpression()
    {
        String filterExpStr = null;
        try
        {
            filterExpStr = this.parsedQuery.constructFilterExpr();
        }
        catch (MithraGeneratorException e)
        {
            throw new MithraGeneratorException("In object "+this.getFromObject()+" relationship "+this.getName()+" with expression "+query+" caused a problem "+e.getMessage(), e);
        }
        return filterExpStr.length() > 0 ? "if (" + filterExpStr + ") " : "";
    }

    public boolean hasFilters()
    {
        return this.hasLeafNodes(null);
    }

    public String getName()
    {
        return this.name;
    }

    public String getAddHandlerName()
    {
        return StringUtility.firstLetterToUpper(this.getName())+"AddHandler";
    }

    public String getQuery()
    {
        return this.query;
    }

    public String getOrderBy()
    {
        return this.orderBy;
    }

    public String getOrderByJavaDoc()
    {
        if (this.getOrderBy() == null)
        {
            return "";
        }
        return " * Order By: <code>" + this.getOrderBy() + "</code>.";
    }

	public RelationshipType getXmlRelationshipType()
	{
		return this.xmlRelationshipType;
	}

    public void addOrderByAttribute(OrderByAttribute orderByAttribute)
    {
        this.orderByAttributes.add(orderByAttribute);
    }

    public OrderByAttribute[] getOrderByAttributes()
    {
        OrderByAttribute[] result = new OrderByAttribute[this.orderByAttributes.size()];
        return this.orderByAttributes.toArray(result);
    }

    public boolean hasOrderBy()
    {
        return this.orderByAttributes.size() > 0;
    }

    public String getCompleteOrderByForRelationship()
    {
        if (!this.hasOrderBy())
        {
            return "null";
        }
        OrderByAttribute[] orderByAttributes = this.getOrderByAttributes();
        String result = orderByAttributes[0].getAttributeComparator();
        for(int i=1;i<orderByAttributes.length;i++)
        {
            result += ".and("+orderByAttributes[i].getAttributeComparator()+")";
        }
        return result;
    }

    public String getGetterNameForList()
    {
        String getterName = getGetter();
        if (!this.getCardinality().isToMany())
        {
            if(!getterName.endsWith("s"))
            {
                if (!getterName.endsWith("y"))
                {
                    getterName += "s";
                }
                else
                {
                    getterName = getterName.substring(0, getterName.length() - 1) + "ies";
                }
            }
        }
        return getterName;
    }

    public String getListClassName()
    {
        return getRelatedObject().getListClassName();
    }

    public String getListInterfaceName()
    {
        return getRelatedObject().getListInterfaceName();
    }

    public String getGetter()
    {
        return "get"+StringUtility.firstLetterToUpper(this.getName());
    }

    public String getGetterOperationMethodName()
    {
        return "zGet"+StringUtility.firstLetterToUpper(this.getName())+"Operation";
    }

    public String getSetter()
    {
        return "set"+StringUtility.firstLetterToUpper(this.getName());
    }

    public void addImports(MithraObjectTypeWrapper owner, Map allObjects, List<String> errors)
    {
        ImportAdditionVisitor importAdditionVisitor = new ImportAdditionVisitor(owner, allObjects, errors);
        this.parsedQuery.childrenAccept(importAdditionVisitor, null);
    }

    public String getGetterExpressionForOperation(String operation)
    {
        StringBuffer buf = new StringBuffer();
        if(this.getCardinality().isToMany())
        {
            buf.append("new ").append(getMithraImplTypeAsString()).append("(");
        }
        else
        {
            buf.append(this.getRelatedObject().getClassName()).append("Finder.zFindOneForRelationship(");
        }
        buf.append(operation);
        buf.append(")");
        return buf.toString();
    }

    public String getGetterExpressionForDetached(String operation)
    {
        StringBuffer buf = new StringBuffer();
        if(this.getCardinality().isToMany())
        {
            buf.append("new ").append(getMithraImplTypeAsString()).append("(");
        }
        else
        {
            buf.append(this.getRelatedObject().getClassName()).append("Finder.zFindOneForRelationship(");
        }
        buf.append(operation);
        buf.append(")");
//        if (this.mustPersistRelationshipChanges())
//        {
//            buf.append(".getDetachedCopy()");
//        }
        return buf.toString();
    }

    public String getGetterExpression()
    {
        return this.getGetterExpressionForOperation(this.getGetterOperationMethodName()+"("+this.getParameterVariables()+")");
    }

    public String getOperationExpression()
    {
        String queryString = this.getMapperVisitor().getOperationExpression();
        if (!isExtractorBasedMultiEquality() && needsDefaultAsOfDatesOperation())
        {
            queryString = queryString+".and("+ getDefaultAsOfDatesOperation()+")";
        }
        return queryString;
    }

    private String getDefaultAsOfDatesOperation()
    {
        return this.getMapperVisitor().getDefaultAsOfDatesOperation();
    }

    private boolean needsDefaultAsOfDatesOperation()
    {
        if (isResolvableInCache())
        {
            return this.getMapperVisitor().needsDefaultAsOfDatesOperation();
        }
        return false;
    }

    public String getTypeAsString()
    {
        if (hasXmlRelationshipType()) return this.xmlRelationshipType.getReturnType();
        return getMithraTypeAsString();
    }

    public String getImplTypeAsString()
    {
        if (hasXmlRelationshipType()) return this.xmlRelationshipType.getReturnType();
        return getMithraImplTypeAsString();
    }

    private boolean hasXmlRelationshipType()
    {
        return this.xmlRelationshipType != null && this.xmlRelationshipType.isReturnTypeSet();
    }

    public String getMithraTypeAsString()
    {
        String type = this.getRelatedObject().getClassName();
        if (this.getCardinality().isToMany())
        {
            type = this.getRelatedObject().getListInterfaceName();
        }
        return StringUtility.trimPackage(type);
    }

    public String getMithraImplTypeAsString()
    {
        String type = this.getRelatedObject().getImplClassName();
        if (this.getCardinality().isToMany())
        {
            type = this.getRelatedObject().getListClassName();
        }
        return StringUtility.trimPackage(type);
    }

    public boolean isListType()
    {
        return this.getCardinality().isToMany();
    }

    public String getFinderAttributeType()
    {
        String result;
        result = this.getFromObject().getClassName()+StringUtility.firstLetterToUpper(this.getName())+"FinderSubclass";
        return result;
    }

    public String getFinderAttributeSuperClassType()
    {
        return this.getFinderAttributeType();
    }

    public String getFinderAttributeTypeSuperClass()
    {
        String result;
        if (this.getCardinality().isToMany())
        {
            result = this.getRelatedObject().getClassName() + "Finder." + this.getRelatedObject().getClassName() + "CollectionFinder";
        }
        else
        {
            result = this.getRelatedObject().getClassName() + "Finder." + this.getRelatedObject().getClassName() + "SingleFinder";
        }
        return result;
    }

	public String getFinderAttributeTypeForRelatedClass()
	{
		String result;
        if (this.getCardinality().isToMany())
        {
            result = this.getRelatedObject().getClassName() + "Finder." + this.getRelatedObject().getClassName() + "CollectionFinderForRelatedClasses";
        }
        else
        {
            result = this.getRelatedObject().getClassName() + "Finder." + this.getRelatedObject().getClassName() + "SingleFinderForRelatedClasses";
        }
		return result;
	}

    public boolean isToMany()
    {
        return cardinality.isToMany();
    }

    public boolean isFromMany()
    {
        return cardinality.isFromMany();
    }

    public String getDeepFetchMethodName()
    {
        if (this.cardinality.isToMany())
        {
            return "SimpleToMany";
        }
        return "ToOne";
    }

    public String getReverseMapperName()
    {
        if (this.isReverseRelationship())
        {
            return this.getRelatedObject().getClassName()+"Finder.zGet"+
                    this.getRelatedObject().getClassName()+
                    StringUtility.firstLetterToUpper(this.reverseName)+"Mapper";
        }
        else
        {
            return this.fromObject.getClassName() +"Finder.zGet"+this.fromObject.getClassName()+
                    StringUtility.firstLetterToUpper(this.getName())+"ReverseMapper";
        }
    }

    public String getMapperName()
    {
        if (this.isReverseRelationship())
        {
            return this.getRelatedObject().getClassName()+"Finder.zGet"+
                    this.getRelatedObject().getClassName()+
                    StringUtility.firstLetterToUpper(this.reverseName)+"ReverseMapper";
        }
        else
        {
            return this.fromObject.getClassName() +"Finder.zGet"+
                   this.fromObject.getClassName()+StringUtility.firstLetterToUpper(this.getName())+"Mapper";
        }
    }

    public boolean isBidirectional()
    {
        return this.reverseName != null;
    }

    public MithraObjectTypeWrapper getRelatedObject()
    {
        return relatedObject;
    }

    public Cardinality getCardinality()
    {
        return cardinality;
    }

    public MithraObjectTypeWrapper getFromObject()
    {
        return fromObject;
    }

    public RelationshipAttribute getReverseRelationshipAttribute()
    {
        String reverseQuery = getReverseQuery();
        String newReverseName = this.reverseName;
        if (newReverseName == null)
        {
            newReverseName = this.name + "_Reverse";
        }
        return new RelationshipAttribute(newReverseName, this.name, reverseQuery, null, this.isRelatedDependent(), this.cardinality.getReverseCardinality(),
                this.relatedObject, this.fromObject, this.reverseParsedQuery, null, this.foreignKeyType, this.directReference, this.xmlRelationshipType);
    }

    public String getReverseQuery()
    {
        ReverseQueryVisitor visitor = new ReverseQueryVisitor(this.getFromObject(), this.getRelatedObject());
        this.parsedQuery.jjtAccept(visitor, null);
        return visitor.getReverseQuery();
    }

    public boolean isReverseRelationship()
    {
        return this.isReverseRelationship;
    }

    public boolean isDependent()
    {
        return this.isReverseRelationship && this.hasParentContainer;
    }

    public String getReverseName()
    {
        return reverseName;
    }

    public String getReverseJavaDocSee()
    {
        if (this.getReverseName() == null)
        {
            return "";
        }

        final String relatedClassName = this.getRelatedObject().getClassName();
        final String reverseMethodName = "get" + StringUtility.firstLetterToUpper(this.getReverseName()) + "()";

        return " * @see " + relatedClassName + '#' + reverseMethodName
                + " reverse relationship " + relatedClassName + '.' + reverseMethodName;
    }

    public String getMapperPartialName()
    {
        return this.fromObject.getClassName()+StringUtility.firstLetterToUpper(this.getName());
    }

    public boolean canSetLocalAttributesFromRelationship()
    {
        return  this.getMapperVisitor().isNextObjectInJoinRelatedObject() && !this.isRelatedDependent() && !this.isToMany();
    }

    public void setParsedQueryNode(SimpleNode simpleNode)
    {
        if (this.parsedQuery == null)
        {
            this.parsedQuery = simpleNode;
            if (this.hasSetter() && ((this.isRelatedDependent() && this.getRelatedObject().isTransactional())
                    || this.canSetLocalAttributesFromRelationship()))
            {
                Attribute[] relatedToSet = this.getAttributesToSetOnRelatedObject();
                for(int i=0;i<relatedToSet.length;i++)
                {
                    this.getAttributeToGetForSetOnRelatedObject(i).addDependentRelationship(this, relatedToSet[i]);
                }
            }
            getOperationExpression();
        }
    }

    public void resolveOwningRelationships()
    {
        if (!this.hasSetter()
                || this.hasFilters()
                || this.hasDangleMapper()
                || !this.dependsOnlyOnFromToObjects())
        {
            return;
        }

        Attribute[] relatedToSet = this.getAttributesToSetOnRelatedObject();
        for (int i = 0; i < relatedToSet.length; i++)
        {
            Attribute attributeToGetForSetOnRelatedObject = this.getAttributeToGetForSetOnRelatedObject(i);
            Attribute attributeToSet = relatedToSet[i];

            if (attributeToGetForSetOnRelatedObject instanceof AsOfAttribute
                    || (attributeToGetForSetOnRelatedObject instanceof SourceAttribute))
            {
                continue;
            }

            boolean oneToOne = !this.cardinality.isFromMany() && !this.cardinality.isToMany();
            boolean oneToOneForward = oneToOne && (this.isRelatedDependent() || !attributeToSet.isPrimaryKey());
            boolean oneToOneReverse = oneToOne && attributeToSet.isPrimaryKey() && (this.isRelatedDependent() || !attributeToGetForSetOnRelatedObject.isPrimaryKey());

            boolean oneToMany = !this.cardinality.isFromMany() && this.cardinality.isToMany();
            boolean manyToOne = this.cardinality.isFromMany() && !this.cardinality.isToMany();

            if (oneToMany || oneToOneForward)
            {
                if (attributeToSet.setOwningRelationship(this))
                {
                    attributeToSet.setOwningReverseRelationshipName(this.fromObject.getPackageName(), this.fromObject.getClassName(), this.name);
                    if (this.reverseName != null)
                    {
                        attributeToSet.setOwningRelationshipName(this.reverseName);
                    }
                }
            }
            else if (oneToOneReverse || manyToOne)
            {
                if (attributeToGetForSetOnRelatedObject.setOwningRelationship(this))
                {
                    if (this.reverseName != null)
                    {
                        attributeToGetForSetOnRelatedObject.setOwningReverseRelationshipName(this.relatedObject.getPackageName(), this.relatedObject.getClassName(), this.reverseName);
                    }
                    attributeToGetForSetOnRelatedObject.setOwningRelationshipName(this.name);
                }
            }
            // *-to-*
        }
    }

    public void setReverseParsedQuery(SimpleNode reverseParsedQuery)
    {
        this.reverseParsedQuery = reverseParsedQuery;
    }

    public boolean isOptimizable()
    {
        return this.isDependent() && this.hasEqualityBetweenFromAndToAttributes();
    }

    public List getEqualityRelationalExpressions()
    {
        return this.parsedQuery.getEqualityRelationalExpressions(this.getRelatedObject(), this.getFromObject());
    }

    public boolean hasEqualityBetweenFromAndToAttributes()
    {
        return this.getEqualityRelationalExpressions().size() > 0;
    }

    public void addIndicies()
    {
        this.getMapperVisitor().addIndicies(this.getCardinality());
	}

    public String getTopFinderName()
    {
        return this.getRelatedObject().getClassName()+"Finder";
    }

    public String getReverseTopFinderName()
    {
        return this.getFromObject().getClassName()+"Finder";
    }

    public boolean dependsOnlyOnFromToObjects()
    {
        if (this.parsedQuery == null)
        {
            return false;
        }
        HashSet set = new HashSet();
        this.parsedQuery.addDependentClassesToSet(set);
        return set.size() <= 1;
    }

    public boolean hasParameters()
    {
        return this.parameters != null && this.parameters.trim().length() > 0;
    }

    public String getParameters()
    {
        if (this.hasParameters())
        {
            return this.parameters;
        }
        return "";
    }

    public String getParameterVariables()
    {
        if (this.hasParameters())
        {
            this.parseParameters(null);
            return this.parameterVariables;
        }
        return "";
    }

    public String getParameterVariablesWithComma()
    {
        if (this.hasParameters())
        {
            return "," +this.getParameterVariables();
        }
        return "";
    }

    public String getParametersWithComma()
    {
        if (this.hasParameters())
        {
            return ", "+this.parameters;
        }
        return "";
    }

	public List validateAndUseMissingValuesFromSuperClass(CommonAttribute superClassAttribute)
	{
		RelationshipAttribute superClassRelationshipAttribute = (RelationshipAttribute)superClassAttribute;
		List<String> errors = new ArrayList<String>();
		if(!superClassRelationshipAttribute.getRelatedObject().equals(this.getRelatedObject()))
		{
			errors.add("related object type mismatch with superclass for attribute '" + this.getName() + "'");
		}
		if(!superClassRelationshipAttribute.getCardinality().equals(this.getCardinality()))
		{
			errors.add("cardinality mismatch with superclass for attribute '" + this.getName() + "'");
		}
		if(this.query == null)
		{
			this.query = superClassRelationshipAttribute.getQuery();
		}
		if(this.orderBy == null)
		{
			this.orderBy = superClassRelationshipAttribute.getOrderBy();
		}
		if(!this.xmlRelationshipType.isCardinalitySet() && superClassRelationshipAttribute.xmlRelationshipType.isCardinalitySet())
		{
			this.cardinality = superClassRelationshipAttribute.cardinality;
		}
		if(!this.xmlRelationshipType.isRelatedIsDependentSet() && superClassRelationshipAttribute.xmlRelationshipType.isRelatedIsDependentSet())
		{
			this.isRelatedDependent = superClassRelationshipAttribute.isRelatedDependent;
		}
        if (this.hasParameters() && this.isBidirectional())
        {
            errors.add("the parametrized relationship "+this.getName()+" cannot have a reverse relationship");
        }
        if (this.hasParameters())
        {
            this.parseParameters(errors);
        }
		return errors;
	}

    public int getParameterCount()
    {
        parseParameters(null);
        if (this.hasParameters())
        {
            return this.parameterTypeList.size();
        }
        return 0;
    }

    public String getParameterTypeAt(int i)
    {
        parseParameters(null);
        return this.parameterTypeList.get(i);
    }

    public String getParameterVariableAt(int i)
    {
        parseParameters(null);
        return this.parameterVariableList.get(i);
    }

    public boolean isResolvableByPrimaryKeyIndex()
    {
        return !this.getCardinality().isToMany()
                && this.dependsOnlyOnFromToObjects() && this.getMapperVisitor().isByPrimaryKey();
    }

    public boolean isResolvableByUniqueIndex()
    {
        //todo: getUniqueMatchingIndex is not working correctly with as of attributes.
        return !this.getCardinality().isToMany()
                && this.dependsOnlyOnFromToObjects() && this.getMapperVisitor().getUniqueMatchingIndex() != null;
    }

    public boolean isResolvableInCache()
    {
        return !this.isFindByUnique() && (this.isResolvableByPrimaryKeyIndex() || this.isResolvableByUniqueIndex());
    }

    public boolean isFindByUnique()
    {
        return (this.isResolvableByPrimaryKeyIndex() || this.isResolvableByUniqueIndex()) && this.hasParameters() && this.getNonAsOfDateParameterCount() > 0;
    }

    public String getFindByUniqueMethodName()
    {
        if (this.isResolvableByPrimaryKeyIndex()) return "findByPrimaryKey";
        return "findBy"+this.getMapperVisitor().getUniqueMatchingIndex().getSanitizedUpperCaseName();
    }

    public String getFindByUniqueParameters()
    {
        return this.getMapperVisitor().getFindByUniqueLookupParameters();
    }

    public boolean isStaticallyResolvableInCache()
    {
        parseParameters(null);
        if (!this.getCardinality().isToMany()
                && this.dependsOnlyOnFromToObjects())
        {
            if (this.getMapperVisitor().isByPrimaryKey())
            {
                return !this.getMapperVisitor().needsParametersOperationForPrimaryKey();
            }
            else if (this.getMapperVisitor().getUniqueMatchingIndex() != null)
            {
                // by some other unique index
                return !this.getMapperVisitor().needsParametersOperationForUniqueIndex();
            }
        }
        return false;
    }

    private void computeNonAsOfAttributeParameters()
    {
        parseParameters(null);
        if (this.hasParameters())
        {
            synchronized (this)
            {
                if (this.nonAsOfAttributeParameterTypeList == null)
                {
                    this.nonAsOfAttributeParameterVariableList = new ArrayList<String>();
                    this.nonAsOfAttributeParameterTypeList = new ArrayList<String>();
                    for (int i = 0; i < this.parameterTypeList.size(); i++)
                    {
                        boolean add = false;
                        if (this.getMapperVisitor().isByPrimaryKey())
                        {
                            add = this.getMapperVisitor().needsParameterOperationForPrimaryKey(parameterVariableList.get(i));
                        }
                        else if (this.getMapperVisitor().getUniqueMatchingIndex() != null)
                        {
                            // by some other unique index
                            add = this.getMapperVisitor().needsParameterOperationForUniqueIndex(parameterVariableList.get(i));
                        }
                        if (add)
                        {
                            this.nonAsOfAttributeParameterVariableList.add(parameterVariableList.get(i));
                            this.nonAsOfAttributeParameterTypeList.add(parameterTypeList.get(i));
                        }
                    }
                }
            }
        }
    }



    public String getRhs()
    {
        if (this.isStaticallyResolvableInCache())
        {
            return "for"+this.getName();
        }
        return "new "+StringUtility.firstLetterToUpper(this.getName())+"Rhs("+this.getNonAsOfAttributeParameterVariables()+")";
    }

    private String getNonAsOfAttributeParameterVariables()
    {
        computeNonAsOfAttributeParameters();
        String result = "";
        if (this.hasParameters())
        {
            for(int i=0;i<this.nonAsOfAttributeParameterVariableList.size();i++)
            {
                if (i > 0) result += ", ";
                result += this.nonAsOfAttributeParameterVariableList.get(i);
            }
        }
        return result;
    }

    protected synchronized void parseParameters(List<String> errors)
    {
        if (this.hasParameters() && this.parameterVariables == null)
        {
            this.parameterVariableList = new ArrayList<String>();
            this.parameterTypeList = new ArrayList<String>();
            this.parameterVariables = "";
            StringTokenizer tokenizer = new StringTokenizer(this.parameters, "\t ,");
            while(tokenizer.hasMoreTokens())
            {
                String type = tokenizer.nextToken();
                this.parameterTypeList.add(type);
                if (tokenizer.hasMoreTokens())
                {
                    if (this.parameterVariables.length() > 0)
                    {
                        this.parameterVariables += ", ";
                    }
                    String variable = tokenizer.nextToken();
                    if (variable.startsWith("_") && errors != null)
                    {
                        errors.add("parameter variables may not start with underscore (_) in relationship "+this.getName());
                    }
                    this.parameterVariableList.add(variable);
                    this.parameterVariables += variable;
                }
                else if (errors != null)
                {
                    errors.add("Could not tokenize parameters "+this.parameters+" for relationship "+this.getName());
                }
            }
        }
    }

    public boolean isSingleAttributeJoinIgnoringAsOfAttributes()
    {
        return this.getMapperVisitor().isSingleAttributeJoinIgnoringAsOfAttributes();
    }

    public boolean isSingleAttributeJoin()
    {
        return this.getMapperVisitor().isSingleAttributeJoin();
    }

    public Attribute[] getAttributesForInClauseEval()
    {
        return this.getMapperVisitor().getAttributesForInClauseEval();
    }

    public Attribute[] getAsOfAttributesForSingleCheck()
    {
        return this.getMapperVisitor().getAsOfAttributesForSingleCheck();
    }

    public Map getAsOfAttributesMap()
    {
        return this.getMapperVisitor().getAsOfAttributesMap();
    }

    public boolean mustPersistRelationshipChanges()
    {
        return this.isRelatedDependent() &&
                    this.getRelatedObject().isTransactional();
    }

    public boolean canRelatedBeDependent()
    {
        boolean canBeDependent = false;
        if (!this.getCardinality().isManyToMany() && this.dependsOnlyOnFromToObjects())
        {
            canBeDependent = true;
            Attribute[] leftAttributes = this.getMapperVisitor().getLeftJoinAttributes();
            Attribute[] pkAttributes = this.getFromObject().getPrimaryKeyAttributes();
            for(int i=0;canBeDependent && i<pkAttributes.length;i++)
            {
                boolean found = false;
                for(int j=0;!found && j<leftAttributes.length;j++)
                {
                    found = leftAttributes[j].getName().equals(pkAttributes[i].getName());
                }
                if (!found)
                {
                    canBeDependent = false;
                }
            }
        }
        if (!canBeDependent)
        {
            System.out.println("WARNING: relationship "+this.getName()+" between "+this.getFromObject().getClassName()+" and "+
                this.getRelatedObject().getClassName()+" is marked as 'relatedIsDependent' but there is no primary key relationship between the two objects."+
                " Relationship persistence will be disabled for this relationship");
            this.isRelatedDependent = false;
        }
        return canBeDependent;
    }

    public boolean hasParentContainer()
    {
        return hasParentContainer;
    }

    public boolean isRelatedDependent()
    {
        return this.isRelatedDependent && this.canRelatedBeDependent();
    }

    public String getAsOfAttributesDataMatchesConditions()
    {
        if (this.relatedObject.hasAsOfAttributes())
        {
            return this.getMapperVisitor().getAsOfAttributesDataMatchesConditions();
        }
        return "true";
    }

    public String getRelatedAsOfAttributeVariables()
    {
        if (this.relatedObject.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = this.relatedObject.getAsOfAttributes();
            String result = "";
            for(AsOfAttribute a: asOfAttributes)
            {
                result += "private AsOfAttribute "+a.getName()+" = "+this.relatedObject.getFinderClassName()+"."+a.getName()+"();\n";
            }
            return result;
        }
        return "";
    }

    public String getEqualsConditions()
    {
        return getMapperVisitor().getEqualsConditions();
    }

    public boolean requiresOverSpecifiedParameterCheck()
    {
        return getMapperVisitor().requiresOverSpecifiedParameterCheck();
    }

    public String getOverSpecificationCheck()
    {
        return getMapperVisitor().getOverSpecificationCheck();
    }

    public boolean hasDifferentOffHeapHash()
    {
        Index index = this.getMapperVisitor().getUniqueMatchingIndex();
        if (index == null)
        {
            return this.getMapperVisitor().hasDifferentOffHeapHashForPk();
        }
        else
        {
            return this.getMapperVisitor().hasDifferentOffHeapHash(index.getAttributes());
        }
    }

    public String getHashCodeComputation()
    {
        Index index = this.getMapperVisitor().getUniqueMatchingIndex();
        if (index == null)
        {
            return this.getMapperVisitor().getHashCodeComputationForPk(false);
        }
        else
        {
            return this.getMapperVisitor().getHashCodeComputation(index.getAttributes(), false);
        }
    }

    public String getOffHeapHashCodeComputation()
    {
        Index index = this.getMapperVisitor().getUniqueMatchingIndex();
        if (index == null)
        {
            return this.getMapperVisitor().getHashCodeComputationForPk(true);
        }
        else
        {
            return this.getMapperVisitor().getHashCodeComputation(index.getAttributes(), true);
        }
    }

    public boolean requiresSrcObjectForEquals()
    {
        return getMapperVisitor().requiresSrcObjectForEquals();
    }

    public boolean requiresSrcDataForEquals()
    {
        return getMapperVisitor().requiresSrcDataForEquals();
    }

    public boolean requiresSrcObjectForHashCode()
    {
        return getMapperVisitor().requiresSrcObjectForHashCode();
    }

    public boolean requiresSrcDataForHashCode()
    {
        return getMapperVisitor().requiresSrcDataForHashCode();
    }

    public boolean hasSetter()
    {
        return !this.hasParameters();
    }

    public Attribute[] getAttributesToSetOnRelatedObject()
    {
        RelationshipConversionVisitor visitor = this.getMapperVisitor();
        return visitor.getAttributesToSetOnRelatedObject();
    }

    public Attribute getAttributeToGetForSetOnRelatedObject(int index)
    {
        RelationshipConversionVisitor visitor = this.getMapperVisitor();
        return visitor.getAttributeToGetForSetOnRelatedObject(index);
    }

    public String getCacheLookupMethod()
    {
        String result;
        if (this.isResolvableByPrimaryKeyIndex())
        {
            result = "getAsOne";
        }
        else
        {
            result = "getAsOneByIndex";
        }
        return result;
    }

    public String getCacheLookUpParameters()
    {
        RelationshipConversionVisitor visitor = this.getMapperVisitor();
        String result = visitor.getCacheLookUpParameters();
        if (!this.isResolvableByPrimaryKeyIndex())
        {
            Index index = this.getMapperVisitor().getUniqueMatchingIndex();
            result += ", " + this.getRelatedObject().getFinderClassName()+".zGetIndex"+index.getName()+"Ref()";
        }
        return result;
    }

    public boolean mustDetach()
    {
        return this.mustPersistRelationshipChanges() || (this.isDependent() && this.getRelatedObject().isTransactional());
    }

    public String getDeepFetchType()
    {
        String result = "COMPLEX_";
        if (this.dependsOnlyOnFromToObjects())
        {
            result = "SIMPLE_";
        }
        if (this.isToMany())
        {
            result += "TO_MANY";
        }
        else
        {
            result += "TO_ONE";
        }
        return result;
    }

    public boolean isToOneDirectReference()
    {
        return !this.isToMany() && this.dependsOnlyOnFromToObjects() && this.directReference && !this.hasParameters();
    }

    public boolean isToManyDirectReference()
    {
        return this.isToMany() && this.dependsOnlyOnFromToObjects() && this.directReference && !this.hasParameters();
    }

    public String getDirectRefVariableName()
    {
        return "__"+this.getName();
    }

    protected boolean dependsOnFromAsOfAttributes()
    {
        return this.getMapperVisitor().dependsOnFromAsOfAttributes();
    }

    public boolean isDirectReference()
    {
        return this.isToOneDirectReference() || this.isToManyDirectReference();
    }

    public boolean isDirectReferenceInData()
    {
        return this.isDirectReference() && !mustBeInBusinessObject();
    }

    public boolean isDirectReferenceInBusinessObject()
    {
        return this.isDirectReference() && mustBeInBusinessObject();
    }

    public boolean isStorableInArray()
    {
        return this.hasSetter() && !isDirectReferenceInData();
    }

    private boolean mustBeInBusinessObject()
    {
        return this.isDirectReference() && (dependsOnFromAsOfAttributes() || this.fromObject.hasOffHeap());
    }

    public boolean needsDirectRefExtractors()
    {
        return isToOneDirectReference() && this.getMapperVisitor().needsDirectRefExtractors();
    }

    public String getFromDirectRefExtractors()
    {
        return this.getMapperVisitor().getFromDirectRefExtractors();
    }

    public String getToDirectRefExtractors()
    {
        return this.getMapperVisitor().getToDirectRefExtractors();
    }

    public String getDirectRefFromExtractorName()
    {
        if (this.needsDirectRefExtractors())
        {
            return "_from"+this.getName();
        }
        else return "null";
    }

    public String getDirectRefToExtractorName()
    {
        if (this.needsDirectRefExtractors())
        {
            return "_to"+this.getName();
        }
        else return "null";
    }

    public String getGetterForDirectRef()
    {
        if (this.isDirectReferenceInBusinessObject())
        {
            return "this."+this.getDirectRefVariableName();
        }
        return "_data."+this.getGetter()+"()";
    }

    public String getSetterForDirectRef()
    {
        if (this.isDirectReferenceInBusinessObject())
        {
            return "this."+this.getDirectRefVariableName()+" = ";
        }
        return "_data."+this.getSetter()+"";
    }

    public String getDirectRefHolder()
    {
        if (this.isDirectReferenceInBusinessObject())
        {
            return "this";
        }
        return "_data";
    }

    public boolean needsPortal()
    {
        return this.isDirectReference() || this.isResolvableInCache();
    }

    public boolean isInhereted()
    {
        return inhereted;
    }

    public void setInhereted(boolean inhereted)
    {
        this.inhereted = inhereted;
    }

    public String getMapperFragmentName()
    {
        return StringUtility.firstLetterToUpper(this.getName());
    }

    public boolean hasMapperFragment()
    {
        return this.getMapperVisitor().hasMapperFragment();
    }

    public boolean hasMapperFragmentParameters()
    {
        return this.getMapperVisitor().hasMapperFragmentParameters();
    }

    public String getMapperFragmentParameterVariables()
    {
        return this.getMapperVisitor().getMapperFragmentParameterVariables();
    }

    public String getMapperFragmentParameters()
    {
        return this.getMapperVisitor().getMapperFragmentParameters();
    }

    public String constructMapperFragment()
    {
        return this.getMapperVisitor().constructMapperFragment();
    }

    public int getNonAsOfDateParameterCount()
    {
        if (!this.hasParameters()) return 0;
        computeNonAsOfAttributeParameters();
        return this.nonAsOfAttributeParameterVariableList.size();
    }

    public String getNonAsOfDateParameterTypeAt(int index)
    {
        computeNonAsOfAttributeParameters();
        return this.nonAsOfAttributeParameterTypeList.get(index);
    }

    public String getNonAsOfDateParameterVariableAt(int index)
    {
        computeNonAsOfAttributeParameters();
        return this.nonAsOfAttributeParameterVariableList.get(index);
    }

    public String getNonAsOfDateParameters()
    {
        computeNonAsOfAttributeParameters();
        String result = "";
        for(int i=0;i<this.nonAsOfAttributeParameterTypeList.size();i++)
        {
            if (i > 0) result += ", ";
            result += this.nonAsOfAttributeParameterTypeList.get(i) + " " + this.nonAsOfAttributeParameterVariableList.get(i);
        }
        return result;
    }

    public List<String> getParameterVariableList()
    {
        return this.parameterVariableList;
    }

    public void warnAboutUniqueness()
    {
        if (!this.isToMany() && this.dependsOnlyOnFromToObjects() && !this.isResolvableInCache())
        {
            this.getFromObject().getLogger().info("Relationship "+this.getName()+" in object "+this.getFromObject().getClassName()+
                " is declared as -to-one, but does not match a unique index in "+this.getRelatedObject().getClassName());
        }
    }

    public boolean isExtractorBasedMultiEquality()
    {
        return !this.hasParameters() && this.dependsOnlyOnFromToObjects() && this.getMapperVisitor().isExtractorBasedMultiEquality();
    }

    public String getRelationshipMultiExtractorConstructor()
    {
        return this.getMapperVisitor().getRelationshipMultiExtractorConstructor();
    }

    public String getPlainTextName()
    {
        StringBuilder builder = new StringBuilder(this.getName());
        for(int i=0;i<builder.length();i++)
        {
            if (Character.isUpperCase(builder.charAt(i)))
            {
                builder.setCharAt(i, Character.toLowerCase(builder.charAt(i)));
                builder.insert(i, ' ');
                i++;
            }
        }
        return builder.toString();
    }

    public boolean isFinalGetter()
    {
        return this.xmlRelationshipType.isFinalGetterSet() ? this.xmlRelationshipType.isFinalGetter() : this.fromObject.isDefaultFinalGetters();
    }

    public boolean isBetterForAttributeOwnership(RelationshipAttribute other, Attribute attribute)
    {
        boolean thisDep = this.isDependent() || this.isRelatedDependent();
        boolean otherDep = other.isDependent() || other.isRelatedDependent();
        if (thisDep && !otherDep)
        {
            return true;
        }
        if (this.name.length() == other.name.length())
        {
            return this.name.compareTo(other.name) < 0;
        }
        return this.name.length() < other.name.length();
    }

    private static class ImportAdditionVisitor extends MithraQLVisitorAdapter
    {
        private Map allObjects;
        private List<String> errorMessages;
        private MithraObjectTypeWrapper owner;

        public ImportAdditionVisitor(MithraObjectTypeWrapper owner, Map allObjects, List<String> errorMessages)
        {
            this.allObjects = allObjects;
            this.errorMessages = errorMessages;
            this.owner = owner;
        }

        public Object visit(SimpleNode node, Object data)
        {
            node.addImport(this.owner, this.allObjects, this.errorMessages);
            return data;
        }
    }

    private static class ReverseQueryVisitor extends MithraQLVisitorAdapter
    {
        private MithraObjectTypeWrapper from;
        private MithraObjectTypeWrapper to;
        String reverseQuery;

        public ReverseQueryVisitor(MithraObjectTypeWrapper from, MithraObjectTypeWrapper to)
        {
            this.from = from;
            this.to = to;
            this.reverseQuery = "";
        }

        public Object visit(ASTAttributeName node, Object data)
        {
            if (node.belongsToThis())
            {
                this.reverseQuery += this.from.getClassName()+"."+node.getAttribute().getName();
            }
            else if (node.getAttribute().getOwner() == this.to)
            {
                this.reverseQuery += "this."+node.getAttribute().getName();
            }
            else
            {
                this.reverseQuery += node.toString();
            }
            return null;
        }

        public Object visit(ASTRelationalOperator node, Object data)
        {
            this.reverseQuery += " "+node.toString()+" ";
            return null;
        }

        public Object visit(ASTOrExpression node, Object data)
        {
            this.reverseQuery += " (";
            for(int i=0;i<node.jjtGetNumChildren();)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
                i++;
                if (i < node.jjtGetNumChildren())
                {
                    this.reverseQuery += " or ";
                }
            }
            this.reverseQuery += " )";
            return null;
        }

        public Object visit(ASTAndExpression node, Object data)
        {
            this.reverseQuery += " (";
            for(int i=0;i<node.jjtGetNumChildren();)
            {
                node.jjtGetChild(i).jjtAccept(this, data);
                i++;
                if (i < node.jjtGetNumChildren())
                {
                    this.reverseQuery += " and ";
                }
            }
            this.reverseQuery += " )";
            return null;
        }

        public Object visit(ASTRelationalExpression node, Object data)
        {
            node.childrenAccept(this, data);
            return null;
        }

        public Object visit(ASTIsNullClause node, Object data)
        {
            node.childrenAccept(this, data);
            this.reverseQuery += " is null ";
            return null;
        }

        public Object visit(ASTequalsEdgePointClause node, Object data)
        {
            node.childrenAccept(this, data);
            this.reverseQuery += " equalsEdgePoint ";
            return null;
        }

        public Object visit(ASTLiteral node, Object data)
        {
            if (node.isJavaLiteral())
            {
                this.reverseQuery += "{";
            }
            else
            {
                this.reverseQuery += " ";
            }
            this.reverseQuery += node.toString();
            if (node.isJavaLiteral())
            {
                this.reverseQuery += "}";
            }
            return null;
        }

        public Object visit(ASTInOperator node, Object data)
        {
            this.reverseQuery += " in ";
            return null;
        }

        public Object visit(ASTInLiteral node, Object data)
        {
            if (node.isJavaLiteral())
            {
                this.reverseQuery += "{" + node.getValue() + "}";
            }
            else
            {
                this.reverseQuery += "(" + node.getValue() + ")";
            }
            return null;
        }

        public String getReverseQuery()
        {
            return this.reverseQuery;
        }
    }
}
