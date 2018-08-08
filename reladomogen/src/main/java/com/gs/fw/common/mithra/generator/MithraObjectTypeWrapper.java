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

import com.gs.fw.common.mithra.generator.metamodel.*;
import com.gs.fw.common.mithra.generator.queryparser.*;
import com.gs.fw.common.mithra.generator.type.JavaTypeException;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.zip.CRC32;


public class MithraObjectTypeWrapper extends MithraBaseObjectTypeWrapper
{

    private static String ASC = "asc";
    private static String DESC = "desc";
    private final static String PREFERRED_ENCODING = "UTF-8";

    private static Set<String> validDirection = new HashSet<String>(Arrays.asList(ASC, DESC));
    private static Set<String> primitiveTypes = new HashSet<String>(Arrays.asList("boolean", "int", "long", "float", "double", "short", "byte", "char"));
    private static Set<String> javaTypes = new HashSet<String>(Arrays.asList("String", "Timestamp", "Date", "byte[]", "BigDecimal", "Time"));

    private char currentSubclassAlias = 'a';
    private List<MithraObjectTypeWrapper> lifeCycleParents = new ArrayList<MithraObjectTypeWrapper>();
    private Set<MithraObjectTypeWrapper> foreignKeys = new HashSet<MithraObjectTypeWrapper>();
    private Map<String, Integer> constantStringSet = new HashMap<String, Integer>();
    private Map<String, Integer> constantIntSet = new HashMap<String, Integer>();
    private Map<String, Short> constantShortSet = new HashMap<String, Short>();
    private List<String> constantStringSetValues = new ArrayList<String>();
    private List<String> constantIntSetValues = new ArrayList<String>();
    private List<String> constantShortSetValues = new ArrayList<String>();

    private Set<String> imports = new HashSet<String>();

    private List<RelationshipAttribute> relationshipAttributes = new ArrayList<RelationshipAttribute>();
    private List<Attribute> attributes;
    private List<ComputedAttributeType> computedAttributes;
    private List<Attribute> inheritedAttributes;
    private List<AsOfAttribute> asOfAttributes;
    private List<Attribute> primaryKeyAttributes;
    private List<Attribute> identityAttributeList;
    private List<Attribute> nullablePrimitiveAttributes;
    private List<Attribute> nullablePrimitiveMutablePkAttributes;
    private List<EmbeddedValue> embeddedValueObjects = new ArrayList<EmbeddedValue>();
    private TransactionalMethodSignature[] transactionalMethodSignatures;
    private TransactionalMethodSignature[] datedTransactionalMethodSignatures;
    private List<MithraObjectTypeWrapper> childClasses = new ArrayList<MithraObjectTypeWrapper>();
    private List<MithraInterfaceType> mithraInterfaces = new ArrayList<MithraInterfaceType>();

    private SourceAttribute sourceAttribute;
    private Attribute optimisticLockAttribute;
    private Map<String, CommonAttribute> attributesByName = new HashMap<String, CommonAttribute>();
    private List<Index> indices;
    private Index pkIndex;
    private static final String SOURCE_ATTRIBUTE_VARIABLE_NAME = "sourceAttribute";
    private int serialVersionId = 0;
    private List<ASTRelationalExpression> joinPool = new ArrayList<ASTRelationalExpression>();
    private List<ASTRelationalExpression> operationPool = new ArrayList<ASTRelationalExpression>();
    private boolean setRelationshipPositions = false;
    private boolean hasMutablePk = false;
    private boolean isReferencedViaForeignKey = false;

    private boolean firstFkFileWrite = true;
    private boolean hasPrimaryKey = false;
    private boolean ignoreTransactionalMethods = false;
    private boolean pure = false;
    private boolean defaultFinalGetters = false;
    private final boolean ignorePackageNamingConvention;

    private int fkCounter = 0;
    private boolean replicated = false;
    private static final Attribute[] EMPTY_ATTRIBUTES = new Attribute[0];
    private boolean temporary;
    private MithraSuperTypeWrapper substituteSuperType;
    private boolean superClassGenerationResolved;
    private Logger logger;
    private boolean enableOffHeap;
    private boolean isOffHeapCompatible;
    private int offHeapSize;

    public MithraObjectTypeWrapper(MithraBaseObjectType wrapped, String sourceFileName, String importedSource,
            boolean isGenerateInterfaces, boolean ignorePackageNamingConvention, Logger logger)
    {
        super(wrapped, sourceFileName, importedSource);
        this.addImports();
        super.createAuxiliaryClassNames(isGenerateInterfaces);
        this.setGenerateInterfaces(isGenerateInterfaces);
        this.ignorePackageNamingConvention = ignorePackageNamingConvention;
        this.logger = logger;
    }

    public boolean disableForeignKeys()
    {
        return getWrapped().isDisableForeignKeys();
    }

    public String getDescription()
    {
        return this.isPure() ? "pure object" : "object";
    }

    public MithraObjectTypeWrapper getSuperClassWrapper()
    {
        return (MithraObjectTypeWrapper) super.getSuperClassWrapper();
    }

    public void setReplicated(boolean replicated)
    {
        this.replicated = replicated;
    }

    public boolean isReplicated()
    {
        return this.replicated;
    }

    public boolean isPure()
    {
        return this.pure;
    }

    public void setPure(boolean pure)
    {
        this.pure = pure;
    }

    public boolean isDefaultFinalGetters()
    {
        return this.defaultFinalGetters;
    }

    public void setDefaultFinalGetters(boolean defaultFinalGetters)
    {
        this.defaultFinalGetters = defaultFinalGetters;
    }

    public boolean hasCascadableInPlaceUpdate()
    {
        if (hasInPlaceUpdateAttribute()) return true;
        HashSet<MithraObjectTypeWrapper> relatedDone = new HashSet<MithraObjectTypeWrapper>();
        relatedDone.add(this);

        ArrayList<MithraObjectTypeWrapper> relatedNotDone = new ArrayList<MithraObjectTypeWrapper>();

        if (searchRelatedIsDependentForInPlaceUpdate(relatedDone, relatedNotDone)) return true;
        while(!relatedNotDone.isEmpty())
        {
            MithraObjectTypeWrapper toSearch = relatedNotDone.remove(relatedNotDone.size() - 1);
            relatedDone.add(toSearch);
            if (searchRelatedIsDependentForInPlaceUpdate(relatedDone, relatedNotDone)) return true;
        }
        return false;
    }

    private boolean searchRelatedIsDependentForInPlaceUpdate(HashSet<MithraObjectTypeWrapper> relatedDone, ArrayList<MithraObjectTypeWrapper> relatedNotDone)
    {
        for(int i=0;i<this.relationshipAttributes.size();i++)
        {
            if (relationshipAttributes.get(i).isRelatedDependent())
            {
                MithraObjectTypeWrapper relatedObject = relationshipAttributes.get(i).getRelatedObject();
                if (relatedObject.hasInPlaceUpdateAttribute())
                {
                    return true;
                }
                else if (!relatedDone.contains(relatedObject))
                {
                    relatedNotDone.add(relatedObject);
                }
            }
        }
        return false;
    }

    private boolean hasInPlaceUpdateAttribute()
    {
        for(int i=0;i<this.attributes.size();i++)
        {
            if (attributes.get(i).isInPlaceUpdate()) return true;
        }
        return false;
    }

    public void setIgnoreTransactionalMethods(boolean ignoreTransactionalMethods)
    {
        this.ignoreTransactionalMethods = ignoreTransactionalMethods;
    }

    public boolean isFirstFkFileWrite()
    {
        return this.firstFkFileWrite;
    }

    public boolean generateTxMethods()
    {
        return !this.isTablePerClassSubClass();
    }

    public void setNotFirstFkFileWrite()
    {
        this.firstFkFileWrite = false;
    }

    public void incrementFkCounter()
    {
        this.fkCounter++;
    }

    public int getFkCounter()
    {
        return this.fkCounter;
    }

    public int getFkCounterAndIncrement()
    {
        int result = this.getFkCounter();
        this.incrementFkCounter();

        return result;
    }

    public boolean requiresNonPkDatabaseTimezone()
    {
        if (this.getSuperClassWrapper() != null && this.getSuperClassWrapper().requiresNonPkDatabaseTimezone())
        {
            return true;
        }
        for (int i = 0; i < this.attributes.size(); i++)
        {
            Attribute attr = this.attributes.get(i);
            if (attr.isDatabaseTimezone()) return true;
        }
        for (int i = 0; i < this.asOfAttributes.size(); i++)
        {
            AsOfAttribute attr = this.asOfAttributes.get(i);
            if (attr.isDatabaseTimezone()) return true;
        }
        return false;
    }

    public boolean requiresPkDatabaseTimezone()
    {
        for (int i = 0; i < attributes.size(); i++)
        {
            Attribute attr = attributes.get(i);
            if (attr.isPrimaryKey() && attr.isDatabaseTimezone()) return true;
        }
        return false;
    }

    public int getHierarchyDepth(Map<String, MithraObjectTypeWrapper> allObjects)
    {
        String superClass = null;
        if (super.getSuperClass() != null)
        {
            superClass = super.getSuperClass().getName();
        }
        int depth = 0;
        while (superClass != null)
        {
            depth++;
            MithraObjectTypeWrapper superClassWrapper = allObjects.get(superClass);
            if (superClassWrapper == null)
            {
                superClass = null;
            }
            else
            {
                if (superClassWrapper.getSuperClass() != null)
                {
                    superClass = superClassWrapper.getSuperClass().getName();
                }
                else
                {
                    superClass = null;
                }
            }
        }
        return depth;
    }

    public Attribute[] getNullablePrimitiveAttributes()
    {
        Attribute[] result = new Attribute[this.nullablePrimitiveAttributes.size()];
        return this.nullablePrimitiveAttributes.toArray(result);
    }

    public Attribute[] getNullableMutablePkAttributes()
    {
        Attribute[] result = new Attribute[this.nullablePrimitiveMutablePkAttributes.size()];
        return this.nullablePrimitiveMutablePkAttributes.toArray(result);
    }

    public Attribute getAttributeByName(String name)
    {
        return (Attribute) attributesByName.get(name);
    }

    public synchronized RelationshipAttribute[] getRelationshipAttributes()
    {
        if (!this.setRelationshipPositions)
        {
            this.setRelationshipPositions = true;
            ArrayList<RelationshipAttribute> copy = new ArrayList<RelationshipAttribute>(this.relationshipAttributes);
            Collections.sort(copy, new Comparator<RelationshipAttribute>()
            {
                public int compare(RelationshipAttribute o1, RelationshipAttribute o2)
                {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            int count = 0;
            for (int i = 0; i < copy.size(); i++)
            {
                RelationshipAttribute r = copy.get(i);
                if (r.isStorableInArray())
                {
                    r.setPositionInObjectArray(count);
                    count++;
                }
            }
        }
        return getRelationshipAttributesWithoutPosition();
    }

    public RelationshipAttribute[] getRelationshipAttributesWithoutPosition()
    {
        RelationshipAttribute[] temp = new RelationshipAttribute[this.relationshipAttributes.size()];
        return this.relationshipAttributes.toArray(temp);
    }

    public boolean hasDirectRefsInBusinessObject()
    {
        for(RelationshipAttribute rel: this.getRelationshipAttributes())
        {
            if (rel.isDirectReferenceInBusinessObject()) return true;
        }
        return false;
    }

    public boolean hasDirectRefs()
    {
        for(RelationshipAttribute rel: this.getRelationshipAttributes())
        {
            if (rel.isDirectReference()) return true;
        }
        return false;
    }

    public RelationshipAttribute getRelationshipAttributeByName(String name)
    {
        for (int i = 0; i < this.relationshipAttributes.size(); i++)
        {
            RelationshipAttribute ra = this.relationshipAttributes.get(i);
            if (ra.getName().equals(name)) return ra;
        }
        return null;
    }

    public Attribute[] getAttributes()
    {
        Attribute[] result = new Attribute[this.attributes.size()];
        return this.attributes.toArray(result);
    }

    public Attribute[] getSortedAttributes()
    {
        Attribute[] attributes = this.getAttributes();
        Arrays.sort(attributes);
        return attributes;
    }

    public EmbeddedValue[] getEmbeddedValueObjects()
    {
        EmbeddedValue[] embeddedValues = new EmbeddedValue[this.embeddedValueObjects.size()];
        embeddedValues = this.embeddedValueObjects.toArray(embeddedValues);
        Arrays.sort(embeddedValues);
        return embeddedValues;
    }

    public EmbeddedValue[] getRootEmbeddedValueObjects()
    {
        List<EmbeddedValue> filteredList = new ArrayList<EmbeddedValue>();
        for (EmbeddedValue object : this.embeddedValueObjects)
        {
            if (object.isRoot())
            {
                filteredList.add(object);
            }
        }
        EmbeddedValue[] embeddedValues = new EmbeddedValue[filteredList.size()];
        embeddedValues = filteredList.toArray(embeddedValues);
        Arrays.sort(embeddedValues);
        return embeddedValues;
    }

    public EmbeddedValue[] getNestedEmbeddedValueObjects()
    {
        List<EmbeddedValue> filteredList = new ArrayList<EmbeddedValue>();
        for (EmbeddedValue object : this.embeddedValueObjects)
        {
            if (object.isNested())
            {
                filteredList.add(object);
            }
        }
        EmbeddedValue[] embeddedValues = new EmbeddedValue[filteredList.size()];
        embeddedValues = filteredList.toArray(embeddedValues);
        Arrays.sort(embeddedValues);
        return embeddedValues;
    }

    public EmbeddedValueMapping[] getEmbeddedAttributes()
    {
        List<EmbeddedValueMapping> filteredList = new ArrayList<EmbeddedValueMapping>();
        for (Attribute attribute : this.getAttributes())
        {
            if (attribute instanceof EmbeddedValueMapping)
            {
                filteredList.add((EmbeddedValueMapping) attribute);
            }
        }
        EmbeddedValueMapping[] attributes = new EmbeddedValueMapping[filteredList.size()];
        attributes = filteredList.toArray(attributes);
        Arrays.sort(attributes);
        return attributes;
    }

    public Attribute[] getNonEmbeddedAttributes()
    {
        List<Attribute> filteredList = new ArrayList<Attribute>();
        for (Attribute attribute : this.getAttributes())
        {
            if (!(attribute instanceof EmbeddedValueMapping))
            {
                filteredList.add(attribute);
            }
        }
        Attribute[] attributes = new Attribute[filteredList.size()];
        attributes = filteredList.toArray(attributes);
        Arrays.sort(attributes);
        return attributes;
    }

    public AbstractAttribute[] getNonEmbeddedNormalAndSourceAttributes()
    {
        List<AbstractAttribute> filteredList = new ArrayList<AbstractAttribute>();
        for (AbstractAttribute attribute : this.getNormalAndSourceAttributes())
        {
            if (!(attribute instanceof EmbeddedValueMapping))
            {
                filteredList.add(attribute);
            }
        }
        AbstractAttribute[] attributes = new AbstractAttribute[filteredList.size()];
        attributes = filteredList.toArray(attributes);
        Arrays.sort(attributes);
        return attributes;
    }

    public boolean hasMithraInterfaces()
    {
        return this.mithraInterfaces.size() > 0;
    }

    public MithraInterfaceType[] getMithraInterfaces()
    {
        MithraInterfaceType[] mithraInterfaceTypes = new MithraInterfaceType[this.mithraInterfaces.size()];
        return this.mithraInterfaces.toArray(mithraInterfaceTypes);
    }

    public String getImplementingMithraInterfacesAsString()
    {
        StringBuilder sb = new StringBuilder();
        if(hasMithraInterfaces())
        {
          for (int i=0;i<mithraInterfaces.size();i++)
           {
              MithraInterfaceType  mithraInterfaceType = mithraInterfaces.get(i);

             if(isTablePerClassSubClass() && !isTablePerSubclassConcreteClass()) // is table per subclass and not concrete class.
             {
                 String superclassName = getTopSuperClassNameForTablePerSubClass();
                 sb.append(mithraInterfaceType.getClassName()+"Finder<"+ superclassName +">");
             }
             else
             {
                 sb.append(mithraInterfaceType.getClassName()+"Finder<"+ this.getWrapped().getClassName() +">");
             }

             if(i < (mithraInterfaces.size()-1) )
             {
                 sb.append(", ");
             }
           }
        }
        return sb.toString();
    }

    public String  getTopSuperClassNameForTablePerSubClass()
    {
        MithraObjectTypeWrapper wrapper  = this.getSuperClassWrapper();

        while(wrapper != null && wrapper.getSuperClassWrapper() != null)
        {
            wrapper = wrapper.getSuperClassWrapper();
        }

        return wrapper.getClassName();

    }

    public boolean isAbstractTablePerSubclass()
    {
        return this.getWrapped().getSuperClassType() != null && this.getWrapped().getSuperClassType().isTablePerSubclass();
    }

    public String getPrimaryKeyOperation()
    {
        String result = "Operation op;\n";
        boolean haveSomething = false;
        Attribute[] normalAttributes = this.getPrimaryKeyAttributes();
        for (int i = 0; i < normalAttributes.length; i++)
        {
            if (normalAttributes[i].isPrimaryKey())
            {
                if(normalAttributes[i].isNullable())
                {
                    result += "if(data."+normalAttributes[i].getNullGetterUseMutableIfApplicable()+"){";

                    if (haveSomething)
                    {
                        result += "op = op.and(";
                    }
                    else
                    {
                        result += "op = ";
                    }

                    result += this.getFinderClassName() + "." + normalAttributes[i].getName() + "().isNull()";

                    if (haveSomething)
                    {
                        result += ")";
                    }

                    result += ";\n";

                    result += "}else{";
                }

                if (haveSomething)
                {
                    result += "op = op.and(";
                }
                else
                {
                    result += "op = ";
                }

                result += this.getFinderClassName() + "." + normalAttributes[i].getName() + "().eq(data." + normalAttributes[i].getGetterOrMutableGetter() + "())";

                if (haveSomething)
                {
                    result += ")";
                }

                result += ";\n";

                if(normalAttributes[i].isNullable())
                {
                    result += "}";
                }

                haveSomething = true;
            }
        }
        if (this.hasSourceAttribute())
        {
             result += "op = op.and(" + this.getFinderClassName() + "." + this.getSourceAttribute().getName() + "().eq(data." + this.getSourceAttribute().getGetter() + "()));\n";
        }
        if (this.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttrs = this.getAsOfAttributes();
            for (int i = 0; i < asOfAttrs.length; i++)
            {
                result += "op = op.and(" + this.getFinderClassName() + "." + asOfAttrs[i].getName() + "().eq(this." + asOfAttrs[i].getGetter() + "()));\n";
            }
        }
        return result;
    }

    public void initializeNullBitHolders()
    {
        int count = this.nullablePrimitiveAttributes.size();
        count += this.nullablePrimitiveMutablePkAttributes.size();
        super.initializeNullBitHolders(count);
    }

    public String getNullSetterExpressionForMutableForIndex(int index)
    {
        return super.getNullSetterExpressionForIndex(index + this.nullablePrimitiveAttributes.size());
    }

    public String getNotNullSetterExpressionForMutablePk(int index)
    {
        return this.getNotNullSetterExpressionForIndex(this.nullablePrimitiveAttributes.size() + index);
    }

    public Set<String> getImportSet()
    {
        return Collections.unmodifiableSet(this.imports);
    }

    public List<String> getImports()
    {
        ArrayList<String> imports = new ArrayList<String>(this.imports);
        Collections.sort(imports);
        return imports;
    }

    public boolean isTransactional()
    {
        return this.getWrapped().getObjectType().isTransactional();
    }

    public String getObjectType()
    {
        String result = this.getWrapped().getObjectType().value();
        if (this.hasAsOfAttributes()) result = BaseMithraGenerator.DATED + result;
        return result;
    }

    public boolean hasData()
    {
        return this.isTransactional() || this.hasAsOfAttributes();
    }

    public boolean isReadOnly()
    {
        return this.getWrapped().getObjectType().isReadOnly();
    }

    public boolean isTablePerSubclassSuperClass()
    {
        return this.getWrapped().getSuperClassType() != null && this.getWrapped().getSuperClassType().isTablePerSubclass();
    }

    public boolean isTableForAllSubclasses()
    {
        return this.getWrapped().getSuperClassType() != null && this.getWrapped().getSuperClassType().isTableForAllSubclasses();
    }

    public boolean hasAsOfAttributes()
    {
        return this.asOfAttributes != null && this.asOfAttributes.size() > 0;
    }

    public String getListSuperInterface()
    {
        String result = "Mithra";
        if (this.isTransactional())
        {
            if (this.hasAsOfAttributes())
            {
                result += "Dated";
            }
            result += "Transactional";
        }
        result += "List";
        return result;
    }

    private void extractRelationshipAttributes(Map<String, MithraObjectTypeWrapper> allObjects, List<String> errors)
    {
        int noOfRelationships = this.getWrapped().getRelationships().size();

        for (int i = 0; i < noOfRelationships; i++)
        {
            RelationshipType relationshipType = (RelationshipType) this.getWrapped().getRelationships().get(i);
            String relationshipClassName = relationshipType.getRelatedObject();
            MithraObjectTypeWrapper relatedObject = allObjects.get(relationshipClassName);
            RelationshipAttribute relationshipAttribute = new RelationshipAttribute(this, relatedObject, relationshipType);

            this.addRelationship(relationshipAttribute);
        }
    }

    private void checkAndExtractOrderByForRelationship(RelationshipAttribute relationshipAttribute, List<String> errors)
    {
        MithraObjectTypeWrapper relatedObject = relationshipAttribute.getRelatedObject();
        String orderByClause = relationshipAttribute.getOrderBy();
        if (orderByClause == null) return;
        String[] orderByAttributes = orderByClause.split(",");

        for (int j = 0; j < orderByAttributes.length; j++)
        {
            String orderByAttributeInput = orderByAttributes[j];
            orderByAttributeInput = orderByAttributeInput.trim();
            String[] attrAndDirection = orderByAttributeInput.split(" ");
            String direction = ASC;
            if (attrAndDirection.length == 0)
            {
                errors.add("no attributes specified in orderBy clause");
            }
            else
            {
                String orderByAttributeName = attrAndDirection[0];
                if (attrAndDirection.length > 1)
                {
                    direction = attrAndDirection[1].toLowerCase();
                }
                Attribute orderByAttribute = relatedObject.getAttributeByName(orderByAttributeName);

                if (orderByAttribute == null)
                {
                    errors.add("order by attribute '" + orderByAttributeName + "' does not exist in related object '" + relatedObject.getClassName() + "'");
                }
                if (!validDirection.contains(direction))
                {
                    errors.add("order by attribute '" + orderByAttributeName + "' has invalid direction '" + attrAndDirection[1] + "'. Use: " + validDirection);
                }
                if (errors.size() == 0)
                {
                    relationshipAttribute.addOrderByAttribute(new OrderByAttribute(orderByAttribute, direction.equals(ASC)));
                }
            }
        }
    }

    public void addRelationship(RelationshipAttribute relationshipAttribute)
    {
        this.relationshipAttributes.add(relationshipAttribute);
        this.setRelationshipPositions = false;
        this.attributesByName.put(relationshipAttribute.getName(), relationshipAttribute);
    }

    private void addLifeCycleParent(MithraObjectTypeWrapper mithraObjectTypeWrapper)
    {
        if(!mithraObjectTypeWrapper.isTablePerSubclassSuperClass())
        {
           lifeCycleParents.add(mithraObjectTypeWrapper);
           this.addToRequiredClasses(mithraObjectTypeWrapper.getPackageName(), mithraObjectTypeWrapper.getFinderClassName());
        }
    }

    private void extractAsOfAttributes(List<String> errors)
    {
        this.asOfAttributes = new ArrayList<AsOfAttribute>();
        for (int i = 0; i < this.getWrapped().getAsOfAttributes().size(); i++)
        {
            AsOfAttributePureType aat = (AsOfAttributePureType) this.getWrapped().getAsOfAttributes().get(i);
            this.asOfAttributes.add(new AsOfAttribute(aat, this));
        }
        this.checkProcessingDateOnAsOfAttributes(errors);
    }

    public Attribute getOptimisticLockAttribute()
    {
        return this.optimisticLockAttribute;
    }

    public boolean hasOptimisticLockAttribute()
    {
        return this.optimisticLockAttribute != null;
    }

    public boolean hasTimestampOptimisticLockAttribute()
    {
        return this.optimisticLockAttribute != null && this.optimisticLockAttribute.getType().isTimestamp();
    }

    private void extractAttributes(List<String> errors) throws JavaTypeException
    {
        this.extractAsOfAttributes(errors);

        this.extractRegularAttributes(errors);

        this.extractComputedAttributes(errors);

        this.validateComputedAttributes(errors);
    }

    private void validateComputedAttributes(List<String> errors)
    {
        for(int i=0;i<computedAttributes.size();i++)
        {
            verifyComputedAttributeDependency(computedAttributes.get(i), errors);
        }
    }

    private void verifyComputedAttributeDependency(ComputedAttributeType attributeToCheck, List<String> errors)
    {
        Set<String> attributeList = attributeToCheck.getAttributeList();
        for(String name: attributeList)
        {
            if (name.equals(attributeToCheck.getName()))
            {
                errors.add("In object "+this.getClassName()+" computed attribute "+name+" has a circular dependency on itself");
            }
            Attribute attributeByName = this.getAttributeByName(name);
            if (attributeByName == null)
            {
                ComputedAttributeType computedAttributeType = this.getComputedAttributeByName(name);
                if (computedAttributeType == null)
                {
                    errors.add("In object "+this.getClassName()+" computed attribute "+attributeToCheck.getName()+" has an unresolved dependency on "+name);
                }
            }
        }
    }

    private ComputedAttributeType getComputedAttributeByName(String name)
    {
        for(int i=0;i<computedAttributes.size();i++)
        {
            if (computedAttributes.get(i).getName().equals(name)) return computedAttributes.get(i);
        }
        return null;
    }

    private void extractComputedAttributes(List<String> errors)
    {
        this.computedAttributes = this.getWrapped().getComputedAttributes();
        for(int i=0;i<this.computedAttributes.size();i++)
        {
            this.computedAttributes.get(i).parseComputedAttribute(errors);
        }
    }

    private void extractRegularAttributes(List<String> errors)
    {
        this.attributes = new ArrayList<Attribute>();
        this.inheritedAttributes = new ArrayList<Attribute>();

        this.identityAttributeList = new ArrayList<Attribute>();
        this.primaryKeyAttributes = new ArrayList<Attribute>();
        this.nullablePrimitiveAttributes = new ArrayList<Attribute>();
        this.nullablePrimitiveMutablePkAttributes = new ArrayList<Attribute>();

        for (int i = 0; i < this.getWrapped().getAttributes().size(); i++)
        {
            AttributePureType attributeType = (AttributePureType) this.getWrapped().getAttributes().get(i);
            Attribute attribute = new Attribute(attributeType, this);
            if (attribute.isUsedForOptimisticLocking())
            {
                if (this.optimisticLockAttribute != null)
                {
                    errors.add(this.getClassName()+" Cannot have multiple optimistic locking attriubtes: "+this.optimisticLockAttribute.getName()+" "
                            +attribute.getName());
                }
                if (!(attribute.getType().isIntOrLong() || attribute.getType().isTimestamp()))
                {
                    errors.add(this.getClassName()+" An optimistic locking attribute must be int or long or timestamp "+attribute.getName());
                }
            }
            if (attribute.isMutablePrimaryKey())
            {
                this.hasMutablePk = true;
                if (!attribute.isPrimaryKey())
                {
                    errors.add(this.getClassName()+" attribute "+attribute.getName()+" is declared as mutablePrimaryKey but primaryKey is not set to true");
                }
            }
            if (attribute.isInPlaceUpdate() && this.hasAsOfAttributes())
            {
                if (attribute.isPrimaryKey() )
                {
                    errors.add(this.getClassName()+" Cannot in-place update primaryKey attribute "+attribute.getName());
                }
                if (!this.hasProcessingDate())
                {
                    errors.add(this.getClassName()+" Inplace update can only be performed if Processing Date is present");
                }
            }
            if (attribute.isIdentity() && this.hasAsOfAttributes())
            {
                errors.add(this.getClassName()+" identity attribute"+attribute.getName()+" cannot and must never be combined with as-of-attributes. This is a serious violation of temporal semantics and will never be supported.");
            }
            attribute.validate(errors);
            addAttribute(attribute);
        }
        for (int i = 0; i < this.asOfAttributes.size(); i++)
        {
            AsOfAttribute asOfAttribute = this.asOfAttributes.get(i);
            addAsOfAttribute(asOfAttribute);
        }
        if (this.getWrapped().getSourceAttribute() != null)
        {
            this.sourceAttribute = new SourceAttribute(this.getWrapped().getSourceAttribute(), this);
            this.attributesByName.put(this.sourceAttribute.getName(), sourceAttribute);
        }
        this.initializeNullBitHolders();
        if (this.hasSourceAttribute() && this.isReplicated())
        {
            errors.add(this.getClassName()+" support for source attributes and replication notification is not implemented yet");
        }
        if (this.hasMutablePk)
        {
            if (this.hasAsOfAttributes())
            {
                errors.add(this.getClassName()+ " a dated object cannot have mutable primary keys");
            }
            if (this.isReadOnly())
            {
                errors.add(this.getClassName()+" a readonly object canoot have a mutable primary key");
            }
        }
    }

    public boolean hasMutablePk()
    {
        return this.hasMutablePk;
    }

    public boolean hasShadowAttributes()
    {
        return this.hasMutablePk() || this.hasTimestampOptimisticLockAttribute();
    }

    private void addAsOfAttribute(AsOfAttribute asOfAttribute)
    {
        this.attributesByName.put(asOfAttribute.getName(), asOfAttribute);
        this.addAttributeForAsOfAttribute(asOfAttribute, "From", asOfAttribute.getFromColumnName(), false);
        this.addAttributeForAsOfAttribute(asOfAttribute, "To", asOfAttribute.getToColumnName(), true);
    }

    private void addAttributeForAsOfAttribute(AsOfAttribute asOfAttribute, String prefix, String columnName, boolean isAsOfAttributeTo)
    {
        AttributeType ati = new AttributeType();
        ati.setColumnName(columnName);
        ati.setJavaType("Timestamp");
        ati.setName(asOfAttribute.getName() + prefix);
        ati.setNullable(isAsOfAttributeTo && asOfAttribute.isInfinityNull());
        ati.setPrimaryKey(false);
        ati.setPoolable(asOfAttribute.isPoolable());
        ati.setTimezoneConversion(asOfAttribute.getTimezoneConversion());
        ati.setProperties(new ArrayList());
        ati.getProperties().addAll(asOfAttribute.getProperty());
        ati.setFinalGetter(asOfAttribute.isFinalGetter());
        ati.setTimestampPrecision(asOfAttribute.getTimestampPrecision());
        Attribute attribute = new Attribute(ati, this);
        attribute.setIsAsOfAttributeTo(isAsOfAttributeTo);
        attribute.setIsAsOfAttributeFrom(!isAsOfAttributeTo);
        attribute.setSetAsString(asOfAttribute.isSetAsString());
        this.attributes.add(attribute);
        this.attributesByName.put(attribute.getName(), attribute);
    }

    protected void addAttribute(Attribute attribute)
    {
        this.attributes.add(attribute);

        if (attribute.isIdentity())
        {
            this.identityAttributeList.add(attribute);
        }

        if (attribute.isPrimaryKey())
        {
            this.primaryKeyAttributes.add(attribute);
        }
        if (attribute.isNullable() && attribute.isPrimitive())
        {
            attribute.setOnHeapNullableIndex(this.nullablePrimitiveAttributes.size());
            this.nullablePrimitiveAttributes.add(attribute);
            if (attribute.isMutablePrimaryKey())
            {
                attribute.setOnHeapMutablePkNullableIndex(this.nullablePrimitiveMutablePkAttributes.size());
                this.nullablePrimitiveMutablePkAttributes.add(attribute);
            }
        }
        this.attributesByName.put(attribute.getName(), attribute);
        this.addToRequiredClasses(attribute.getTypeAsString());
        if (attribute.isUsedForOptimisticLocking())
        {
            this.optimisticLockAttribute = attribute;
        }
    }

    public boolean hasSourceAttribute()
    {
        return this.sourceAttribute != null;
    }

    public List<Index> getPkAndUniqueIndices()
    {
        List result = new ArrayList();
        result.add(pkIndex);
        for(int i=0;i<this.indices.size();i++)
        {
            Index index = this.indices.get(i);
            if (index.isUnique())
            {
                result.add(index);
                if(index.getFindByParameters().equals(pkIndex.getFindByParameters()))
                {
                    index.setIsSameAsPk(true);
                    index.setSameIndex(this.pkIndex);
                }
            }
        }
        return result;
    }

    public List<Index> getPkAndAllIndices()
    {
        List result = new ArrayList();
        result.add(pkIndex);
        for(int i=0;i<this.indices.size();i++)
        {
            Index index = this.indices.get(i);
            result.add(index);
        }
        return result;
    }

    public List<String> resolveIndices()
    {
        Attribute[] pkIndexAttributes = new Attribute[this.primaryKeyAttributes.size() + (this.hasSourceAttribute() ? 1 : 0)];
        for (int i = 0; i < this.primaryKeyAttributes.size(); i++)
        {
            pkIndexAttributes[i] = this.primaryKeyAttributes.get(i);
        }
        if (this.hasSourceAttribute()) pkIndexAttributes[pkIndexAttributes.length - 1] = this.getSourceAttribute();
        this.pkIndex = new Index(pkIndexAttributes, "primaryKey", true, this);
        this.pkIndex.setPkStatus(true);

        List<String> errors = new ArrayList<String>();
        this.indices = new ArrayList<Index>();
        for (int i = 0; i < this.getWrapped().getIndexes().size(); i++)
        {
            IndexType indexType = (IndexType) this.getWrapped().getIndexes().get(i);
            Index index = new Index(indexType, this);
            index.checkConsistency(errors);

            if(index.isRedundantIndex(this.primaryKeyAttributes))
            {
                index.setIsSameAsPk(true);
                index.setSameIndex(this.pkIndex);
            }
            else
            {
                for (Index idx : this.indices)
                {
                    if(index.getName().equals(idx.getName()))
                    {
                        errors.add(index.getName() + " already exists in " + this.getSourceFileName());
                    }
                    List<AbstractAttribute> indexAttributes = new ArrayList<AbstractAttribute>();
                    for (int j = 0; j < idx.getAttributes().length; j++)
                    {
                        indexAttributes.add(idx.getAttributes()[j]);
                    }
                    if (index.isRedundantIndex(indexAttributes))
                    {
                        index.setSameIndex(idx);
                    }
                }
            }
            this.indices.add(index);
        }
        return errors;
    }

    public String getPkIndexColumns()
    {
        Attribute[] pkAttributes = this.getPrimaryKeyAttributes();
        String result = pkAttributes[0].getColumnName();
        for (int i = 1; i < pkAttributes.length; i++)
        {
            if (!pkAttributes[i].isSourceAttribute())
            {
                result += " , " + pkAttributes[i].getColumnName();
            }
        }
        result += getAsOfAttributeIndexColumns();
        return result;
    }

    public String getAsOfAttributeIndexColumns()
    {
        String result = "";
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            result += " , " + asOfAttributes[i].getToColumnName();
        }
        return result;
    }

    public void addIndex(List attributes, MithraObjectTypeWrapper relatedObject)
    {
        List noAsOfAttributesList = new ArrayList(attributes);
        for (Iterator it = noAsOfAttributesList.iterator(); it.hasNext();)
        {
            AbstractAttribute attribute = (AbstractAttribute) it.next();
            if (attribute.isAsOfAttribute()) it.remove();
        }
        if (this.pkIndex.isRedundantIndex(noAsOfAttributesList)) return;
        for (int i = 0; i < this.indices.size(); i++)
        {
            Index index = this.indices.get(i);
            if (index.isRedundantIndex(noAsOfAttributesList)) return;
        }
        if (this.isImported() && !relatedObject.isImported())
        {
            logger.warn("Could not add relationship index to imported object "+this.getClassName()+" from "+relatedObject.getClassName());
        }
        if (noAsOfAttributesList.size() > 0)
        {
            Attribute[] copy = new Attribute[noAsOfAttributesList.size()];
            noAsOfAttributesList.toArray(copy);
            Arrays.sort(copy);
            this.indices.add(new Index(copy, this.indices.size() + " Index", false, this));
        }
    }

    public Logger getLogger()
    {
        return logger;
    }

    public List<Index> getIndices()
    {
        return this.indices;
    }

    public ArrayList<Index> getPrefixFreeIndices()
    {
        ArrayList<Index> mustIncludeIndices = new ArrayList<Index>();
        ArrayList<String> indiciesSoFar = new ArrayList<String>();

        if (this.primaryKeyAttributes.size() > 0)
        {
            this.hasPrimaryKey = true;

            Attribute[] pkAttributes = new Attribute[this.primaryKeyAttributes.size()];

            for (int i = 0; i < this.primaryKeyAttributes.size(); i++)
            {
                pkAttributes[i] = this.primaryKeyAttributes.get(i);
            }

            Attribute[] pkAndAsOfAttributes = getWithAsOfAttributes(pkAttributes);

            Index index = new Index(pkAndAsOfAttributes, getCaseCorrectIndexPrefix(getDefaultTable(), "pk"), true, this);
            index.setPkStatus(true);
            mustIncludeIndices.add(index);
            indiciesSoFar.add(index.getIndexColumns());
        }

        for (int i = 0; i < this.indices.size(); i++)
        {
            Index index = this.indices.get(i);

            if (this.hasAsOfAttributes())
            {
                index = new Index(getWithAsOfAttributes(getWithAsOfAttributes((Attribute[]) index.getAttributes())), "", index.isUnique(), this);
            }

            if (index.isUnique() && !indiciesSoFar.contains(index.getIndexColumns()))
            {
                index.setName(getCaseCorrectIndexPrefix(getDefaultTable(), "idx", mustIncludeIndices.size() - (this.hasPrimaryKey ? 1 : 0)));
                mustIncludeIndices.add(index);
            }
        }
        return this.getPrefixFreeList(mustIncludeIndices, indices);
    }

    private Attribute[] getWithAsOfAttributes(Attribute[] original)
    {
        if (this.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = getAsOfAttributes();
            ArrayList<AsOfAttribute> asOfToAdd = new ArrayList<AsOfAttribute>(asOfAttributes.length);

            /* What are the AsOfAttributes to add */
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                asOfToAdd.add(asOfAttributes[i]);
            }

            /* Check if AsOfAttributes are already in the Attribute[] array */
            for (int i = 0; i < original.length; i++)
            {
                for (int j = 0; j < asOfToAdd.size(); j++)
                {
                    if (!original[i].getClass().equals(Attribute.class))
                    {
                        continue;
                    }

                    if ((asOfToAdd.get(j)).getToColumnName().equals((original[i]).getColumnName()))
                    {
                        asOfToAdd.remove(j);
                    }
                }
            }
            Attribute[] newAttributes = new Attribute[original.length + asOfToAdd.size()];

            System.arraycopy(original, 0, newAttributes, 0, original.length);

            for (int i = 0; i < asOfToAdd.size(); i++)
            {
                newAttributes[i + original.length] = new Attribute(this, new AttributeType());
                newAttributes[i + original.length].setColumnName((asOfToAdd.get(i)).getToColumnName());
            }
            return newAttributes;
        }
        else
        {
            return original;
        }
    }

    private String getCaseCorrectIndexPrefix(String tableName, String prefix, int i)
    {
        String s;

        if (getDefaultTable().toUpperCase().equals(tableName))
        {
            s = getDefaultTable() + "_" + prefix.toUpperCase() + i;
        }
        else
        {
            s = getDefaultTable() + "_" + prefix + i;
        }
        return s;
    }

    private String getCaseCorrectIndexPrefix(String tableName, String prefix)
    {
        String s;

        if (getDefaultTable().toUpperCase().equals(tableName))
        {
            s = getDefaultTable() + "_" + prefix.toUpperCase();
        }
        else
        {
            s = getDefaultTable() + "_" + prefix;
        }
        return s;
    }

    public void addToRequiredClasses(String fullClassName)
    {
        if (!primitiveTypes.contains(fullClassName) && !javaTypes.contains(fullClassName))
        {
            this.imports.add(fullClassName);
        }
    }

    public void addToRequiredClasses(String packageName, String className)
    {
        if (!this.getPackageName().equals(packageName))
        {
            this.imports.add(packageName + "." + className);
        }
    }

    private void addImports()
    {
        this.imports.add("java.util.*");
        this.imports.add("java.sql.Timestamp");
        this.imports.add("java.math.BigDecimal");
        List<String> importPackageNames = this.getExtraImports();
        if (importPackageNames != null && !importPackageNames.isEmpty())
        {
            for (String importPackageName : importPackageNames)
            this.imports.add(importPackageName);
        }
    }

    public String getDataClassInterface()
    {
        return "MithraDataObject";
    }

    public List<String> resolveAttributes(Map<String, MithraObjectTypeWrapper> allObjects) throws JavaTypeException
    {
        ArrayList<String> errors = new ArrayList<String>();
        this.extractAttributes(errors);
        this.extractRelationshipAttributes(allObjects, errors);
        //this.extractTransactionalMethodSignatures(errors);
        return errors;
    }

    public TransactionalMethodSignature[] getTransactionalMethodSignatures()
    {
        return this.transactionalMethodSignatures;
    }

    public TransactionalMethodSignature[] getDatedTransactionalMethodSignatures()
    {
        return this.datedTransactionalMethodSignatures;
    }

    private void extractTransactionalMethodSignatures(List<String> errors)
    {
        if (this.ignoreTransactionalMethods)
        {
            this.transactionalMethodSignatures = new TransactionalMethodSignature[0];
        }
        else
        {
            List incomingTransactionalMethodSignatures = this.getWrapped().getTransactionalMethodSignatures();
            this.transactionalMethodSignatures = new TransactionalMethodSignature[incomingTransactionalMethodSignatures.size()];
            for (int i = 0; i < incomingTransactionalMethodSignatures.size(); i++)
            {
                TransactionalMethodSignatureType type = (TransactionalMethodSignatureType) incomingTransactionalMethodSignatures.get(i);
                this.transactionalMethodSignatures[i] = new TransactionalMethodSignature(type);
            }
        }
        this.addAutoTransactionalMethods();
    }

    private void addAutoTransactionalMethods()
    {
        if (this.isTransactional())
        {
            List<TransactionalMethodSignature> transactionalMethods = new ArrayList<TransactionalMethodSignature>(Arrays.asList(this.transactionalMethodSignatures));
            this.transactionalMethodSignatures = new TransactionalMethodSignature[transactionalMethods.size()];
            transactionalMethods.toArray(this.transactionalMethodSignatures);

            if(this.hasBusinessDateAsOfAttribute())
            {
                this.datedTransactionalMethodSignatures = new TransactionalMethodSignature[0];
                List<TransactionalMethodSignature> datedTransactionalMethods = new ArrayList<TransactionalMethodSignature>(Arrays.asList(this.datedTransactionalMethodSignatures));
                this.datedTransactionalMethodSignatures = new TransactionalMethodSignature[datedTransactionalMethods.size()];
                datedTransactionalMethods.toArray(this.datedTransactionalMethodSignatures);
            }
        }
    }

    public List<String> checkAllNames(Map<String, MithraObjectTypeWrapper> allObjects)
    {
        List<String> errorMessages = new ArrayList<String>();
        if (!this.pure && !this.isTemporary())
        {
            if (this.isTablePerSubclassSuperClass() && this.getDefaultTable() != null)
            {
                errorMessages.add("DefaultTable not allowed in super class - found table name '" + this.getWrapped().getDefaultTable() + "'");
            }
            if (!this.isTablePerSubclassSuperClass() && this.getDefaultTable() == null)
            {
                errorMessages.add("DefaultTable not specified");
            }
        }
        if (!this.isTransactional() && this.getWrapped().getUpdateListener() != null)
        {
            errorMessages.add("UpdateListener can only be used with transactional objects");
        }
        this.checkClassAndPackageName(errorMessages);
        this.checkAttributes(errorMessages, allObjects);
        return errorMessages;
    }

    public List<String> resolveSuperClasses(Map<String, MithraObjectTypeWrapper> allObjects)
    {
        List<String> errorMessages = new ArrayList<String>();
        if (super.getSuperClass() != null && super.getSuperClass().isGenerated())
        {
            this.ensureAttributeConsistencyWithSuperClass(errorMessages, allObjects);
        }
        if (this.isTablePerSubclassSuperClass())
        {
            if (this.getWrapped().getIndexes() != null && this.getWrapped().getIndexes().size() > 0)
            {
                errorMessages.add("Indices are not allowed in super class");
            }
        }
        this.extractTransactionalMethodSignatures(errorMessages);
        return errorMessages;
    }

    public List resolveEmbeddedValueObjects(Map<String, MithraEmbeddedValueObjectTypeWrapper> allEmbeddedValueObjects, Map<String, MithraObjectTypeWrapper> mithraObjects)
    {
        List errorMessages = new ArrayList();
        this.extractEmbeddedValueObjects(errorMessages);
        if (this.embeddedValueObjects != null && !this.embeddedValueObjects.isEmpty())
        {
            for (EmbeddedValue embeddedValue : this.embeddedValueObjects)
            {
                //TODO: ledav validate optimistic locking
                embeddedValue.resolveTypes(allEmbeddedValueObjects, mithraObjects);
            }
        }
        return errorMessages;
    }

    private void extractEmbeddedValueObjects(List errors)
    {
        if (this.getWrapped().getEmbeddedValues() != null)
        {
            for (int i = 0; i < this.getWrapped().getEmbeddedValues().size(); i++)
            {
                EmbeddedValueType embeddedValueType = (EmbeddedValueType) this.getWrapped().getEmbeddedValues().get(i);
                EmbeddedValue embeddedValue = new EmbeddedValue(this, embeddedValueType);
                this.addEmbeddedValueObject(embeddedValue);
            }
        }
    }

    private void addEmbeddedValueObject(EmbeddedValue embeddedValue)
    {
        if (embeddedValue.isRoot() && !this.containsEmbeddedValueWithName(embeddedValue.getNestedName()))
        {
            this.embeddedValueObjects.add(embeddedValue);
            if (embeddedValue.getDescendants() != null)
            {
                this.embeddedValueObjects.addAll(Arrays.asList(embeddedValue.getDescendants()));
            }
        }
    }

    private boolean containsEmbeddedValueWithName(String name)
    {
        for (EmbeddedValue embeddedValue : this.embeddedValueObjects)
        {
            if (embeddedValue.getNestedName().equals(name))
            {
                return true;
            }
        }
        return false;
    }

    public List resolveMithraInterfaces(Map<String, MithraInterfaceType> mithraInterfaces)
    {
        List errorMessages = new ArrayList();
        this.extractMithraInterfaces(mithraInterfaces, errorMessages);
        return errorMessages;
    }

    //extractMithraInterfaces
    // For each finder interface validate the
    private void extractMithraInterfaces(Map<String, MithraInterfaceType> mithraInterfaces, List<String> errors)
    {
        for (String mithraInterfaceType : this.getWrapped().getMithraInterfaces())
        {
            MithraInterfaceType mithraInterfaceObject = mithraInterfaces.get(mithraInterfaceType);

            if (mithraInterfaceObject != null)
            {
                //boolean hasNoSuperClass  = validateObjectHasNoSuperClass(errors);
                boolean hasAllInterfaceAttributes = validateObjectHasAllMithraInterfaceAttributes(mithraInterfaceObject, errors);

                boolean hasAllInterfaceRelationships = validateObjectHasAllMithraInterfaceRelationships(mithraInterfaceObject, mithraInterfaces, errors);

                if (hasAllInterfaceAttributes && hasAllInterfaceRelationships)
                {
                    this.mithraInterfaces.add(mithraInterfaceObject);
                    this.addToRequiredClasses(mithraInterfaceObject.getPackageName(), mithraInterfaceObject.getClassName());
                    this.addToRequiredClasses(mithraInterfaceObject.getPackageName(), mithraInterfaceObject.getClassName() + "Finder");
                }
            }
        }
    }

    private boolean validateObjectHasAllMithraInterfaceRelationships(MithraInterfaceType mithraInterfaceObject, Map<String, MithraInterfaceType> mithraInterfaces, List<String> errors)
    {
        for (RelationshipInterfaceType relationshipInterfaceType : mithraInterfaceObject.getAllRelationships())
        {
            RelationshipAttribute localRelationshipAttribute = this.getRelationshipAttributeByName(relationshipInterfaceType.getName());

            if (!relationshipInterfaceType.validateRelationship(mithraInterfaces, localRelationshipAttribute, this.getClassName(), mithraInterfaceObject.getClassName(), errors))
            {
                return false;
            }

        }
        return true;
    }

    private boolean validateObjectHasAllMithraInterfaceAttributes(MithraInterfaceType mithraInterfaceObject, List<String> errors)
    {
        for (AttributeInterfaceType attributeInterfaceType : mithraInterfaceObject.getAllAttributes())
        {
            Attribute localAttribute = this.getAttributeByName(attributeInterfaceType.getName());
            if (localAttribute == null || !attributeInterfaceType.validAttribute(localAttribute, this.getClassName(), mithraInterfaceObject.getClassName(), errors))
            {
                return false;
            }
        }

        for (AsOfAttributeInterfaceType asOfAttribute : mithraInterfaceObject.getAsOfAttributesAsArray())
        {
            AsOfAttribute localAsOfAttribute = (AsOfAttribute) this.getAttributeByName(asOfAttribute.getName());

            if (localAsOfAttribute == null || !asOfAttribute.validAttribute(localAsOfAttribute, this.getClassName(), mithraInterfaceObject.getClassName(), errors))
            {
                return false;
            }

        }
        if (mithraInterfaceObject.hasSourceAttribute())
        {
            SourceAttributeInterfaceType sourceAttributeInterfaceType = mithraInterfaceObject.getSourceAttribute();

            SourceAttribute localSourceAttribute = (SourceAttribute) this.getAttributeByName(sourceAttributeInterfaceType.getName());

            if (localSourceAttribute == null || !sourceAttributeInterfaceType.validAttribute(localSourceAttribute, this.getClassName(), mithraInterfaceObject.getClassName(), errors))
            {
                return false;
            }
        }

        return true;
    }

    protected boolean hasMappingToColumn(String columnName)
    {
        for (int i = 0; i < this.attributes.size(); i++)
        {
            Attribute attribute = attributes.get(i);
            if (attribute.getColumnName().equals(columnName))
            {
                return true;
            }
        }
        return false;
    }

    public List<String> postValidate()
    {
        ArrayList<String> errorMessages = new ArrayList<String>();
        if (this.primaryKeyAttributes.size() == 0 && !this.isAbstractTablePerSubclass()) errorMessages.add("No primary key defined!");
        if (this.hasAsOfAttributes() && this.getObjectType().equals(BaseMithraGenerator.DATED_TRANSACTIONAL))
        {
            AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
            if (asOfAttributes.length > 2)
            {
                errorMessages.add("Cannot have more than two as of attributes in object " + this.getClassName());
            }
            else if (asOfAttributes.length > 1)
            {
                boolean hasProcessingDate = false;
                for (int i = 0; i < asOfAttributes.length; i++)
                {
                    if (asOfAttributes[i].isProcessingDate()) hasProcessingDate = true;
                }
                if (!hasProcessingDate)
                {
                    errorMessages.add("Must specify the processing date when there are two as of attributes. " +
                            "Please add 'isProcessingDate=\"true\" to the AsOfAttribute tag for the " +
                            "processing date in object " + this.getClassName());
                }
            }
        }
        if (enableOffHeap)
        {
            determineOffHeapCompatibility();
        }
        if (this.hasOffHeap())
        {
            assignOffHeapAttributeOffsets();
        }
        return errorMessages;
    }

    private void assignOffHeapAttributeOffsets()
    {
        AbstractAttribute[] normalAndInheritedAttributes = getNormalAndInheritedAttributes();
        Arrays.sort(normalAndInheritedAttributes, new Comparator<AbstractAttribute>() {
            @Override
            public int compare(AbstractAttribute o1, AbstractAttribute o2)
            {
                if (o1.isPrimaryKey() != o2.isPrimaryKey())
                {
                    if (o1.isPrimaryKey())
                    {
                        return -1;
                    }
                    else if (o2.isPrimaryKey())
                    {
                        return 1;
                    }
                }
                return o1.getName().compareTo(o2.getName());
            }
        });
        int currentNullBitsOffset = 0;
        int currentNullBitsPosition = 0;
        // note: source attributes never require a null bit
        if (hasAsOfAttributes())
        {
            currentNullBitsOffset += 4; // for dataVersion, only need 1, but let's keep things even
        }
        //set the null bit positions
        for(AbstractAttribute attr: normalAndInheritedAttributes)
        {
            if (attr.isNullablePrimitive() && !attr.getType().isBoolean())
            {
                attr.setOffHeapNullBitsOffset(currentNullBitsOffset);
                attr.setOffHeapNullBitsPosition(currentNullBitsPosition);
                currentNullBitsPosition++;
                if (currentNullBitsPosition == 32)
                {
                    currentNullBitsOffset += 4; // null bits are ints
                    currentNullBitsPosition = 0;
                }
            }
        }
        int currentOffset = currentNullBitsPosition == 0 ? currentNullBitsOffset : currentNullBitsOffset + 4;
        if (this.hasSourceAttribute())
        {
            this.getSourceAttribute().setOffHeapFieldOffset(currentOffset);
            currentOffset += getSourceAttribute().getType().getOffHeapSize();
        }
        for(AbstractAttribute attr: normalAndInheritedAttributes)
        {
            attr.setOffHeapFieldOffset(currentOffset);
            currentOffset += attr.getType().getOffHeapSize();
        }
        this.offHeapSize = currentOffset;
        if ((this.offHeapSize & 1) == 1)
        {
            this.offHeapSize++;
        }
    }

    private void determineOffHeapCompatibility()
    {
        this.isOffHeapCompatible = this.hasAsOfAttributes() && !this.hasBigDecimalAttribute()
                && !this.hasEmbeddedValueObjects() && !this.hasByteArrayAttribute();
    }

    private boolean hasByteArrayAttribute()
    {
        AbstractAttribute[] allAttributes = this.getNormalAndInheritedAttributes();
        for(AbstractAttribute attribute: allAttributes)
        {
            if (attribute.getType().isArray())
            {
                return true;
            }
        }
        return false;
    }

    private boolean hasBigDecimalAttribute()
    {
        AbstractAttribute[] allAttributes = this.getNormalAndInheritedAttributes();
        for(AbstractAttribute attribute: allAttributes)
        {
            if (attribute.getType().isBigDecimal())
            {
                return true;
            }
        }
        return false;
    }

    private void ensureAttributeConsistencyWithSuperClass(List<String> errorMessages, Map<String, MithraObjectTypeWrapper> allObjects)
    {
        SuperClassAttributeType superClassType = super.getSuperClass();
        super.setSuperClassWrapper(allObjects.get(superClassType.getName()));
        if (this.getSuperClassWrapper() == null)
        {
            errorMessages.add("SuperClass name '" + superClassType.getName() + "'" + "not defined. Have you added the xml file for this class to the Mithra class list?");
        }
        else
        {
            if (!this.getSuperClassWrapper().getPackageName().equals(this.getPackageName()))
            {
                this.addToRequiredClasses(this.getSuperClassWrapper().getPackageName(), "*");
            }
            this.getSuperClassWrapper().addChildClass(this);
            if (this.isTablePerClassSubClass())
            {
                this.setUniqueAlias(this.getSuperClassWrapper().assignUniqueAlias());
            }
            if (this.getSourceAttribute() != null && this.getSuperClassWrapper().getSourceAttribute() != null)
            {
                errorMessages.add("source attributes are not allowed to be overridden in "+
                        this.getClassName()+"."+this.getSourceAttribute().getName()+" overriding "+
                        this.getSuperClassWrapper().getClassName()+"."+this.getSuperClassWrapper().getSourceAttribute().getName());
            }
            else
            {
                if (this.getSuperClassWrapper().getSourceAttribute() != null)
                {
                    this.sourceAttribute = (SourceAttribute) this.getSuperClassWrapper().getSourceAttribute().cloneForNewOwner(this);
                    this.attributesByName.put(this.sourceAttribute.getName(), sourceAttribute);
                }
            }
            Attribute[] superObjectAttributes = this.getSuperClassWrapper().getAttributes();
            for (int i = 0; i < superObjectAttributes.length; i++)
            {
                Attribute superClassAttribute = superObjectAttributes[i];
                Attribute attribute = this.getAttributeByName(superClassAttribute.getName());
                if (attribute != null)
                {
                    List<String> errorsIfAny = attribute.validateAndUseMissingValuesFromSuperClass(superClassAttribute);
                    errorMessages.addAll(errorsIfAny);
                }
                else if (!this.isTablePerClassSubClass())
                {
                    this.addAttribute(superClassAttribute.cloneForNewOwner(this));
                }
                else
                {
                    this.addInheritedAttributes(superClassAttribute);
                }
            }
            superObjectAttributes = this.getSuperClassWrapper().getInheritedAttributes();
            for (int i = 0; i < superObjectAttributes.length; i++)
            {
                Attribute superClassAttribute = superObjectAttributes[i];
                Attribute attribute = this.getAttributeByName(superClassAttribute.getName());
                if (attribute != null)
                {
                    List<String> errorsIfAny = attribute.validateAndUseMissingValuesFromSuperClass(superClassAttribute);
                    errorMessages.addAll(errorsIfAny);
                }
                else
                {
                    this.addInheritedAttributes(superClassAttribute);
                }
            }
            this.initializeNullBitHolders();
            EmbeddedValue[] superEmbeddedValues = this.getSuperClassWrapper().getEmbeddedValueObjects();
            for (EmbeddedValue embeddedValue : superEmbeddedValues)
            {
                this.addEmbeddedValueObject(embeddedValue);
            }
            if (this.getSuperClassWrapper().hasAsOfAttributes())
            {
                AsOfAttribute[] superAsOfAttributes = this.getSuperClassWrapper().getAsOfAttributes();
                for (int i = 0; i < superAsOfAttributes.length; i++)
                {
                    AsOfAttribute superClassAttribute = superAsOfAttributes[i];
                    Attribute attribute = this.getAttributeByName(superClassAttribute.getName());
                    if (attribute != null)
                    {
                        List<String> errorsIfAny = attribute.validateAndUseMissingValuesFromSuperClass(superClassAttribute);
                        errorMessages.addAll(errorsIfAny);
                    }
                    else
                    {
                        Attribute clonedAttribute = superClassAttribute.cloneForNewOwner(this);
                        this.asOfAttributes.add((AsOfAttribute) clonedAttribute);
                        if (this.isTablePerClassSubClass())
                        {
                            this.addAsOfAttribute((AsOfAttribute) clonedAttribute);
                        }
                        else
                        {
                            attributesByName.put(clonedAttribute.getName(), clonedAttribute);
                        }
                    }
                }
                this.checkProcessingDateOnAsOfAttributes(errorMessages);
            }
            RelationshipAttribute[] relationshipAttributes = this.getSuperClassWrapper().getRelationshipAttributesWithoutPosition();
            for (int i = 0; i < relationshipAttributes.length; i++)
            {
                RelationshipAttribute superClassRelationshipAttribute = relationshipAttributes[i];
                RelationshipAttribute relationshipAttribute = this.getRelationshipAttributeByName(superClassRelationshipAttribute.getName());
                if (relationshipAttribute != null)
                {
                    List<String> errorsIfAny = relationshipAttribute.validateAndUseMissingValuesFromSuperClass(superClassRelationshipAttribute);
                    errorMessages.addAll(errorsIfAny);
                }
                else
                {
                    RelationshipAttribute newRelationship = new RelationshipAttribute(this,
                            superClassRelationshipAttribute.getRelatedObject(),
                            superClassRelationshipAttribute.getXmlRelationshipType());
                    newRelationship.setInhereted(true);
                    this.addRelationship(newRelationship);
                }
            }
        }
    }

    private void checkProcessingDateOnAsOfAttributes(List<String> errorMessages)
    {
        if (this.asOfAttributes.size() > 2 && this.isTransactional())
        {
            errorMessages.add("cannot have more than 2 as of attributes with a transactional object.");
            return;
        }
        if (this.asOfAttributes.size() == 2)
        {
            int processingDateCount = 0;
            AsOfAttribute candidateProcessingDate = null;
            for (int i = 0; i < this.asOfAttributes.size(); i++)
            {
                AsOfAttribute asOfAttribute = asOfAttributes.get(i);
                if (asOfAttribute.isProcessingDate())
                {
                    processingDateCount++;
                }
                else if (asOfAttribute.getName().equals("processingDate"))
                {
                    candidateProcessingDate = asOfAttribute;
                }
            }
            if (processingDateCount == 0 && candidateProcessingDate != null)
            {
                candidateProcessingDate.setProcessingDate(true);
            }
        }
        else if (this.asOfAttributes.size() == 1)
        {
            AsOfAttribute asOfAttribute = asOfAttributes.get(0);
            if (asOfAttribute.getName().equals("processingDate") && !asOfAttribute.isProcessingDateSet())
            {
                System.out.println("There is a single as of attribute in " + this.getClassName() + " but the 'isProcessingDate' value is not set. Please set it accordingly");
            }
        }
    }

    private void checkBooleanAttributeName(AttributePureType attribute)
    {
        // warn the user that the boolean name should not contain "is" in the XML.
        String name = attribute.getName();

        if (attribute.getJavaType().equals("boolean") && name.length() >= 3 &&
                attribute.getName().substring(0, 2).equals("is") && Character.isUpperCase(name.charAt(2)))
        {
            System.out.println("Warning: boolean variable " + name + " should not have prefix \"is\" in " +
                    this.getSourceFileName() + "\n" +
                    "Please consider changing the variable name to \"" +
                    StringUtility.firstLetterToLower(name.substring(2, name.length())) + "\""
            );
        }
    }

    private void checkAttributes(List<String> errorMessages, Map<String, MithraObjectTypeWrapper> allObjects)
    {
        Set<String> attributes = new HashSet<String>();
        Set<String> columns = new HashSet<String>();
        for (int i = 0; i < this.getWrapped().getAttributes().size(); i++)
        {
            AttributePureType attribute = ((AttributePureType) this.getWrapped().getAttributes().get(i));
            this.checkBooleanAttributeName(attribute);

            this.checkAttributeName(attributes, attribute.getName(), errorMessages);
            if (this.isTemporary())
            {
                this.checkStringAttributeLength(attribute, errorMessages);
                this.assignColumnName(attribute, columns, errorMessages);
            }
            else
            {
                this.checkAttributeColumnName(columns, attribute.getColumnName(), errorMessages);
            }

            if (!primitiveTypes.contains(attribute.getJavaType()) && !javaTypes.contains(attribute.getJavaType()))
            {
                errorMessages.add("Attribute '" + attribute.getName() + "' has invalid type '" + attribute.getJavaType() + "' : valid types are " + primitiveTypes + " or " + javaTypes);
            }
            if (!primitiveTypes.contains(attribute.getJavaType()))
            {
                if (attribute.getDefaultIfNull() != null)
                {
                    errorMessages.add("Attribute '" + attribute.getName() + "' must not set defaultIfNull. defaultIfNull is only supported for primitive attributes");
                }
            }
        }

        for (int i = 0; i < this.getWrapped().getRelationships().size(); i++)
        {
            RelationshipType relationshipType = ((RelationshipType) this.getWrapped().getRelationships().get(i));
            this.checkAttributeName(attributes, relationshipType.getName(), errorMessages);
            if (this.isTablePerSubclassSuperClass() && relationshipType.isReverseRelationshipNameSet())
            {
                errorMessages.add("Reverse relationship not allowed in super class - found realtionship '" + relationshipType.getReverseRelationshipName() + "'");
            }
            if (!allObjects.containsKey(relationshipType.getRelatedObject()))
            {
                errorMessages.add("Class name '" + relationshipType.getRelatedObject() + "' not defined. Have you added the xml file for this class to the Mithra class list?");
            }
        }
    }

    private void assignColumnName(AttributePureType attribute, Set<String> columns, List<String> errorMessages)
    {
        int count = 0;
        String firstLetter = attribute.getName().substring(0, 1).toUpperCase();
        String columnName = firstLetter +count;
        while(columns.contains(columnName))
        {
            columnName = firstLetter + (++count);
        }
        columns.add(columnName);
        attribute.setColumnName(columnName);
    }

    private void checkStringAttributeLength(AttributePureType attribute, List<String> errorMessages)
    {
        if (attribute.getJavaType().equalsIgnoreCase("string") && !attribute.isMaxLengthSet())
        {
            errorMessages.add("For temp objects maxLength must be specified for string attribute: '"+attribute.getName()+"'");
        }
    }

    public List<String> checkAttributeNamesInIndices()
    {
        List<String> errorMessages = new ArrayList<String>();
        for (int i = 0; i < this.getWrapped().getIndexes().size(); i++)
        {
            IndexType indexType = (IndexType) this.getWrapped().getIndexes().get(i);
            String[] attributeNames = indexType.value().split(", *");
            HashSet<Attribute> foundAttributes = new HashSet<Attribute>(attributeNames.length * 2);
            for (int j = 0; j < attributeNames.length; j++)
            {
                Attribute attribute = this.getAttributeByName(attributeNames[j].trim());
                if (attribute == null)
                {
                    errorMessages.add("Index '" + indexType.getName() + "': Attribute '" + attributeNames[j] + "' does not exist");
                }
                else if (attribute.isAsOfAttribute() || attribute.isAsOfAttributeFrom() || attribute.isAsOfAttributeTo())
                {
                    errorMessages.add("Index '" + indexType.getName() + "' is invalid. AsOfAttributes or part of AsOfAttributes are not allowed in an Index for dated objects: " + attributeNames[j]);
                }
                else if (foundAttributes.contains(attribute))
                {
                    errorMessages.add("Index '" + indexType.getName() + "': Attribute '" + attributeNames[j] + "' is duplicated");
                }
                else
                {
                    foundAttributes.add(attribute);
                }
            }
        }
        return errorMessages;
    }

    public Map<String, List<String>> checkRelationships(Map<String, MithraObjectTypeWrapper> allObjects)
    {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        for (int i = 0; i < relationshipAttributes.size(); i++)
        {
            RelationshipAttribute relationshipAttribute = relationshipAttributes.get(i);
            if (!relationshipAttribute.isReverseRelationship())
            {
                List<String> errorMessages = new ArrayList<String>();
                if (!hasRelatedObjectInPredicate(relationshipAttribute))
                {
                    errorMessages.add("relationship query does not have any relational expression involving the related object: " + relationshipAttribute.getRelatedObject().getClassName());
                }
                if (!hasThisInPredicate(relationshipAttribute))
                {
                    errorMessages.add("relationship query does not have any relational expression involving 'this'");
                }

                String query = relationshipAttribute.getQuery();
                if (errorMessages.isEmpty())
                {
                    MithraQL mithraQL = new MithraQL(new StringReader(query));

                    try
                    {
                        ASTCompilationUnit compilationUnit = mithraQL.CompilationUnit();
                        compilationUnit.childrenAccept(new AttributeValidationVisitor(allObjects, errorMessages, this), null);
                        if (compilationUnit.jjtGetNumChildren() > 1)
                        {
                            errorMessages.add("can't handle more than one expression");
                            compilationUnit.dump("\t");
                        }
                        else
                        {
                            relationshipAttribute.setParsedQueryNode((SimpleNode) compilationUnit.jjtGetChild(0));
                            mithraQL = new MithraQL(new StringReader(relationshipAttribute.getReverseQuery()));
                            compilationUnit = mithraQL.CompilationUnit();
                            compilationUnit.childrenAccept(new AttributeValidationVisitor(allObjects, errorMessages, relationshipAttribute.getRelatedObject()), null);
                            relationshipAttribute.setReverseParsedQuery((SimpleNode) compilationUnit.jjtGetChild(0));
                            relationshipAttribute.resolveOwningRelationships();

                            relationshipAttribute.addIndicies();
                            relationshipAttribute.addImports(this, allObjects, errorMessages);
                            if (relationshipAttribute.isBidirectional())
                            {
                                MithraObjectTypeWrapper relatedObject = relationshipAttribute.getRelatedObject();
                                if (!this.isTablePerClassSubClass())
                                {
                                    addReverseRelationship(allObjects, relationshipAttribute, errorMessages, compilationUnit, relatedObject);
                                }
                                else
                                {
                                    RelationshipAttribute exisitingRelationship = relatedObject.getRelationshipAttributeByName(relationshipAttribute.getName());
                                    if (!relationshipAttribute.isInhereted() && exisitingRelationship == null)
                                    {
                                        addReverseRelationship(allObjects, relationshipAttribute, errorMessages, compilationUnit, relatedObject);
                                    }
                                }
                            }
                        }
                    }
                    catch (Throwable e)
                    {
                        errorMessages.add(e.getClass().getName() + " : " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                checkAndExtractOrderByForRelationship(relationshipAttribute, errorMessages);

                if (errorMessages.size() > 0)
                {
                    result.put(relationshipAttribute.getName(), errorMessages);
                }
                if (relationshipAttribute.isRelatedDependent())
                {
                    relationshipAttribute.getRelatedObject().addLifeCycleParent(this);
                }
            }
        }
        for (int i = 0; i < relationshipAttributes.size(); i++)
        {
            RelationshipAttribute relationshipAttribute = relationshipAttributes.get(i);
            relationshipAttribute.warnAboutUniqueness();
        }
        return result;
    }

    private void addReverseRelationship(Map<String, MithraObjectTypeWrapper> allObjects, RelationshipAttribute relationshipAttribute, List<String> errorMessages, ASTCompilationUnit compilationUnit, MithraObjectTypeWrapper relatedObject)
    {
        if (relatedObject.getImportedSource() == null || (relatedObject.getImportedSource() != null && relatedObject.getImportedSource().equals(super.getImportedSource())))
        {
            relatedObject.addRelationship(relationshipAttribute.getReverseRelationshipAttribute());
            RelationshipAttribute reverseRelationshipAttribute =
                    relatedObject.getRelationshipAttributeByName(relationshipAttribute.getReverseName());
            reverseRelationshipAttribute.setParsedQueryNode((SimpleNode) compilationUnit.jjtGetChild(0));

            // Ensure that the related object has the necessary imports to look up this object
            reverseRelationshipAttribute.addImports(relatedObject, allObjects, errorMessages);
        }
        else
        {
            errorMessages.add("can't add reverse relationship to imported object '" + relatedObject.getClassName() + "'");
        }
    }

    private boolean hasRelatedObjectInPredicate(RelationshipAttribute relationshipAttribute)
    {
        String relatedObjectPrefix = relationshipAttribute.getRelatedObject().getClassName() + ".";
        return containsWholeWord(relationshipAttribute.getQuery(), relatedObjectPrefix);
    }

    private boolean hasThisInPredicate(RelationshipAttribute relationshipAttribute)
    {
        return containsWholeWord(relationshipAttribute.getQuery(), "this.");
    }

    private boolean containsWholeWord(String query, String relatedObjectPrefix)
    {
        query = query.replace('\t',' ');
        int index = query.indexOf(relatedObjectPrefix);
        if (index == 0) return true;
        while (index > 0)
        {
            char c = query.charAt(index - 1);
            if (Character.isWhitespace(c) || c == '(' || c == '=')
            {
                return true;
            }
            index = query.indexOf(relatedObjectPrefix, index+1);
        }
        return false;
    }

    private void checkClassAndPackageName(List<String> errorMessages)
    {
        String packageName = this.getPackageName();
        if (!ignorePackageNamingConvention && !packageName.equals(packageName.toLowerCase()))
        {
            errorMessages.add("Package name '" + packageName + "' should be in lowercase");
        }
        String className = this.getClassName();
        char first = className.charAt(0);
        if (!Character.isLetter(first))
        {
            errorMessages.add("Class name '" + className + "' should begin with a letter");
        }
        else if (!Character.isUpperCase(first))
        {
            errorMessages.add("Class name '" + className + "' should begin with capital letter");
        }
        else if (!isJavaIdentifierString(className))
        {
            errorMessages.add("Class name '" + className + "' has character(s) that are not permitted");
        }
    }

    private void checkAttributeName(Set<String> attributes, String name, List<String> errorMessages)
    {
        char first = name.charAt(0);
        if (!Character.isLetter(first))
        {
            errorMessages.add("Attribute name '" + name + "' should begin with a letter");
        }
        else if (!Character.isLowerCase(first))
        {
            errorMessages.add("Attribute name '" + name + "' should begin with a lowercase letter");
        }
        else if (!isJavaIdentifierString(name))
        {
            errorMessages.add("Attribute name '" + name + "' has character(s) that are not permitted");
        }
        else if (attributes.contains(name))
        {
            errorMessages.add("Attribute name '" + name + "' is used more than once");
        }
        else
        {
            attributes.add(name);
        }
    }

    private void checkAttributeColumnName(Set<String> columns, String columnName, List<String> errorMessages)
    {
        if (this.pure) return;
        if (columns.contains(columnName))
        {
            errorMessages.add("Column name '" + columnName + "' is used more than once");
        }
        else
        {
            columns.add(columnName);
        }
    }

    private boolean isJavaIdentifierString(String str)
    {
        for (int i = 0; i < str.length(); i++)
        {
            char chr = str.charAt(i);
            if (!Character.isJavaIdentifierPart(chr) || chr == '$' || chr == '_')
            {
                return false;
            }
        }
        return true;
    }

    public String getPortalClassForTxRuntime()
    {
        if (this.isReadOnly())
        {
            return "MithraReadOnlyPortal";
        }
        else return "MithraTransactionalPortal";
    }

    public String getPortalClassForNoTxRuntime()
    {
        return "MithraReadOnlyPortal";
    }

    public String getLocalObjectPersister()
    {
        if (this.isPure())
        {
            return "new PureMithraObjectPersister(getFinderInstance())";
        }
        else if (this.isReadOnly())
        {
            return "(MithraObjectReader) objectFactory";
        }
        return "(MithraObjectPersister) objectFactory";
    }

    public String getRemoteObjectPersister()
    {
        return "new RemoteMithraObjectPersister(config.getRemoteMithraService(), getFinderInstance(), "+this.hasAsOfAttributes()+")";
    }

    public String getPrimaryKeyWhereSql()
    {
        String result = "";
        for (int i = 0; i < this.primaryKeyAttributes.size(); i++)
        {
            Attribute attribute = this.primaryKeyAttributes.get(i);
            if (i > 0) result = result + " AND ";
            result += attribute.getColumnName() + " = ?";
        }
        return result;
    }

    public String getPrimaryKeyWhereSqlWithAlias()
    {
        String result = "";
        for (int i = 0; i < this.primaryKeyAttributes.size(); i++)
        {
            Attribute attribute = this.primaryKeyAttributes.get(i);
            if (i > 0) result = result + " AND ";
            result += "t0."+attribute.getColumnName() + " = ?";
        }
        return result;
    }

    public String getPrimaryKeyWithOptimisticLockWhereSql()
    {
        String result = this.getPrimaryKeyWhereSql();
        result += " AND "+this.optimisticLockAttribute.getColumnName()+" = ?";
        return result;
    }

    public String getPrimaryKeyWithOptimisticLockWhereSqlWithAlias()
    {
        String result = this.getPrimaryKeyWhereSqlWithAlias();
        result += " AND t0."+this.optimisticLockAttribute.getColumnName()+" = ?";
        return result;
    }

    public String getPrimaryKeyWithAsOfToAttributeWhereSql()
    {
        String result = this.getPrimaryKeyWhereSql();
        if (this.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                result = result + " AND ";
                result += asOfAttributes[i].getToColumnName() + " = ?";
            }
        }
        return result;
    }

    public String getAsOfAttributeWhereSql()
    {
        String result = "";
        if (this.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
            result = result + "String result = \"\";\n";
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                result = result + "result += \" AND \";\n";
                if (asOfAttributes[i].isInfinityNull())
                {
                    result += "if ((("+this.getDataClassName()+")data).get"+asOfAttributes[i].getName().substring(0,1).toUpperCase()+asOfAttributes[i].getName().substring(1,asOfAttributes[i].getName().length())+"To() == null)\n";
                    result += "{\n";
                    result += "result +=\""+asOfAttributes[i].getToColumnName()+" is null\";\n";
                    result += "} else {\n";
                    result += "result +=\""+asOfAttributes[i].getToColumnName()+" = ?\";\n";
                    result += "}\n";
                }
                else
                {
                    result += "result +=\""+asOfAttributes[i].getToColumnName()+" = ?\";\n";
                }
            }
        }
        result += "return result;";
        return result;
    }

    public String getInsertFields()
    {
        Attribute[] attrs = this.getAttributesIncludingInheritedPks();
        String result = "";
        for (int i = 0; i < attrs.length; i++)
        {
            if (!attrs[i].isIdentity())
            {
                if (result.length() > 0) result += ",";
                result += attrs[i].getColumnName();
            }
        }
        return result;
    }

    public String getInsertQuestionMarks()
    {
        Attribute[] attrs = this.getAttributesIncludingInheritedPks();
        String result = "";
        for (int i = 0; i < attrs.length; i++)
        {
            if (!attrs[i].isIdentity())
            {
                if (result.length() > 0) result += ",";
                result += "?";
            }
        }
        return result;
    }

    public int getTotalColumnsInResultSet()
    {
        String columns = this.getColumnListWithDefaultAlias();
        int count = 1;
        for(int i=0;i<columns.length();i++)
        {
            if (columns.charAt(i) == ',') count++;
        }
        return count;
    }

    public Attribute getIdentityAttribute()
    {
        if (this.identityAttributeList.size()>0)
        {
            return this.identityAttributeList.get(0);
        }
        else
        {
            return null;
        }
    }

    public int getIdentityCount()
    {
        return this.identityAttributeList.size();
    }

    public int getTotalColumnsInInsert()
    {
        int identityColumnCount = this.getIdentityCount();
        int result = this.attributes.size();
        if (this.inheritedAttributes != null)
        {
            for(int i=0;i<this.inheritedAttributes.size();i++)
            {
                Attribute a = this.inheritedAttributes.get(i);
                if (a.isPrimaryKey() && !a.isIdentity()) result++;
            }
        }
        return result-identityColumnCount;
    }

    public String getPkColumnListWithDefaultAlias()
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < this.primaryKeyAttributes.size(); i++)
        {
            Attribute attribute = this.primaryKeyAttributes.get(i);
            sb.append("t0.").append(attribute.getColumnName()).append(",");
        }
        return StringUtility.removeLastCharacter(sb.toString());
    }

    public String getDatabaseObjectSuperClass()
    {
        if (this.isTemporary())
        {
            if (this.hasSourceAttribute())
            {
                return "MithraAbstractTempObjectDatabaseObjectWithSource";
            }
            return "MithraAbstractTempObjectDatabaseObject";
        }
        return "MithraAbstractTransactionalDatabaseObject";
    }

    public boolean hasBusinessDateAsOfAttribute()
    {
        if (this.hasAsOfAttributes())
        {
            for (int i = 0; i < this.asOfAttributes.size(); i++)
            {
                AsOfAttribute asOfAttribute = this.asOfAttributes.get(i);
                if (!asOfAttribute.isProcessingDate()) return true;
            }
        }
        return false;
    }

    public AsOfAttribute getBusinessDateAsOfAttribute()
    {
        if (this.hasAsOfAttributes())
        {
            for (int i = 0; i < this.asOfAttributes.size(); i++)
            {
                AsOfAttribute asOfAttribute = asOfAttributes.get(i);
                if (!asOfAttribute.isProcessingDate()) return asOfAttribute;
            }
        }
        return null;
    }

    public String getBusinessDateAsOfAttributeName()
    {
        AsOfAttribute busAsOfAttribute = this.getBusinessDateAsOfAttribute();
        if (busAsOfAttribute != null)
        {
            return busAsOfAttribute.getName();
        }
        return null;
    }

    public String getTemporalDirectorClass()
    {
        if (this.getWrapped().getDatedTransactionalTemporalDirector() == null)
        {
            if (this.hasProcessingDate() && this.hasBusinessDateAsOfAttribute())
            {
                return "GenericBiTemporalDirector";
            }
            else if (this.hasBusinessDateAsOfAttribute())
            {
                return "GenericNonAuditedTemporalDirector";
            }
            else return "AuditOnlyTemporalDirector";
        }
        return this.getWrapped().getDatedTransactionalTemporalDirector();
    }

    public boolean hasProcessingDate()
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            if (asOfAttributes[i].isProcessingDate()) return true;
        }
        return false;
    }

    public AsOfAttribute getProcessingDateAttribute()
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            if (asOfAttributes[i].isProcessingDate()) return asOfAttributes[i];
        }
        throw new RuntimeException("can't get here");
    }

    public String getTemporalContainerClass()
    {
        if (this.getAsOfAttributes().length == 2)
        {
            return "BiTemporalTransactionalDataContainer";
        }
        else if (this.hasProcessingDate())
        {
            return "AuditOnlyTransactionalDataContainer";
        }
        else
        {
            return "NonAuditedTransactionalDataContainer";
        }
    }

    public int getDoubleAttributeCount()
    {
        int result = 0;
        Attribute[] attributes = this.getAttributes();
        for (int i = 0; i < attributes.length; i++)
        {
            if (attributes[i].isDoubleAttribute()) result++;
        }
        Attribute[] inheritedAttributes = this.getInheritedAttributes();
        for (int i = 0; i < inheritedAttributes.length; i++)
        {
            if (inheritedAttributes[i].isDoubleAttribute()) result++;
        }
        return result;
    }

    public int getBigDecimalAttributeCount()
    {
        int result = 0;
        Attribute[] attributes = this.getAttributes();
        for (int i = 0; i < attributes.length; i++)
        {
            if (attributes[i].isBigDecimalAttribute()) result++;
        }
        Attribute[] inheritedAttributes = this.getInheritedAttributes();
        for (int i = 0; i < inheritedAttributes.length; i++)
        {
            if (inheritedAttributes[i].isBigDecimalAttribute()) result++;
        }
        return result;
    }

    public boolean hasNullablePrimaryKeys()
    {
        Attribute[] pkAttributes = this.getPrimaryKeyAttributes();
        for (int i = 0; i < pkAttributes.length; i++)
        {
            if (pkAttributes[i].isNullable())
                return true;
        }
        return false;
    }

    public AsOfAttribute getCompatibleAsOfAttribute(AsOfAttribute asOfAttribute)
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            if (asOfAttributes[i].getName().equals(asOfAttribute.getName()) && asOfAttributes[i].isProcessingDate() == asOfAttribute.isProcessingDate())
            {
                return asOfAttributes[i];
            }
        }
        return null;
    }

    public int getJoinIndexFromConstantPool(ASTRelationalExpression exp)
    {
        if (this.isImported()) return -1;
        for (int i = 0; i < this.joinPool.size(); i++)
        {
            ASTRelationalExpression join = this.joinPool.get(i);
            if (join.equalsOther(exp)) return i;
        }
        return -1;
    }

    public MithraObjectTypeWrapper chooseForRelationshipAdd(MithraObjectTypeWrapper other)
    {
        if (this.isImported())
        {
            //todo: this can potentially break when an imported list is used for a many-to-many relationship
            return other;
        }
        else
        {
            return this;
        }
    }

    public boolean addJoinToConstantPool(ASTRelationalExpression exp)
    {
        boolean added = false;
        MithraObjectTypeWrapper rightOwner = ((ASTAttributeName) exp.getRight()).getOwner();
        if (!rightOwner.isAbstractTablePerSubclass() && !exp.getLeft().getOwner().isAbstractTablePerSubclass())
        {
            if (getJoinIndexFromConstantPool(exp) < 0)
            {
                this.joinPool.add(exp);
                this.addToRequiredClasses(rightOwner.getPackageName(), rightOwner.getFinderClassName());
                added = true;
            }
            else
            {
                added = true;
            }
        }
        return added;
    }

    public int getConstantOperationIndexFromPool(ASTRelationalExpression node)
    {
        for (int i = 0; i < this.operationPool.size(); i++)
        {
            ASTRelationalExpression join = this.operationPool.get(i);
            if (join.equalsOther(node)) return i;
        }
        return -1;
    }

    public boolean addConstantOperationToPool(ASTRelationalExpression node)
    {
        boolean cachable = !this.isImported();
        if (cachable && node.getRight() != null)
        {
            if (node.getRight() instanceof ASTLiteral)
            {
                cachable = !((ASTLiteral) node.getRight()).isJavaLiteral();
            }
            else if (node.getRight() instanceof ASTInLiteral)
            {
                cachable = !((ASTInLiteral) node.getRight()).isJavaLiteral();
            }
        }
        if (cachable && getConstantOperationIndexFromPool(node) < 0)
        {
            this.operationPool.add(node);
        }
        return cachable;
    }

    public ASTRelationalExpression[] getConstantJoins()
    {
        ASTRelationalExpression[] result = new ASTRelationalExpression[this.joinPool.size()];
        this.joinPool.toArray(result);
        return result;
    }

    public ASTRelationalExpression[] getConstantOperations()
    {
        ASTRelationalExpression[] result = new ASTRelationalExpression[this.operationPool.size()];
        this.operationPool.toArray(result);
        return result;
    }

    public int getSettableRelationshipCount()
    {
        int count = 0;
        RelationshipAttribute[] relationships = this.getRelationshipAttributes();
        for (int i = 0; i < relationships.length; i++)
        {
            if (relationships[i].isStorableInArray()) count++;
        }
        return count;
    }

    public String getOptimisticLockingWhereSql()
    {
        if (this.optimisticLockAttribute != null)
        {
            return "AND "+this.optimisticLockAttribute.getColumnName() +" = ?";
        }
        AsOfAttribute processingDateAttribute = this.getProcessingDateAttribute();
        return "AND " + processingDateAttribute.getFromColumnName() + " = ?";
    }

    public String getRelationshipRemoveHandlerClass()
    {
        if (this.hasAsOfAttributes())
        {
            return "TerminateOnRemoveHandler";
        }
        return "DeleteOnRemoveHandler";
    }

    public String getDeleteOrTerminate()
    {
        if (this.hasAsOfAttributes())
        {
            return "terminate";
        }
        return "delete";
    }

    public String getCascadeDeleteOrTerminate()
    {
        return "cascade"+StringUtility.firstLetterToUpper(getDeleteOrTerminate());
    }

    public String getCopyDetachedValueUntilMethodNameForList()
    {
        if(this.hasAsOfAttributes())
        {
            return "copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved( exclusiveUntil )";
        }
         return "copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved()";
    }

    public String getCopyDetachedValueUntilMethodName()
    {
        if(this.hasAsOfAttributes())
        {
            return "copyDetachedValuesToOriginalOrInsertIfNewUntil( exclusiveUntil )";
        }
        return "copyDetachedValuesToOriginalOrInsertIfNew()";
    }

    public String getCopyNonPrimaryKeyAttributesUntilMethodName(String relationshipName)
    {
        if(this.hasAsOfAttributes())
        {
            return "copyNonPrimaryKeyAttributesUntilFrom( "+relationshipName+", exclusiveUntil )";
        }
        return "copyNonPrimaryKeyAttributesFrom( "+relationshipName+" )";
    }

    public boolean isDepenedentObject()
    {
        RelationshipAttribute[] relationships = this.getRelationshipAttributes();
        for (int i = 0; i < relationships.length; i++)
        {
            if (relationships[i].hasParentContainer()) return true;
        }
        return false;
    }

    private synchronized String assignUniqueAlias()
    {
        if (this.getSuperClassWrapper() != null)
        {
            return this.getSuperClassWrapper().assignUniqueAlias();
        }
        char result = this.currentSubclassAlias;
        this.currentSubclassAlias++;
        if (this.currentSubclassAlias > 'z')
        {
            throw new RuntimeException("too many subclasses! not yet implemented!");
        }
        return ""+result;
    }

    private void addInheritedAttributes(Attribute superClassAttribute)
    {
        Attribute attribute = superClassAttribute.cloneForNewOwner(this);
        attribute.setInherited(true);
        this.inheritedAttributes.add(attribute);
        Attribute attributeByName = (Attribute) this.attributesByName.get(attribute.getName());
        if(attributeByName == null)
        {
            this.attributesByName.put(attribute.getName(), attribute);
        }
        if (superClassAttribute.isPrimaryKey())
        {
            this.primaryKeyAttributes.add(attribute);
            if (attribute.isNullable() && attribute.isPrimitive())
            {
                attribute.setOnHeapNullableIndex(this.nullablePrimitiveAttributes.size());
                this.nullablePrimitiveAttributes.add(attribute);

                if (attribute.isMutablePrimaryKey())
                {
                    attribute.setOnHeapMutablePkNullableIndex(this.nullablePrimitiveMutablePkAttributes.size());
                    this.nullablePrimitiveMutablePkAttributes.add(attribute);
                }
            }
        }
    }

    public boolean isTablePerClassSuperClass()
    {
        return (this.getWrapped().getSuperClassType() != null && this.getWrapped().getSuperClassType().isTablePerClass()) ||
                (this.getSuperClassWrapper() != null && this.getSuperClassWrapper().isTablePerClassSuperClass());
    }

    public boolean isTablePerSubclassConcreteClass()
    {
        return this.getSuperClassWrapper() != null && this.getSuperClassWrapper().isTablePerSubclassSuperClass();
    }

    public boolean isTablePerClassSubClass()
    {
        return this.getSuperClassWrapper() != null && this.getSuperClassWrapper().isTablePerClassSuperClass();
    }

    public synchronized void addChildClass(MithraObjectTypeWrapper child)
    {
        this.childClasses.add(child);
        if (this.getSuperClassWrapper() != null)
        {
            this.getSuperClassWrapper().addChildClass(child);
        }
    }

    public Attribute[] getAttributesIncludingInheritedPks()
    {
        ArrayList<Attribute> all = new ArrayList<Attribute>();
        for(int i=0;i<this.primaryKeyAttributes.size();i++)
        {
            Attribute pkAttr = this.primaryKeyAttributes.get(i);
            if (pkAttr.isInherited())
            {
                all.add(pkAttr);
            }
        }
        all.addAll(this.attributes);
        Attribute[] result = new Attribute[all.size()];
        return all.toArray(result);
    }

    public Attribute[] getInheritedAttributes()
    {
        ArrayList<Attribute> all = new ArrayList<Attribute>();
        all.addAll(this.inheritedAttributes);
        if (this.getSuperClassWrapper() != null)
        {
            Attribute[] inheritedAttributes = this.getSuperClassWrapper().getInheritedAttributes();
            for(int i=0;i<inheritedAttributes.length;i++)
            {
                all.add(inheritedAttributes[i]);
            }
        }
        Attribute[] result = new Attribute[all.size()];
        return all.toArray(result);
    }

    public String getCommonDataModifier()
    {
        if (this.isTablePerClassSuperClass())
        {
            return "protected";
        }
        return "private";
    }

    public MithraObjectTypeWrapper[] getSuperClasses()
    {
        MithraObjectTypeWrapper currentSuper = this.getSuperClassWrapper();
        Stack<MithraObjectTypeWrapper> stack = new Stack<MithraObjectTypeWrapper>();
        while(currentSuper != null)
        {
            if (currentSuper.isTablePerClassSuperClass())
            {
                stack.push(currentSuper);
            }
            currentSuper = currentSuper.getSuperClassWrapper();
        }
        if (stack.isEmpty()) return null;
        MithraObjectTypeWrapper[] supers = new MithraObjectTypeWrapper[stack.size()];
        int count = 0;
        while(!stack.isEmpty())
        {
            supers[count] = stack.pop();
            count++;
        }
        return supers;
    }

    public int getHierarchyDepth()
    {
        MithraObjectTypeWrapper[] superClasses = this.getSuperClasses();
        if (superClasses == null)
        {
            return 0;
        }
        return superClasses.length;
    }

    public MithraObjectTypeWrapper[] getSubClasses()
    {
        if (this.childClasses.size() > 0)
        {
            MithraObjectTypeWrapper[] children = new MithraObjectTypeWrapper[this.childClasses.size()];
            this.childClasses.toArray(children);
            return children;
        }
        else return null;
    }

    public String getSuperClassFinders()
    {
        MithraObjectTypeWrapper[] superClasses = this.getSuperClasses();
        if (superClasses != null)
        {
            String result = "new RelatedFinder[] {";
            for(int i=0;i<superClasses.length;i++)
            {
                if (i > 0)
                {
                    result += ",";
                }
                result += superClasses[i].getFinderClassName()+".getFinderInstance()";
            }
            result+= " } ";
            return result;
        }
        else
        {
            return "null";
        }
    }

    public String getSubClassFinders()
    {
        if (this.childClasses.isEmpty())
        {
            return "null";
        }
        else
        {
            String result = "new RelatedFinder[] {";
            boolean addComma = false;
            for(int i=0;i<childClasses.size();i++)
            {
                if (addComma)
                {
                    result += ",";
                }
                MithraObjectTypeWrapper child = childClasses.get(i);
                result += child.getFinderClassName()+".getFinderInstance()";
                addComma = true;
            }
            result+= " } ";
            return result;
        }
    }

    public int getResultSetStartPosition()
    {
        return 1+this.childClasses.size();
    }

    private void appendPkColumns(MithraObjectTypeWrapper wrapper, StringBuffer sb)
    {
        for (int i = 0; i < wrapper.primaryKeyAttributes.size(); i++)
        {
            Attribute attribute = wrapper.primaryKeyAttributes.get(i);
            sb.append("t0.").append(attribute.getColumnName()).append(",");
        }
    }

    private void appendColumns(MithraObjectTypeWrapper wrapper, StringBuffer sb, String alias)
    {
        for (int i = 0; i < wrapper.attributes.size(); i++)
        {
            Attribute attribute = wrapper.attributes.get(i);
            if (!attribute.isPrimaryKey())
            {
                sb.append(alias).append('.').append(attribute.getColumnName()).append(",");
            }
        }
    }

    public AbstractAttribute[] getNormalAndInheritedAttributes()
    {
        List<AbstractAttribute> attributes = new ArrayList<AbstractAttribute>();
        attributes.addAll(this.attributes);
        for (int i = 0; i < this.inheritedAttributes.size(); i++)
        {
            AbstractAttribute inheritedAttribute = this.inheritedAttributes.get(i);
            boolean notDuplicate = true;
            for (int j = 0; j < this.attributes.size(); j++)
            {
                AbstractAttribute normalAttribute = this.attributes.get(j);
                if (inheritedAttribute.getName().equals(normalAttribute.getName()))
                {
                    notDuplicate = false;
                }
            }
            if (notDuplicate)
            {
                attributes.add(inheritedAttribute);
            }
        }
        AbstractAttribute[] result = new AbstractAttribute[attributes.size()];
        return attributes.toArray(result);
    }

    public AbstractAttribute[] getNormalAndInheritedAndSourceAttributes()
    {
        List<AbstractAttribute> attributes = new ArrayList<AbstractAttribute>();
        attributes.addAll(Arrays.asList(this.getNormalAndInheritedAttributes()));
//        attributes.addAll(this.inheritedAttributes);
        if (this.hasSourceAttribute())
        {
            attributes.add(this.getSourceAttribute());
        }
        AbstractAttribute[] result = new AbstractAttribute[attributes.size()];
        return attributes.toArray(result);
    }

    public int getNonPkAttributeCount()
    {
        int result = this.attributes.size();
        for(int i=0;i<this.attributes.size();i++)
        {
            if ((this.attributes.get(i)).isPrimaryKey()) result--;
        }
        return result;
    }

    public int getNonPkResultSetStart()
    {
        int result = this.primaryKeyAttributes.size() + 1;
        result += this.childClasses.size();
        return result;
    }

    public MithraObjectTypeWrapper getRootWrapper()
    {
        return this.getSuperClasses()[0];
    }

    public boolean isTablePerClassRootClass()
    {
        return this.isTablePerClassSuperClass() && this.getSuperClassWrapper() == null;
    }

    public boolean hasMultipleLifeCycleParents()
    {
        return lifeCycleParents.size() > 1;
    }

    public boolean isIndependent()
    {
        return !this.isReferencedViaForeignKey && !hasForeignKeys();
    }

    public Attribute[] getShadowAttributes()
    {
        if (this.hasShadowAttributes())
        {
            List<Attribute> result = new ArrayList<Attribute>();
            for(int i=0;i<primaryKeyAttributes.size();i++)
            {
                Attribute attr = primaryKeyAttributes.get(i);
                if (attr.isMutablePrimaryKey())
                {
                    result.add(attr);
                }
            }
            if (this.hasTimestampOptimisticLockAttribute())
            {
                result.add(this.optimisticLockAttribute);
            }
            Attribute[] finalResult = new Attribute[result.size()];
            result.toArray(finalResult);
            return finalResult;
        }
        else
        {
            return EMPTY_ATTRIBUTES;
        }
    }

    public String getNullGetterExpressionForMutableIndex(int index)
    {
        return this.getNullGetterExpressionForIndex(this.nullablePrimitiveAttributes.size()+index);
    }

    public boolean hasEmbeddedValueObjects()
    {
        return this.embeddedValueObjects.size() > 0;
    }

    public void setTemporary(boolean temporary)
    {
        this.temporary = temporary;
    }

    public boolean isTemporary()
    {
        return temporary;
    }

    public boolean hasForeignKeys()
    {
        if (this.foreignKeys.size() > 0) return true;
        for(MithraObjectTypeWrapper parent: lifeCycleParents)
        {
            if (!parent.hasAsOfAttributes()) return true;
        }
        return false;
    }

    public MithraObjectTypeWrapper[] getForeignKeys()
    {
        Set<MithraObjectTypeWrapper> all = new HashSet<MithraObjectTypeWrapper>();
        all.addAll(foreignKeys);
        for(MithraObjectTypeWrapper parent: lifeCycleParents)
        {
            if (!parent.hasAsOfAttributes()) all.add(parent);
        }
        MithraObjectTypeWrapper[] result = new MithraObjectTypeWrapper[all.size()];
        all.toArray(result);
        Arrays.sort(result, new Comparator<MithraObjectTypeWrapper>()
        {
            public int compare(MithraObjectTypeWrapper o1, MithraObjectTypeWrapper o2)
            {
                return o1.getClassName().compareTo(o2.getClassName());
            }
        });
        return result;
    }

    public void processForeignKeys()
    {
        for(RelationshipAttribute relationshipAttribute: relationshipAttributes)
        {
            ForeignKeyType foreignKeyType = relationshipAttribute.getForeignKeyType();
            if (!foreignKeyType.isFalse() && relationshipAttribute.dependsOnlyOnFromToObjects()
                    && !relationshipAttribute.hasFilters())
            {
                if (!relationshipAttribute.isFromMany() && !relationshipAttribute.isReverseRelationship())
                {
                    // one-to-x
                    relationshipAttribute.getRelatedObject().addForeignKey(this);
                }
                else if (!relationshipAttribute.isToMany())
                {
                    // x-to-one
                    this.addForeignKey(relationshipAttribute.getRelatedObject());
                }
            }
        }
        if (this.isTablePerClassSubClass())
        {
            this.addForeignKey(this.getSuperClassWrapper());
        }
    }

    private void addForeignKey(MithraObjectTypeWrapper mithraObjectTypeWrapper)
    {
        if (!mithraObjectTypeWrapper.hasAsOfAttributes())
        {
            if (mithraObjectTypeWrapper.isTablePerSubclassSuperClass())
            {
                MithraObjectTypeWrapper[] subClasses = mithraObjectTypeWrapper.getSubClasses();
                if (subClasses == null)
                {
                    logger.warn("Class "+mithraObjectTypeWrapper.getClassName()+" is marked as a table-per-subclass superclass, but has no subclasses");
                    return;
                }
                for (MithraObjectTypeWrapper subclass: subClasses)
                {
                    addForeignKey(subclass);
                }
            }
            else
            {
                this.foreignKeys.add(mithraObjectTypeWrapper);
                mithraObjectTypeWrapper.isReferencedViaForeignKey = true;
                if (!mithraObjectTypeWrapper.getPackageName().equals(this.getPackageName()))
                {
                    this.imports.add(mithraObjectTypeWrapper.getPackageName()+".*");
                }
            }
        }
    }

    public String getIsProcessingDateCurrentExperssion()
    {
        if (this.hasProcessingDate())
        {
            String exp = null;
            if (this.getProcessingDateAttribute().isInfinityNull())
            {
                exp = "this."+this.getProcessingDateAttribute().getName()+" == null";
            }
            else
            {
                exp = this.getFinderClassName()+"."+this.getProcessingDateAttribute().getName()+"().getInfinityDate().equals(this."
                        + this.getProcessingDateAttribute().getName()+")";
            }
            exp += " || threadTx.isInFuture(this."+this.getProcessingDateAttribute().getName()+".getTime())";
            return exp;
        }
        return "true";
    }

    public String getDataClassNameIfHasData()
    {
        if (this.hasData()) return this.getDataClassName();
        return this.getImplClassName();
    }

    public void setEnableOffHeap(boolean enableOffHeap)
    {
        this.enableOffHeap = enableOffHeap;
    }

    private static class AttributeValidationVisitor extends MithraQLVisitorAdapter
    {
        private Map<String, MithraObjectTypeWrapper> allObjects;
        private List<String> errorMessages;
        private MithraObjectTypeWrapper owner;

        public AttributeValidationVisitor(Map<String, MithraObjectTypeWrapper> allObjects, List<String> errorMessages, MithraObjectTypeWrapper owner)
        {
            this.allObjects = allObjects;
            this.errorMessages = errorMessages;
            this.owner = owner;
        }

        public Object visit(SimpleNode node, Object data)
        {
            node.checkConsistency(this.owner, this.allObjects, this.errorMessages);
            return data;
        }
    }

    public Attribute[] getPrimaryKeyAttributes()
    {
        Attribute[] result = new Attribute[primaryKeyAttributes.size()];
        return primaryKeyAttributes.toArray(result);
    }

    private ArrayList<Index> getPrefixFreeList(List<Index> mustIncludeIndices, List<Index> indicesOriginal)
    {
        ArrayList<Index> prefixFreeList = new ArrayList<Index>(mustIncludeIndices);

        boolean prefix = true;

        /* Must sort indicesSorted first to allow for proper prefix checking. Sort decreasing length of indices. */
        ArrayList<Index> indicesSorted = this.sortIndicesByLength(indicesOriginal);

        for (int i = 0; i < indicesSorted.size(); i++)
        {
            Index currentIndex = indicesSorted.get(i);

            if (this.hasAsOfAttributes())
            {
                currentIndex = new Index(getWithAsOfAttributes(this.getWithAsOfAttributes((Attribute[]) currentIndex.getAttributes())), "", currentIndex.isUnique(), this);
            }

            ArrayList<String> columnNamesCurrent = currentIndex.getIndexColumnsNames();

            /* Check for prefix with indicesSorted already added */
            for (int j = 0; j < prefixFreeList.size(); j++)
            {
                Index checkIndex = prefixFreeList.get(j);
                ArrayList<String> columnNamesCheck = checkIndex.getIndexColumnsNames();

                prefix = isAPrefixOfB(columnNamesCurrent, columnNamesCheck);

                if (prefix)
                {
                    break;
                }
            }

            if (!prefix)
            {
                currentIndex.setName(this.getCaseCorrectIndexPrefix(this.getDefaultTable(), "idx", prefixFreeList.size() - (hasPrimaryKey ? 1 : 0)));
                prefixFreeList.add(currentIndex);
            }
        }
        return prefixFreeList;
    }

    private ArrayList<Index> sortIndicesByLength(List<Index> indicesUnsorted)
    {
        ArrayList<Index> sorted = new ArrayList<Index>();

        Index biggest;
        Index temp;

        /* Insertion sort... who cares, we have like 10 things! */
        for (int i = 0; i < indicesUnsorted.size(); i++)
        {
            biggest = indicesUnsorted.get(i);

            for (int j = i + 1; j < indicesUnsorted.size(); j++)
            {
                temp = indicesUnsorted.get(j);

                if (biggest.getAttributes().length < temp.getAttributes().length)
                {
                    biggest = temp;
                }
            }
            sorted.add(biggest);
        }
        return sorted;
    }

    private boolean isAPrefixOfB(ArrayList<String> columnANames, ArrayList<String> columnBNames)
    {
        if (columnANames.size() > columnBNames.size())
        {
            return false;
        }

        ArrayList<String> subsetB = new ArrayList<String>();

        for (int i = 0; i < columnANames.size(); i++)
        {
            subsetB.add(columnBNames.get(i));
        }

        /* Remove A's elements from subsetB and check for emptiness */
        for (int i = 0; i < columnANames.size(); i++)
        {
            String stringA = columnANames.get(i);

            for (int j = 0; j < subsetB.size(); j++)
            {
                String stringB = subsetB.get(j);

                if (stringA.equals(stringB))
                {
                    subsetB.remove(j);
                    break;
                }
            }
        }
        return subsetB.size() == 0;
    }

    public String getDefaultTable()
    {
        return this.getWrapped().getDefaultTable();
    }

    public String getSourceAttributeVariableDeclaration()
    {
        if (this.sourceAttribute != null)
        {
            return this.sourceAttribute.getTypeAsString() + " " + SOURCE_ATTRIBUTE_VARIABLE_NAME;
        }
        else
            return "";
    }

    public String getUserFriendlySourceAttributeVariableDeclaration()
    {
        if (this.sourceAttribute != null)
        {
            return this.sourceAttribute.getTypeAsString() + " " + this.sourceAttribute.getName();
        }
        else
            return "";
    }

    public String getUserFriendlySourceAttributeVariableName()
    {
        if (this.sourceAttribute != null)
        {
            return this.sourceAttribute.getName();
        }
        else
            return "";
    }

    public String getSourceAttributeVariableDeclarationWithComma()
    {
        if (this.hasSourceAttribute())
        {
            return ", " + this.sourceAttribute.getTypeAsString() + " " + SOURCE_ATTRIBUTE_VARIABLE_NAME;
        }
        else
            return "";
    }

    public String getColumnListWithoutPk()
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < this.attributes.size(); i++)
        {
            Attribute attribute = this.attributes.get(i);
            if (!attribute.isPrimaryKey())
                sb.append(attribute.getColumnName()).append(",");
        }
        return StringUtility.removeLastCharacter(sb.toString());
    }

    public String getColumnListWithoutPkWithAlias()
    {
        StringBuffer sb = new StringBuffer();
        MithraObjectTypeWrapper[] superClasses = this.getSuperClasses();
        if (superClasses != null)
        {
            MithraObjectTypeWrapper root = superClasses[0];
            appendColumns(root, sb, "t0");
            for(int i=1;i<superClasses.length;i++)
            {
                appendColumns(superClasses[i], sb, "t0"+superClasses[i].getUniqueAlias());
            }
            appendColumns(this, sb, "t0"+this.getUniqueAlias());
        }
        else
        {
            appendColumns(this, sb, "t0");
        }
        return StringUtility.removeLastCharacter(sb.toString());
    }

    public String getColumnListWithDefaultAlias()
    {
        StringBuffer sb = new StringBuffer();
        MithraObjectTypeWrapper[] superClasses = this.getSuperClasses();
        for(int i=this.childClasses.size() - 1;i >= 0;i--)
        {
            MithraObjectTypeWrapper child = this.childClasses.get(i);
            Attribute firstPk = child.primaryKeyAttributes.get(0);
            sb.append("t0").append(child.getUniqueAlias()).append('.').append(firstPk.getColumnName()).append(',');
        }
        if (superClasses != null)
        {
            MithraObjectTypeWrapper root = superClasses[0];
            appendPkColumns(root, sb);
            appendColumns(root, sb, "t0");
            for(int i=1;i<superClasses.length;i++)
            {
                appendColumns(superClasses[i], sb, "t0"+superClasses[i].getUniqueAlias());
            }
            appendColumns(this, sb, "t0"+this.getUniqueAlias());
        }
        else
        {
            appendPkColumns(this, sb);
            appendColumns(this, sb, "t0");
        }
        for(int i=0;i<this.childClasses.size();i++)
        {
            MithraObjectTypeWrapper child = this.childClasses.get(i);
            appendColumns(child, sb, "t0"+child.getUniqueAlias());
        }
        return StringUtility.removeLastCharacter(sb.toString());
    }

    public String extractSourceAttribute(String extractee)
    {
        return this.sourceAttribute.getType().convertToPrimitive(extractee);
    }

    public String boxSourceAttribute(String extractee)
    {
        return this.sourceAttribute.getType().convertToObject(extractee);
    }

    public SourceAttribute getSourceAttribute()
    {
        return this.sourceAttribute;
    }

    public CommonAttribute[] getAllAttributes()
    {
        List<CommonAttribute> allAttributes = new ArrayList<CommonAttribute>();
        allAttributes.addAll(Arrays.asList(this.getNormalAndInheritedAttributes()));
        allAttributes.addAll(this.asOfAttributes);
        allAttributes.addAll(this.relationshipAttributes);
        if (this.hasSourceAttribute())
        {
            allAttributes.add(this.getSourceAttribute());
        }
        CommonAttribute[] result = new CommonAttribute[allAttributes.size()];
        return allAttributes.toArray(result);
    }

    public AbstractAttribute[] getPersistentAttributes()
    {
        List<AbstractAttribute> attributes = new ArrayList<AbstractAttribute>(this.attributes.size());
        for(int i=0;i<this.primaryKeyAttributes.size();i++)
        {
            Attribute pkAttr = this.primaryKeyAttributes.get(i);
            if (pkAttr.isInherited())
            {
                attributes.add(pkAttr);
            }
        }
        attributes.addAll(this.attributes);
        AbstractAttribute[] result = new AbstractAttribute[attributes.size()];
        return attributes.toArray(result);
    }

    public AbstractAttribute[] getNormalAndSourceAttributes()
    {
        List<AbstractAttribute> attributes = new ArrayList<AbstractAttribute>();
        if (this.attributes != null)
        {
            attributes.addAll(this.attributes);
        }
        if (this.hasSourceAttribute())
        {
            attributes.add(this.getSourceAttribute());
        }
        AbstractAttribute[] result = new AbstractAttribute[attributes.size()];
        return attributes.toArray(result);
    }

    public AbstractAttribute[] getSortedNormalAndSourceAttributes()
    {
        AbstractAttribute[] attributes = this.getNormalAndSourceAttributes();
        Arrays.sort(attributes);
        return attributes;
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        AsOfAttribute[] result = new AsOfAttribute[asOfAttributes.size()];
        asOfAttributes.toArray(result);
        Arrays.sort(result, new AsOfAttributeComparator());
        return result;
    }

    public String getSourceAttributeVariable()
    {
        if (this.hasSourceAttribute())
            return SOURCE_ATTRIBUTE_VARIABLE_NAME;
        else
            return "";
    }

    public String getConnectionManagerClassName()
    {
        if (this.hasSourceAttribute())
            return this.getSourceAttribute().getType().getConnectionManagerClassName();
        else
            return "com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager";
    }

    public String toString()
    {
        return this.getClassName();
    }

    public String getSourceAttributeGetterForObject(String objectName)
    {
        if (this.hasSourceAttribute())
        {
            return objectName + "." + this.getSourceAttribute().getGetter();
        }
        else return "";
    }

    public String getSourceAttributeVariableWithComma()
    {
        if (this.hasSourceAttribute())
            return ", " + SOURCE_ATTRIBUTE_VARIABLE_NAME;
        else
            return "";
    }

    public boolean hasCompatibleSourceAttribute(MithraObjectTypeWrapper other)
    {
        if (this.hasSourceAttribute())
        {
            if (other.hasSourceAttribute())
            {
                if (this.getSourceAttribute().getName().equals(other.getSourceAttribute().getName())
                        && this.getSourceAttribute().getType().equals(other.getSourceAttribute().getType()))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private byte[] convertStringToByteArray(String s)
    {
        try
        {
            return s.getBytes(PREFERRED_ENCODING);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("could not convert string to " + PREFERRED_ENCODING);
        }
    }

    public int getSerialVersionId()
    {
        if (this.serialVersionId == 0)
        {
            this.serialVersionId = computeSerialId();
        }
        return this.serialVersionId;
    }

    protected int computeSerialId()
    {
        CRC32 crc = new CRC32();
        if (this.hasSourceAttribute())
        {
            crc.update(0x78);
        }
        if (this.hasAsOfAttributes())
        {
            crc.update(0x12);
            AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
            Arrays.sort(asOfAttributes);
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                crc.update(this.convertStringToByteArray(asOfAttributes[i].getName()));
            }
        }
        Attribute[] normalAttributes = this.getAttributes();
        Arrays.sort(normalAttributes);
        for (int i = 0; i < normalAttributes.length; i++)
        {
            crc.update(this.convertStringToByteArray(normalAttributes[i].getName()));
            crc.update(normalAttributes[i].isNullable() ? 0x43 : 0x98);
            crc.update(normalAttributes[i].isPrimaryKey() ? 0x93 : 0x15);
            crc.update(this.convertStringToByteArray(normalAttributes[i].getTypeAsString()));
        }
        return (int) crc.getValue();
    }

    public boolean hasPkGeneratorStrategy()
    {
        boolean result = false;
        Attribute[] pkAttributes = this.getPrimaryKeyAttributes();
        for (int i = 0; i < pkAttributes.length && !result; i++)
        {
            result = pkAttributes[i].isSetPrimaryKeyGeneratorStrategy();
        }
        return result;
    }

    public boolean hasSimulatedSequencePkGeneratorStrategy()
    {
        boolean result = false;
        Attribute[] pkAttributes = this.getPrimaryKeyAttributes();
        for (int i = 0; i < pkAttributes.length && !result; i++)
        {
            result = pkAttributes[i].isPrimaryKeyUsingSimulatedSequence();
        }
        return result;
    }

    public boolean hasArraySettableRelationships()
    {
        RelationshipAttribute[] relationships = this.getRelationshipAttributes();
        for (int i = 0; i < relationships.length; i++)
        {
            if (relationships[i].isStorableInArray()) return true;
        }
        return false;
    }

    public boolean hasDependentRelationships()
    {
        RelationshipAttribute[] relationships = this.getRelationshipAttributes();
        for (int i = 0; i < relationships.length; i++)
        {
            if (relationships[i].hasSetter() && relationships[i].isRelatedDependent()) return true;
        }
        return false;
    }

    public boolean hasSingleAttributePrimaryKey()
    {
        return this.primaryKeyAttributes.size() == 1;
    }

    public boolean hasJustOneAsOfAttribute()
    {
        return this.asOfAttributes.size() == 1;
    }

    public AsOfAttribute getSingleAsOfAttribute()
    {
        return this.asOfAttributes.get(0);
    }

    public static class AsOfAttributeComparator implements Comparator<AsOfAttribute>
    {
        public int compare(AsOfAttribute left, AsOfAttribute right)
        {
            if (left.isProcessingDate() == right.isProcessingDate()) return left.getName().compareTo(right.getName());
            if (left.isProcessingDate()) return 1;
            if (right.isProcessingDate()) return -1;
            return left.getName().compareTo(right.getName());
        }
    }

    public boolean hasOptimisticLocking()
    {
        return this.isTransactional() && (this.hasProcessingDate() || this.hasOptimisticLockAttribute());
    }

    public synchronized String getConstantStringSet(String values)
    {
        Integer pos = this.constantStringSet.get(values);
        if (pos == null)
        {
            this.constantStringSetValues.add(values);
            pos = Integer.valueOf(this.constantStringSetValues.size() - 1);
            this.constantStringSet.put(values, pos);
        }
        return this.getFinderClassName()+".zGetConstantStringSet("+pos+")";
    }

    public synchronized String getConstantIntSet(String values)
    {
        Integer pos = this.constantIntSet.get(values);
        if (pos == null)
        {
            this.constantIntSetValues.add(values);
            pos = Integer.valueOf(this.constantIntSetValues.size() - 1);
            this.constantIntSet.put(values, pos);
        }
        return this.getFinderClassName()+".zGetConstantIntSet("+pos+")";
    }

    public synchronized String getConstantShortSet(String values)
    {
        Short pos = this.constantShortSet.get(values);
        if (pos == null)
        {
            this.constantShortSetValues.add(values);
            pos = Short.valueOf((short)(this.constantShortSetValues.size() - 1));
            this.constantShortSet.put(values, pos);
        }
        return this.getFinderClassName()+".zGetConstantShortSet("+pos+")";
    }

    public String[] getConstantStringSetValues()
    {
        String[] result = new String[this.constantStringSetValues.size()];
        return this.constantStringSetValues.toArray(result);
    }

    public String[] getConstantIntSetValues()
    {
        String[] result = new String[this.constantIntSetValues.size()];
        return this.constantIntSetValues.toArray(result);
    }

    public String[] getConstantShortSetValues()
    {
        String[] result = new String[this.constantShortSetValues.size()];
        return this.constantShortSetValues.toArray(result);
    }

    public String getFullCacheClass(boolean transactional)
    {
        String result = appendCache("Full", transactional);
        result += "(";
        result += getCacheConstructionParams(transactional);
        result += ")";
        return result;
    }

    public String getOffHeapFullCacheClass(boolean transactional)
    {
        String result = appendCache("OffHeapFull", transactional);
        result += "(";
        result += getCacheConstructionParams(transactional);
        result += ", dataStorage)";
        return result;
    }

    public String getPartialCacheClass(boolean transactional)
    {
        String result = appendCache("Partial", transactional);
        result += "(";
        result += getCacheConstructionParams(transactional);
        result += ", config.getCacheTimeToLive(), config.getRelationshipCacheTimeToLive()";
        result += ")";
        return result;
    }

    private String appendCache(String result, boolean transactional)
    {
        if (this.hasAsOfAttributes())
        {
            result += "Dated";
        }
        else
        {
            result += "NonDated";
        }
        if (this.isTransactional() && transactional)
        {
            result += "Transactional";
        }
        return result+"Cache";
    }

    public String getCacheConstructionParams(boolean transactional)
    {
        String result = this.getFinderClassName()+".getPrimaryKeyAttributes()";
        if (this.hasAsOfAttributes())
        {
            result += ", "+this.getFinderClassName()+".getAsOfAttributes()";
        }
        result += ", this";
        result += ", "+this.getFinderClassName()+".getImmutableAttributes()";
        if (this.isTransactional() && !this.hasAsOfAttributes() && !transactional)
        {
            result += ", new NonTransactionalUnderlyingObjectGetter()";
        }
        return result;
    }

    public boolean canExtendNonGeneratedSuperClass()
    {
        return !this.hasSuperClass() && hasUniformChildren() && (this.isTransactional() || childrenAreTransactional());
    }

    public String getNonGeneratedSuperClassName()
    {
        String result = "com.gs.fw.common.mithra.superclassimpl.Mithra";
        if (this.hasAsOfAttributes() || this.childrenAreDated())
        {
            result += "Dated";
        }
        result += "TransactionalObjectImpl";
        return result;
    }

    private boolean childrenAreDated()
    {
        for(int i=0;i<this.childClasses.size();i++)
        {
            MithraObjectTypeWrapper child = this.childClasses.get(i);
            if (child.hasAsOfAttributes()) return true;
        }
        return false;
    }

    private boolean childrenAreTransactional()
    {
        for(int i=0;i<this.childClasses.size();i++)
        {
            MithraObjectTypeWrapper child = this.childClasses.get(i);
            if (child.isTransactional()) return true;
        }
        return false;
    }

    private boolean hasUniformChildren()
    {
        if (this.childClasses.size() > 0)
        {
            Boolean isDated = null;
            for(int i=0;i<this.childClasses.size();i++)
            {
                MithraObjectTypeWrapper child = this.childClasses.get(i);
                if (!child.isAbstractTablePerSubclass())
                {
                    if (isDated == null)
                    {
                        isDated = child.hasAsOfAttributes();
                    }
                    else if (this.childClasses.get(i).hasAsOfAttributes() != isDated.booleanValue())
                    {
                        return false;
                    }
                }
            }
            Boolean isReadOnly = null;
            for(int i=0;i<this.childClasses.size();i++)
            {
                MithraObjectTypeWrapper child = this.childClasses.get(i);
                if (!child.isAbstractTablePerSubclass())
                {
                    if (isReadOnly == null)
                    {
                        isReadOnly = child.isReadOnly();
                    }
                    else if (child.isReadOnly() != isReadOnly.booleanValue())
                    {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public String getMithraInterfaceName()
    {
        String result = "Mithra";
        if (this.hasAsOfAttributes())
        {
            result += "Dated";
        }
        if (this.isTransactional())
        {
            result += "Transactional";
        }
        return result+"Object";
    }

    public MithraSuperTypeWrapper getSubstituteSuperType()
    {
        return substituteSuperType;
    }

    public void setSubstituteSuperType(MithraSuperTypeWrapper substituteSuperType)
    {
        this.substituteSuperType = substituteSuperType;
    }

    public String getFullyQualifiedSuperClassType()
    {
        if (this.substituteSuperType != null)
        {
            return this.substituteSuperType.getFullyQualifiedClassName();
        }
        return super.getFullyQualifiedSuperClassType();
    }

    public List<String> resolveSuperClassGeneration()
    {
        List<String> errorMessages = new ArrayList<String>();
        if (superClassGenerationResolved || hierarchyIsReadOnly()) return errorMessages;
        if (this.hasSuperClass())
        {
            if (this.getSuperClassWrapper() != null)
            {
                errorMessages.addAll(this.getSuperClassWrapper().resolveSuperClassGeneration());
            }
            else
            {
                //we are at the root with a non-generated superclass, possibly with children
                if (hasUniformChildren())
                {
                    String superClassName = this.getSuperClass().getName();
                    if (this.hasAsOfAttributes() || childrenAreDated())
                    {
                        superClassName += "Dated";
                    }
                    superClassName += "CommonAbstract";
                    createSubstituteSuperType(superClassName);
                }
                else
                {
                    chooseChildNonUniformSuperClass();
                }
            }

        }
        else if (!hasUniformChildren())
        {
            // we are at the root, with no superclass
            chooseChildNonUniformSuperClass();
        }
        superClassGenerationResolved = true;
        return errorMessages;
    }

    private boolean hierarchyIsReadOnly()
    {
        return (this.isReadOnly() && (this.childClasses.isEmpty() || !childrenAreTransactional()));
    }

    private void chooseChildNonUniformSuperClass()
    {
        for(int i=0;i<this.childClasses.size();i++)
        {
            MithraObjectTypeWrapper child = this.childClasses.get(i);
            if (child.getSuperClassWrapper().equals(this))
            {
                child.chooseNonUniformSuperClass();
            }
        }
    }

    private void chooseNonUniformSuperClass()
    {
        if (superClassGenerationResolved || hierarchyIsReadOnly()) return;
        if (this.hasUniformChildren())
        {
            String superClassName = super.getFullyQualifiedSuperClassType();
            if (this.hasAsOfAttributes() || childrenAreDated())
            {
                superClassName += "Dated";
            }
            superClassName += "CommonAbstract";
            createSubstituteSuperType(superClassName);
        }
        else
        {
            chooseChildNonUniformSuperClass();
        }
        superClassGenerationResolved = true;
    }

    private void createSubstituteSuperType(String superClassName)
    {
        this.substituteSuperType = new MithraSuperTypeWrapper();
        this.substituteSuperType.setClassName(superClassName);
        this.substituteSuperType.setSuperClass(super.getFullyQualifiedSuperClassType());
        this.substituteSuperType.setDated(this.hasAsOfAttributes() || childrenAreDated());
        this.substituteSuperType.setImports(this.getImportSet());
    }

    public String getImplClassNameWithSlashes()
    {
        return this.getPackageName().replace('.', '/')+"/"+this.getImplClassName();
    }

    public String getBusinessClassNameWithDots()
    {
        return this.getPackageName()+"."+this.getInterfaceName();
    }

    public boolean hasUpdateListener()
    {
        return this.getWrapped().getUpdateListener() != null;
    }

    public String getUpdateListener()
    {
        return getWrapped().getUpdateListener();
    }

    public boolean hasOffHeap()
    {
        return this.enableOffHeap && this.isOffHeapCompatible;
    }

    public String getOnHeapDataClassName()
    {
        if (this.hasOffHeap())
        {
            return this.getDataClassName()+"."+this.getDataClassName()+"OnHeap";
        }
        return this.getDataClassName();
    }

    public String getOffHeapDataClassName()
    {
        if (this.hasOffHeap())
        {
            return this.getDataClassName()+"."+this.getDataClassName()+"OffHeap";
        }
        return this.getDataClassName();
    }

    public int getOffHeapDataSize()
    {
        return offHeapSize;
    }
}
