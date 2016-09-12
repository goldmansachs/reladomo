<%--
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
--%>
<%@ page import="java.util.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.mapper.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.queryparser.ASTAttributeName" %>
<%@ page import="com.gs.fw.common.mithra.generator.queryparser.ASTRelationalExpression" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
	RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    Attribute[] pkAttributes = wrapper.getPrimaryKeyAttributes();
    CommonAttribute[] allAttributes = wrapper.getAllAttributes();
    AbstractAttribute[] normalAttributes = wrapper.getNormalAndInheritedAndSourceAttributes();
    AbstractAttribute[] persistantAttributes = wrapper.getPersistentAttributes();
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    List indicies = wrapper.getIndices();
    String className = wrapper.getClassName() + "Finder";
    String[] constantStringSetValues = wrapper.getConstantStringSetValues();
    String[] constantIntSetValues = wrapper.getConstantIntSetValues();
    String[] constantShortSetValues = wrapper.getConstantShortSetValues();
    List<Index> indexList = wrapper.getPkAndUniqueIndices();
    MithraInterfaceType[] mithraInterfaceTypes = wrapper.getMithraInterfaces();
%>
package <%= wrapper.getPackageName() %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.behavior.txparticipation.*;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.bean.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.booleanop.*;
import com.gs.fw.common.mithra.finder.integer.*;
import com.gs.fw.common.mithra.finder.longop.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.finder.string.*;
import com.gs.fw.common.mithra.extractor.NormalAndListValueSelector;
import com.gs.fw.common.mithra.list.NulledRelation;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.QueryCache;
import com.gs.fw.common.mithra.portal.*;
import com.gs.fw.common.mithra.remote.*;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import com.gs.fw.common.mithra.util.TimestampPool;
import com.gs.collections.impl.map.mutable.UnifiedMap;
<% if (wrapper.isTemporary()) { %>
import com.gs.fw.common.mithra.tempobject.*;
<% } %>

import java.io.Serializable;

<%@  include file="../DoNotModifyWarning.jspi" %>
public class <%= className %>
{
    <% for (AsOfAttribute asOfAttribute : asOfAttributes) { %>
        private static final Timestamp <%= asOfAttribute.getName() %>Infinity = TimestampPool.getInstance().getOrAddToCache(<%= asOfAttribute.getInfinityExpression() %>, true);
        private static final Timestamp <%= asOfAttribute.getName() %>Default  = TimestampPool.getInstance().getOrAddToCache(<%= asOfAttribute.getDefaultDateExpression() %>, true);
    <% } %>
    <% if (wrapper.hasSourceAttribute()) { %>
        private static final SourceAttributeType zSourceAttributeType = SourceAttributeType.create("<%= wrapper.getSourceAttribute().getName() %>",
            <%= StringUtility.firstLetterToUpper(wrapper.getSourceAttribute().getFinderAttributeType()) %>.class);
    <% } %>
    private static final String IMPL_CLASS_NAME_WITH_SLASHES = "<%= wrapper.getImplClassNameWithSlashes()%>";
    private static final String BUSINESS_CLASS_NAME_WITH_DOTS = "<%= wrapper.getBusinessClassNameWithDots()%>";
    private static final FinderMethodMap finderMethodMap;
    private static boolean isFullCache;
    private static boolean isOffHeap;
    private static volatile MithraObjectPortal objectPortal = new UninitializedPortal("<%= wrapper.getPackageName() %>.<%= wrapper.getClassName() %>");
    private static final <%= wrapper.getClassName() %>SingleFinder<<%= wrapper.getClassName() %>, Object, <%= wrapper.getClassName() %>> finder = new <%=wrapper.getClassName() %>SingleFinder<<%= wrapper.getClassName() %>, Object, <%= wrapper.getClassName() %>>();
    private static ConstantStringSet[] constantStringSets = new ConstantStringSet[<%= constantStringSetValues.length %>];
    private static ConstantIntSet[] constantIntSets = new ConstantIntSet[<%= constantIntSetValues.length %>];
    private static ConstantShortSet[] constantShortSets = new ConstantShortSet[<%= constantShortSetValues.length %>];
    <% if (wrapper.isTemporary()) { %>
    private static TempContextContainer tempContextContainer = new TempContextContainer("<%= wrapper.getPackageName() %>.<%= wrapper.getClassName()%>");
    <% } %>

    <% for(int i=0;i<indicies.size();i++) { %>
        <% Index index = (Index) indicies.get(i); %>
        <% if (index.isUnique()) { %>
            private static int INDEX_<%= index.getName() %> = -1;
        <% } %>
    <% } %>

    static
    {
        finderMethodMap = new FinderMethodMap(<%=className%>.<%= wrapper.getClassName()%>RelatedFinder.class);
        <% for (AbstractAttribute normalAttribute : normalAttributes) { %>
            finderMethodMap.addNormalAttributeName("<%= normalAttribute.getName() %>");
        <% } %>
        <% for (AsOfAttribute asOfAttribute : asOfAttributes) { %>
            finderMethodMap.addNormalAttributeName("<%= asOfAttribute.getName() %>");
        <% } %>
        <% for (RelationshipAttribute relationshipAttribute : relationshipAttributes) {
            if (!relationshipAttribute.hasParameters()) { %>
                finderMethodMap.addRelationshipName("<%= relationshipAttribute.getName() %>");
            <% } %>
        <% } %>
        <% for (int i= 0; i < constantStringSetValues.length; i++) { %>
            constantStringSets[<%= i %>] = new ConstantStringSet(Arrays.asList(new String[]{<%= constantStringSetValues[i] %>}));
        <% } %>
        <% for (int i= 0; i < constantIntSetValues.length; i++) { %>
            constantIntSets[<%= i %>] = new ConstantIntSet(new int[]{<%= constantIntSetValues[i] %>});
        <% } %>
        <% for (int i= 0; i < constantShortSetValues.length; i++) { %>
            constantShortSets[<%= i %>] = new ConstantShortSet(new short[]{<%= constantShortSetValues[i] %>});
        <% } %>
    }

    <% for (AbstractAttribute normalAttribute : normalAttributes) { %>
        <% if (normalAttribute.hasProperties()) { %>
            private static final Map<String, Object> <%=normalAttribute.getName()%>Properties = new UnifiedMap<String, Object>();
            static
            {
            <% for (Map.Entry<String, String> property : normalAttribute.getProperties().entrySet()) { %>
                <%=normalAttribute.getName()%>Properties.put(<%= property.getKey() %>, <%= property.getValue() %>);
            <% } %>
            }
        <% } %>
    <% } %>
    <% for (AsOfAttribute asOfAttribute : asOfAttributes) { %>
        <% if (asOfAttribute.hasProperties()) { %>
            private static final Map<String, Object> <%=asOfAttribute.getName()%>Properties = new UnifiedMap<String, Object>();
            static
            {
            <% for (Map.Entry<String, String> property : asOfAttribute.getProperties().entrySet()) { %>
                <%=asOfAttribute.getName()%>Properties.put(<%= property.getKey() %>, <%= property.getValue() %>);
            <% } %>
            }
        <% } %>
    <% } %>
    public static Attribute[] allPersistentAttributes()
    {
        return finder.getPersistentAttributes();
    }

    public static List<RelatedFinder> allRelatedFinders()
    {
       return finder.getRelationshipFinders();
    }

    public static List<RelatedFinder> allDependentRelatedFinders()
    {
       return finder.getDependentRelationshipFinders();
    }

    public static ConstantStringSet zGetConstantStringSet(int index)
    {
        return constantStringSets[index];
    }

    public static ConstantIntSet zGetConstantIntSet(int index)
    {
        return constantIntSets[index];
    }

    public static ConstantShortSet zGetConstantShortSet(int index)
    {
        return constantShortSets[index];
    }

    <% ASTRelationalExpression[] constantJoins = wrapper.getConstantJoins(); %>
    <% if (constantJoins.length > 0) { %>
        public static Mapper zGetConstantJoin(int index)
        {
            return getConstantJoinPool()[index];
        }

        private static Mapper[] constantJoinPool;

        private static Mapper[] getConstantJoinPool()
        {
            if (constantJoinPool == null)
            {
                Mapper[] result = new Mapper[<%= constantJoins.length %>];
            <% for(int i = 0; i < constantJoins.length; i++) { %>
                result[<%= i %>] = <%= constantJoins[i].getJoinExpression() %>;
            <% } %>
                constantJoinPool = result;
            }
            return constantJoinPool;
        }
    <% } %>
    <% ASTRelationalExpression[] constantOps = wrapper.getConstantOperations(); %>
    <% if (constantOps.length > 0) { %>
        public static Operation zGetConstantOperation(int index)
        {
            return getConstantOperationPool()[index];
        }

        private static Operation[] constantOperationPool;

        private static Operation[] getConstantOperationPool()
        {
            if (constantOperationPool == null)
            {
                Operation[] result = new Operation[<%= constantOps.length %>];
            <% for(int i = 0; i < constantOps.length; i++) { %>
                result[<%= i %>] = <%= constantOps[i].getConstantExpression(wrapper) %>;
            <% } %>
                constantOperationPool = result;
            }
            return constantOperationPool;
        }
    <% } %>

    public static SourceAttributeType getSourceAttributeType()
    {
    <% if (wrapper.hasSourceAttribute()) { %>
        return <%= wrapper.getFinderClassName() %>.zSourceAttributeType;
    <% } else { %>
       return null;
    <% } %>
    }
    public static <%= wrapper.getClassName() %> findOne(com.gs.fw.finder.Operation operation)
    {
        return findOne(operation, false);
    }

    public static <%= wrapper.getClassName() %> findOneBypassCache(com.gs.fw.finder.Operation operation)
    {
        return findOne(operation, true);
    }

    public static <%= wrapper.getListInterfaceName() %> findMany(com.gs.fw.finder.Operation operation)
    {
        return (<%= wrapper.getListClassName() %>) finder.findMany(operation);
    }

    public static <%= wrapper.getListInterfaceName() %> findManyBypassCache(com.gs.fw.finder.Operation operation)
    {
        return (<%= wrapper.getListClassName() %>) finder.findManyBypassCache(operation);
    }

    private static <%= wrapper.getClassName() %> findOne(com.gs.fw.finder.Operation operation, boolean bypassCache)
    {
        List found = getMithraObjectPortal().find((Operation) operation, bypassCache);
        return (<%= wrapper.getImplClassName() %>) FinderUtils.findOne(found);
    }

    <% for(Index index: indexList) { %>
    public static <%= wrapper.getClassName() %> findBy<%= index.getSanitizedUpperCaseName() %>(<%= index.getFindByParameters() %>)
    {
        return finder.findBy<%= index.getSanitizedUpperCaseName() %>(<%= index.getFindByVariables() %>);
    }

        <% if (index.hasFastPathLookup()) { %>
        private static final RelationshipHashStrategy for<%= index.getSanitizedUpperCaseName() %> = new <%= index.getSanitizedUpperCaseName() %>Rhs();

        private static final class <%= index.getSanitizedUpperCaseName() %>Rhs implements RelationshipHashStrategy
        {
            public boolean equalsForRelationship(Object _srcObject, Object _srcData, Object _targetData, Timestamp _asOfDate0, Timestamp _asOfDate1)
            {
                <%= index.getBeanType() %> _bean = (<%= index.getBeanType() %>) _srcData;
                <%= wrapper.getDataClassNameIfHasData() %> _castedTargetData = (<%= wrapper.getDataClassNameIfHasData() %>) _targetData;
                if (<%= index.getLookupEqualsCondition() %>)
                {
                    return <%= index.getAsOfAttributeCheckCondition() %>;
                }

                return false;
            }

            public int computeHashCodeFromRelated(Object _srcObject, Object _srcData)
            {
                <%= index.getBeanType() %> _bean = (<%= index.getBeanType() %>) _srcData;
                return <%= index.getLookupHashCompute(false) %>;
            }

            public int computeOffHeapHashCodeFromRelated(Object _srcObject, Object _srcData)
            {
                <%= index.getBeanType() %> _bean = (<%= index.getBeanType() %>) _srcData;
                return <%= index.getLookupHashCompute(true) %>;
            }
        }
        <% } %>
    <% } %>

    public static <%= wrapper.getImplClassName() %> zFindOneForRelationship(Operation operation)
    {
        List found = getMithraObjectPortal().findAsCachedQuery(operation, null, false, true, 0).getResult();
        return (<%= wrapper.getImplClassName() %>) FinderUtils.findOne(found);
    }

    public static MithraObjectPortal getMithraObjectPortal()
    {
        <% if (wrapper.isTemporary()) { %>
        return tempContextContainer.getMithraObjectPortal();
        <% } else { %>
        return objectPortal.getInitializedPortal();
        <% } %>
    }

    public static void clearQueryCache()
    {
        <% if (wrapper.isTemporary()) { %>
        FinderUtils.clearTempObjectQueryCache(tempContextContainer);
        <% } else { %>
        objectPortal.clearQueryCache();
        <% } %>
    }

    public static void reloadCache()
    {
        <% if (!wrapper.isTemporary()) { %>
        objectPortal.reloadCache();
        <% } %>
    }

    <% for (EmbeddedValue evo : embeddedValueObjects) { %>
        <% String attributeClassName = wrapper.getClassName() + StringUtility.firstLetterToUpper(evo.getNestedName()) + "Attribute"; %>
        <% String objectType = evo.getType(); %>

        public static class <%= attributeClassName %> extends <%= evo.getQualifiedAttributeClassName() %>
        {
            private final <%= wrapper.getClassName() %>RelatedFinder finder;
            private final com.gs.collections.api.block.function.Function parentSelector;
            <% for (RelationshipAttribute rel: evo.getRelationshipAttributes()) { %>
                <% if (!rel.hasParameters()) { %>
                private final Extractor<Object, <%=rel.getTypeAsString()%>> <%= rel.getName() %>;
                <% } %>
            <% } %>
            public <%= attributeClassName %>(<%= wrapper.getClassName() %>RelatedFinder finder, com.gs.collections.api.block.function.Function parentSelector)
            {
                this.finder = finder;
                this.parentSelector = parentSelector;
            <% for (RelationshipAttribute rel: evo.getRelationshipAttributes()) { %>
                <% if (!rel.hasParameters()) { %>
                this.<%= rel.getName()%> = new <%= attributeClassName %><%= StringUtility.firstLetterToUpper(rel.getName())%>Extractor(this.parentSelector);
                <% } %>
            <% } %>
            }

            public MithraObjectPortal getOwnerPortal()
            {
                return <%= className %>.getMithraObjectPortal();
            }

            public Class valueType()
            {
                return <%= objectType %>.class;
            }

            public <%= objectType %> <%= evo.getExtractorValueOf() %>(Object o)
            {
                if (this.parentSelector != null)
                {
                    o = this.parentSelector.valueOf(o);
                    if (o == null) return null;
                }
                return ((<%= wrapper.getClassName() %>) o).<%= evo.getNestedGetter() %>();
            }

            public void <%= evo.getExtractorSetValue() %>(Object o, <%= evo.getType() %> newValue)
            {
                if (this.parentSelector != null)
                {
                    o = this.parentSelector.valueOf(o);
                }
                ((<%= wrapper.getClassName() %>) o).<%= evo.getNestedCopyValuesFrom() %>(newValue);
            }

            public void <%= evo.getExtractorSetValueUntil() %>(Object o, <%= evo.getType() %> newValue, Timestamp exclusiveUntil)
            {
                if (this.parentSelector != null)
                {
                    o = this.parentSelector.valueOf(o);
                }
                <% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
                    ((<%= wrapper.getClassName() %>) o).<%= evo.getNestedCopyValuesFromUntil() %>(newValue, exclusiveUntil);
                <% } else { %>
                    throw new UnsupportedOperationException("<%= evo.getExtractorSetValueUntil() %> is only implemented for dated objects.");
                <% } %>
            }

            <% for (EmbeddedValueMapping attribute : evo.getMappings()) { %>
                public <%= StringUtility.firstLetterToUpper(attribute.getType().getJavaTypeStringPrimary()) %>Attribute <%= attribute.getShortName() %>()
                {
                    return this.finder.<%= attribute.getName() %>();
                }
            <% } %>

            <% for (RelationshipAttribute rel: evo.getRelationshipAttributes()) { %>
                public <%= rel.getFinderAttributeTypeForRelatedClass() %> <%= rel.getName() %>(<%= rel.getParameters() %>)
                {
                    return this.finder.<%= evo.getAliasedRelationshipName(rel) %>(<%= rel.getParameterVariables() %>);
                }

                public Extractor<Object, <%=rel.getTypeAsString()%>> z<%= StringUtility.firstLetterToUpper(rel.getName()) %>Extractor(<%= rel.getParameters() %>)
                {
                    <% if (rel.hasParameters()) { %>
                    return new <%= attributeClassName %><%= StringUtility.firstLetterToUpper(rel.getName())%>Extractor(this.parentSelector, <%= rel.getParameterVariables() %>);
                    <% } else { %>
                    return this.<%= rel.getName() %>;
                    <% } %>
                }
            <% } %>
            <% for (EmbeddedValue nestedObject : evo.getChildren()) { %>
                public <%= nestedObject.getQualifiedAttributeClassName() %> <%= nestedObject.getName() %>()
                {
                    return this.finder.<%= nestedObject.getNestedName() %>();
                }
            <% } %>

            protected <%= evo.getQualifiedAttributeClassName() %> retrieveAttribute()
            {
                return this.finder.<%= StringUtility.firstLetterToLower(evo.getChainedInvocation()) %>();
            }
        }
    <% for (RelationshipAttribute rel: evo.getRelationshipAttributes()) { %>
        public static class <%= attributeClassName %><%= StringUtility.firstLetterToUpper(rel.getName())%>Extractor extends NonPrimitiveExtractor
        {
            private final com.gs.collections.api.block.function.Function parentSelector;
            <% for (int j = 0; j < rel.getParameterCount(); j++) { %>
                private <%= rel.getParameterTypeAt(j) %> <%= rel.getParameterVariableAt(j) %>;
            <% } %>

            public <%= attributeClassName %><%= StringUtility.firstLetterToUpper(rel.getName())%>Extractor(com.gs.collections.api.block.function.Function parentSelector <%= rel.getParametersWithComma() %>)
            {
                this.parentSelector = parentSelector;
                <% for (int j = 0; j < rel.getParameterCount(); j++) { %>
                    this.<%= rel.getParameterVariableAt(j)%> = <%= rel.getParameterVariableAt(j) %>;
                <% } %>
            }

            public Object valueOf(Object o)
            {
                if (this.parentSelector != null)
                {
                    o = this.parentSelector.valueOf(o);
                    if (o == null) return null;
                }
                return ((<%= wrapper.getClassName() %>) o).get<%= StringUtility.firstLetterToUpper(evo.getAliasedRelationshipName(rel)) %>(<%= rel.getParameterVariables() %>);
            }

            public void setValue(Object o, Object newValue)
            {
                <% if (rel.hasParameters()) { %>
                throw new MithraBusinessException("parametrized relationships cannot be set");
                <% } else { %>
                if (this.parentSelector != null)
                {
                    o = this.parentSelector.valueOf(o);
                    throw new RuntimeException("Could not set deep relationship as intermediate object is null");
                }
                ((<%= wrapper.getClassName() %>) o).set<%= StringUtility.firstLetterToUpper(evo.getAliasedRelationshipName(rel)) %>((<%=rel.getTypeAsString()%>) o);
                <% } %>
            }
        }
    <% } %>
    <% } %>

    <% for (AbstractAttribute normalAttribute : normalAttributes) { %>
        <% MithraObjectTypeWrapper owner = normalAttribute.getOwner(); %>
        <% String getter = normalAttribute.getGetter(); %>
        <% String setter = normalAttribute.getSetter(); %>
        <% String nullGetter = normalAttribute.getNullGetter(); %>
        <% if (normalAttribute.needsGeneratedAttribute()) { %>
        <% if (normalAttribute.isAsOfAttributeTo() && ((AsOfAttribute)normalAttribute.getOwner().getAttributeByName(normalAttribute.getAsOfAttributeNameForAsOfAttributeTo())).isInfinityNull()){ %>
            <%= normalAttribute.getVisibility() %> static class <%= wrapper.getClassName() %><%= StringUtility.firstLetterToUpper(normalAttribute.getName()) %>Attribute extends TimestampAttributeAsOfAttributeToInfiniteNull
        <%} else {%>
            <%= normalAttribute.getVisibility() %> static class <%= wrapper.getClassName() %><%= StringUtility.firstLetterToUpper(normalAttribute.getName()) %>Attribute extends <%= normalAttribute.getFinderAttributeSuperClassType() %>
        <%} %>
        {
            public <%= wrapper.getClassName() %><%= StringUtility.firstLetterToUpper(normalAttribute.getName()) %>Attribute()
            {
                super(<%= normalAttribute.getConstructorParameters()%>);
            }

            public <%= normalAttribute.getTypeAsString() %> <%= normalAttribute.getExtractionMethodName() %>(Object o)
            {
                if (o instanceof <%= owner.getClassName() %>Data) return ((<%= owner.getClassName() %>Data) o).<%= getter %>();
                return ((<%= owner.getClassName() %>) o).<%= getter %>();
            }

            public void <%= normalAttribute.getValueSetterMethodName() %>(Object o, <%= normalAttribute.getTypeAsString() %> newValue)
            {
                if (o instanceof <%= owner.getClassName() %>Data)
                {
                    ((<%= owner.getClassName() %>Data) o).<%= setter %>(newValue);
                }
                else
                {
                    ((<%= owner.getClassName() %>) o).<%= setter %>(newValue);
                }
            }
            <% if (normalAttribute.isPrimitive()) { %>
                public boolean isAttributeNull(Object o)
                {
                    <% if (!normalAttribute.isPrimitive() || normalAttribute.isNullable()) { %>
                    if (o instanceof <%= owner.getClassName() %>Data) return ((<%= owner.getClassName() %>Data) o).<%= nullGetter %>;
                    return ((<%= owner.getClassName() %>) o).<%= nullGetter %>;
                    <% } else { %>
                    return false;
                    <% } %>
                }
                <% if (normalAttribute.isNullablePrimitive()) { %>
                    public void setValueNull(Object o)
                    {
                        if (o instanceof <%= wrapper.getClassName() %>Data)
                        {
                            ((<%= owner.getClassName() %>Data) o).<%= normalAttribute.getSetter() %>Null();
                        }
                        else
                        {
                            ((<%= owner.getClassName() %>) o).<%= normalAttribute.getSetter() %>Null();
                        }
                    }
                <% } else { %>
                    public void setValueNull(Object o)
                    {
                        throw new MithraBusinessException("Attribute " + this.getClass().getName() + " cannot be set to null");
                    }
                <% } %>
            <% } %>
            <% if (normalAttribute.isDoubleAttribute()) { %>
                public void increment(Object o, double increment)
                {
                <% if (normalAttribute.needsUntilImplementation()) { %>
                    ((<%= owner.getImplClassName() %>) o).<%= normalAttribute.getIncrementer() %>(increment);
                <% } else { %>
                    throw new RuntimeException("not implemented");
                <% } %>
                }

                public void incrementUntil(Object o, double increment, Timestamp exclusiveUntil)
                {
                <% if (normalAttribute.needsUntilImplementation()) { %>
                    ((<%= owner.getImplClassName() %>) o).<%= normalAttribute.getIncrementer() %>Until(increment, exclusiveUntil);
                <% } else { %>
                    throw new RuntimeException("not implemented");
                <% } %>
                }
            <% } %>
            <% if (normalAttribute.isBigDecimalAttribute()) { %>
                public void increment(Object o, BigDecimal increment)
                {
                <% if (normalAttribute.needsUntilImplementation()) { %>
                    ((<%= owner.getImplClassName() %>) o).<%= normalAttribute.getIncrementer() %>(increment);
                <% } else { %>
                    throw new RuntimeException("not implemented");
                <% } %>
                }

                public void incrementUntil(Object o, BigDecimal increment, Timestamp exclusiveUntil)
                {
                <% if (normalAttribute.needsUntilImplementation()) { %>
                    ((<%= owner.getImplClassName() %>) o).<%= normalAttribute.getIncrementer() %>Until(increment, exclusiveUntil);
                <% } else { %>
                    throw new RuntimeException("not implemented");
                <% } %>
                }
            <% } %>
                public void setUntil(Object o, <%= normalAttribute.getTypeAsString() %> newValue, Timestamp exclusiveUntil)
                {
                <% if (normalAttribute.needsUntilImplementation()) { %>
                    ((<%= owner.getImplClassName() %>) o).<%= normalAttribute.getSetter() %>Until(newValue, exclusiveUntil);
                <% } else { %>
                    throw new RuntimeException("not implemented");
                <% } %>
                }
                <% if (normalAttribute.isPrimitive()) { %>
                    public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
                    {
                        <% if (normalAttribute.isNullable() && normalAttribute.needsUntilImplementation()) { %>
                        ((<%= owner.getImplClassName() %>) o).<%= normalAttribute.getSetter() %>NullUntil(exclusiveUntil);
                        <% } else { %>
                        throw new MithraBusinessException("Attribute " + this.getClass().getName() + " cannot be set to null");
                        <% } %>
                    }
                <% } %>
            <% if (normalAttribute.getType().isIntOrLong() || normalAttribute.isTimestampAttribute()) { %>
                public boolean hasSameVersion(MithraDataObject first, MithraDataObject second)
                {
                    <% if (normalAttribute == wrapper.getOptimisticLockAttribute()) { %>
                            <% if (wrapper.getOptimisticLockAttribute().isPrimitive()) { %>
                                return ((<%= owner.getDataClassName() %>) first).zGetPersistedVersion() == ((<%= owner.getDataClassName() %>) second).zGetPersistedVersion();
                            <% } else { %>
                                return ((<%= owner.getDataClassName() %>) first).zGetPersistedVersion().equals(((<%= owner.getDataClassName() %>) second).zGetPersistedVersion());
                            <% } %>
                    <% } else { %>
                        throw new RuntimeException("not implemented");
                    <% } %>
                }
            <% } %>
            <% if (normalAttribute.getType().isIntOrLong()) { %>
                public boolean isSequenceSet(Object o)
                {
                    <% if (normalAttribute.isPrimaryKeyUsingSimulatedSequence()) { %>
                        return ((<%= owner.getImplClassName() %>) o).zGetIs<%= StringUtility.firstLetterToUpper(normalAttribute.getName()) %>Set();
                    <% } else { %>
                        return false;
                    <% } %>
                }
            <% } %>
        }
        <% } %>
    <% } %>


    <% if (wrapper.isTablePerSubclassConcreteClass()) { %>
	    public static class <%= wrapper.getClassName() %>RelatedFinder<ParentOwnerType, ReturnType, ReturnListType extends List, OwnerType> extends AbstractRelatedFinder<<%= wrapper.getClassName() %>, ParentOwnerType, ReturnType, ReturnListType, OwnerType> implements <%= wrapper.getSuperClassWrapper().getFinderClassName() %><<%= wrapper.getClassName() %>>
        <% if (wrapper.hasMithraInterfaces()) { %>
            , <%= wrapper.getImplementingMithraInterfacesAsString() %>
        <% } %>
<% } else if (wrapper.isTablePerClassSubClass()) { %>
	    public static class <%= wrapper.getClassName() %>RelatedFinder<ParentOwnerType, ReturnType, ReturnListType extends List, OwnerType> extends <%= wrapper.getSuperClassWrapper().getFinderClassName() %>.<%= wrapper.getSuperClassWrapper().getClassName() %>RelatedFinder<ParentOwnerType, ReturnType, ReturnListType, OwnerType>
        <% if (wrapper.hasMithraInterfaces()) { %>
            implements <%= wrapper.getImplementingMithraInterfacesAsString() %>
        <% } %>
<% } else { %>
	    public static class <%= wrapper.getClassName() %>RelatedFinder<ParentOwnerType, ReturnType, ReturnListType extends List, OwnerType> extends AbstractRelatedFinder<<%= wrapper.getClassName() %>, ParentOwnerType, ReturnType, ReturnListType, OwnerType>
        <% if (wrapper.hasMithraInterfaces()) { %>
            implements <%= wrapper.getImplementingMithraInterfacesAsString() %>
        <% } %>
<% } %>

    {
        private List<RelatedFinder> relationshipFinders;
        private List<RelatedFinder> dependentRelationshipFinders;

        <% if (wrapper.hasSourceAttribute()) { %>
        public static SourceAttributeType zGetSourceAttributeType()
        {
                return <%= wrapper.getFinderClassName() %>.zSourceAttributeType;
        }
        <% } %>

        <% for (CommonAttribute attribute : allAttributes) { %>
            <% if (!attribute.hasParameters()) { %>
                private <%= attribute.getFinderAttributeType() %><ParentOwnerType> <%= attribute.getName() %>;
            <% } %>
        <% } %>
        <% for (EmbeddedValue evo : embeddedValueObjects) { %>
            private <%= evo.getQualifiedAttributeClassName() %> <%= evo.getNestedName() %>;
        <% } %>
        <% if (wrapper.hasAsOfAttributes()) { %>
            private transient AsOfAttribute[] asOfAttributes;
            public synchronized AsOfAttribute[] getAsOfAttributes()
            {
                if (asOfAttributes == null)
                {
                    asOfAttributes = new AsOfAttribute[<%= wrapper.getAsOfAttributes().length %>];
                    <% for (int i = 0; i < asOfAttributes.length; i++) { %>
                        asOfAttributes[<%= i %>] = this.<%= asOfAttributes[i].getName() %>();
                    <% } %>
                }
                return this.asOfAttributes;
            }
        <% } %>
        public <%= wrapper.getClassName() %>RelatedFinder()
        {
            super();
        }

        public <%= wrapper.getClassName() %>RelatedFinder(Mapper mapper)
        {
            super(mapper);
        }

        public String getFinderClassName()
        {
            return "<%= wrapper.getPackageName()%>.<%= wrapper.getFinderClassName() %>";
        }

        public RelatedFinder getRelationshipFinderByName(String relationshipName)
        {
            return <%= wrapper.getClassName() %>Finder.finderMethodMap.getRelationshipFinderByName(relationshipName, this);
        }

        public Attribute getAttributeByName(String attributeName)
        {
            return <%= wrapper.getClassName() %>Finder.finderMethodMap.getAttributeByName(attributeName, this);
        }

        public <%= valueSelectorClassName %> getAttributeOrRelationshipSelector(String attributeName)
        {
            return <%= wrapper.getClassName() %>Finder.finderMethodMap.getAttributeOrRelationshipSelector<%= valueSelectorSimpleClassName %>(attributeName, this);
        }

        public Attribute[] getPersistentAttributes()
        {
            Attribute[] attributes = new Attribute[<%= persistantAttributes.length %>];
            <% for (int i = 0; i < persistantAttributes.length; i++) { %>
                attributes[<%= i %>] = this.<%= persistantAttributes[i].getName() %>();
            <% } %>
            return attributes;
        }

        public List<RelatedFinder> getRelationshipFinders()
        {
           if (relationshipFinders == null)
           {
                <% int relatedFinderCount = 0; %>
                <% for(RelationshipAttribute relationshipAttribute : relationshipAttributes) { %>
                    <% if (!relationshipAttribute.hasParameters()) { %>
                        <% relatedFinderCount++; %>
                    <% } %>
                <% } %>
                List<RelatedFinder> relatedFinders = new ArrayList<RelatedFinder>(<%= relatedFinderCount %>);
                <% for (RelationshipAttribute relationshipAttribute : relationshipAttributes) { %>
                    <% if (!relationshipAttribute.hasParameters()) { %>
                        relatedFinders.add(this.<%= relationshipAttribute.getName() %>());
                    <% } %>
                <% } %>
                relationshipFinders = Collections.unmodifiableList(relatedFinders);
           }
           return relationshipFinders;
        }

        public List<RelatedFinder> getDependentRelationshipFinders()
        {
           if (dependentRelationshipFinders == null)
           {
                <% int dependentRelationshipCount = 0; %>
                <% for (RelationshipAttribute relationshipAttribute : relationshipAttributes) { %>
                    <% if (relationshipAttribute.isRelatedDependent()) { %>
                        <% dependentRelationshipCount++; %>
                    <% } %>
                <% } %>
                List<RelatedFinder> dependentRelatedFinders = new ArrayList<RelatedFinder>(<%= dependentRelationshipCount %>);
                <% for (RelationshipAttribute relationshipAttribute : relationshipAttributes) { %>
                    <% if (relationshipAttribute.isRelatedDependent()) { %>
                        dependentRelatedFinders.add(this.<%= relationshipAttribute.getName() %>());
                    <% } %>
                <% } %>
                dependentRelationshipFinders = Collections.unmodifiableList(dependentRelatedFinders);
           }
           return dependentRelationshipFinders;
        }

        public <%= wrapper.getClassName() %> findOne(com.gs.fw.finder.Operation operation)
        {
            return <%= wrapper.getFinderClassName() %>.findOne(operation, false);
        }

        public <%= wrapper.getClassName() %> findOneBypassCache(com.gs.fw.finder.Operation operation)
        {
            return <%= wrapper.getFinderClassName() %>.findOne(operation, true);
        }

        public MithraList<? extends <%= wrapper.getClassName() %>> findMany(com.gs.fw.finder.Operation operation)
        {
            return new <%= wrapper.getListClassName() %>((Operation) operation);
        }

        public MithraList<? extends <%= wrapper.getClassName() %>> findManyBypassCache(com.gs.fw.finder.Operation operation)
        {
            <%= wrapper.getListClassName() %> result = (<%= wrapper.getListClassName() %>) this.findMany(operation);
            result.setBypassCache(true);
            return result;
        }

        public MithraList<? extends <%= wrapper.getClassName() %>> constructEmptyList()
        {
            return new <%= wrapper.getListClassName() %>();
        }

        public int getSerialVersionId()
        {
            return <%= wrapper.getSerialVersionId() %>;
        }

        public boolean isPure()
        {
            return <%= wrapper.isPure() %>;
        }

        public boolean isTemporary()
        {
            return <%= wrapper.isTemporary() %>;
        }

        public int getHierarchyDepth()
        {
            return <%= wrapper.getHierarchyDepth() %>;
        }

        <% if (wrapper.hasSourceAttribute()) { %>
            public SourceAttributeType getSourceAttributeType()
            {
                return <%= wrapper.getFinderClassName() %>.zSourceAttributeType;
            }
        <% } %>
        <% for (EmbeddedValue evo : embeddedValueObjects) { %>
            <% String attributeClassName = wrapper.getClassName() + StringUtility.firstLetterToUpper(evo.getNestedName()) + "Attribute"; %>
            public <%= evo.getQualifiedAttributeClassName() %> <%= evo.getNestedName() %>()
            {
                <%= evo.getQualifiedAttributeClassName() %> result = this.<%= evo.getNestedName() %>;
                if (result == null)
                {
                    result = new <%= attributeClassName %>(this, this.zGetValueSelector());
                }
                this.<%= evo.getNestedName() %> = result;
                return result;
            }
        <% } %>
        <% for (AbstractAttribute normalAttribute : normalAttributes) { %>
            public <%= normalAttribute.getFinderAttributeType() %><ParentOwnerType> <%= normalAttribute.getName() %>()
            {
                <%= normalAttribute.getFinderAttributeType() %><ParentOwnerType> result = this.<%= normalAttribute.getName() %>;
                if (result == null)
                {
                    <% if (normalAttribute.needsGeneratedAttribute()) { %>
                    result = mapper == null ? new <%= wrapper.getClassName() %><%= StringUtility.firstLetterToUpper(normalAttribute.getName()) %>Attribute():
                        new Mapped<%= normalAttribute.getFinderAttributeType() %>(<%= wrapper.getClassName() %>Finder.<%= normalAttribute.getName() %>(), this.mapper, this.zGetValueSelector());
                    <% } else { %>
                    result = mapper == null ? <%= normalAttribute.getFinderAttributeSuperClassType() %>.generate(<%= normalAttribute.getGeneratorParameters()%>) :
                        new Mapped<%= normalAttribute.getFinderAttributeType() %>(<%= wrapper.getClassName() %>Finder.<%= normalAttribute.getName() %>(), this.mapper, this.zGetValueSelector());
                        <% if (normalAttribute instanceof Attribute && ((Attribute) normalAttribute).getOwningRelationshipName() != null) { %>
                    result.zSetOwningRelationship("<%= ((Attribute) normalAttribute).getOwningRelationshipName()%>");
                        <% } %>
                        <% if (normalAttribute instanceof Attribute && ((Attribute) normalAttribute).getOwningReverseRelationshipName() != null) { %>
                    result.zSetOwningReverseRelationship("<%= ((Attribute) normalAttribute).getOwningReverseRelationshipOwningClassPackage()%>", "<%= ((Attribute) normalAttribute).getOwningReverseRelationshipOwningClass()%>", "<%= ((Attribute) normalAttribute).getOwningReverseRelationshipName()%>");
                        <% } %>
                    <% } %>
                    this.<%= normalAttribute.getName() %> = result;
                }
                return result;
            }
        <% } %>
        <% for (AsOfAttribute asOfAttribute : asOfAttributes) { %>
            public <%= asOfAttribute.getFinderAttributeType() %><ParentOwnerType> <%= asOfAttribute.getName() %>()
            {
                <%= asOfAttribute.getFinderAttributeType() %><ParentOwnerType> result = this.<%= asOfAttribute.getName() %>;
                if (result == null)
                {

                    result = mapper == null ? <% if (asOfAttribute.isInfinityNull()){%>AsOfAttributeInfiniteNull<%} else {%>AsOfAttribute<%}%>.generate(<%= asOfAttribute.getGeneratorParameters()%>) :
                        new Mapped<%= asOfAttribute.getFinderAttributeType() %>(<%= wrapper.getClassName() %>Finder.<%= asOfAttribute.getName() %>(), this.mapper, this.zGetValueSelector());
                    this.<%= asOfAttribute.getName() %> = result;
                }
                return result;
            }
        <% } %>
        <% for (RelationshipAttribute relationshipAttribute : relationshipAttributes) { %>
            <% String attributeValueSelector = ""; %>
            <% attributeValueSelector = ", this.zGetValueSelector()"; %>
            <% if(wrapper.isTablePerClassSuperClass()){%>
            public <%= relationshipAttribute.getFinderAttributeTypeForRelatedClass() %><ParentOwnerType, <%= relationshipAttribute.getTypeAsString() %>, ? extends <%= wrapper.getClassName() %>> <%= relationshipAttribute.getName() %>(<%= relationshipAttribute.getParameters() %>)
            <%}else{%>
            public <%= relationshipAttribute.getFinderAttributeTypeForRelatedClass() %><ParentOwnerType, <%= relationshipAttribute.getTypeAsString() %>, <%= wrapper.getClassName() %>> <%= relationshipAttribute.getName() %>(<%= relationshipAttribute.getParameters() %>)
            <%}%>
            {
                <% if (!relationshipAttribute.hasParameters()) { %>
                    <%= relationshipAttribute.getFinderAttributeType() %><ParentOwnerType> result = this.<%= relationshipAttribute.getName() %>;
                    if (result == null)
                    {
                <% } else { %>
                    <%= relationshipAttribute.getFinderAttributeType() %><ParentOwnerType> result = null;
                <% } %>
                Mapper newMapper = combineWithMapperIfExists(<%= relationshipAttribute.getReverseMapperName() %>(<%= relationshipAttribute.getParameterVariables() %>));
                <% if (relationshipAttribute.isOptimizable()) { %>
                    result = new <%= relationshipAttribute.getFinderAttributeType() %><ParentOwnerType>(newMapper <%= attributeValueSelector %> <%= relationshipAttribute.getParameterVariablesWithComma() %>)
                    {
                        <% List replacedAttributes = relationshipAttribute.getEqualityRelationalExpressions(); %>
                        <% for (int k = 0; k < replacedAttributes.size(); k++) { %>
                            <% ASTRelationalExpression exp = (ASTRelationalExpression) replacedAttributes.get(k); %>
                            <% AbstractAttribute rightAttr = exp.getLeft().getAttribute(); %>
                            <% AbstractAttribute leftAttr = ((ASTAttributeName)exp.getRight()).getAttribute(); %>
                            public <%= leftAttr.getFinderAttributeType() %> <%= rightAttr.getName() %>()
                            {
                                return <%= wrapper.getClassName() %>RelatedFinder.this.<%= leftAttr.getName() %>();
                            }
                        <% } %>
                    };
                <% } else { %>
                    <% if (relationshipAttribute.getCardinality().isToMany()) { %>
                        newMapper.setToMany(true);
                    <% } else { %>
                        newMapper.setToMany(false);
                    <% } %>
                    result = new <%= relationshipAttribute.getFinderAttributeType() %><ParentOwnerType>(newMapper <%= attributeValueSelector %> <%= relationshipAttribute.getParameterVariablesWithComma() %>);
                <% } %>
                <% if (!relationshipAttribute.hasParameters()) { %>
                        this.<%= relationshipAttribute.getName() %> = result;
                    }
                <% } %>
                return result;
            }
        <% } %>

        <% if (wrapper.hasSourceAttribute()) { %>
            public Attribute getSourceAttribute()
            {
               return <%= wrapper.getClassName() %>Finder.<%= wrapper.getSourceAttribute().getName() %>();
            }
        <% } else { %>
            public Attribute getSourceAttribute()
            {
               return null;
            }
        <% } %>

        private Mapper combineWithMapperIfExists(Mapper newMapper)
        {
            if (this.mapper != null)
            {
                return new LinkedMapper(this.mapper, newMapper);
            }
            return newMapper;
        }

        public Attribute[] getPrimaryKeyAttributes()
        {
             return <%= wrapper.getClassName() %>Finder.getPrimaryKeyAttributes();
        }

        public VersionAttribute getVersionAttribute()
        {
            <% if (wrapper.hasOptimisticLockAttribute()) { %>
                return (VersionAttribute) <%= wrapper.getClassName() %>Finder.<%= wrapper.getOptimisticLockAttribute().getName() %>();
            <% } else { %>
                return null;
            <% } %>
        }

        public MithraObjectPortal getMithraObjectPortal()
        {
            return <%= wrapper.getClassName() %>Finder.getMithraObjectPortal();
        }

    }

    public static class <%= wrapper.getClassName() %>CollectionFinder<ParentOwnerType, ReturnType extends List, OwnerType> extends <%= wrapper.getClassName() %>RelatedFinder<ParentOwnerType, ReturnType, <%= wrapper.getListInterfaceName() %>, OwnerType>
    {
        public <%= wrapper.getClassName() %>CollectionFinder(Mapper mapper)
        {
            super(mapper);
        }

    }

    public static abstract class <%= wrapper.getClassName() %>CollectionFinderForRelatedClasses<ParentOwnerType, ReturnType extends List, OwnerType>
			extends <%= wrapper.getClassName() %>CollectionFinder<ParentOwnerType, ReturnType, OwnerType>
			implements DeepRelationshipAttribute<ParentOwnerType, ReturnType>
	{
		public <%= wrapper.getClassName() %>CollectionFinderForRelatedClasses(Mapper mapper)
		{
			super(mapper);
		}

        protected NormalAndListValueSelector zGetValueSelector()
        {
            return this;
        }
	}

	public static class <%= wrapper.getClassName() %>SingleFinder<ParentOwnerType, ReturnType, OwnerType> extends <%= wrapper.getClassName() %>RelatedFinder<ParentOwnerType, ReturnType, <%= wrapper.getListInterfaceName() %>, OwnerType>
            implements ToOneFinder
    {
        public <%= wrapper.getClassName() %>SingleFinder(Mapper mapper)
        {
            super(mapper);
        }

        public <%= wrapper.getClassName() %>SingleFinder()
        {
            super(null);
        }

        public Operation eq(<%= wrapper.getClassName() %> other)
        {
            return this.<%= pkAttributes[0].getName() %>().eq(other.<%= pkAttributes[0].getGetter() %>())
            <% for(int i = 1; i < pkAttributes.length; i++) { %>
                .and(this.<%= pkAttributes[i].getName()%>().eq(other.<%= pkAttributes[i].getGetter() %>()))
            <% }
               if (wrapper.hasSourceAttribute()) { %>
                .and(this.<%= wrapper.getSourceAttribute().getName() %>().eq(other.<%= wrapper.getSourceAttribute().getGetter() %>()))
            <% } %>
            ;
        }

    <% for(Index index: indexList) { %>
        // this implementation uses private API. Do NOT copy to application code. Application code must use normal operations for lookups.
        public <%= wrapper.getClassName() %> findBy<%= index.getSanitizedUpperCaseName() %>(<%= index.getFindByParameters() %>)
        {
            <%if(index.isSameAsPk()) { %>
            return this.findByPrimaryKey(<%= index.getSameIndex().getFindByVariables() %>);
            <%} else if(index.getSameIndex() != null) { %>
            return this.findBy<%=index.getSameIndex().getSanitizedUpperCaseName()%>(<%=index.getSameIndex().getFindByVariables()%>);
            <%} else {%>
            <%= wrapper.getClassName() %> _result = null;
            Operation _op = null;
            Object _related = null;

            <% if (index.hasFastPathLookup()) { %>
            <%= index.getLookupNotNullCheck() %>
            {
                <%= index.getBeanType() %> _bean = <%= index.getBeanType() %>.POOL.getOrConstruct();
                <%= index.getLookupBeanSetters() %>
                MithraObjectPortal _portal = this.getMithraObjectPortal();
                _related = _portal.<%= index.getCacheLookupMethod() %>(<%= index.getLookupCacheParameters() %>);
                _bean.release();
            }
            <% } %>
            if (!(_related instanceof NulledRelation)) _result = (<%= wrapper.getClassName() %>) _related;

            if (_related == null)
            {
                _op = <%= index.getLookupOperation() %>;
            }
            if (_op != null)
            {
                _result = this.findOne(_op);
            }

            return _result;
            <%}%>
        }

    <% } %>

    }

    public static abstract class <%= wrapper.getClassName() %>SingleFinderForRelatedClasses<ParentOwnerType, ReturnType, OwnerType> extends <%= wrapper.getClassName() %>SingleFinder<ParentOwnerType, ReturnType, OwnerType> implements DeepRelationshipAttribute<ParentOwnerType, ReturnType>
	{
		public <%= wrapper.getClassName() %>SingleFinderForRelatedClasses(Mapper mapper)
		{
			super(mapper);
		}

        protected NormalAndListValueSelector zGetValueSelector()
        {
            return this;
        }
	}
    <% for (RelationshipAttribute relationshipAttribute : relationshipAttributes) { %>
        <% if (!relationshipAttribute.isReverseRelationship()) { %>
            <% if (relationshipAttribute.hasParameters()) { %>
                public static Mapper zGet<%= relationshipAttribute.getMapperPartialName() %>ReverseMapper(<%= relationshipAttribute.getParameters() %>)
                {
                    return zConstruct<%= relationshipAttribute.getMapperPartialName() %>ReverseMapper(<%= relationshipAttribute.getParameterVariables() %>);
                }

                public static Mapper zGet<%= relationshipAttribute.getMapperPartialName() %>Mapper(<%= relationshipAttribute.getParameters() %>)
                {
                    return zConstruct<%= relationshipAttribute.getMapperPartialName() %>Mapper(<%= relationshipAttribute.getParameterVariables() %>);
                }
            <% } else { %>
                private static Mapper <%= relationshipAttribute.getName() %>ReverseMapper = null;

                public static Mapper zGet<%= relationshipAttribute.getMapperPartialName() %>ReverseMapper()
                {
                    if (<%= relationshipAttribute.getName() %>ReverseMapper == null)
                    {
                        <%= relationshipAttribute.getName() %>ReverseMapper = zConstruct<%= relationshipAttribute.getMapperPartialName() %>ReverseMapper();
                    }
                    return <%= relationshipAttribute.getName() %>ReverseMapper;
                }

                private static Mapper <%= relationshipAttribute.getName() %>Mapper = null;

                public static Mapper zGet<%= relationshipAttribute.getMapperPartialName() %>Mapper()
                {
                    if (<%= relationshipAttribute.getName() %>Mapper == null)
                    {
                        <%= relationshipAttribute.getName() %>Mapper = zConstruct<%= relationshipAttribute.getMapperPartialName() %>Mapper();
                    }
                    return <%= relationshipAttribute.getName() %>Mapper;
                }
            <% } %>
            <% if (relationshipAttribute.hasDangleMapper()) { %>
                public static Mapper zGet<%= relationshipAttribute.getMapperPartialName() %>DangleMapper(<%= relationshipAttribute.getParameters() %>)
                {
                    <%= relationshipAttribute.constructDangleMapper() %>
                }
            <% } %>
            private static Mapper <%= relationshipAttribute.getName()%>PureReverseMapper = null;

            public static Mapper zGet<%= relationshipAttribute.getMapperPartialName() %>PureReverseMapper()
            {
                if (<%= relationshipAttribute.getName() %>PureReverseMapper == null)
                {
                    <%= relationshipAttribute.getName() %>PureReverseMapper = zConstruct<%= relationshipAttribute.getMapperPartialName() %>PureReverseMapper();
                }
                return <%= relationshipAttribute.getName() %>PureReverseMapper;
            }

            private static Mapper zConstruct<%= relationshipAttribute.getMapperPartialName() %>PureReverseMapper()
            {
                    <%= relationshipAttribute.constructPureReverseMapper() %>
            }

            private static Mapper zConstruct<%= relationshipAttribute.getMapperPartialName() %>ReverseMapper(<%= relationshipAttribute.getParameters() %>)
            {
                <%= relationshipAttribute.constructReverseMapper() %>
            }

            private static Mapper zConstruct<%= relationshipAttribute.getMapperPartialName() %>Mapper(<%= relationshipAttribute.getParameters() %>)
            {
                <%= relationshipAttribute.constructMapper() %>
            }
        <% } %>
        <% if (relationshipAttribute.hasMapperFragment()) { %>
            <% if (relationshipAttribute.hasMapperFragmentParameters()) { %>
                public static Mapper zGet<%= relationshipAttribute.getMapperFragmentName() %>MapperFragment(<%= relationshipAttribute.getMapperFragmentParameters() %>)
                {
                    return zConstruct<%= relationshipAttribute.getMapperFragmentName() %>MapperFragment(<%= relationshipAttribute.getMapperFragmentParameterVariables() %>);
                }
            <% } else { %>
                private static Mapper <%= relationshipAttribute.getName() %>MapperFragment = null;

                public static Mapper zGet<%= relationshipAttribute.getMapperFragmentName() %>MapperFragment()
                {
                    if (<%= relationshipAttribute.getName() %>MapperFragment == null)
                    {
                        <%= relationshipAttribute.getName() %>MapperFragment = zConstruct<%= relationshipAttribute.getMapperFragmentName() %>MapperFragment();
                    }
                    return <%= relationshipAttribute.getName() %>MapperFragment;
                }
            <% } %>
            private static Mapper zConstruct<%= relationshipAttribute.getMapperFragmentName() %>MapperFragment(<%= relationshipAttribute.getMapperFragmentParameters() %>)
            {
                <%= relationshipAttribute.constructMapperFragment() %>
            }
        <% } %>
    <% } %>
    <% for (EmbeddedValue evo : embeddedValueObjects) { %>
        <%= evo.getVisibility() %> static <%= evo.getQualifiedAttributeClassName() %> <%= evo.getNestedName() %>()
        {
            return finder.<%= evo.getNestedName() %>();
        }
    <% } %>
    <% for (AbstractAttribute normalAttribute : normalAttributes) { %>
        <% if (normalAttribute.isMapped()) { %>
        /** maps to <%= normalAttribute.getTableQualifiedMappedColumnName() %> **/
        <% } %>
        public static <%= normalAttribute.getFinderAttributeType() %><<%= wrapper.getClassName() %>> <%= normalAttribute.getName() %>()
        {
            return finder.<%= normalAttribute.getName() %>();
        }
    <% } %>
    <% for (AsOfAttribute asOfAttribute : asOfAttributes) { %>
        public static <%= asOfAttribute.getFinderAttributeType() %><<%= wrapper.getClassName() %>> <%= asOfAttribute.getName() %>()
        {
            return finder.<%= asOfAttribute.getName() %>();
        }
    <% } %>
    <% for (int i = 0; i < relationshipAttributes.length; i++) { %>
        <%if(wrapper.isTablePerClassSuperClass()){%>
        public static <%= relationshipAttributes[i].getFinderAttributeTypeForRelatedClass() %><<%= wrapper.getClassName() %>, <%= relationshipAttributes[i].getTypeAsString() %>, ? extends <%= wrapper.getClassName() %>> <%= relationshipAttributes[i].getName() %>(<%= relationshipAttributes[i].getParameters() %>)
        <%}else{%>
        public static <%= relationshipAttributes[i].getFinderAttributeTypeForRelatedClass() %><<%= wrapper.getClassName() %>, <%= relationshipAttributes[i].getTypeAsString() %>, <%= wrapper.getClassName() %>> <%= relationshipAttributes[i].getName() %>(<%= relationshipAttributes[i].getParameters() %>)
        <%}%>
        {
            return finder.<%= relationshipAttributes[i].getName() %>(<%= relationshipAttributes[i].getParameterVariables() %>);
        }

	public static class <%= relationshipAttributes[i].getFinderAttributeType() %><ParentOwnerType> extends <%= relationshipAttributes[i].getFinderAttributeTypeForRelatedClass() %><ParentOwnerType, <%= relationshipAttributes[i].getTypeAsString() %>, <%= wrapper.getClassName() %>>
        {
            <% for (int j = 0; j < relationshipAttributes[i].getParameterCount(); j++) { %>
                private <%= relationshipAttributes[i].getParameterTypeAt(j) %> <%= relationshipAttributes[i].getParameterVariableAt(j) %>;
            <% } %>

            public <%= relationshipAttributes[i].getFinderAttributeType() %>(Mapper mapper, NormalAndListValueSelector parentSelector <%= relationshipAttributes[i].getParametersWithComma() %>)
            {
                super(mapper);
                this._parentSelector = (AbstractRelatedFinder) parentSelector;
                <% for (int j = 0; j < relationshipAttributes[i].getParameterCount(); j++) { %>
                    this.<%= relationshipAttributes[i].getParameterVariableAt(j)%> = <%= relationshipAttributes[i].getParameterVariableAt(j) %>;
                <% } %>
                this._orderBy = <%= relationshipAttributes[i].getCompleteOrderByForRelationship()%>;
                this._type = <%= relationshipAttributes[i].getDeepFetchType()%>;
                this._name = "<%= relationshipAttributes[i].getName() %>";
                <% if (relationshipAttributes[i].isExtractorBasedMultiEquality()) { %>
                this.zSetRelationshipMultiExtractor(<%= relationshipAttributes[i].getRelationshipMultiExtractorConstructor() %>);
                <% } %>
            }

            public DeepRelationshipAttribute copy()
            {
                return new <%= relationshipAttributes[i].getFinderAttributeType() %>(zGetMapper(), (NormalAndListValueSelector) this._parentSelector
                    <% for(int j = 0; j < relationshipAttributes[i].getParameterCount(); j++) { %>
                        , this.<%= relationshipAttributes[i].getParameterVariableAt(j) %>
                    <% } %>
                );
            }

            <% if (wrapper.isTransactional() && relationshipAttributes[i].mustPersistRelationshipChanges()){ %>
                public boolean isModifiedSinceDetachment(MithraTransactionalObject _obj)
                {
                    return ((<%= wrapper.getImplClassName() %>) _obj).is<%= StringUtility.firstLetterToUpper(relationshipAttributes[i].getName()) %>ModifiedSinceDetachment();
                }
            <% } %>

	    protected <%= relationshipAttributes[i].getTypeAsString() %> plainValueOf(<%= wrapper.getClassName() %> _obj)
            {
                return _obj.<%= relationshipAttributes[i].getGetter() %>(<%= relationshipAttributes[i].getParameterVariables() %>);
            }

            protected <%= relationshipAttributes[i].getListInterfaceName() %> plainListValueOf(Object _obj)
            {
                return ((<%= wrapper.getListInterfaceName() %>)_obj).<%= relationshipAttributes[i].getGetterNameForList() %>(<%= relationshipAttributes[i].getParameterVariables() %>);
            }
        }
    <% } %>

    public static Operation eq(<%= wrapper.getClassName() %> other)
    {
        return finder.eq(other);
    }

    public static Operation all()
    {
        return new All(<%= pkAttributes[0].getName() %>());
    }

    public static <%= wrapper.getClassName() %>SingleFinder<<%= wrapper.getClassName() %>, Object, <%= wrapper.getClassName() %>> getFinderInstance()
    {
        return finder;
    }

    public static Attribute[] getPrimaryKeyAttributes()
    {
        return new Attribute[] {
            <%= pkAttributes[0].getName() %>()
            <% for (int i = 1; i < pkAttributes.length; i++) { %>
                , <%= pkAttributes[i].getName() %>()
            <% } %>
            <% if (wrapper.hasSourceAttribute()) { %>
                , <%= wrapper.getSourceAttribute().getName() %>()
            <% } %>
        };
    }

    public static Attribute[] getImmutableAttributes()
    {
        return new Attribute[] {
            <%= persistantAttributes[0].getName() %>()
            <% for (int i = 0; i < persistantAttributes.length; i++) { %>
                <% if (persistantAttributes[i].isImmutable()) { %>
                    , <%= persistantAttributes[i].getName() %>()
                <% } %>
            <% } %>
            <% if (wrapper.hasSourceAttribute()) { %>
                , <%= wrapper.getSourceAttribute().getName() %>()
            <% } %>
        };
    }

    public static AsOfAttribute[] getAsOfAttributes()
    {
        <% if (wrapper.hasAsOfAttributes()) { %>
            return new AsOfAttribute[] {
                <%= asOfAttributes[0].getName() %>()
                <% for (int i = 1; i < asOfAttributes.length; i++) { %>
                    , <%= asOfAttributes[i].getName() %>()
                <% } %>
            };
        <% } else { %>
            return null;
        <% } %>
    }

    protected static void initializeIndicies(Cache cache)
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            <% for (int i = 0; i < indicies.size(); i++) { %>
                <% Index index = (Index) indicies.get(i); %>
                <% AbstractAttribute[] indexAttributes = index.getAttributes(); %>
                <% if (index.isUnique()) { %>
                    INDEX_<%= index.getName()%> = cache.addTypedUniqueIndex(new Attribute[] {
                        <%= indexAttributes[0].getName() %>()
                        <% for (int j = 1; j < indexAttributes.length; j++) { %>
                            , <%= indexAttributes[j].getName() %>()
                        <% } %>
                    }, <%= wrapper.getClassName()%>.class, <%= wrapper.getDataClassName()%>.class);
                <% } else { %>
                    cache.addTypedIndex(new Attribute[] {
                        <%= indexAttributes[0].getName() %>()
                        <% for (int j = 1; j < indexAttributes.length; j++) { %>
                            , <%= indexAttributes[j].getName() %>()
                        <% } %>
                    }, <%= wrapper.getClassName() %>.class, <%= wrapper.getDataClassName() %>.class);
                <% } %>
            <% } %>
        <% } else { %>
            <% for (int i = 0; i < indicies.size(); i++) { %>
                <% Index index = (Index) indicies.get(i); %>
                <% AbstractAttribute[] indexAttributes = index.getAttributes(); %>
                <% if (index.isUnique()) { %>
                    INDEX_<%= index.getName()%> = cache.addUniqueIndex("<%= index.getName() %>", new Attribute[] {
                        <%= indexAttributes[0].getName() %>()
                        <% for(int j = 1; j < indexAttributes.length; j++) { %>
                            , <%= indexAttributes[j].getName() %>()
                        <% } %>
                    });
                <% } else { %>
                    cache.addIndex("<%= index.getName() %>", new Attribute[] {
                        <%= indexAttributes[0].getName() %>()
                        <% for (int j = 1; j < indexAttributes.length; j++) { %>
                            , <%= indexAttributes[j].getName() %>()
                        <% } %>
                    });
                <% } %>
            <% } %>
        <% } %>
    }

    <% for (int i = 0; i < indicies.size(); i++) { %>
        <% Index index = (Index) indicies.get(i); %>
        <% if (index.isUnique()) { %>
            public static int zGetIndex<%= index.getName() %>Ref() { return  INDEX_<%= index.getName() %>; }
        <% } %>
    <% } %>
    protected static void initializePortal(MithraObjectDeserializer objectFactory, Cache cache,
        MithraConfigurationManager.Config config)
    {
        initializeIndicies(cache);
        isFullCache = cache.isFullCache();
        isOffHeap = cache.isOffHeap();
        MithraObjectPortal portal;
        if (config.isParticipatingInTx())
        {
            portal = new <%= wrapper.getPortalClassForTxRuntime() %>(objectFactory, cache, getFinderInstance(),
            config.getRelationshipCacheSize(), config.getMinQueriesToKeep(), <%= wrapper.getSuperClassFinders() %>,
            <%= wrapper.getSubClassFinders()%>, <%= wrapper.getUniqueAliasAsString()%>, <%= wrapper.getHierarchyDepth() %>,
            <%= wrapper.getLocalObjectPersister() %>);
        }
        else
        {
            portal = new <%= wrapper.getPortalClassForNoTxRuntime() %>(objectFactory, cache, getFinderInstance(),
            config.getRelationshipCacheSize(), config.getMinQueriesToKeep(), <%= wrapper.getSuperClassFinders() %>,
            <%= wrapper.getSubClassFinders()%>, <%= wrapper.getUniqueAliasAsString()%>, <%= wrapper.getHierarchyDepth() %>,
            <%= wrapper.getLocalObjectPersister() %>);
        }
        <% if (wrapper.hasOptimisticLockAttribute()) { %>
            portal.setDefaultTxParticipationMode(ReadCacheWithOptimisticLockingTxParticipationMode.getInstance());
        <% } %>
        <% if (wrapper.isIndependent()) { %>
            portal.setIndependent(true);
        <% } %>
        <% if (wrapper.hasShadowAttributes()) { %>
            portal.setUseMultiUpdate(false);
        <% } %>
        <% if (wrapper.hasForeignKeys()) { %>
            <% MithraObjectTypeWrapper[] fks = wrapper.getForeignKeys(); %>
            portal.setParentFinders(new RelatedFinder[] { <% for(MithraObjectTypeWrapper fk: fks) { %><%=fk.getFinderClassName()%>.getFinderInstance(),<% } %>});
        <% } %>
        config.initializePortal(portal);
        <% if (!wrapper.isTemporary()) { %>
            objectPortal.destroy();
        <% } %>
        objectPortal = portal;
    }

    protected static void initializeClientPortal(MithraObjectDeserializer objectFactory, Cache cache,
        MithraConfigurationManager.Config config)
    {
        initializeIndicies(cache);
        isFullCache = cache.isFullCache();
        isOffHeap = cache.isOffHeap();
        MithraObjectPortal portal;
        if (config.isParticipatingInTx())
        {
            portal = new <%= wrapper.getPortalClassForTxRuntime() %>(objectFactory, cache, getFinderInstance(),
                config.getRelationshipCacheSize(), config.getMinQueriesToKeep(),
                <%= wrapper.getSuperClassFinders() %>, <%= wrapper.getSubClassFinders() %>,
                <%= wrapper.getUniqueAliasAsString() %>, <%= wrapper.getHierarchyDepth() %>,
                <%= wrapper.getRemoteObjectPersister() %>);
        }
        else
        {
            portal = new <%= wrapper.getPortalClassForNoTxRuntime() %>(objectFactory, cache, getFinderInstance(),
                config.getRelationshipCacheSize(), config.getMinQueriesToKeep(),
                <%= wrapper.getSuperClassFinders() %>, <%= wrapper.getSubClassFinders() %>,
                <%= wrapper.getUniqueAliasAsString() %>, <%= wrapper.getHierarchyDepth() %>,
                <%= wrapper.getRemoteObjectPersister() %>);
        }
        <% if (wrapper.hasOptimisticLockAttribute()) { %>
            portal.setDefaultTxParticipationMode(ReadCacheWithOptimisticLockingTxParticipationMode.getInstance());
        <% } %>
        <% if (wrapper.isIndependent()) { %>
            portal.setIndependent(true);
        <% } %>
        <% if (wrapper.hasShadowAttributes()) { %>
            portal.setUseMultiUpdate(false);
        <% } %>
        <% if (wrapper.hasForeignKeys()) { %>
            <% MithraObjectTypeWrapper[] fks = wrapper.getForeignKeys(); %>
            portal.setParentFinders(new RelatedFinder[] { <% for(MithraObjectTypeWrapper fk: fks) { %><%=fk.getFinderClassName()%>.getFinderInstance(),<% } %>});
        <% } %>
        config.initializePortal(portal);
        <% if (!wrapper.isTemporary()) { %>
            objectPortal.destroy();
        <% } %>
        objectPortal = portal;
    }

    public static boolean isFullCache()
    {
        return isFullCache;
    }

    public static boolean isOffHeap()
    {
        return isOffHeap;
    }

    public static Attribute getAttributeByName(String attributeName)
    {
        return finder.getAttributeByName(attributeName);
    }

    public static <%= valueSelectorClassName %> getAttributeOrRelationshipSelector(String attributeName)
    {
        return finder.getAttributeOrRelationshipSelector(attributeName);
    }

    public static RelatedFinder getRelatedFinderByName(String relationshipName)
    {
        return finder.getRelationshipFinderByName(relationshipName);
    }

    public static DoubleAttribute[] zGetDoubleAttributes()
    {
        DoubleAttribute[] result = new DoubleAttribute[<%= wrapper.getDoubleAttributeCount() %>];
        <% int pos = 0; %>
        <% for (int i = 0; i < normalAttributes.length; i++) { %>
            <% if (normalAttributes[i].isDoubleAttribute()) { %>
                result[<%= pos++ %>] = <%= normalAttributes[i].getName() %>();
            <% } %>
        <% } %>
        return result;
    }

    public static BigDecimalAttribute[] zGetBigDecimalAttributes()
    {
        BigDecimalAttribute[] result = new BigDecimalAttribute[<%= wrapper.getBigDecimalAttributeCount() %>];
        <% int pos_bd = 0; %>
        <% for (int i = 0; i < normalAttributes.length; i++) { %>
            <% if (normalAttributes[i].isBigDecimalAttribute()) { %>
                result[<%= pos_bd++ %>] = <%= normalAttributes[i].getName() %>();
            <% } %>
        <% } %>
        return result;
    }

    public static void zResetPortal()
    {
        objectPortal.destroy();
        objectPortal = new UninitializedPortal("<%= wrapper.getPackageName() %>.<%= wrapper.getClassName() %>");
        isFullCache = false;
        isOffHeap = false;
    }

    public static void setTransactionModeFullTransactionParticipation(MithraTransaction tx)
    {
        tx.setTxParticipationMode(objectPortal, FullTransactionalParticipationMode.getInstance());
    }

    <% if (wrapper.hasOptimisticLocking()) { %>
        public static void setTransactionModeReadCacheWithOptimisticLocking(MithraTransaction tx)
        {
            tx.setTxParticipationMode(objectPortal, ReadCacheWithOptimisticLockingTxParticipationMode.getInstance());
        }
    <% } else if (wrapper.isTransactional() && !wrapper.hasAsOfAttributes()) { %>
        public static void setTransactionModeReadCacheUpdateCausesRefreshAndLock(MithraTransaction tx)
        {
            tx.setTxParticipationMode(objectPortal, ReadCacheUpdateCausesRefreshAndLockTxParticipationMode.getInstance());
        }

    <% } else if (wrapper.isTransactional()) { %>
        public static void setTransactionModeDangerousNoLocking(MithraTransaction tx)
        {
            tx.setTxParticipationMode(objectPortal, ReadCacheUpdateCausesRefreshAndLockTxParticipationMode.getInstance());
        }
    <% } %>

    <% if (wrapper.isTemporary()) { %>
    public static void setTempConfig(MithraConfigurationManager.TempObjectConfig config)
    {
        tempContextContainer.setTempConfig(config);
    }

    public static TemporaryContext createTemporaryContext(<%= wrapper.getUserFriendlySourceAttributeVariableDeclaration() %>)
    {
        return tempContextContainer.createTemporaryContext(<%= wrapper.getUserFriendlySourceAttributeVariableName() %>);
    }

    public static Operation existsWithJoin(
            <% if (wrapper.hasSourceAttribute()) { %>
                <%= wrapper.getSourceAttribute().getFinderAttributeType() %> joined<%= StringUtility.firstLetterToUpper(wrapper.getSourceAttribute().getName()) %>,
            <% } %>
            <%= pkAttributes[0].getFinderAttributeType() %> joined<%= StringUtility.firstLetterToUpper(pkAttributes[0].getName()) %>
            <% for (int i = 1; i < pkAttributes.length; i++) { %>
                , <%= pkAttributes[i].getFinderAttributeType() %> joined<%= StringUtility.firstLetterToUpper(pkAttributes[i].getName()) %>
            <% } %> )
    {
        return MappedOperation.createExists(
            new Attribute[] {
            <% if (wrapper.hasSourceAttribute()) { %>
                <%= wrapper.getSourceAttribute().getName() %>(),
            <% } %>
            <%= pkAttributes[0].getName() %>()
            <% for (int i = 1; i < pkAttributes.length; i++) { %>
                , <%= pkAttributes[i].getName() %>()
            <% } %>}
            <% if (wrapper.hasSourceAttribute()) { %>
                , joined<%= StringUtility.firstLetterToUpper(wrapper.getSourceAttribute().getName()) %>
            <% } %>
            <% for (int i = 0; i < pkAttributes.length; i++) { %>
                , joined<%= StringUtility.firstLetterToUpper(pkAttributes[i].getName()) %>
            <% } %>
            );
    }
    <% } %>

    <% if (wrapper.hasSourceAttribute()) { %>
        public static void registerForNotification(<%= wrapper.getSourceAttribute().getType().getJavaTypeString() %> <%= wrapper.getSourceAttribute().getName() %>, MithraApplicationClassLevelNotificationListener listener)
        {
            Set sourceValueSet = new HashSet();
            sourceValueSet.add(<%= wrapper.getSourceAttribute().getName() %>);
            getMithraObjectPortal().registerForApplicationClassLevelNotification(sourceValueSet, listener);
        }

        public static void registerForNotification(Set<<%= wrapper.getSourceAttribute().getType().getJavaTypeStringPrimary() %>> <%= wrapper.getSourceAttribute().getName() %>Set, MithraApplicationClassLevelNotificationListener listener)
        {
            getMithraObjectPortal().registerForApplicationClassLevelNotification(<%= wrapper.getSourceAttribute().getName() %>Set, listener);
        }
    <% } else { %>
        public static void registerForNotification(MithraApplicationClassLevelNotificationListener listener)
        {
            getMithraObjectPortal().registerForApplicationClassLevelNotification(listener);
        }
    <% } %>
}
