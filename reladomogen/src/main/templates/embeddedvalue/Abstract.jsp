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
<%@ page import="com.gs.fw.common.mithra.generator.MithraEmbeddedValueObjectTypeWrapper" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%
	MithraEmbeddedValueObjectTypeWrapper wrapper = (MithraEmbeddedValueObjectTypeWrapper) request.getAttribute("mithraWrapper");
	String packageName = wrapper.getPackageName();
	String superClassType = wrapper.getFullyQualifiedSuperClassType();
	String classType = wrapper.getClassName();
    String lowerCaseClassType = StringUtility.firstLetterToLower(classType);
    String abstractClassType = wrapper.getAbstractClassName();
    String extractorsClassType = classType + "Extractors";
    String extractorClassType = classType + "Extractor";
    String dataClassType = classType + "Data";
    String attributeClassType = classType + "Attribute";
    String managedAttributeClassType = "Managed" + attributeClassType;
    String unmanagedAttributeClassType = "Unmanaged" + attributeClassType;
    String lowerCaseAttributeClassType = StringUtility.firstLetterToLower(attributeClassType);
	MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper[] attributes = wrapper.getSortedAttributes();
    MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper[] embeddedValueObjects = wrapper.getSortedNestedObjects();
    List<RelationshipAttribute> relationships = wrapper.getRelationshipAttributes();
%>
package <%= packageName %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.Operation;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;

<%@  include file="../DoNotModifyWarning.jspi" %>
// Generated from templates/embeddedvalue/Abstract.jsp

<% if (wrapper.hasSuperClass()) { %>
public abstract class <%= abstractClassType %> extends <%= superClassType %> implements Serializable
<% } else { %>
public abstract class <%= abstractClassType %> implements Serializable
<% } %>
{
    private static final <%= extractorsClassType %> UNMANAGED_ATTRIBUTES = new <%= unmanagedAttributeClassType %>();
    private final Object ref;
    private final <%= extractorsClassType %> attributes;

    public <%= abstractClassType %>()
    {
        this.ref = new <%= dataClassType %>();
        this.attributes = UNMANAGED_ATTRIBUTES;
    }

    protected <%= abstractClassType %>(MithraObject ref, <%= extractorsClassType %> attributes)
    {
        this.ref = ref;
        this.attributes = attributes;
    }

    public static <%= classType %> managedInstance(MithraObject ref, <%= extractorsClassType %> attributes)
    {
        return new <%= classType %>(ref, attributes);
    }

    <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
        public <%= attribute.getTypeAsString() %> <%= attribute.getGetter() %>()
        {
            return this.attributes.<%= attribute.getName() %>().<%= attribute.getExtractionMethodName() %>(ref);
        }

        public void <%= attribute.getSetter() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>)
        {
            this.attributes.<%= attribute.getName() %>().<%= attribute.getValueSetterMethodName() %>(ref, <%= attribute.getName() %>);
        }

        public boolean <%= attribute.getNullGetter() %>()
        {
            return this.attributes.<%= attribute.getName() %>().isAttributeNull(ref);
        }

        public void <%= attribute.getNullSetter() %>()
        {
            this.attributes.<%= attribute.getName() %>().setValueNull(ref);
        }

        public void <%= attribute.getSetterUntil() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>, Timestamp exclusiveUntil)
        {
            this.attributes.<%= attribute.getName() %>().setValueUntil(ref, <%= attribute.getName() %>, exclusiveUntil);
        }

        <% if (attribute.isDoubleAttribute() ){ %>
            public void <%= attribute.getIncrementer() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>)
            {
                DoubleExtractor extractor = this.attributes.<%= attribute.getName() %>();
                extractor.increment(ref, <%= attribute.getName() %>);
            }

            public void <%= attribute.getIncrementerUntil() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>, Timestamp exclusiveUntil)
            {
                DoubleExtractor extractor = this.attributes.<%= attribute.getName() %>();
                extractor.incrementUntil(ref, <%= attribute.getName() %>, exclusiveUntil);
            }
        <% } %>
        <% if (attribute.isBigDecimalAttribute()) { %>
            public void <%= attribute.getIncrementer() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>)
            {
                BigDecimalExtractor extractor = this.attributes.<%= attribute.getName() %>();
                extractor.increment(ref, <%= attribute.getName() %>);
            }

            public void <%= attribute.getIncrementerUntil() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>, Timestamp exclusiveUntil)
            {
                BigDecimalExtractor extractor = this.attributes.<%= attribute.getName() %>();
                extractor.incrementUntil(ref, <%= attribute.getName() %>, exclusiveUntil);
            }
        <% } %>
    <% } %>

    <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
        public <%= nestedObject.getTypeAsString() %> <%= nestedObject.getGetter() %>()
        {
            return this.attributes.<%= nestedObject.getName() %>().<%= nestedObject.getExtractionMethodName() %>(ref);
        }

        public void <%= nestedObject.getCopyValuesFrom() %>(<%= nestedObject.getTypeAsString() %> <%= nestedObject.getName() %>)
        {
            this.attributes.<%= nestedObject.getName() %>().<%= nestedObject.getValueSetterMethodName() %>(ref, <%= nestedObject.getName() %>);
        }

        public void <%= nestedObject.getCopyValuesFromUntil() %>(<%= nestedObject.getTypeAsString() %> <%= nestedObject.getName() %>, Timestamp exclusiveUntil)
        {
            this.attributes.<%= nestedObject.getName() %>().<%= nestedObject.getValueSetterUntilMethodName() %>(ref, <%= nestedObject.getName() %>, exclusiveUntil);
        }
    <% } %>

    <% for (RelationshipAttribute rel: relationships) { %>
    	public <%=rel.getTypeAsString()%> <%=rel.getGetter()%>(<%=rel.getParameters() %>)
        {
            return this.attributes.z<%= StringUtility.firstLetterToUpper(rel.getName()) %>Extractor(<%= rel.getParameterVariables() %>).valueOf(ref);
        }

        <% if (rel.hasSetter()) { %>
        public void <%=rel.getSetter()%>(<%=rel.getTypeAsString()%> <%= rel.getName()%>)
        {
            this.attributes.z<%= StringUtility.firstLetterToUpper(rel.getName()) %>Extractor().setValue(ref, <%= rel.getName() %>);
        }
        <% } %>
    <% } %>
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        final <%= classType %> other = (<%= classType %>) o;

        <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
            <% if (attribute.isPrimitive()) { %>
                if (this.<%= attribute.getGetter() %>() != other.<%= attribute.getGetter() %>()) return false;
            <% } else { %>
                if (this.<%= attribute.getGetter() %>() != null ? !this.<%= attribute.getGetter() %>().equals(other.<%= attribute.getGetter() %>()) : other.<%= attribute.getGetter() %>() != null) return false;
            <% } %>
        <% } %>
        <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
            if (!this.<%= nestedObject.getGetter() %>().equals(other.<%= nestedObject.getGetter() %>())) return false;
        <% } %>
        return true;
    }

    public int hashCode()
    {
        int result = 17;
        <% int longCounter = 0; %>
        <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
            <% if (attribute.isPrimitive()) { %>
                <% if (attribute.getTypeAsString().equalsIgnoreCase("boolean")) { %>
                    result *= 37 + (this.<%= attribute.getGetter() %>() ? 0 : 1);
                <% } else if (attribute.getTypeAsString().equalsIgnoreCase("long")) { %>
                    result *= 37 + (int) (this.<%= attribute.getGetter() %>() ^ (<%= attribute.getGetter() %>() >>> 32));
                <% } else if (attribute.getTypeAsString().equalsIgnoreCase("float")) { %>
                    result *= 37 + Float.floatToIntBits(this.<%= attribute.getGetter() %>());
                <% } else if (attribute.getTypeAsString().equalsIgnoreCase("double")) { %>
                    final long l<%= longCounter %> = Double.doubleToLongBits(this.<%= attribute.getGetter() %>());
                    result *= 37 + (int) (l<%= longCounter %> ^ (l<%= longCounter++ %> >>> 32));
                <% } else if (attribute.getTypeAsString().equalsIgnoreCase("int")) { %>
                    result *= 37 + this.<%= attribute.getGetter() %>();
                <% } else { %>
                    result *= 37 + (int) this.<%= attribute.getGetter() %>();
                <% } %>
            <% } else { %>
                result *= 37 + (this.<%= attribute.getGetter() %>() != null ? this.<%= attribute.getGetter() %>().hashCode() : 0);
            <% } %>
        <% } %>
        <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
            result *= 37 + (this.<%= nestedObject.getGetter() %>().hashCode());
        <% } %>
        return result;
    }

    public static interface <%= extractorClassType %>
    {
        public <%= classType %> <%= lowerCaseClassType %>ValueOf(Object o);
        public void set<%= classType %>Value(Object o, <%= classType %> <%= lowerCaseClassType %>);
        public void set<%= classType %>ValueUntil(Object o, <%= classType %> <%= lowerCaseClassType %>, Timestamp exclusiveUntil);
    }

    public static interface <%= extractorsClassType %>
    {
        <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
            public <%= attribute.getExtractorClassName() %> <%= attribute.getName() %>();
        <% } %>
        <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
            public <%= nestedObject.getTypeAsString() %>.<%= nestedObject.getExtractorClassName() %> <%= nestedObject.getName() %>();
        <% } %>
        <% for (RelationshipAttribute rel: relationships) { %>
            public Extractor< Object, <%=rel.getTypeAsString()%>> z<%= StringUtility.firstLetterToUpper(rel.getName()) %>Extractor(<%= rel.getParameters() %>);
        <% } %>
    }

    public static abstract class <%= managedAttributeClassType %> implements <%= extractorClassType %>, <%= extractorsClassType %>, EmbeddedValueExtractor
    {
        public Object valueOf(Object o)
        {
            return this.<%= lowerCaseClassType %>ValueOf(o);
        }

        public void setValue(Object o, Object newValue)
        {
            this.set<%= classType %>Value(o, (<%= classType %>) newValue);
        }

        //TODO: ledav check whether to override this
        public void setValueNull(Object o)
        {
            throw new UnsupportedOperationException("setValueNull should not be called on embedded value objects.");
        }

        public Operation eq(<%= classType %> <%= lowerCaseClassType %>)
        {
            <%= managedAttributeClassType %> <%= lowerCaseAttributeClassType %> = this.retrieveAttribute();
            <% int numEqOperations = 1; %>
            <% for (int i = 0; i < attributes.length; i++, numEqOperations++) { %>
                Operation op<%= numEqOperations %> = <%= lowerCaseAttributeClassType %>.<%= attributes[i].getName() %>().eq(<%= lowerCaseClassType %>.<%= attributes[i].getGetter() %>());
            <% } %>
            <% for (int i = 0; i < embeddedValueObjects.length; i++, numEqOperations++) { %>
                Operation op<%= numEqOperations %> = <%= lowerCaseAttributeClassType %>.<%= embeddedValueObjects[i].getName() %>().eq(<%= lowerCaseClassType %>.<%= embeddedValueObjects[i].getGetter() %>());
            <% } %>
            return op1
            <% for (int i = 2; i < numEqOperations; i++) { %>
                .and(op<%= i %>)
            <% } %>
            ;
        }

        public Operation notEq(<%= classType %> <%= lowerCaseClassType %>)
        {
            <%= managedAttributeClassType %> <%= lowerCaseAttributeClassType %> = this.retrieveAttribute();
            <% int numNotEqOperations = 1; %>
            <% for (int i = 0; i < attributes.length; i++, numNotEqOperations++) { %>
                Operation op<%= numNotEqOperations %> = <%= lowerCaseAttributeClassType %>.<%= attributes[i].getName() %>().notEq(<%= lowerCaseClassType %>.<%= attributes[i].getGetter() %>());
            <% } %>
            <% for (int i = 0; i < embeddedValueObjects.length; i++, numNotEqOperations++) { %>
                Operation op<%= numNotEqOperations %> = <%= lowerCaseAttributeClassType %>.<%= embeddedValueObjects[i].getName() %>().notEq(<%= lowerCaseClassType %>.<%= embeddedValueObjects[i].getGetter() %>());
            <% } %>
            return op1
            <% for (int i = 2; i < numNotEqOperations; i++) { %>
                .or(op<%= i %>)
            <% } %>
            ;
        }

        <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
            public abstract <%= attribute.getAttributeClassName() %> <%= attribute.getName() %>();
        <% } %>
        <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
            public abstract <%= nestedObject.getTypeAsString() %>.Managed<%= nestedObject.getAttributeClassName() %> <%= nestedObject.getName() %>();
        <% } %>
        <% for (RelationshipAttribute rel: relationships) { %>
            public abstract <%= rel.getFinderAttributeTypeForRelatedClass() %> <%= rel.getName() %>(<%= rel.getParameters() %>);
        <% } %>
        protected abstract <%= managedAttributeClassType %> retrieveAttribute();
    }

    public static class <%= unmanagedAttributeClassType %> implements <%= extractorsClassType %>
    {
        <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
            private final <%= attribute.getExtractorClassName() %> <%= attribute.getExtractor() %> = new <%= classType + StringUtility.firstLetterToUpper(attribute.getName()) %>Extractor();
        <% } %>
        <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
            private final <%= nestedObject.getTypeAsString() %>.<%= nestedObject.getExtractorClassName() %> <%= nestedObject.getName() %>Extractor = new <%= classType + StringUtility.firstLetterToUpper(nestedObject.getName()) %>Extractor();
        <% } %>

        <% for (RelationshipAttribute rel: relationships) { %>
            <% if (!rel.hasParameters()) { %>
            private final Extractor< Object, <%=rel.getTypeAsString()%>> <%= rel.getName() %>Extractor = new <%= StringUtility.firstLetterToUpper(rel.getName()) %>Extractor();
            <% } %>
        <% } %>

        <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
            public <%= attribute.getExtractorClassName() %> <%= attribute.getName() %>()
            {
                return this.<%= attribute.getExtractor() %>;
            }
        <% } %>
        <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
            public <%= nestedObject.getTypeAsString() %>.<%= nestedObject.getExtractorClassName() %> <%= nestedObject.getName() %>()
            {
                return this.<%= nestedObject.getExtractor() %>;
            }
        <% } %>
        <% for (RelationshipAttribute rel: relationships) { %>
            public Extractor< Object, <%=rel.getTypeAsString()%>> z<%= StringUtility.firstLetterToUpper(rel.getName()) %>Extractor(<%= rel.getParameters() %>)
            {
                <% if (rel.hasParameters()) { %>
                throw new RuntimeException("not supported");
                <% } else { %>
                return this.<%= rel.getName() %>Extractor;
                <% } %>
            }

            public <%= rel.getFinderAttributeTypeForRelatedClass() %> <%= rel.getName() %>(<%= rel.getParameters() %>)
            {
                throw new RuntimeException("not implemented");
            }
        <% } %>
    }

    <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
        public static class <%= classType + StringUtility.firstLetterToUpper(attribute.getName()) %>Extractor extends Abstract<%= attribute.getExtractorClassName() %>
        {
            public <%= attribute.getTypeAsString() %> <%= attribute.getExtractionMethodName() %>(Object o)
            {
                return ((<%= dataClassType %>) o).<%= attribute.getGetter() %>();
            }

            public void <%= attribute.getValueSetterMethodName() %>(Object o, <%= attribute.getTypeAsString() %> newValue)
            {
                ((<%= dataClassType %>) o).<%= attribute.getSetter() %>(newValue);
            }

            public boolean isAttributeNull(Object o)
            {
                return ((<%= dataClassType %>) o).<%= attribute.getNullGetter() %>();
            }

            public void setValueNull(Object o)
            {
                ((<%= dataClassType %>) o).<%= attribute.getNullSetter() %>();
            }
        }
    <% } %>
    <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : embeddedValueObjects) { %>
        public static class <%= classType + StringUtility.firstLetterToUpper(nestedObject.getName()) %>Extractor implements <%= nestedObject.getTypeAsString() %>.<%= nestedObject.getExtractorClassName() %>
        {
            public <%= nestedObject.getTypeAsString() %> <%= nestedObject.getExtractionMethodName() %>(Object o)
            {
                return ((<%= dataClassType %>) o).<%= nestedObject.getGetter() %>();
            }

            public void <%= nestedObject.getValueSetterMethodName() %>(Object o, <%= nestedObject.getTypeAsString() %> <%= nestedObject.getName() %>)
            {
                ((<%= dataClassType %>) o).<%= nestedObject.getCopyValuesFrom() %>(<%= nestedObject.getName() %>);
            }

            public void <%= nestedObject.getValueSetterUntilMethodName() %>(Object o, <%= nestedObject.getTypeAsString() %> <%= nestedObject.getName() %>, Timestamp exclusiveUntil)
            {
                throw new UnsupportedOperationException("<%= nestedObject.getValueSetterUntilMethodName() %> should not be called on transient objects.");
            }
        }
    <% } %>
    <% for (RelationshipAttribute rel: relationships) { %>
        <% if (!rel.hasParameters()) { %>
        public static class <%= StringUtility.firstLetterToUpper(rel.getName()) %>Extractor extends NonPrimitiveExtractor
        {
            public Object valueOf(Object o)
            {
                return ((<%= dataClassType %>) o).<%= rel.getGetter() %>();
            }

            public void setValue(Object o, Object newValue)
            {
                ((<%= dataClassType %>) o).<%= rel.getSetter() %>((<%=rel.getTypeAsString()%>) o);
            }
        }
        <% } %>
    <% } %>
}