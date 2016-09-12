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
<%@ page import="com.gs.fw.common.mithra.generator.type.DateJavaType" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
    AbstractAttribute[] attributes = wrapper.getSortedNormalAndSourceAttributes();
    RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    Attribute[] pkAttributes = wrapper.getPrimaryKeyAttributes();
    Attribute[] shadowAttributes = wrapper.getShadowAttributes();
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
	String className = wrapper.getClassName() + "Data";
	String onHeapClassName = className + (wrapper.hasOffHeap() ? "OnHeap" : "");
	String offHeapClassName = className + "OffHeap";
    String classNameAsVariable = StringUtility.firstLetterToLower(className);
    MithraObjectTypeWrapper[] superClasses = wrapper.getSuperClasses();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    Boolean hasIdentity = Boolean.FALSE;
    String identityValue = "null";
    if (wrapper.getIdentityCount() > 0) {
        hasIdentity = Boolean.TRUE;
        identityValue = wrapper.getIdentityAttribute().getType().convertToObject("this." + wrapper.getIdentityAttribute().getGetter() + "()");
    }
%>
package <%= wrapper.getPackageName() %>;

import java.util.*;
import java.sql.Timestamp;
import java.sql.Date;
<%@ include file="../Import.jspi" %>
import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.cache.offheap.MithraOffHeapDataObject;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDataStorage;

<%@  include file="../DoNotModifyWarning.jspi" %>
public <%= wrapper.hasOffHeap() ? "interface" : "class" %> <%=className%>
<% if (wrapper.isTablePerClassSubClass()) { %>
extends <%= wrapper.getSuperClassWrapper().getDataClassName()%>
<% } %>
<%= wrapper.hasOffHeap() ? "extends" : "implements" %> <%= wrapper.getDataClassInterface() %>
{
<% if (wrapper.hasOffHeap()) { %>

    public static final int OFF_HEAP_DATA_SIZE = <%= wrapper.getOffHeapDataSize() %>;

    <% for (AbstractAttribute attribute : attributes) { %>
        boolean <%= attribute.getNullGetter() %>;
        <%= attribute.getTypeAsString() %> <%= attribute.getGetter() %>();
        void <%= attribute.getSetter() %>(<%= attribute.getSetterTypeAsString() %> value);
        void <%= attribute.getSetter() %>Null();

        <% if (attribute.isTimestampAttribute()) { %>
            long <%= attribute.getTimestampLongGetter() %>;
        <% } %>
        <% if (attribute.isTimeAttribute()) { %>
            long <%= attribute.getOffHeapTimeLongGetter() %>;
        <% } %>
        <% if (attribute.isStringAttribute()) { %>
            int <%= attribute.getStringOffHeapIntGetter() %>;
        <% } %>
        <% if (attribute.isNullablePrimitive() && attribute.isMutablePrimaryKey()) { %>
            void _old<%= attribute.getSetter() %>Null();
        <% } %>
    <% } %>

    <% for(int i= 0; i < pkAttributes.length; i++) { %>
         <% if(pkAttributes[i].isSetPrimaryKeyGeneratorStrategy()) { %>
            boolean zGetIs<%= StringUtility.firstLetterToUpper(pkAttributes[i].getName()) %>Set();
        <% } %>
    <% } %>

    <% if (wrapper.hasAsOfAttributes()) { %>
        byte zGetDataVersion();
        void zSetDataVersion(byte version);
        void zIncrementDataVersion();
    <% } %>

    <% if (wrapper.hasOptimisticLockAttribute()) { %>
    boolean mustIncrementVersion();
    <%= wrapper.getOptimisticLockAttribute().getTypeAsString()%> zGetPersistedVersion();
    void zMarkDirty();
    boolean zIsDirty();
        <% if (!wrapper.hasTimestampOptimisticLockAttribute()) { %>
        <%= wrapper.getOptimisticLockAttribute().getTypeAsString()%> _old<%= wrapper.getOptimisticLockAttribute().getGetter()%>();
        boolean _old<%= wrapper.getOptimisticLockAttribute().getNullGetter()%>;
        <% } // timestamp optimistic%>

    <% } // optimistic attribute %>

    <% for (int i = 0; i < relationshipAttributes.length; i ++) { %>
        <% if (relationshipAttributes[i].hasSetter()) { %>
            Object <%= relationshipAttributes[ i ].getGetter()%>();
            void <%= relationshipAttributes[i].getSetter()%>(Object related);
        <% } %>

    <% } %>

    public class <%= onHeapClassName %> implements <%= className %>
    {
    <%@  include file="./DataOnHeap.jspi" %>
    }

    public class <%= offHeapClassName %> extends MithraOffHeapDataObject implements <%= className %>
    {
    <%@  include file="./DataOffHeap.jspi" %>
    }
<% } else { %>
    <%@  include file="./DataOnHeap.jspi" %>
<% } %>
}
