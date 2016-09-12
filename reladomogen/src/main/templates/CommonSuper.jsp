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
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.ObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%
	MithraSuperTypeWrapper wrapper = (MithraSuperTypeWrapper) request.getAttribute("mithraWrapper");
%>
package <%= wrapper.getPackageName() %>;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.transaction.MithraTempTransaction;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.*;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.behavior.state.DatedPersistedState;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.DeepRelationshipUtility;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.math.BigDecimal;

<%@  include file="DoNotModifyWarning.jspi" %>
// Generated from templates/CommonSuper.jsp

<% if (wrapper.hasSuperClass()) { %>
public abstract class <%= wrapper.getClassName() %> extends <%= wrapper.getFullyQualifiedSuperClassType() %> implements <%= wrapper.getMithraInterfaceName() %>
<% } else {%>
public abstract class <%= wrapper.getClassName() %> implements <%= wrapper.getMithraInterfaceName() %>
<% } %>
{
<%
        wrapper.writeAbstractTemplate(out, "main");
%>

}
