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
<%@ page import="java.util.*,
                 com.gs.fw.common.mithra.generator.*,
                 com.gs.fw.common.mithra.generator.metamodel.MithraObjectType,
                 com.gs.fw.common.mithra.generator.metamodel.AttributeType,
                 com.gs.fw.common.mithra.generator.metamodel.RelationshipType,
                 com.gs.fw.common.mithra.generator.util.StringUtility,
                     com.gs.fw.common.mithra.generator.type.StringJavaType,
                     com.gs.fw.common.mithra.generator.type.JavaType,
                     com.gs.fw.common.mithra.generator.type.CharJavaType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
	String localClassName = wrapper.getClassName() + "DatabaseObjectAbstract";
%>
package <%= wrapper.getPackageName() %>;

<%@  include file="../Import.jspi" %>
import java.util.*;
import java.sql.*;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.cache.offheap.*;
import com.gs.fw.common.mithra.connectionmanager.*;
import com.gs.fw.common.mithra.database.*;
import com.gs.fw.common.mithra.databasetype.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.finder.integer.IntegerResultSetParser;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.remote.RemoteMithraService;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.UpdateOperation;

<%@  include file="../DoNotModifyWarning.jspi" %>
public abstract class <%=localClassName%> extends <%= wrapper.getDatabaseObjectSuperClass()%> implements MithraTransactionalDatabaseObject, MithraObjectFactory
{
    <%@  include file="../CommonTransactionalDatabaseObjectAbstract.jspi" %>
    <%@  include file="../CommonNonDatedDatabaseObjectAbstract.jspi" %>



    public String getPkColumnList(String databaseAlias)
    {
        if (databaseAlias.equals(SqlQuery.DEFAULT_DATABASE_ALIAS))
        {
            return "<%= wrapper.getPkColumnListWithDefaultAlias() %>";
        }
        StringBuffer result = new StringBuffer((databaseAlias.length()+15)*<%= attributes.length %>);
        <% for(int i=0;i<primaryKeyAttributes.length;i++) { %>
        <%  if (i > 0) { %>result.append(",");<% } %>
        result.append(databaseAlias);
        result.append(".");
        result.append("<%= primaryKeyAttributes[i].getColumnNameWithEscapedQuote() %>");
        <% } // for primary key attributes%>
        return result.toString();
    }

}



