/*
 Copyright 2019 Goldman Sachs.
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

package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.finder.Navigation;
import graphql.language.Field;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

// todo: use streaming and MultiThreadedBatchProcessor (see ExecutionStrategy in https://www.graphql-java.com/documentation/v10/execution/)
class ReladomoQueryFetcher implements DataFetcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger (ReladomoQueryFetcher.class);
    private final boolean many;
    private final RelatedFinder finder;

    protected ReladomoQueryFetcher (final RelatedFinder finder, final boolean many)
    {
        this.many = many;
        this.finder = finder;
    }

    @Override
    public Object get (final DataFetchingEnvironment dataFetchingEnvironment)
    {
        return this.getResult (dataFetchingEnvironment.getField().getSelectionSet(), dataFetchingEnvironment);
    }

    private Object getResult (final SelectionSet selectionSet, final DataFetchingEnvironment dataFetchingEnvironment)
    {
        Operation op = null;
        OrderBy orderBy = null;
        int limit = -1;
        try
        {
            for (final Map.Entry<String, Object> each : dataFetchingEnvironment.getArguments ().entrySet ())
            {
                final String name = each.getKey ();
                final Object val = each.getValue ();
                Operation anOp = null;
                final Attribute attr = finder.getAttributeByName (name);
                if ("order_by".equals (name) || "orderBy".equals (name))
                {
                    orderBy = new OrderByBuilder ().build ((Map<String, String>) val, this.finder);
                    continue;
                }
                else if ("limit".equals (name))
                {
                    limit = (Integer) val;
                    continue;
                }
                else if ("findMany".equals (name) || "filter".equals (name) || "where".equals (name))
                {
                    anOp = new FilterQueryBuilder ().buildOperation ((Map) val, this.finder);
                }
                else if (attr instanceof IntegerAttribute)
                {
                    final int v = Integer.valueOf ("" + val);
                    anOp = ((IntegerAttribute) attr).eq (v);
                }
                else
                {
                    anOp = ((StringAttribute) attr).eq ((String) val);
                }

                op = op == null ? anOp : op.and (anOp);
            }

            if (op == null) op = finder.all ();

            final MithraList<? extends MithraObject> list = finder.findMany (op);
            if (orderBy != null) list.setOrderBy (orderBy);
            if (limit >= 0) list.setMaxObjectsToRetrieve (limit);

            this.injectDeepFetchNavigation (selectionSet, list);

            LOGGER.debug ("OP: {} found {} records.", op, list.size ());

            if (many)
            {
                return list;
            }
            else
            {
                if (list.size () > 1)
                {
                    throw new RuntimeException ("expected one but found " + list.size () + " with operation: " + op);
                }
                return list.get (0);
            }
        } catch (final Exception e)
        {
            return this.throwDescriptiveException(selectionSet, op, e);
        }
    }

    private Object throwDescriptiveException(final SelectionSet selectionSet, final Operation op, final Exception e)
    {
        String str;
        try
        {
            str = "" + op;
        } catch (final Exception e2)
        {
            str = "" + selectionSet;
        }
        throw new RuntimeException ("fail to fetch for " + str, e);
    }

    private void injectDeepFetchNavigation (final SelectionSet selectionSet, final MithraList<? extends MithraObject> list)
    {
        for (final Selection each : selectionSet.getSelections ())
        {
            if (!each.getChildren ().isEmpty ())
            {
                final String name = ((Field) each).getName ();
                final Navigation navigation = (Navigation) finder.getRelationshipFinderByName (name);
                list.deepFetch (navigation);

                this.injectDeepFetchNavigation (((Field) each).getSelectionSet(), list);
            }
        }
    }
}
