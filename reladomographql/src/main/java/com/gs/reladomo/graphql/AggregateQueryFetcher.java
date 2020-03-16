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

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import graphql.language.Field;
import graphql.language.Node;
import graphql.language.Selection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AggregateQueryFetcher implements DataFetcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger (AggregateQueryFetcher.class);

    private final RelatedFinder finder;
    protected static final UnifiedMap<String, Function<NumericAttribute, MithraAggregateAttribute>> AGGREGATE_METHODS = new UnifiedMap<>();

    static
    {
        AGGREGATE_METHODS.put ("sum", NumericAttribute::sum);
        AGGREGATE_METHODS.put ("mean", NumericAttribute::avg);
        AGGREGATE_METHODS.put ("min", NumericAttribute::min);
        AGGREGATE_METHODS.put ("max", NumericAttribute::max);
        AGGREGATE_METHODS.put ("count", NumericAttribute::count);
        AGGREGATE_METHODS.put ("standardDeviationSample", NumericAttribute::standardDeviationSample);
        AGGREGATE_METHODS.put ("standardDeviationPopulation", NumericAttribute::standardDeviationPopulation);
        AGGREGATE_METHODS.put ("varianceSample", NumericAttribute::varianceSample);
        AGGREGATE_METHODS.put ("variancePopulation", NumericAttribute::variancePopulation);
    }

    public AggregateQueryFetcher (final RelatedFinder finder)
    {
        this.finder = finder;
    }

    @Override
    public Object get (final DataFetchingEnvironment dataFetchingEnvironment)
    {
        // todo: support unions with+ fields by calling getMergedField() instead
        return this.getResult (dataFetchingEnvironment.getField(), dataFetchingEnvironment.getArguments ());
    }

    private Object getResult (final Field request, final Map<String, Object> arguments)
    {
        final long time = System.currentTimeMillis ();
        try
        {
            final Object filterDom = arguments.get ("filter");
            final Operation op = new FilterQueryBuilder ().buildOperation ((Map) filterDom, this.finder);

            final AggregateList aggregateList = new AggregateList (op);

            final List<Attribute> groupbyAttributes = new FastList<> ();
            final List<String> groupbyNames = new FastList<>();

            final Map<String, BeanWrapper.Bean> beanMap = new UnifiedMap<> ();

            final List<BeanWrapper.AggregateNode> aggregateNodes = new FastList<> ();
            for (final Selection selection : request.getSelectionSet ().getSelections ())
            {
                final Field eachField = (Field) selection;
                final String attributeName = eachField.getName ();

                final List<String> aggregateTypes = findAggregateTypes (eachField);
                if (aggregateTypes != null)
                {
                    final BeanWrapper.AggregateNode aggregateNode =  new BeanWrapper.AggregateNode(attributeName, aggregateTypes);
                    for (final String aggregateType : aggregateTypes)
                    {
                        final Attribute attr = this.finder.getAttributeByName(attributeName);

                        final MithraAggregateAttribute aggregateAttribute = AGGREGATE_METHODS.get(aggregateType).apply((NumericAttribute) attr);
                        aggregateList.addAggregateAttribute(attributeName + '.' + aggregateType, aggregateAttribute);
                    }
                    aggregateNodes.add(aggregateNode);

                }
                else
                {
                    beanMap.put (attributeName, injectRelationshipNavigation(eachField, groupbyAttributes, groupbyNames, this.finder));
                }
            }

            for (int i = 0; i < groupbyAttributes.size (); i++)
            {
                aggregateList.addGroupBy (groupbyNames.get (i), groupbyAttributes.get (i));
            }

            int index = groupbyAttributes.size ();
            for (final BeanWrapper.AggregateNode each: aggregateNodes)
            {
                each.setIndex(index);
                beanMap.put (each.getAttributeName(), each);
                index+= each.getTypes().size();
            }

            final List resultList = new FastList ();
            for (final AggregateData each : aggregateList)
            {
                resultList.add (new BeanWrapper (each, beanMap));
            }
            LOGGER.debug ("result {} records in {}ms Operation: {}", aggregateList.size (), System.currentTimeMillis () - time, op);

            return resultList;
        } catch (final Exception e)
        {
            throw new RuntimeException ("fail to fetch for " + arguments, e);
        }
    }

    private List<String> findAggregateTypes(final Field aggregateAttribute)
    {
        if (aggregateAttribute.getSelectionSet() == null) return null;

        final List<String> aggregateTypes = FastList.newList();
        for (final Node each : aggregateAttribute.getSelectionSet().getSelections())
        {
            final String name = ((Field) each).getName();
            if (AGGREGATE_METHODS.containsKey(name))
            {
                aggregateTypes.add(name);
            }
        }

        if (aggregateTypes.size() == 0) return null;
        return aggregateTypes;
    }

    private BeanWrapper.Bean injectRelationshipNavigation(final Field field, final List<Attribute> groupbyAttributes, final List<String> groupbyNames, final RelatedFinder relatedFinder)
    {
        final String fieldName = field.getName ();
        final Attribute attribute = relatedFinder.getAttributeByName (fieldName);

        if (attribute != null)
        {
            final int index = groupbyNames.size ();
            groupbyAttributes.add (attribute);
            groupbyNames.add ("C" + index); // internal reference to the column i.e. "C15"
            return new BeanWrapper.StringLeaf (index);
        }

        final Map<String, BeanWrapper.Bean> map = new UnifiedMap<> ();
        for (final Selection each : field.getSelectionSet ().getSelections ())
        {
            final RelatedFinder relationshipFinder = relatedFinder.getRelationshipFinderByName (fieldName);
            map.put (((Field) each).getName (), this.injectRelationshipNavigation((Field) each, groupbyAttributes, groupbyNames, relationshipFinder));
        }
        return new BeanWrapper.Node (map);
    }
}
