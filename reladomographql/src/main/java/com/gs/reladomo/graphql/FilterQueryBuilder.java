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

import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.MithraArrayTupleTupleSet;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.function.BiFunction;

class FilterQueryBuilder<R>
{
    private static final Map<String, BiFunction<StringAttribute, Object, Operation>> STRING_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<IntegerAttribute, Object, Operation>> INTEGER_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<LongAttribute, Object, Operation>> LONG_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<ShortAttribute, Object, Operation>> SHORT_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<DoubleAttribute, Object, Operation>> DOUBLE_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<FloatAttribute, Object, Operation>> FLOAT_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<TimestampAttribute, Object, Operation>> TIMESTAMP_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<AsOfAttribute, Object, Operation>> ASOF_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<BooleanAttribute, Object, Operation>> BOOLEAN_OPS = new UnifiedMap<>();
    private static final Map<String, BiFunction<CharAttribute, Object, Operation>> CHAR_OPS = new UnifiedMap<>();

    public static final String AND = "AND";
    public static final String OR = "OR";

    static
    {
        STRING_OPS.put ("eq", (attr, val) -> attr.eq ((String) val));
        STRING_OPS.put ("notEq", (attr, val) -> attr.notEq ((String) val));
        STRING_OPS.put ("contains", (attr, val) -> attr.contains ((String) val));
        STRING_OPS.put ("notContains", (attr, val) -> attr.notContains ((String) val));
        STRING_OPS.put ("endsWith", (attr, val) -> attr.endsWith ((String) val));
        STRING_OPS.put ("startsWith", (attr, val) -> attr.startsWith ((String) val));
        STRING_OPS.put ("notStartsWith", (attr, val) -> attr.notStartsWith ((String) val));
        STRING_OPS.put ("notEndsWith", (attr, val) -> attr.notEndsWith ((String) val));
        STRING_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        STRING_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        STRING_OPS.put ("in", (attr, val) -> attr.in (new UnifiedSet((List) val)));
        STRING_OPS.put ("notIn", (attr, val) -> attr.notIn (new UnifiedSet ((List) val)));
        STRING_OPS.put ("eqWithTrim", (attr, val) -> attr.eqWithTrim ((String) val));
        STRING_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan ((String) val));
        STRING_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals ((String) val));
        STRING_OPS.put ("lessThan", (attr, val) -> attr.lessThan ((String) val));
        STRING_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals ((String) val));
        STRING_OPS.put ("wildCardEq", (attr, val) -> attr.wildCardEq ((String) val));
        STRING_OPS.put ("wildCardNotEq", (attr, val) -> attr.wildCardNotEq ((String) val));

        INTEGER_OPS.put ("eq", (attr, val) -> attr.eq ((int) val));
        INTEGER_OPS.put ("notEq", (attr, val) -> attr.notEq ((int) val));
        INTEGER_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        INTEGER_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        INTEGER_OPS.put ("in", (attr, val) -> attr.in (asIntSet ((List) val)));
        INTEGER_OPS.put ("notIn", (attr, val) -> attr.notIn (asIntSet ((List) val)));
        INTEGER_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan ((int) val));
        INTEGER_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals ((int) val));
        INTEGER_OPS.put ("lessThan", (attr, val) -> attr.lessThan ((int) val));
        INTEGER_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals ((int) val));

        LONG_OPS.put ("eq", (attr, val) -> attr.eq ((long) val));
        LONG_OPS.put ("notEq", (attr, val) -> attr.notEq ((long) val));
        LONG_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        LONG_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        LONG_OPS.put ("in", (attr, val) -> attr.in (asLongSet ((List) val)));
        LONG_OPS.put ("notIn", (attr, val) -> attr.notIn (asLongSet ((List) val)));
        LONG_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan ((long) val));
        LONG_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals ((long) val));
        LONG_OPS.put ("lessThan", (attr, val) -> attr.lessThan ((long) val));
        LONG_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals ((long) val));

        SHORT_OPS.put ("eq", (attr, val) -> attr.eq ((short) val));
        SHORT_OPS.put ("notEq", (attr, val) -> attr.notEq ((short) val));
        SHORT_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        SHORT_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        SHORT_OPS.put ("in", (attr, val) -> attr.in (asShortSet ((List) val)));
        SHORT_OPS.put ("notIn", (attr, val) -> attr.notIn (asShortSet ((List) val)));
        SHORT_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan ((short) val));
        SHORT_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals ((short) val));
        SHORT_OPS.put ("lessThan", (attr, val) -> attr.lessThan ((short) val));
        SHORT_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals ((short) val));

        DOUBLE_OPS.put ("eq", (attr, val) -> attr.eq ((double) val));
        DOUBLE_OPS.put ("notEq", (attr, val) -> attr.notEq ((double) val));
        DOUBLE_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        DOUBLE_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        DOUBLE_OPS.put ("in", (attr, val) -> attr.in (asDoubleSet ((List) val)));
        DOUBLE_OPS.put ("notIn", (attr, val) -> attr.notIn (asDoubleSet ((List) val)));
        DOUBLE_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan ((double) val));
        DOUBLE_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals ((double) val));
        DOUBLE_OPS.put ("lessThan", (attr, val) -> attr.lessThan ((double) val));
        DOUBLE_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals ((double) val));

        FLOAT_OPS.put ("eq", (attr, val) -> attr.eq ((float) val));
        FLOAT_OPS.put ("notEq", (attr, val) -> attr.notEq ((float) val));
        FLOAT_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        FLOAT_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        FLOAT_OPS.put ("in", (attr, val) -> attr.in (asFloatSet ((List) val)));
        FLOAT_OPS.put ("notIn", (attr, val) -> attr.notIn (asFloatSet ((List) val)));
        FLOAT_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan ((float) val));
        FLOAT_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals ((float) val));
        FLOAT_OPS.put ("lessThan", (attr, val) -> attr.lessThan ((float) val));
        FLOAT_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals ((float) val));

        CHAR_OPS.put ("eq", (attr, val) -> attr.eq (asChar(val)));
        CHAR_OPS.put ("notEq", (attr, val) -> attr.notEq (asChar(val)));
        CHAR_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        CHAR_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        CHAR_OPS.put ("in", (attr, val) -> attr.in (asCharSet ((List) val)));
        CHAR_OPS.put ("notIn", (attr, val) -> attr.notIn (asCharSet ((List) val)));
        CHAR_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan (asChar(val)));
        CHAR_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals (asChar(val)));
        CHAR_OPS.put ("lessThan", (attr, val) -> attr.lessThan (asChar(val)));
        CHAR_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals (asChar(val)));

        TIMESTAMP_OPS.put ("eq", (attr, val) -> attr.eq (asTimestampOrInfinity(attr, val)));
        TIMESTAMP_OPS.put ("notEq", (attr, val) -> attr.notEq (asTimestampOrInfinity(attr, val)));
        TIMESTAMP_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        TIMESTAMP_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
        TIMESTAMP_OPS.put ("in", (attr, val) -> attr.in ((asTimestampSet(attr, (List)val))));
        TIMESTAMP_OPS.put ("notIn", (attr, val) -> attr.notIn ((asTimestampSet(attr, (List)val))));
        TIMESTAMP_OPS.put ("greaterThan", (attr, val) -> attr.greaterThan (asTimestampOrInfinity(attr, val)));
        TIMESTAMP_OPS.put ("greaterThanEquals", (attr, val) -> attr.greaterThanEquals (asTimestampOrInfinity(attr, val)));
        TIMESTAMP_OPS.put ("lessThan", (attr, val) -> attr.lessThan (asTimestampOrInfinity(attr, val)));
        TIMESTAMP_OPS.put ("lessThanEquals", (attr, val) -> attr.lessThanEquals (asTimestampOrInfinity(attr, val)));

        ASOF_OPS.put ("eq", (attr, val) -> attr.eq (asTimestampOrInfinity(attr, val)));
        ASOF_OPS.put ("equalsEdgePoint", (attr, val) -> attr.equalsEdgePoint ());
        ASOF_OPS.put ("equalsInfinity", (attr, val) -> attr.equalsInfinity ());

        BOOLEAN_OPS.put ("eq", (attr, val) -> attr.eq ((boolean) val));
        BOOLEAN_OPS.put ("notEq", (attr, val) -> attr.notEq ((boolean) val));
        BOOLEAN_OPS.put ("isNull", (attr, val) -> attr.isNull ());
        BOOLEAN_OPS.put ("isNotNull", (attr, val) -> attr.isNull ());
    }

    FilterQueryBuilder()
    {
    }

    Operation buildOperation (final Map filterDom, final RelatedFinder<R> finder)
    {
        if (filterDom == null) return finder.all();
        final List<String> keys = new ArrayList<>(filterDom.keySet());
        final boolean hadAnd = keys.remove(AND);
        final boolean hadOr = keys.remove(OR);

        if (hadAnd && hadOr) throw new RuntimeException("AND OR are ambiguous in " + keys);
        if (keys.size() > 1) throw new RuntimeException("Operations are ambiguous in " + keys);

        Operation op = null;

        if (keys.size() == 1)
        {
            final String name = keys.get(0);
            if ("tupleIn".equals(name))
            {
                op = this.buildTupleInOperation(finder, (List<Map<String, Object>>) filterDom.get(name));
            }
            else if ("EXPR".equals(name))
            {
                op = ExpressionOperationBuilder.buildExpressionOperation(
                        this, finder, (Map)(((Collection)filterDom.get(name)).iterator().next()));
            }
            else
            {
                op = this.buildLeafOperation(filterDom, finder, name);
            }
        }

        if (hadAnd)
        {
            for (Map each : (List<Map>) filterDom.get(AND))
            {
                Operation other = buildOperation(each, finder);
                op = op == null ? other : op.and(other);
            }
        }
        if (hadOr)
        {
            for (Map each : (List<Map>) filterDom.get(OR))
            {
                Operation other = buildOperation(each, finder);
                op = op == null ? other : op.or(other);
            }
        }

        return op;
    }

    private Operation buildLeafOperation(final Map filterDom, final RelatedFinder<R> finder, final String name)
    {
        final Attribute attr = finder.getAttributeByName(name);
        final Map<String, Object> valueMap = (Map<String, Object>) filterDom.get(name);

        if (attr == null)
        {
            final RelatedFinder relationshipFinder = finder.getRelationshipFinderByName(name);
            if (relationshipFinder == null)
            {
                throw new RuntimeException(finder + " not related to " + name);
            }
            return buildOperation(valueMap, relationshipFinder);
        }
        if (valueMap.size() != 1)
        {
            throw new RuntimeException("cannot create operation for " + valueMap);
        }

        final String key = valueMap.keySet().iterator().next();
        final Object val = valueMap.get(key);

        return this.buildOperationFromAttribute(attr, key, val);
    }

    protected Operation buildOperationFromAttribute(final Attribute attr, final String opName, final Object val)
    {
        if (attr instanceof StringAttribute) return this.applyStringAttribute((StringAttribute) attr, opName, val);
        if (attr instanceof CharAttribute) return this.applyCharAttribute((CharAttribute) attr, opName, val);
        if (attr instanceof AsOfAttribute) return this.applyAsOfAttribute((AsOfAttribute) attr, opName, val);
        if (attr instanceof TimestampAttribute) return this.applyTimestampAttribute((TimestampAttribute) attr, opName, val);
        if (attr instanceof BooleanAttribute) return this.applyBooleanAttribute((BooleanAttribute) attr, opName, val);
        if (attr instanceof IntegerAttribute) return this.applyIntegerAttribute((IntegerAttribute) attr, opName, val);
        if (attr instanceof LongAttribute) return this.applyLongAttribute((LongAttribute) attr, opName, val);
        if (attr instanceof ShortAttribute) return this.applyShortAttribute((ShortAttribute) attr, opName, val);
        if (attr instanceof DoubleAttribute) return this.applyDoubleAttribute((DoubleAttribute) attr, opName, val);
        if (attr instanceof FloatAttribute) return this.applyFloatAttribute((FloatAttribute) attr, opName, val);
        throw new RuntimeException(attr + " is not supported yet (derived from " + attr.getClass().getSuperclass() + ")");
    }

    private Operation buildTupleInOperation(final RelatedFinder finder, final List<Map<String, Object>> tupleList)
    {
        if (tupleList == null || tupleList.isEmpty()) throw new RuntimeException("todo: define none operation");

        // todo: extend support to mapped attributes as well.
        final List<String> tupleNames = new ArrayList<>(tupleList.get(0).keySet());

        if (tupleNames.size() < 2)
            throw new RuntimeException("this can be done with \"in: []\" operation.");

        TupleAttribute tupleAttribute = finder.getAttributeByName(tupleNames.get(0))
                .tupleWith(finder.getAttributeByName(tupleNames.get(1)));
        for (int i = 2; i < tupleNames.size(); i++)
        {
            tupleAttribute = tupleAttribute.tupleWith(finder.getAttributeByName(tupleNames.get(i)));
        }

        final MithraArrayTupleTupleSet tupleSet = new MithraArrayTupleTupleSet();

        for (final Map<String, Object> each : tupleList)
        {
            if (each.size() != tupleNames.size())
            {
                throw new RuntimeException("mismatched attributes expected " + tupleNames + " but got " + each.keySet());
            }
            final Object[] values = new Object[tupleNames.size()];
            for (int i = 0; i < values.length; i++)
            {
                values[i] = each.get(tupleNames.get(i));
                if (values[i] == null)
                {
                    throw new RuntimeException("missing tuple value for " + tupleNames.get(i));
                }
            }
            tupleSet.add(values);
        }

        return tupleAttribute.in(tupleSet);
    }

    private Operation applyStringAttribute(final StringAttribute attr, final String key, final Object val)
    {
        final BiFunction<StringAttribute, Object, Operation> func = STRING_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyCharAttribute(final CharAttribute attr, final String key, final Object val)
    {
        final BiFunction<CharAttribute, Object, Operation> func = CHAR_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyIntegerAttribute(final IntegerAttribute attr, final String key, final Object val)
    {
        final BiFunction<IntegerAttribute, Object, Operation> func = INTEGER_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyBooleanAttribute(final BooleanAttribute attr, final String key, final Object val)
    {
        final BiFunction<BooleanAttribute, Object, Operation> func = BOOLEAN_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyTimestampAttribute(final TimestampAttribute attr, final String key, final Object val)
    {
        final BiFunction<TimestampAttribute, Object, Operation> func = TIMESTAMP_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyAsOfAttribute(final AsOfAttribute attr, final String key, final Object val)
    {
        final BiFunction<AsOfAttribute, Object, Operation> func = ASOF_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyLongAttribute(final LongAttribute attr, final String key, final Object val)
    {
        final BiFunction<LongAttribute, Object, Operation> func = LONG_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyShortAttribute(final ShortAttribute attr, final String key, final Object val)
    {
        final BiFunction<ShortAttribute, Object, Operation> func = SHORT_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyDoubleAttribute(final DoubleAttribute attr, final String key, final Object val)
    {
        final BiFunction<DoubleAttribute, Object, Operation> func = DOUBLE_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private Operation applyFloatAttribute(final FloatAttribute attr, final String key, final Object val)
    {
        final BiFunction<FloatAttribute, Object, Operation> func = FLOAT_OPS.get(key);
        if (func == null) throw new RuntimeException(key + " is not implemented.");
        return func.apply(attr, val);
    }

    private static char asChar(final Object val)
    {
        final String str = (String) val;
        if (str.length() != 1)
        {
            throw new RuntimeException("expected a single character instead of \"" + str + '\"');
        }
        return str.charAt(0);
    }

    private static CharHashSet asCharSet(final List list)
    {
        final CharHashSet set = new CharHashSet();
        for (final Object each : list) set.add(asChar(each));
        return set;
    }

    private static IntHashSet asIntSet(final List list)
    {
        final IntHashSet set = new IntHashSet();
        for (final Object each : list) set.add((int) each);
        return set;
    }

    private static LongHashSet asLongSet(final List list)
    {
        final LongHashSet set = new LongHashSet();
        for (final Object each : list) set.add((long) each);
        return set;
    }

    private static ShortHashSet asShortSet(final List list)
    {
        final ShortHashSet set = new ShortHashSet();
        for (final Object each : list) set.add((short) each);
        return set;
    }

    private static DoubleHashSet asDoubleSet(final List list)
    {
        final DoubleHashSet set = new DoubleHashSet();
        for (final Object each : list) set.add((float) each);
        return set;
    }

    private static FloatHashSet asFloatSet(final List list)
    {
        final FloatHashSet set = new FloatHashSet();
        for (final Object each : list) set.add((float) each);
        return set;
    }

    private static Set<Timestamp> asTimestampSet(final TimestampAttribute attr, final List list)
    {
        final Set<Timestamp> set = UnifiedSet.newSet();
        for (final Object each : list) set.add(asTimestampOrInfinity(attr, each));
        return set;
    }

    private static Timestamp asTimestampOrInfinity(final TimestampAttribute attr, final Object val)
    {
        return TimestampScalar.INFINITY_TIMESTAMP_MARKER.equals(val)
                ? attr.getAsOfAttributeInfinity()
                : (Timestamp) val;
    }

    private static Timestamp asTimestampOrInfinity(final AsOfAttribute attr, final Object val)
    {
        return TimestampScalar.INFINITY_TIMESTAMP_MARKER.equals(val)
                ? attr.getInfinityDate()
                : (Timestamp) val;
    }

}
