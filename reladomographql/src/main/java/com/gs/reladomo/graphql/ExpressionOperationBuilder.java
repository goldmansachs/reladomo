package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpressionOperationBuilder
{
    public static final Set<String> BINARY_OPERATORS = Sets.fixedSize.of("plus", "minus", "times", "dividedBy");
    public static final Set<String> UNARY_OPERATORS = Sets.fixedSize.of("absoluteValue");
    private static final Map<Class, Class>PRIMITIVIZE = UnifiedMap.newMap();

    static
    {
        PRIMITIVIZE.put(Byte.class, byte.class);
        PRIMITIVIZE.put(Character.class, char.class);
        PRIMITIVIZE.put(Double.class, double.class);
        PRIMITIVIZE.put(Float.class, float.class);
        PRIMITIVIZE.put(Integer.class, int.class);
        PRIMITIVIZE.put(Long.class, long.class);
        PRIMITIVIZE.put(Short.class, short.class);
    }

    private static Object buildExpressionNode(final RelatedFinder finder, final Map<String, Object> domTree)
    {
        final String exprName = getOnlySetElement(domTree.keySet());
        return buildExpressionNode(finder, exprName, domTree);
    }

    private static Object buildExpressionNode(final RelatedFinder finder, final String exprName, final Map<String, Object> domTree)
    {
        if (BINARY_OPERATORS.contains(exprName))
        {
            List<Map<String, Object>>  arguments = (List<Map<String, Object>>)domTree.get(exprName);
            Attribute attr = (Attribute)buildExpressionNode(finder, arguments.get(0));

            for (int i = 1; i < arguments.size(); i++)
            {
                attr = apply(attr, exprName, buildExpressionNode(finder, arguments.get(i)));
            }

            return attr;
        }
        if (UNARY_OPERATORS.contains(exprName))
        {
            return apply((Attribute)buildExpressionNode(finder, (Map<String, Object>)domTree.get(exprName)), exprName);
        }

        if ("VAL".equals(exprName))
        {
            return domTree.get(exprName);
        }

        final Attribute attr = finder.getAttributeByName(exprName);
        if (attr == null)
        {
            throw new RuntimeException ("Unexpected operator " + exprName);
        }
        return attr;
    }

    private static Attribute apply(final Attribute subject, final String mathOp, final Object object)
    {
        Method method = findMethod(subject.getClass(), mathOp, object.getClass());
        try
        {
            return (Attribute)method.invoke(subject, object);
        }
        catch (final Exception e)
        {
            throw new RuntimeException("failed to call " + method, e);
        }
    }

    private static Method findMethod(final Class<? extends Attribute> subjectClass, String name, Class objectClass)
    {
        if (PRIMITIVIZE.containsKey(objectClass))
        {
            objectClass = PRIMITIVIZE.get(objectClass);
        }
        for (final Method each : subjectClass.getMethods())
        {
            if (each.getName().equals(name)  && each.getParameterCount() == 1)
            {
                if (each.getParameterTypes()[0].isAssignableFrom(objectClass))
                    return each;
            }
        }
        throw new RuntimeException("cannot find method to call for " + name + "(" + objectClass + ")");
    }

    private static Attribute apply(final Attribute subject, final String mathOp)
    {
        Method method = findMethod(subject.getClass(), mathOp);
        try
        {
            return (Attribute)method.invoke(subject);
        }
        catch (final Exception e)
        {
            throw new RuntimeException("failed to call " + method, e);
        }
    }

    private static Method findMethod(final Class<? extends Attribute> subjectClass, String name)
    {
        for (final Method each : subjectClass.getMethods())
        {
            if (each.getName().equals(name)  && each.getParameterCount() == 0)
            {
                return each;
            }
        }
        throw new RuntimeException("cannot find method to call for " + name + "()");
    }

    protected static Operation buildExpressionOperation(final FilterQueryBuilder filterQueryBuilder, final RelatedFinder finder, final Map<String, Object> domTree)
    {
        if (domTree == null || domTree.isEmpty()) throw new RuntimeException("incomplete");
        final Set<String> keySet = UnifiedSet.newSet(domTree.keySet());
        for (final String exprName : keySet)
        {
            if (BINARY_OPERATORS.contains(exprName) || UNARY_OPERATORS.contains(exprName))
            {
                final Attribute attr = (Attribute)buildExpressionNode(finder, exprName, domTree);
                keySet.remove(exprName);
                final String opName =  getOnlySetElement(keySet);
                return filterQueryBuilder.buildOperationFromAttribute(attr, opName, domTree.get(opName));
            }
        }
        throw new RuntimeException("The expression is incomplete " + domTree);
    }

    static String getOnlySetElement(Set<String> set)
    {
        if (set.size() != 1)
        {
            throw new RuntimeException("Expected a single element in statement: " +  set);
        }

        return set.iterator().next();
    }
}
