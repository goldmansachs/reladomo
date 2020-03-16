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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.transaction.MithraTransactionalResource;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.function.BiFunction;

public class SDLGenerator
{
    private static final Logger LOGGER = LoggerFactory.getLogger (SDLGenerator.class);
    private static boolean withComments = false;

    public void generate (final String filename)
    {
        try (final Writer out = new FileWriter (filename))
        {
            if (withComments) out.write ("# generated on " + LocalDateTime.now () + "\n");
            this.generate (out);

        } catch (final Exception e)
        {
            throw new RuntimeException ("failed to write into " + new File (filename).getAbsolutePath (), e);
        }

        LOGGER.debug ("The complete schema was written to " + new File (filename).getAbsolutePath ());
    }

    protected void generate (final Writer out) throws IOException
    {
        this.writeMutationBlock (out);

        final Set<String> declaredTypes = this.writeQueryBlock (out);

        for (final MithraRuntimeCacheController controller : MithraManagerProvider.getMithraManager ().getRuntimeCacheControllerSet ())
        {
            String name = controller.getClassName ();
            name = StringKit.decapitalize (name, name.lastIndexOf ('.') + 1);
            name = StringKit.capitalize (name);
            final RelatedFinder finder = controller.getFinderInstance ();

            out.write ("#################### " + controller.getClassName () + " ##################\n");

            generateType (out, declaredTypes, name, finder);
            generateAggregate (out, declaredTypes, name, finder);
            generateInput (out, name, finder);
            generateOrderBy (out, name, finder);
            generateFinder (out, declaredTypes, name, finder);
            generateTupleInput (out, name, finder);
        }
    }

    private void generateType (final Writer out, final Set<String> declaredTypes, final String name, final RelatedFinder finder) throws IOException
    {
        out.write ("type " + name + "\n{\n");
        for (final Attribute attr : finder.getPersistentAttributes ())
        {
            this.writeAttribute (attr, out, (aName, type) -> "    " + aName + ": " + type + "\n");
        }
        if (finder.getAsOfAttributes () != null)
        {
            for (final AsOfAttribute attr : finder.getAsOfAttributes ())
            {
                out.write ("    " + attr.getAttributeName () + ": Timestamp\n");
            }
        }

        if (finder.getSourceAttribute () != null)
        {
            this.writeAttribute (finder.getSourceAttribute(), out, (aName, type) -> "    " + aName + ": " + type + "\n");
        }

        this.generateRelationshipBlock (out, finder, declaredTypes, false);
        out.write ("}\n\n");
    }

    private void generateTupleInput (final Writer out, final String name, final RelatedFinder finder) throws IOException
    {
        out.write ("input " + name + "FinderTuple\n{\n");
        for (final Attribute attr : finder.getPersistentAttributes ())
        {
            this.writeAttribute (attr, out, (aName, type) -> "    " + aName + ": " + type + "\n");
        }

//        this.writeRelationshipsBlock (out, finder, declaredTypes);
        out.write ("}\n\n");
    }


    private void generateFinder (final Writer out, final Set<String> declaredTypes, final String name, final RelatedFinder finder) throws IOException
    {
        out.write ("input " + name + "Finder\n{\n");
        final BiFunction<String, String, String> func = (aName, type) ->
        {
            String attrType = null;
            switch (type)
            {
                case "Int":
                    attrType = "IntegerAttribute";
                    break;
                case "Float":
                    attrType = "FloatAttribute";
                    break;
                case "String":
                    attrType = "StringAttribute";
                    break;
                case "Timestamp":
                    attrType = "TimestampAttribute";
                    break;
                case "Boolean":
                    attrType = "BooleanAttribute";
                    break;
            }

            return "    " + aName + ": " + attrType + "\n";
        };

        for (final Attribute attr : finder.getPersistentAttributes ())
        {
            this.writeAttribute (attr, out, func);

        }

        if (finder.getAsOfAttributes () != null)
        {
            for (final AsOfAttribute attr : finder.getAsOfAttributes ())
            {
                out.write ("    " + attr.getAttributeName () + ": TimestampAttribute\n");
            }
        }

        if (finder.getSourceAttribute () != null)
        {
            this.writeAttribute (finder.getSourceAttribute (), out, func);
        }

        final String capName = StringKit.capitalize (name);

        this.generateRelationshipBlock (out, finder, declaredTypes, true);

        out.write ("    AND: [" + capName + "Finder]\n");
        out.write ("    OR: [" + capName + "Finder]\n");
        out.write ("    EXPR: [" + capName + "FloatExpr]\n");
        out.write ("    tupleIn: [" + capName + "FinderTuple]\n");
        out.write ("}\n\n");

        out.write ("input " + capName + "FloatExpr\n");
        out.write ("{\n");
        generateMathOperators(out, capName);
        out.write ("    eq: Float\n");
        out.write ("    notEq: Float\n");
        out.write ("    in: [Float]\n");
        out.write ("    notIn: [Float]\n");
        out.write ("    greaterThan: Float\n");
        out.write ("    greaterThanEquals: Float\n");
        out.write ("    lessThan: Float\n");
        out.write ("    lessThanEquals: Float\n");
        out.write ("}\n");

        out.write ("input " + capName + CALCULATED_ATTRIBUTE + "\n");
        out.write ("{\n");
        generateMathOperators(out, capName);
        out.write ("    id: IntegerAttribute\n");
        out.write ("    orderId: IntegerAttribute\n");
        out.write ("    productId: IntegerAttribute\n");
        out.write ("    quantity: FloatAttribute\n");
        out.write ("    originalPrice: FloatAttribute\n");
        out.write ("    discountPrice: FloatAttribute\n");
        out.write ("    state: StringAttribute\n");
        out.write ("    VAL: Float\n");
        out.write ("}\n");
    }

    private void generateMathOperators(final Writer out, final String capName) throws IOException
    {
        out.write ("   plus: [" + capName + CALCULATED_ATTRIBUTE + "]\n");
        out.write ("   minus: [" + capName + CALCULATED_ATTRIBUTE + "]\n");
        out.write ("   times: [" + capName + CALCULATED_ATTRIBUTE + "]\n");
        out.write ("   dividedBy: [" + capName + CALCULATED_ATTRIBUTE + "]\n");
        out.write ("   absoluteValue: " + capName + CALCULATED_ATTRIBUTE + "\n");
    }

    private static final String CALCULATED_ATTRIBUTE = "CalculatedAttribute";

    private void generateInput (final Writer out, final String name, final RelatedFinder finder) throws IOException
    {
        out.write ("input " + name + "Input\n{\n");
        BiFunction<String, String, String> func = (aName, type) -> "    " + aName + ": " + type + "\n";
        for (final Attribute attr : finder.getPersistentAttributes ())
        {
            this.writeAttribute (attr, out, func);
        }
        if (finder.getSourceAttribute() != null)
        {
            this.writeAttribute (finder.getSourceAttribute(), out, func);
        }
        out.write ("}\n\n");
    }

    private void generateOrderBy (final Writer out, final String name, final RelatedFinder finder) throws IOException
    {
        out.write ("input " + name + "OrderBy\n{\n");
        for (final Attribute attr : finder.getPersistentAttributes ())
        {
            this.writeAttribute (attr, out, (aName, type) -> "    " + aName + ": AcsDesc\n");
        }
        out.write ("}\n\n");
    }

    private void generateAggregate (final Writer out, final Set<String> declaredTypes, final String name, final RelatedFinder finder) throws IOException
    {
        out.write ("type " + name + "Aggregate\n{\n");
        for (final Attribute attr : finder.getPersistentAttributes ())
        {
            this.writeAttribute (attr, out, (aName, type) -> {
                final StringBuilder buf = new StringBuilder ();
                if (!"Float".equals (type))
                {
                    buf.append ("    ").append (aName).append (": ").append (type).append ("\n");
                }
                else
                {
                    buf.append ("    ").append (aName).append (": FloatAggregate\n");
                }
                return buf.toString ();
            });
        }

        generateRelationshipBlock (out, finder, declaredTypes, false);
        out.write ("}\n\n");
    }

    private void generateRelationshipBlock (final Writer out, final RelatedFinder finder, final Set<String> declaredTypes, final boolean withFinderSufix) throws IOException
    {
        for (final Object each : finder.getRelationshipFinders ())
        {
            if (!(each instanceof AbstractRelatedFinder))
            {
                if (withComments) out.write ("# unrecognised relationship " + each.getClass () + "\n");
                continue;
            }

            final AbstractRelatedFinder relationshipFinder = (AbstractRelatedFinder) each;
            final Mapper mapper = relationshipFinder.zGetMapper ();
            String type = relationshipFinder.getFinderClassName ();
            type = type.substring (type.lastIndexOf ('.') + 1);
            type = type.substring (0, type.length () - "Finder".length ());
            if (declaredTypes.contains (type))  // ensure that all relationships have the other side in the runtimeConfig.xml as well
            {
                out.write ("    " + mapper.getRelationshipPath () + ": ");
                if (mapper.isToMany () && !withFinderSufix) out.write ("[" + type + "]");
                else out.write (type + (withFinderSufix ? "Finder" : ""));
                out.write ("\n");
            }
        }
    }

    private Set<String> writeQueryBlock (final Writer out) throws IOException
    {
        out.write ("type Query\n{\n");
        final Set<String> declaredTypes = new UnifiedSet<>();
        for (final MithraRuntimeCacheController controller : MithraManagerProvider.getMithraManager ().getRuntimeCacheControllerSet ())
        {
            String name = controller.getClassName ();
            name = StringKit.decapitalize (name, name.lastIndexOf ('.') + 1);
            final String capName = StringKit.capitalize (name);
            declaredTypes.add (capName);
            out.write ("    " + name + "ById(");
            final RelatedFinder finder = controller.getFinderInstance ();
            for (final Attribute attr : finder.getPrimaryKeyAttributes ())
            {
                this.writeAttribute (attr, out, (aName, type) -> " " + aName + ": " + type + "! ");
            }

            out.write ("): " + capName + "\n");
            out.write ("    " + StringKit.englishPluralize(name) + "(findMany: " + capName + "Finder filter: " + capName + "Finder where: " + capName + "Finder order_by: " + capName + "OrderBy limit: Int): [" + capName + "]\n");
            out.write ("    " + name + "_aggregate(findMany: " + capName + "Finder filter: " + capName + "Finder where: " + capName + "Finder order_by: " + capName + "OrderBy limit: Int): [" + capName + "Aggregate]\n\n");
        }
        out.write ("}\n\n\n");

        return declaredTypes;
    }

    private void writeMutationBlock (final Writer out) throws IOException
    {
        out.write ("type Mutation\n{\n");
        for (final MithraRuntimeCacheController controller : MithraManagerProvider.getMithraManager ().getRuntimeCacheControllerSet ())
        {
            if (isTransactional (controller))
            {
                String name = controller.getClassName ();
                name = StringKit.decapitalize (name, name.lastIndexOf ('.') + 1);
                final String capName = StringKit.capitalize (name);
                out.write ("    " + name + "_insert(" + name + ": " + capName + "Input");

                AsOfAttribute[] asOfAttributes = controller.getFinderInstance().getAsOfAttributes();
                if (asOfAttributes != null)
                {
                    for (final AsOfAttribute asOfAttribute : asOfAttributes)
                    {
                        if (!asOfAttribute.isProcessingDate()) out.write(" " + asOfAttribute.getAttributeName() + ": Timestamp!");
                    }
                }
                out.write ("): " + capName + "\n");
            }
        }
        out.write ("}\n\n\n");
    }

    private boolean isTransactional (final MithraRuntimeCacheController controller)
    {
        try
        {
            return MithraTransactionalResource.class.isAssignableFrom (Class.forName (controller.getClassName ()));
        } catch (final Exception e)
        {
            return false;
        }
    }

    private void writeAttribute (final Attribute attr, final Writer out, final BiFunction<String, String, String> writeOne) throws IOException
    {
        final String name = attr.getAttributeName ();

         /* JSON types:
        Int: A signed 32-bit integer.
        Float: A signed double-precision floating-point value.
        String: A UTF-8 character sequence.
        Boolean: true or false.
          */
        if (withComments)
            out.write ("    # " + name + " of " + attr.getClass ().getSuperclass ().getSimpleName () + "\n");
        String jsonType = null;
        if (attr instanceof StringAttribute || attr instanceof CharAttribute) jsonType = "String";
        else if (attr instanceof IntegerAttribute || attr instanceof LongAttribute || attr instanceof ShortAttribute)
            jsonType = "Int";
        else if (attr instanceof FloatAttribute || attr instanceof DoubleAttribute) jsonType = "Float";
        else if (attr instanceof TimestampAttribute) jsonType = "Timestamp";
        else if (attr instanceof BooleanAttribute) jsonType = "Boolean";

        if (jsonType != null)
        {
            out.write (writeOne.apply (name, jsonType));
        }
    }
}