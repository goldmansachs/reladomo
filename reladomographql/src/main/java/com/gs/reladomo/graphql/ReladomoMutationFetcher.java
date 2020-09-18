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
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class ReladomoMutationFetcher<T> implements DataFetcher<T>
{
    private final RelatedFinder<T> finder;
    private final String name;
    private final Class<T> reladomoClass;


    public ReladomoMutationFetcher (final String name, final String reladomoClassName, final RelatedFinder<T> finder)
    {
        this.finder = finder;
        this.name = name;
        try
        {
            this.reladomoClass = (Class<T>)Class.forName(reladomoClassName);
        }
        catch (final Exception e)
        {
            throw new RuntimeException ("failed to find class " + reladomoClassName, e);
        }
    }

    @Override
    public T get (final DataFetchingEnvironment environment)
    {
        //
        // The graphql specification dictates that input object arguments MUST
        // be maps.  You can convert them to POJOs inside the data fetcher if that
        // suits your code better
        //
        // See http://facebook.github.io/graphql/October2016/#sec-Input-Objects
        //
        final Map<String, Object> objectInputMap = environment.getArgument (this.name);

        try
        {
            final T obj = this.createReladomoObject(environment);

            this.setAttributeValues(objectInputMap, obj);

            MithraManagerProvider.getMithraManager().executeTransactionalCommand((TransactionalCommand) tx ->
            {
                ((MithraTransactionalObject)obj).insert ();
                return null;
            });

            return obj;
        } catch (final Exception e)
        {
            throw new RuntimeException ("failed to insert " + objectInputMap, e);
        }
    }

    private void setAttributeValues(final Map<String, Object> objectInputMap, final T obj)
    {
        for (final String each : objectInputMap.keySet ())
        {
            final Attribute attr = finder.getAttributeByName (each);
            final Object val = objectInputMap.get (each);
            if (attr instanceof IntegerAttribute)
            {
                ((IntegerAttribute) attr).setIntValue (obj, (int) val);
            }
            if (attr instanceof LongAttribute)
            {
                ((LongAttribute) attr).setLongValue (obj, (long) val);
            }
            if (attr instanceof ShortAttribute)
            {
                ((ShortAttribute) attr).setShortValue (obj, (short) val);
            }
            if (attr instanceof StringAttribute)
            {
                ((StringAttribute) attr).setStringValue (obj, (String) val);
            }
            if (attr instanceof TimestampAttribute)
            {
                ((TimestampAttribute) attr).setTimestampValue (obj, (Timestamp)val);
            }
            if (attr instanceof BooleanAttribute)
            {
                ((BooleanAttribute) attr).setBooleanValue (obj, (boolean) val);
            }
            if (attr instanceof DoubleAttribute)
            {
                ((DoubleAttribute) attr).setDoubleValue (obj, (double) val);
            }
            if (attr instanceof FloatAttribute)
            {
                ((FloatAttribute) attr).setFloatValue (obj, (float) val);
            }
        }
    }

    private T createReladomoObject(final DataFetchingEnvironment environment) throws Exception
    {
        final List<Timestamp> paramList = FastList.newList();
        if (this.finder.getAsOfAttributes() == null)
        {
            return reladomoClass.newInstance();
        }

        for (AsOfAttribute each : this.finder.getAsOfAttributes())
        {
            if (!each.isProcessingDate())
            {
                paramList.add(environment.getArgument(each.getAttributeName()));
            }
        }

        Class<Timestamp> paramTypes[] = new Class[paramList.size()];
        Object params[] = new Object[paramList.size()];
        for (int i = 0; i <paramList.size(); i++)
        {
            paramTypes[i] = Timestamp.class;
            params[i] = paramList.get(i);
        }
        return reladomoClass.getConstructor(paramTypes).newInstance(params);
    }
}

