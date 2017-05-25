/*
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
 */

package com.gs.fw.common.mithra.util.serializer;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraTransaction;

import javax.transaction.Status;
import javax.transaction.Synchronization;

public class Serialized<T extends MithraObject>
{
    private T wrapped;
    private ReladomoDeserializer<T> deserializer;
    private SerializationConfig config;

    public Serialized(T wrapped, SerializationConfig config)
    {
        this.wrapped = wrapped;
        this.config = config;
        checkConfig();
    }

    private void checkConfig()
    {
        if (config == null)
        {
            throw new IllegalStateException("config must not be null");
        }
    }

    public Serialized(T wrapped, String configName)
    {
        this.wrapped = wrapped;
        this.config = SerializationConfig.byName(configName);
        checkConfig();
    }

    protected Serialized(ReladomoDeserializer<T> deserializer) throws DeserializationException
    {
        this.deserializer = deserializer;
        deserializer.checkSingleObjectDeserialized();
    }

    public T getWrapped()
    {
        if (this.wrapped == null && this.deserializer != null)
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            if (tx != null)
            {
                tx.registerSynchronization(new Synchronization()
                {
                    @Override
                    public void beforeCompletion()
                    {
                        //do nothing
                    }

                    @Override
                    public void afterCompletion(int status)
                    {
                        if (status != Status.STATUS_COMMITTED && status != Status.STATUS_COMMITTING)
                        {
                            wrapped = null;
                        }

                    }
                });
            }
            this.wrapped = deserializer.getDeserializationResultAsObject();
        }
        return wrapped;
    }

    public SerializationConfig getConfig()
    {
        if (this.config == null)
        {
            throw new IllegalStateException("No config found. Was this the result of a deserialization?");
        }
        return config;
    }

}

