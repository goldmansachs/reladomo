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

package com.gs.reladomo.serial.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;

public class JacksonReladomoSerialContext extends ReladomoSerializationContext
{
    private final JsonGenerator jgen;
    private final SerializerProvider provider;

    public JacksonReladomoSerialContext(SerializationConfig serializationConfig, SerialWriter writer, JsonGenerator jgen, SerializerProvider provider)
    {
        super(serializationConfig, writer);
        this.jgen = jgen;
        this.provider = provider;
    }

    public JsonGenerator getJgen()
    {
        return jgen;
    }

    public SerializerProvider getSerializerProvider()
    {
        return provider;
    }


}
