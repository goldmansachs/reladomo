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
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;

import java.io.IOException;

public class ExampleJacksonReladomoSerializer<T extends MithraObject> extends StdSerializer<T>
{
    private String serialConfigName;

    public ExampleJacksonReladomoSerializer(Class<T> t)
    {
        super(t);
    }

    public ExampleJacksonReladomoSerializer(Class<T> t, String serialConfigName)
    {
        super(t);
        this.serialConfigName = serialConfigName;
    }


    @Override
    public void serialize(T t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
    {
        RelatedFinder finder = t.zGetPortal().getFinder();
        SerializationConfig serializationConfig;
        if (serialConfigName != null)
        {
            serializationConfig = SerializationConfig.byName(this.serialConfigName);
        }
        else
        {
            serializationConfig = SerializationConfig.shallowWithDefaultAttributes(finder);
        }
        JacksonReladomoSerialContext jacksonReladomoSerialContext = new JacksonReladomoSerialContext(serializationConfig, new JacksonReladomoSerialWriter(), jsonGenerator, serializerProvider);
        jacksonReladomoSerialContext.serializeReladomoObject(t);
    }
}
