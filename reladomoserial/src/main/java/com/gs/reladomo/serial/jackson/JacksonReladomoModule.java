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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import com.gs.fw.common.mithra.util.serializer.SerializedList;

public class JacksonReladomoModule extends SimpleModule
{
    public JacksonReladomoModule()
    {
        super("JacksonReladomo", new Version(1,0,0,"", null, null));
        addSerializer(Serialized.class, new JacksonWrappedSerializer(Serialized.class));
        addSerializer(SerializedList.class, new JacksonWrappedListSerializer(SerializedList.class));
        addDeserializer(Serialized.class, new JacksonReladomoWrappedDeserializer());
        addDeserializer(SerializedList.class, new JacksonReladomoWrappedListDeserializer());
    }
}
