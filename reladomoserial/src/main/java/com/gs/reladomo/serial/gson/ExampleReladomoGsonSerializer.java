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

package com.gs.reladomo.serial.gson;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;

import java.lang.reflect.Type;

public class ExampleReladomoGsonSerializer<T extends MithraObject> implements JsonSerializer<T>
{

    @Override
    public JsonElement serialize(T t, Type type, JsonSerializationContext gsonContext)
    {
        RelatedFinder finder = t.zGetPortal().getFinder();
        GsonReladomoSerialWriter writer = new GsonReladomoSerialWriter();
        GsonRelodomoSerialContext gsonRelodomoSerialContext = new GsonRelodomoSerialContext(SerializationConfig.shallowWithDefaultAttributes(finder), writer, gsonContext);
        gsonRelodomoSerialContext.serializeReladomoObject(t);
        return gsonRelodomoSerialContext.getResult();
    }
}
