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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.reladomo.serial.gson;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;

public class GsonRelodomoSerialContext extends ReladomoSerializationContext
{
    private List<JsonElement> stack = FastList.newList();
    private JsonSerializationContext gsonContext;
    private JsonObject result;

    public GsonRelodomoSerialContext(SerializationConfig serializationConfig, SerialWriter writer, JsonSerializationContext gsonContext)
    {
        super(serializationConfig, writer);
        this.gsonContext = gsonContext;
    }

    public JsonObject getCurrentResultAsObject()
    {
        return (JsonObject) stack.get(stack.size() - 1);
    }

    public JsonObject pushNewObject(String name)
    {
        JsonObject newObj = new JsonObject();
        if (name == null)
        {
            if (stack.size() == 0)
            {
                result = newObj;
                stack.add(newObj);
            }
            else
            {
                JsonElement jsonElement = stack.get(stack.size() - 1);
                if (jsonElement instanceof JsonArray)
                {
                    ((JsonArray)jsonElement).add(newObj);
                    stack.add(newObj);
                }
            }
        }
        else
        {
            getCurrentResultAsObject().add(name, newObj);
            stack.add(newObj);
        }
        return newObj;
    }

    public JsonElement pop()
    {
        return stack.remove(stack.size() - 1);
    }

    public JsonArray pushNewArray(String name)
    {
        JsonArray newObj = new JsonArray();
        getCurrentResultAsObject().add(name, newObj);
        stack.add(newObj);
        return newObj;
    }

    public JsonObject getResult()
    {
        return result;
    }

    public JsonSerializationContext getGsonContext()
    {
        return gsonContext;
    }
}
