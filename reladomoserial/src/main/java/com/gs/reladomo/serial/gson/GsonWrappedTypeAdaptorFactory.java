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

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import com.gs.fw.common.mithra.util.serializer.SerializedList;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class GsonWrappedTypeAdaptorFactory implements TypeAdapterFactory
{
    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken)
    {
        if (Serialized.class.equals(typeToken.getRawType()))
        {
            Type type = typeToken.getType();
            if (type instanceof ParameterizedType)
            {
                Type[] typeArguments = ((ParameterizedType) type).getActualTypeArguments();
                return new GsonWrappedTypedAdapter((Class) typeArguments[0]);
            }
            return new GsonWrappedTypedAdapter(null);
        }
        if (SerializedList.class.equals(typeToken.getRawType()))
        {
            Type type = typeToken.getType();
            if (type instanceof ParameterizedType)
            {
                Type[] typeArguments = ((ParameterizedType) type).getActualTypeArguments();
                return new GsonWrappedListTypedAdapter((Class) typeArguments[0]);
            }
            return new GsonWrappedListTypedAdapter(null);
        }
        return null;
    }
}
