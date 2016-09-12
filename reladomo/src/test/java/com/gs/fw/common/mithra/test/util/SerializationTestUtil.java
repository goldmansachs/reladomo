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

package com.gs.fw.common.mithra.test.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializationTestUtil
{
    public static byte[] serialize(Object o) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2000);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.flush();
        bos.flush();
        byte[] pileOfBytes = bos.toByteArray();
        bos.close();
        return pileOfBytes;
    }

    public static Object deserialize(byte[] input) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(input);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object result = ois.readObject();
        ois.close();
        bis.close();
        return result;
    }

    public static <T> T serializeDeserialize(Object object) throws IOException, ClassNotFoundException
    {
        byte[] pileOfBytes = serialize(object);
        return (T) deserialize(pileOfBytes);
    }
}
