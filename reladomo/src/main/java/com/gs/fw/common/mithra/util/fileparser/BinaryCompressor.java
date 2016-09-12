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

package com.gs.fw.common.mithra.util.fileparser;


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.primitive.ObjectByteHashMap;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.lz4.LZ4BlockInputStream;
import com.gs.fw.common.mithra.util.lz4.LZ4BlockOutputStream;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;
import java.util.TimeZone;

public class BinaryCompressor
{
    private static final int VERSION = 1;

    private static final ObjectByteHashMap<Class> CLASS_TO_TYPE_MAP = ObjectByteHashMap.newMap();

    private static final byte BOOLEAN_TYPE = 1;
    private static final byte BYTE_TYPE = 2;
    private static final byte CHAR_TYPE = 3;
    private static final byte SHORT_TYPE = 4;
    private static final byte INTEGER_TYPE = 5;
    private static final byte LONG_TYPE = 6;
    private static final byte FLOAT_TYPE = 7;
    private static final byte DOUBLE_TYPE = 8;
    private static final byte STRING_TYPE = 9;
    private static final byte DATE_TYPE = 10;
    private static final byte TIMESTAMP_TYPE = 11;
    private static final byte TIME_TYPE = 12;
    private static final byte BYTE_ARRAY_TYPE = 13;
    private static final byte BIG_DECIMAL_TYPE = 14;

    static
    {
        CLASS_TO_TYPE_MAP.put(Boolean.class, (BOOLEAN_TYPE));
        CLASS_TO_TYPE_MAP.put(Byte.class, (BYTE_TYPE));
        CLASS_TO_TYPE_MAP.put(Character.class, (CHAR_TYPE));
        CLASS_TO_TYPE_MAP.put(Short.class, (SHORT_TYPE));
        CLASS_TO_TYPE_MAP.put(Integer.class, (INTEGER_TYPE));
        CLASS_TO_TYPE_MAP.put(Long.class, (LONG_TYPE));
        CLASS_TO_TYPE_MAP.put(Float.class, (FLOAT_TYPE));
        CLASS_TO_TYPE_MAP.put(Double.class, (DOUBLE_TYPE));
        CLASS_TO_TYPE_MAP.put(String.class, (STRING_TYPE));
        CLASS_TO_TYPE_MAP.put(java.util.Date.class, (DATE_TYPE));
        CLASS_TO_TYPE_MAP.put(Timestamp.class, (TIMESTAMP_TYPE));
        CLASS_TO_TYPE_MAP.put(Time.class, (TIME_TYPE));
        CLASS_TO_TYPE_MAP.put(byte[].class, (BYTE_ARRAY_TYPE));
        CLASS_TO_TYPE_MAP.put(BigDecimal.class, (BIG_DECIMAL_TYPE));
    }

    public void compressData(List<MithraParsedData> results, OutputStream fos) throws IOException
    {
        LZ4BlockOutputStream lz4 = new LZ4BlockOutputStream(fos, 1024*1024, true);
        DataOutputStream dataOut = new DataOutputStream(lz4);
        dataOut.writeInt(VERSION);
        writeString(TimeZone.getDefault().getID(), dataOut);
        dataOut.writeInt(results.size());
        for(int i=0;i<results.size();i++)
        {
            MithraParsedData mithraParsedData = results.get(i);
            writeString(mithraParsedData.getParsedClassName(), dataOut);
            RelatedFinder finder = mithraParsedData.getFinder();
            Attribute[] attributes = finder.getPersistentAttributes();
            dataOut.writeInt(attributes.length);
            for(Attribute a: attributes)
            {
                writeString(a.getAttributeName(), dataOut);
                dataOut.write(getType(a));
            }
            List<MithraDataObject> data = mithraParsedData.getDataObjects();
            dataOut.writeInt(data.size());
            ColumnarOutStream columnarOutStream = new ColumnarOutStream(dataOut);
            for(Attribute a: attributes)
            {
                SingleColumnAttribute sca = (SingleColumnAttribute) a;
                sca.zEncodeColumnarData(data, columnarOutStream);
            }
        }
        dataOut.flush();
        lz4.flush();
    }

    protected InputStream getInputStreamFromFilename(String filename) throws IOException
    {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(filename);

        if (is == null)
        {
            is = getInputStreamFromFile(new File(filename));
        }

        return is;
    }

    protected InputStream getInputStreamFromFile(File file) throws IOException
    {
        InputStream is;

        try
        {
            is = new FileInputStream(file);
        }
        catch (FileNotFoundException e)
        {
            FileNotFoundException e2 = new FileNotFoundException("could not find file " + file.getName());
            e2.initCause(e);
            throw e2;
        }

        return is;
    }

    public List<MithraParsedData> decompress(String filename)
    {
        MithraFastList<MithraParsedData> result = null;
        try
        {
            InputStream fis = getInputStreamFromFilename(filename);
            result = decompress(new URL("file://"+filename), fis);
            fis.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not decompress data in file "+filename, e);
        }
        return result;
    }

    public MithraFastList<MithraParsedData> decompress(URL url, InputStream fis)
    {
        try
        {
            LZ4BlockInputStream lz4 = new LZ4BlockInputStream(fis);
            DataInputStream dataIn = new DataInputStream(lz4);
            ColumnarInStream columnarInStream = new ColumnarInStream(dataIn);
            int version = dataIn.readInt();
            if (version != VERSION)
            {
                throw new RuntimeException("Unsupported version. Expecting "+VERSION+" got "+version);
            }
            String timezoneId = readString(dataIn);
            columnarInStream.setTimezoneId(timezoneId);
            int classes = dataIn.readInt();
            MithraFastList<MithraParsedData> result = new MithraFastList(classes);
            for(int i=0;i<classes;i++)
            {
                MithraParsedData parsedData = new MithraParsedData();
                result.add(parsedData);
                String className = readString(dataIn);
                parsedData.setParsedClassName(className);
                RelatedFinder finder = parsedData.getFinder();
                int attrCount = dataIn.readInt();
                Attribute[] attrs = new Attribute[attrCount];
                for(int a = 0; a < attrCount; a++)
                {
                    String attributeName = readString(dataIn);
                    attrs[a] = finder.getAttributeByName(attributeName);
                    if (attrs[a] == null)
                    {
                        throw new RuntimeException("Could not find attribute "+attributeName);
                    }
                    byte type = dataIn.readByte();
                    if (getType(attrs[a]) != type)
                    {
                        throw new RuntimeException("Attribute type changed for attribute "+attributeName);
                    }
                }
                parsedData.setAttributes(FastList.newListWith(attrs));
                int rows = dataIn.readInt();
                for(int r = 0; r < rows; r++)
                {
                    parsedData.createAndAddDataObject(r);
                }
                for(Attribute a: attrs)
                {
                    SingleColumnAttribute singleColumnAttribute = (SingleColumnAttribute) a;
                    singleColumnAttribute.zDecodeColumnarData(parsedData.getDataObjects(), columnarInStream);
                }
            }
            return result;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not decompress data at location "+url.toString(), e);
        }
    }

    private byte getType(Attribute a)
    {
        byte result = CLASS_TO_TYPE_MAP.getIfAbsent(a.valueType(), (byte) -1);
        if (result < 0)
            throw new RuntimeException("Unrecognized type for attribute "+a.getAttributeName());
        return result;
    }

    private void writeString(String s, DataOutputStream dataOut) throws IOException
    {
        byte[] bytes = s.getBytes("UTF-8");
        dataOut.writeInt(bytes.length);
        dataOut.write(bytes);
    }

    private String readString(DataInputStream dataIn) throws IOException
    {
        int len = dataIn.readInt();
        byte[] bytes = new byte[len];
        dataIn.readFully(bytes);
        return new String(bytes, Charset.forName("UTF-8"));
    }

}
