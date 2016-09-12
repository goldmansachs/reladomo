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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public abstract class OutputStreamWriter
{
    protected abstract byte getRequestType();

    protected abstract void writeParameters(ObjectOutputStream objectOutputStream) throws IOException;

    public static int flushErrorStream(HttpURLConnection urlc)
    {
        try
        {
            if (urlc != null)
            {
                int code = urlc.getResponseCode();
                InputStream es = urlc.getErrorStream();
                while (es.read() > 0)
                {
                    // eat the response
                }
                // close the errorstream
                es.close();
                return code;
            }
        }
        catch (IOException e)
        {
            // ignore
        }
        return -1;
    }

    public static int flushInputStream(HttpURLConnection urlc)
    {
        try
        {
            if (urlc != null)
            {
                int code = urlc.getResponseCode();
                InputStream es = urlc.getInputStream();
                while (es.read() > 0)
                {
                    // eat the response
                }
                // close the errorstream
                es.close();
                return code;
            }
        }
        catch (IOException e)
        {
            // ignore
        }
        return -1;
    }

    public static HttpURLConnection post(URL url, OutputStreamWriter writer) throws IOException
    {
        HttpURLConnection urlc = (HttpURLConnection) url.openConnection();
        if (FastServletProxyFactory.serverSupportsChunking(url))
        {
            streamWriter(urlc, writer);
        }
        else
        {
            bufferWriter(urlc, writer);
        }
        return urlc;
    }

    public static void streamWriter(HttpURLConnection urlc, OutputStreamWriter writer) throws IOException
    {
        urlc.setRequestMethod("POST");
        urlc.setDoInput(true);
        urlc.setDoOutput(true);
        urlc.setChunkedStreamingMode(2048);
        OutputStream outputStream = urlc.getOutputStream();
        writeStuff(outputStream, writer);
        outputStream.close();
    }

    public static void bufferWriter(HttpURLConnection urlc, OutputStreamWriter writer) throws IOException
    {
        urlc.setRequestMethod("POST");
        urlc.setDoInput(true);
        urlc.setDoOutput(true);
        ByteArrayOutputStream buffered = new ByteArrayOutputStream(2048);
        writeStuff(buffered, writer);
        buffered.close();
        byte[] result = buffered.toByteArray();

        urlc.setRequestProperty("Content-Length", String.valueOf(result.length));
        OutputStream outputStream = urlc.getOutputStream();
        outputStream.write(result);
        outputStream.close();
    }

    public static void writeStuff(OutputStream outputStream, OutputStreamWriter writer) throws IOException
    {
        outputStream.write(writer.getRequestType());
        ObjectOutputStream out = new ObjectOutputStream(outputStream);
        //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
        writer.writeParameters(out);
        out.flush();
    }

    public static String getResponseBodyAsString(HttpURLConnection urlc) throws IOException
    {
        byte[] buf = new byte[64];
        InputStream is = urlc.getResponseCode() >= 400 ? urlc.getErrorStream() : urlc.getInputStream();
        int ret = 0;
        String result = "";
        while ((ret = is.read(buf)) > 0)
        {
            result += new String(buf, 0, ret, "ISO8859_1");
        }
        // close the inputstream
        is.close();
        return result;
    }
}
