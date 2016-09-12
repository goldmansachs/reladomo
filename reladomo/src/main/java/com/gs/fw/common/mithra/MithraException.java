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

package com.gs.fw.common.mithra;

import java.io.*;


public class MithraException extends RuntimeException
{

    private transient Throwable cause;

    public MithraException(String message)
    {
        super(message);
    }

    public MithraException(String message, Throwable nestedException)
    {
        super(message);
        this.cause = nestedException;
    }

    public synchronized Throwable initCause(Throwable cause)
    {
        this.cause = cause;
        return this;
    }

    public Throwable getCause()
    {
        return this.cause;
    }

    protected byte[] serializeObject(Object o) throws IOException
    {
        byte[] pileOfBytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2000);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.flush();
        bos.flush();
        pileOfBytes = bos.toByteArray();
        bos.close();
        return pileOfBytes;
    }

    protected byte[] serializeCause() throws IOException
    {
        byte[] result = null;
        try
        {
            result = serializeObject(this.cause);
        }
        catch(Exception e)
        {
            result = serializeObject(new MithraException("cause was not serializable. "+this.cause.getClass().getName()+": "+this.cause.getMessage()));
        }
        return result;
    }

    private Throwable deserializeCause(byte[] buf, String causeClass, String causeMessage) throws IOException, ClassNotFoundException
    {
        try
        {
            ByteArrayInputStream bis  = new ByteArrayInputStream(buf);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object result = ois.readObject();
            ois.close();
            bis.close();
            return (Throwable) result;
        }
        catch (Exception e)
        {
            return new MithraException("Could not desrialize cause "+causeClass+": "+causeMessage, e);
        }
    }

    private void writeObject(ObjectOutputStream out)
        throws IOException
    {
        this.getStackTrace(); // make sure it's filled in.
        out.defaultWriteObject();
        if (this.cause != null)
        {
            byte[] buf = serializeCause();
            out.writeInt(buf.length);
            out.writeObject(this.cause.getClass().getName());
            out.writeObject(this.cause.getMessage());
            out.write(buf);
        }
        else
        {
            out.writeInt(0);
        }
    }

    private synchronized void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        int size = in.readInt();
        if (size > 0)
        {
            String causeClass = (String) in.readObject();
            String causeMessage = (String) in.readObject();
            byte[] buf = new byte[size];
            in.readFully(buf);
            this.cause = deserializeCause(buf, causeClass, causeMessage);
        }
    }
}
