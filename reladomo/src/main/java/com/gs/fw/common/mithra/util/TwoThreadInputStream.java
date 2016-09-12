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

package com.gs.fw.common.mithra.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;


//todo: close doesn't work right when the reader thread is done
//todo: exception handling has to be improved
public class TwoThreadInputStream extends InputStream
{
    private static Logger logger = LoggerFactory.getLogger(TwoThreadInputStream.class.getName());
    private int bufferSize;
    private InputStream in;
    private int currentPos;
    private Throwable exception;
    private Buffer currentBuffer;
    private LinkedBlockingQueue bufferQueue;
    private LinkedBlockingQueue readQueue;
    private static final Object CLOSE = new Object();
    private static final Object FINISHED = new Object();

    public TwoThreadInputStream(InputStream inputStream, int bufferSize)
    {
        this.bufferSize = bufferSize;
        this.in = inputStream;
        bufferQueue = new LinkedBlockingQueue();
        readQueue = new LinkedBlockingQueue();
        for(int i=0;i<4;i++)
        {
            bufferQueue.add(new Buffer(new byte[bufferSize]));
        }
        ExceptionCatchingThread.submitTask(new ExceptionHandlingTask()
        {
            @Override
            public void execute()
            {
                boolean done = false;
                while(!done)
                {
                    try
                    {
                        Object o = bufferQueue.take();
                        if (o == CLOSE)
                        {
                            in.close();
                            break;
                        }
                        Buffer buf = (Buffer) o;
                        int read = in.read(buf.buf);
                        int total = read;
                        while(read >= 0 && total < buf.buf.length)
                        {
                            read = in.read(buf.buf, total, buf.buf.length - total);
                            if (read > 0) total += read;
                        }
                        buf.goodBytes = total;
                        if (total > 0) readQueue.put(buf);
                        if (read < 0)
                        {
                            readQueue.put(FINISHED);
                        }
                    }
                    catch (Throwable e)
                    {
                        logger.error("Error reading stream", e);
                        done = true;
                        exception = e;
                        reallyPut(FINISHED);
                    }
                }
            }

            private void reallyPut(Object o)
            {
                try
                {
                    readQueue.put(o);
                }
                catch (InterruptedException e)
                {
                    // ignore
                }

            }
        });
    }

    @Override
    public int read() throws IOException
    {
        ensureCurrent();
        if (currentPos == -1) return -1;
        return currentBuffer.buf[currentPos++];
    }

    private void ensureCurrent() throws IOException
    {
        if (exception != null)
        {
            if (exception instanceof IOException)
            {
                throw (IOException) exception;
            }
            throwIoException(exception, "Could not read archive");
        }
        if (currentBuffer == null || currentBuffer.goodBytes == currentPos)
        {
            try
            {
                if (currentBuffer != null)
                {
                    bufferQueue.put(currentBuffer);
                }
                Object o = readQueue.take();
                if (o == FINISHED)
                {
                    currentPos = -1;
                }
                else
                {
                    currentBuffer = (Buffer) o;
                    currentPos = 0;
                }
            }
            catch (InterruptedException e)
            {
                throwIoException(e, "how did this get interrupted?");
            }
        }
    }

    private void throwIoException(Throwable e, String msg)
            throws IOException
    {
        IOException exp = new IOException(msg);
        exp.initCause(e);
        throw exp;
    }

    @Override
    public int available() throws IOException
    {
        return currentBuffer.goodBytes - currentPos;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        ensureCurrent();
        if (currentPos == -1) return -1;
        int left = currentBuffer.goodBytes - currentPos;
        int toCopy = Math.min(left, len);
        int totalCopied = 0;
        System.arraycopy(currentBuffer.buf, currentPos, b, off, toCopy);
        currentPos += toCopy;
        totalCopied += toCopy;
        while (totalCopied < len)
        {
            ensureCurrent();
            if (currentPos == -1) return totalCopied;
            left = currentBuffer.goodBytes - currentPos;
            toCopy = Math.min(left, len - totalCopied);
            System.arraycopy(currentBuffer.buf, currentPos, b, off + totalCopied, toCopy);
            currentPos += toCopy;
            totalCopied += toCopy;
        }
        return totalCopied;
    }

    @Override
    public void close() throws IOException
    {
        bufferQueue.clear();
        try
        {
            bufferQueue.put(CLOSE);
        }
        catch (InterruptedException e)
        {
            throwIoException(e, "couldn't close");
        }
    }

    private static class Buffer
    {
        private byte[] buf;
        private int goodBytes;

        private Buffer(byte[] buf)
        {
            this.buf = buf;
        }
    }
}
