
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import com.mockobjects.Verifiable;
import junit.framework.TestCase;

import com.gs.fw.common.mithra.util.execute.StreamPumper;


public class StreamPumperTest extends TestCase
{

    public void testStreamPumper() throws Exception
    {
        MockInputStream in = new MockInputStream();
        MockOutputStream out = new MockOutputStream();

        StreamPumper streamPumper = new StreamPumper(in, out);
        streamPumper.start();

        // Wait and make sure the pumper hasn't copied anything
        assertFalse("Didn't expect the pumper to have copied anything", out.join(1000));

        // Now give the pumper something to copy
        in.write("Something");

        assertTrue("Expected the pumper to have copied something", out.join(1000));
        assertEquals("Pumper copied the wrong bytes", "Something", out.toString());

        out.clearNotificationFlag();

        // Give the pumper something else to copy
        out.reset();
        in.write("Else");

        assertTrue("Expected the pumper to have copied something", out.join(1000));
        assertEquals("Pumper copied the wrong bytes", "Else", out.toString());

        out.clearNotificationFlag();

        // Make sure we can shut down the pumper
        out.reset();
        streamPumper.finish();

        streamPumper.join(5000);
        assertFalse("Expected the pumper to have completed", streamPumper.isAlive());

        in.verify();
    }

    private static class MockInputStream extends InputStream implements Verifiable
    {
        private final LinkedList streams = new LinkedList();

        public void write(String input)
        {
            this.streams.add(input.getBytes());
        }

        public int available() throws IOException
        {
            int available = 0;
            if (! this.streams.isEmpty())
            {
                byte[] bytes = (byte[]) this.streams.getFirst();
                available = bytes.length;
            }

            return available;
        }

        public int read() throws IOException
        {
            throw new IOException("Expected bytes to be read in a block.");
        }

        public int read(byte b[], int off, int len) throws IOException
        {
            if (this.streams.isEmpty())
            {
                return -1;
            }

            byte[] bytes = (byte[]) this.streams.removeFirst();

            if (bytes.length != len)
            {
                throw new IOException("Expected read (" + len  + ") to be for the size of the bytes in the list (" + bytes.length + ").");
            }

            System.arraycopy(bytes, 0, b, off, len);
            return len;
        }

        public void verify()
        {
            assertTrue("Expected all parts of the stream to have been read. There are " + this.streams.size() + " remaining.", this.streams.isEmpty());
        }
    }

    private static class MockOutputStream extends ByteArrayOutputStream
    {
        private boolean notified;

        public synchronized boolean join(long timeout) throws InterruptedException
        {
            wait(timeout);
            return this.notified;
        }

        public synchronized void clearNotificationFlag()
        {
            this.notified = false;
        }

        public synchronized void write(byte b[], int off, int len)
        {
            super.write(b, off, len);
            this.notified = true;
            this.notifyAll();
        }
    }
}
