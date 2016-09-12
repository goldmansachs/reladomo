
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

package com.gs.fw.common.mithra.util.execute;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Pumps the contents of one stream over to another.</p>

 */
public class StreamPumper extends Thread
{

    private static final Logger logger = LoggerFactory.getLogger(StreamPumper.class.getName());

    private final InputStream in;
    private final OutputStream out;
    private volatile boolean done;

    public StreamPumper(InputStream in, OutputStream out)
    {
        this.in = in;
        this.out = out;
    }

    /**
     * Tells the thread to finish.
     */
    public void finish()
    {
        this.done = true;
        this.interrupt();
    }

    public void run()
    {
        try
        {
            while (! this.done)
            {
                int available = this.in.available();
                while (! this.done && available > 0)
                {
                    byte[] buf = new byte[available];

                    // If we'd allowed asserts then we could check that we have the correct length but we aren't...
                    this.in.read(buf);
                    this.out.write(buf);

                    available = this.in.available();
                }

                sleep(200);
            }
        }
        catch (InterruptedException e)
        {
            // Ignore this
        }
        catch (IOException e)
        {
            logger.error("Could not write output", e);
        }
    }
}
