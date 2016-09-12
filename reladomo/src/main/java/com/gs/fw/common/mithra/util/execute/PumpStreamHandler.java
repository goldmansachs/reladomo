
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

import java.io.InputStream;
import java.io.OutputStream;


public class PumpStreamHandler implements StreamHandler
{

    private final OutputStream inputStream;
    private final OutputStream errorStream;

    private StreamPumper error;
    private StreamPumper input;

    public PumpStreamHandler(OutputStream inputStream, OutputStream errorStream)
    {
        this.inputStream = inputStream;
        this.errorStream = errorStream;
    }

    public void setErrorStream(InputStream errorStream)
    {
        this.error = new StreamPumper(errorStream, this.errorStream);
    }

    public void setOutputStream(OutputStream outputStream)
    {
        // Ignored
    }

    public void setInputStream(InputStream inputStream)
    {
        this.input = new StreamPumper(inputStream, this.inputStream);
    }

    public void start()
    {
        this.error.start();
        this.input.start();
    }

    public void stop()
    {
        this.error.finish();
        this.input.finish();

        try
        {
            this.error.join();
        }
        catch (InterruptedException e)
        {
            // Ignore this
        }

        try
        {
            this.input.join();
        }
        catch (InterruptedException e)
        {
            // Ignore this
        }
    }
}
