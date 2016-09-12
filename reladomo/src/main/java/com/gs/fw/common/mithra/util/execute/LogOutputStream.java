
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;

/**
 * <p>Writes the contents of a stream to a log.<p>

 */
public abstract class LogOutputStream extends OutputStream
{

    protected final Logger logger;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private boolean skip;

    protected LogOutputStream(Logger logger)
    {
        this.logger = logger;
    }

    public void write(int b) throws IOException
    {
        if (b == '\n' || b == '\r')
        {
            if (! this.skip)
            {
                this.logLine(this.buffer.toString());
                this.buffer.reset();
            }
        }
        else
        {
            this.buffer.write(b);
        }

        this.skip = (b == '\r');
    }

    protected abstract void logLine(String line);

    public static LogOutputStream logDebug(Logger logger)
    {
        return new LogDebugOutputStream(logger);
    }

    public static LogOutputStream logInfo(Logger logger)
    {
        return new LogInfoOutputStream(logger);
    }

    public static LogOutputStream logWarn(Logger logger)
    {
        return new LogWarnOutputStream(logger);
    }

    public static LogOutputStream logError(Logger logger)
    {
        return new LogErrorOutputStream(logger);
    }

    private static class LogDebugOutputStream extends LogOutputStream
    {
        protected LogDebugOutputStream(Logger logger)
        {
            super(logger);
        }

        protected void logLine(String line)
        {
            if (this.logger.isDebugEnabled())
            {
                this.logger.debug(line);
            }
        }
    }

    private static class LogInfoOutputStream extends LogOutputStream
    {
        protected LogInfoOutputStream(Logger logger)
        {
            super(logger);
        }

        protected void logLine(String line)
        {
            if (this.logger.isInfoEnabled())
            {
                this.logger.info(line);
            }
        }
    }

    private static class LogWarnOutputStream extends LogOutputStream
    {
        protected LogWarnOutputStream(Logger logger)
        {
            super(logger);
        }

        protected void logLine(String line)
        {
            if (this.logger.isWarnEnabled())
            {
                this.logger.warn(line);
            }
        }
    }

    private static class LogErrorOutputStream extends LogOutputStream
    {
        protected LogErrorOutputStream(Logger logger)
        {
            super(logger);
        }

        protected void logLine(String line)
        {
            if (this.logger.isErrorEnabled())
            {
                this.logger.error(line);
            }
        }
    }
}
