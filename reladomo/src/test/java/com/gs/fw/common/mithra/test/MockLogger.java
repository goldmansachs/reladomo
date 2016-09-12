
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

package com.gs.fw.common.mithra.test;

import com.mockobjects.ExpectationCounter;
import com.mockobjects.Verifiable;

import org.slf4j.Logger;
import org.slf4j.Marker;


public class MockLogger implements Logger, Verifiable
{
    /** Expectation counter for <code>debug/info/warn/error</code> */
    private ExpectationCounter logCounter = new ExpectationCounter("log");

    /**
     * Sets the number of expected calls to <code>log</code>
     * @param count The number of expected calls.
     */
    public void setExpectedLogCalls(int count)
    {
        this.logCounter.setExpected(count);
    }

    /**
     * Sets the expected calls to <code>log</code> to zero.
     */
    public void setExpectLogNothing()
    {
        this.logCounter.setExpectNothing();
    }

    private boolean isDebugEnabled;
    private boolean isInfoEnabled;
    private boolean isWarnEnabled;
    private boolean isErrorEnabled;

    public void setDebugEnabled(boolean debugEnabled)
    {
        this.isDebugEnabled = debugEnabled;
    }

    public void setInfoEnabled(boolean infoEnabled)
    {
        this.isInfoEnabled = infoEnabled;
    }

    public void setWarnEnabled(boolean warnEnabled)
    {
        this.isWarnEnabled = warnEnabled;
    }

    public void setErrorEnabled(boolean errorEnabled)
    {
        this.isErrorEnabled = errorEnabled;
    }

    public boolean isDebugEnabled()
    {
        return this.isDebugEnabled;
    }

    public boolean isInfoEnabled()
    {
        return this.isInfoEnabled;
    }

    public boolean isWarnEnabled()
    {
        return this.isWarnEnabled;
    }

    public boolean isErrorEnabled()
    {
        return this.isErrorEnabled;
    }

    public void verify()
    {
        this.logCounter.verify();
    }

    public String getName()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isTraceEnabled()
    {
        throw new RuntimeException("not implemented");
    }

    public void trace(String s)
    {
        this.logCounter.inc();
    }

    public void trace(String s, Object o)
    {
        this.logCounter.inc();
    }

    public void trace(String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void trace(String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void trace(String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public boolean isTraceEnabled(Marker marker)
    {
        throw new RuntimeException("not implemented");
    }

    public void trace(Marker marker, String s)
    {
        this.logCounter.inc();
    }

    public void trace(Marker marker, String s, Object o)
    {
        this.logCounter.inc();
    }

    public void trace(Marker marker, String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void trace(Marker marker, String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void trace(Marker marker, String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public void debug(String s)
    {
        this.logCounter.inc();
    }

    public void debug(String s, Object o)
    {
        this.logCounter.inc();
    }

    public void debug(String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void debug(String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void debug(String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public boolean isDebugEnabled(Marker marker)
    {
        throw new RuntimeException("not implemented");
    }

    public void debug(Marker marker, String s)
    {
        this.logCounter.inc();
    }

    public void debug(Marker marker, String s, Object o)
    {
        this.logCounter.inc();
    }

    public void debug(Marker marker, String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void debug(Marker marker, String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void debug(Marker marker, String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public void info(String s)
    {
        this.logCounter.inc();
    }

    public void info(String s, Object o)
    {
        this.logCounter.inc();
    }

    public void info(String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void info(String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void info(String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public boolean isInfoEnabled(Marker marker)
    {
        throw new RuntimeException("not implemented");
    }

    public void info(Marker marker, String s)
    {
        this.logCounter.inc();
    }

    public void info(Marker marker, String s, Object o)
    {
        this.logCounter.inc();
    }

    public void info(Marker marker, String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void info(Marker marker, String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void info(Marker marker, String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public void warn(String s)
    {
        this.logCounter.inc();
    }

    public void warn(String s, Object o)
    {
        this.logCounter.inc();
    }

    public void warn(String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void warn(String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void warn(String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public boolean isWarnEnabled(Marker marker)
    {
        throw new RuntimeException("not implemented");
    }

    public void warn(Marker marker, String s)
    {
        this.logCounter.inc();
    }

    public void warn(Marker marker, String s, Object o)
    {
        this.logCounter.inc();
    }

    public void warn(Marker marker, String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void warn(Marker marker, String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void warn(Marker marker, String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public void error(String s)
    {
        this.logCounter.inc();
    }

    public void error(String s, Object o)
    {
        this.logCounter.inc();
    }

    public void error(String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void error(String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void error(String s, Throwable throwable)
    {
        this.logCounter.inc();
    }

    public boolean isErrorEnabled(Marker marker)
    {
        throw new RuntimeException("not implemented");
    }

    public void error(Marker marker, String s)
    {
        this.logCounter.inc();
    }

    public void error(Marker marker, String s, Object o)
    {
        this.logCounter.inc();
    }

    public void error(Marker marker, String s, Object o, Object o1)
    {
        this.logCounter.inc();
    }

    public void error(Marker marker, String s, Object[] objects)
    {
        this.logCounter.inc();
    }

    public void error(Marker marker, String s, Throwable throwable)
    {
        this.logCounter.inc();
    }
}
