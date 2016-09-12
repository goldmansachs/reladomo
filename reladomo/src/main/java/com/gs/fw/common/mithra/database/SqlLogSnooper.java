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

package com.gs.fw.common.mithra.database;

import org.slf4j.Logger;
import org.slf4j.Marker;



public class SqlLogSnooper implements Logger
{
    private Logger delegate;
    private static ThreadLocal<StringBuilder> threadLocalStringBuilder = new ThreadLocal();

    public static void startSqlSnooping()
    {
        threadLocalStringBuilder.set(new StringBuilder());
    }

    public static String completeSqlSnooping()
    {
        String result = threadLocalStringBuilder.get().toString();
        threadLocalStringBuilder.remove();

        return result;
    }

    public SqlLogSnooper(Logger delegate)
    {
        this.delegate = delegate;
    }

    public boolean isDebugEnabled()
    {
        return this.delegate.isDebugEnabled() || threadLocalStringBuilder.get() != null;
    }

    public void debug(String msg)
    {
        StringBuilder stringBuilder = threadLocalStringBuilder.get();
        if (stringBuilder != null)
        {
            stringBuilder.append(msg);
        }
        this.delegate.debug(msg);
    }

    public void debug(String format, Object arg)
    {
        this.delegate.debug(format, arg);
    }

    public void debug(String format, Object arg1, Object arg2)
    {
        this.delegate.debug(format, arg1, arg2);
    }

    public void debug(String format, Object[] argArray)
    {
        this.delegate.debug(format, argArray);
    }

    public void debug(String msg, Throwable t)
    {
        this.delegate.debug(msg, t);
    }

    public boolean isDebugEnabled(Marker marker)
    {
        return this.delegate.isDebugEnabled(marker);
    }

    public void debug(Marker marker, String msg)
    {
        this.delegate.debug(marker, msg);
    }

    public void debug(Marker marker, String format, Object arg)
    {
        this.delegate.debug(marker, format, arg);
    }

    public void debug(Marker marker, String format, Object arg1, Object arg2)
    {
        this.delegate.debug(marker, format, arg1, arg2);
    }

    public void debug(Marker marker, String format, Object[] argArray)
    {
        this.delegate.debug(marker, format, argArray);
    }

    public void debug(Marker marker, String msg, Throwable t)
    {
        this.delegate.debug(marker, msg, t);
    }


    public String getName()
    {
        return this.delegate.getName();
    }

    public boolean isTraceEnabled()
    {
        return this.delegate.isTraceEnabled();
    }

    public void trace(String msg)
    {
        this.delegate.trace(msg);
    }

    public void trace(String format, Object arg)
    {
        this.delegate.trace(format, arg);
    }

    public void trace(String format, Object arg1, Object arg2)
    {
        this.delegate.trace(format, arg1, arg2);
    }

    public void trace(String format, Object[] argArray)
    {
        this.delegate.trace(format, argArray);
    }

    public void trace(String msg, Throwable t)
    {
        this.delegate.trace(msg, t);
    }

    public boolean isTraceEnabled(Marker marker)
    {
        return this.delegate.isTraceEnabled(marker);
    }

    public void trace(Marker marker, String msg)
    {
        this.delegate.trace(marker, msg);
    }

    public void trace(Marker marker, String format, Object arg)
    {
        this.delegate.trace(marker, format, arg);
    }

    public void trace(Marker marker, String format, Object arg1, Object arg2)
    {
        this.delegate.trace(marker, format, arg1, arg2);
    }

    public void trace(Marker marker, String format, Object[] argArray)
    {
        this.delegate.trace(marker, format, argArray);
    }

    public void trace(Marker marker, String msg, Throwable t)
    {
        this.delegate.trace(marker, msg, t);
    }

   public boolean isInfoEnabled()
    {
        return this.delegate.isInfoEnabled();
    }

    public void info(String msg)
    {
        this.delegate.info(msg);
    }

    public void info(String format, Object arg)
    {
        this.delegate.info(format, arg);
    }

    public void info(String format, Object arg1, Object arg2)
    {
        this.delegate.info(format, arg1, arg2);
    }

    public void info(String format, Object[] argArray)
    {
        this.delegate.info(format, argArray);
    }

    public void info(String msg, Throwable t)
    {
        this.delegate.info(msg, t);
    }

    public boolean isInfoEnabled(Marker marker)
    {
        return this.delegate.isInfoEnabled(marker);
    }

    public void info(Marker marker, String msg)
    {
        this.delegate.info(marker, msg);
    }

    public void info(Marker marker, String format, Object arg)
    {
        this.delegate.info(marker, format, arg);
    }

    public void info(Marker marker, String format, Object arg1, Object arg2)
    {
        this.delegate.info(marker, format, arg1, arg2);
    }

    public void info(Marker marker, String format, Object[] argArray)
    {
        this.delegate.info(marker, format, argArray);
    }

    public void info(Marker marker, String msg, Throwable t)
    {
        this.delegate.info(marker, msg, t);
    }

    public boolean isWarnEnabled()
    {
        return this.delegate.isWarnEnabled();
    }

    public void warn(String msg)
    {
        this.delegate.warn(msg);
    }

    public void warn(String format, Object arg)
    {
        this.delegate.warn(format, arg);
    }

    public void warn(String format, Object[] argArray)
    {
        this.delegate.warn(format, argArray);
    }

    public void warn(String format, Object arg1, Object arg2)
    {
        this.delegate.warn(format, arg1, arg2);
    }

    public void warn(String msg, Throwable t)
    {
        this.delegate.warn(msg, t);
    }

    public boolean isWarnEnabled(Marker marker)
    {
        return this.delegate.isWarnEnabled(marker);
    }

    public void warn(Marker marker, String msg)
    {
        this.delegate.warn(marker, msg);
    }

    public void warn(Marker marker, String format, Object arg)
    {
        this.delegate.warn(marker, format, arg);
    }

    public void warn(Marker marker, String format, Object arg1, Object arg2)
    {
        this.delegate.warn(marker, format, arg1, arg2);
    }

    public void warn(Marker marker, String format, Object[] argArray)
    {
        this.delegate.warn(marker, format, argArray);
    }

    public void warn(Marker marker, String msg, Throwable t)
    {
        this.delegate.warn(marker, msg, t);
    }

    public boolean isErrorEnabled()
    {
        return this.delegate.isErrorEnabled();
    }

    public void error(String msg)
    {
        this.delegate.error(msg);
    }

    public void error(String format, Object arg)
    {
        this.delegate.error(format, arg);
    }

    public void error(String format, Object arg1, Object arg2)
    {
        this.delegate.error(format, arg1, arg2);
    }

    public void error(String format, Object[] argArray)
    {
        this.delegate.error(format, argArray);
    }

    public void error(String msg, Throwable t)
    {
        this.delegate.error(msg, t);
    }

    public boolean isErrorEnabled(Marker marker)
    {
        return this.delegate.isErrorEnabled(marker);
    }

    public void error(Marker marker, String msg)
    {
        this.delegate.error(marker, msg);
    }

    public void error(Marker marker, String format, Object arg)
    {
        this.delegate.error(marker, format, arg);
    }

    public void error(Marker marker, String format, Object arg1, Object arg2)
    {
        this.delegate.error(marker, format, arg1, arg2);
    }

    public void error(Marker marker, String format, Object[] argArray)
    {
        this.delegate.error(marker, format, argArray);
    }

    public void error(Marker marker, String msg, Throwable t)
    {
        this.delegate.error(marker, msg, t);
    }

}

    

