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

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ErrorHandler;

import java.util.ArrayList;
import java.util.List;


public class Log4JRecordingAppender implements Appender
{

    private ArrayList<LoggingEvent> events = new ArrayList();

    public List<LoggingEvent> getEvents()
    {
        return events;
    }

    public void addFilter(Filter filter)
    {
    }

    public void clearFilters()
    {
    }

    public void close()
    {
    }

    public void doAppend(LoggingEvent event)
    {
        this.events.add(event);
    }

    public ErrorHandler getErrorHandler()
    {
        return null;
    }

    public Filter getFilter()
    {
        return null;
    }

    public Layout getLayout()
    {
        throw new RuntimeException("not implemented");
    }

    public String getName()
    {
        return "Log4JRecordingAppender";
    }

    public boolean requiresLayout()
    {
        return false;
    }

    public void setErrorHandler(ErrorHandler errorHandler)
    {
    }

    public void setLayout(Layout layout)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void setName(String s)
    {
    }
}
