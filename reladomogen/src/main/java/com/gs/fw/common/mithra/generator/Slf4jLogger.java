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

package com.gs.fw.common.mithra.generator;

public class Slf4jLogger implements Logger
{
    private org.slf4j.Logger logger;

    public Slf4jLogger(org.slf4j.Logger logger)
    {
        this.logger = logger;
    }

    @Override
    public void info(String msg)
    {
        this.logger.info(msg);
    }

    @Override
    public void info(String msg, Throwable t)
    {
        this.logger.info(msg, t);
    }

    @Override
    public void warn(String msg)
    {
        this.logger.warn(msg);
    }

    @Override
    public void warn(String msg, Throwable t)
    {
        this.logger.warn(msg, t);
    }

    @Override
    public void error(String msg)
    {
        this.logger.error(msg);
    }

    @Override
    public void error(String msg, Throwable t)
    {
        this.logger.error(msg, t);
    }

    @Override
    public void debug(String msg)
    {
        this.logger.debug(msg);
    }

    @Override
    public void debug(String msg, Throwable t)
    {
        this.logger.debug(msg, t);
    }
}
