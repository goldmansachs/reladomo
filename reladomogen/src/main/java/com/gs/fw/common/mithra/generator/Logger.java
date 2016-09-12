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


public interface Logger
{
    public void info(String msg);
    public void info(String msg, Throwable t);

    public void warn(String msg);
    public void warn(String msg, Throwable t);

    public void error(String msg);
    public void error(String msg, Throwable t);

    public void debug(String msg);
    public void debug(String msg, Throwable t);
}
