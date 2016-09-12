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

package com.gs.fw.common.mithra.ui.gwt.server;

import com.gs.fw.common.mithra.ui.gwt.client.CachedClassData;
import com.gs.fw.common.mithra.ui.gwt.client.JvmMemory;

import java.util.List;



public interface MithraCacheRemoteService
{
    public JvmMemory getJvmMemory();

    public JvmMemory forceGc();

    public List<CachedClassData> clearAllQueryCaches();

    public CachedClassData clearCache(String className);

    public List<CachedClassData> getCachedClasses();

    public CachedClassData reloadCache(String className);

    public CachedClassData turnSqlOff(String className);

    public CachedClassData turnSqlOn(String className);

    public CachedClassData turnSqlMaxOn(String className);
}
