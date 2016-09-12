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

package com.gs.fw.common.mithra.ui.gwt.client;

import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.core.client.GWT;

import java.util.List;



@RemoteServiceRelativePath("MithraCacheUiRemoteService")
public interface MithraCacheUiRemoteService extends RemoteService
{

    public JvmMemory getJvmMemory(String mithraManagerLocation);

    public JvmMemory forceGc(String mithraManagerLocation);

    public List<CachedClassData> clearAllQueryCaches(String mithraManagerLocation);

    public CachedClassData clearCache(String mithraManagerLocation, String className);

    public List<CachedClassData> getCachedClasses(String mithraManagerLocation);

    public CachedClassData reloadCache(String mithraManagerLocation, String className);

    public CachedClassData turnSqlOff(String mithraManagerLocation, String className);

    public CachedClassData turnSqlOn(String mithraManagerLocation, String className);

    public CachedClassData turnSqlMaxOn(String mithraManagerLocation, String className);

    /**
     * Utility/Convenience class.
     * Use MithraCacheUiRemoteService.App.getInstance() to access static instance of MithraCacheUiRemoteServiceAsync
     */
    public static class App
    {
        private static MithraCacheUiRemoteServiceAsync instance = null;

        public static synchronized MithraCacheUiRemoteServiceAsync getInstance()
        {
            if (instance == null)
            {
                instance = GWT.create(MithraCacheUiRemoteService.class);
            }
            return instance;
        }
    }
}
