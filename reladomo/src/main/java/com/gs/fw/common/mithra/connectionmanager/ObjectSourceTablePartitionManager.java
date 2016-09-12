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

package com.gs.fw.common.mithra.connectionmanager;



public interface ObjectSourceTablePartitionManager
{
    /**
     * Returns the table name corresponding to the name in the runtime configuration file. For example, with a
     * configuration file that says:
     * <MithraObjectConfiguration className="com.gs.fw.myProject.mithra.Order" cacheType="partial" getTableNameFromConnectionManager="true"/>
     * The method will be passed "ORDERS" and expects to get the actual table (e.g. "ORDERS_ABC")
     * @param defaultTable DefaultTable value set in the mithra object xml file
     * @param source source attribute value
     * @return the table name that will be use to query
     */
    public String getTableName(String defaultTable, Object source);
}
