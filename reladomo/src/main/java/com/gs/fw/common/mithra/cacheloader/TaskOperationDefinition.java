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

package com.gs.fw.common.mithra.cacheloader;

import com.gs.fw.common.mithra.finder.Operation;

/**
 * Created with IntelliJ IDEA.
 * User: borisv
 * Date: 9/19/13
 * Time: 6:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class TaskOperationDefinition
{
    private final Operation operaiton;
    private final boolean needDependentLoaders;

    public TaskOperationDefinition(Operation operation, boolean needDependentLoaders)
    {
        this.operaiton = operation;
        this.needDependentLoaders = needDependentLoaders;
    }

    public Operation getOperation()
    {
        return operaiton;
    }

    public boolean needDependentLoaders()
    {
        return needDependentLoaders;
    }
}
