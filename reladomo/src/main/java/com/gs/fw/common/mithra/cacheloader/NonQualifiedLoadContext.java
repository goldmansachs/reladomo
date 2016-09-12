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
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.util.List;

/**
 * used in place of the LimitedLoaderContext but with no limiting.
 */
public class NonQualifiedLoadContext implements QualifiedLoadContext
{
    private static AdditionalOperationBuilder testOperationBuilder = null;

    public boolean qualifies(String className, boolean isDependentTask)
    {
        return true;
    }

    public AdditionalOperationBuilder getAdditionalOperationBuilder(String className)
    {
        return testOperationBuilder;
    }

    @Override
    public boolean qualifiesDependentsFor(String ownerClassName, String dependentClassName)
    {
        return false;
    }

    @Override
    public List getKeyHoldersForQualifiedDependents(Operation operation, RelatedFinder finder)
    {
        throw new RuntimeException("unexpected");
    }

    public static void setTestOperationBuilder(AdditionalOperationBuilder testOperationBuilder)
    {
        NonQualifiedLoadContext.testOperationBuilder = testOperationBuilder;
    }
}
