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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.fw.common.mithra.cacheloader.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.BooleanFilter;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.List;

public class AbstractLoaderFactoryTest extends TestCase
{
    private static final String DATE_INVARIANT_CLASS = "XYZ";

    public void testDateInvariantAdditionalOperationBuilders()
    {
        assertTrue(new BaseLoaderFactory().areAllAdditionalOperationBuildersDateInvariant(DATE_INVARIANT_CLASS, createTestCacheLoaderContext()));
    }

    private static CacheLoaderContext createTestCacheLoaderContext()
    {
        CacheLoaderContext cacheLoaderContext = new CacheLoaderContext(new CacheLoaderManagerImpl(), null);
        cacheLoaderContext.setQualifiedLoadContext(new NonQualifiedLoadContext());
        return cacheLoaderContext;
    }

    private static class DateInvariantAdditionalOperationBuilder implements AdditionalOperationBuilder
    {
        @Override
        public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
        {
            return null;
        }

        @Override
        public boolean isBusinessDateInvariant()
        {
            return true;
        }
    }

    private static class DateVariantAdditionalOperationBuilder implements AdditionalOperationBuilder
    {
        @Override
        public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
        {
            return null;
        }

        @Override
        public boolean isBusinessDateInvariant()
        {
            return false;
        }
    }

    private static class TestQualifiedLoadContext extends QualifiedByOwnerObjectListLoadContext
    {
        public TestQualifiedLoadContext(List ownerObejcts)
        {
            super(ownerObejcts);
        }

        @Override
        public AdditionalOperationBuilder getAdditionalOperationBuilder(String className)
        {
            if (DATE_INVARIANT_CLASS.equals(className))
            {
                return new DateInvariantAdditionalOperationBuilder();
            }
            else
            {
                return new DateVariantAdditionalOperationBuilder();
            }
        }
    }

    private static class BaseLoaderFactory extends AbstractLoaderFactory
    {
        @Override
        public List<TaskOperationDefinition> buildRefreshTaskDefinitions(CacheLoaderContext context, Operation loadOperation)
        {
            return null;
        }

        @Override
        public Operation createFindAllOperation(List<Timestamp> businessDates, Object sourceAttribute)
        {
            return null;
        }

        @Override
        public BooleanFilter createCacheFilterOfDatesToDrop(Timestamp loadedDate)
        {
            return null;
        }
    }

    private static class DateShiftingLoaderFactory extends BaseLoaderFactory
    {
        @Override
        protected Timestamp shiftBusinessDate(Timestamp businessDate)
        {
            return new Timestamp(businessDate.getTime() + 1000L);
        }
    }
}
