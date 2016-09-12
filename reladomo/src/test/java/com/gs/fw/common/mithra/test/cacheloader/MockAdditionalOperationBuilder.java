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


import com.gs.fw.common.mithra.cacheloader.AdditionalOperationBuilder;
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.Pair;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MockAdditionalOperationBuilder implements AdditionalOperationBuilder
{
    private static final List<Pair<Timestamp, String>> calls = new CopyOnWriteArrayList();

    public static List<Pair<Timestamp, String>> getCallStack()
    {
        return calls;
    }

    public static void reset()
    {
        calls.clear();
    }

    @Override
    public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
    {
        calls.add(Pair.of(businessDate, relatedFinder.getFinderClassName()));
        return NoOperation.instance();
    }

    @Override
    public boolean isBusinessDateInvariant()
    {
        return true;
    }
}
