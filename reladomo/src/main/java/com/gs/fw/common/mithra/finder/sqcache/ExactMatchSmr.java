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

package com.gs.fw.common.mithra.finder.sqcache;

import com.gs.fw.common.mithra.querycache.QueryCache;

import java.util.List;

public class ExactMatchSmr extends ShapeMatchResult
{
    public static ExactMatchSmr INSTANCE = new ExactMatchSmr();

    private ExactMatchSmr()
    {

    }

    @Override
    public boolean isExactMatch()
    {
        return true;
    }

    @Override
    public List resolve(QueryCache queryCache)
    {
        // we return null here, because a queryCache lookup of findByEquality(newOp)
        // should've already been performed before we get here.
        return null;
    }
}
