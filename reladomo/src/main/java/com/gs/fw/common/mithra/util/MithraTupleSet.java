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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;


public interface MithraTupleSet extends TupleSet, Serializable
{
    public void markAsReadOnly();

    public ConcurrentFullUniqueIndex getAsIndex();

    public Extractor[] getExtractors();

    public List getTupleList();

    public boolean contains(Object valueHolder, Extractor[] attributes);

    public boolean doUntil(DoUntilProcedure procedure);

    public Object getFirstDataHolder();

    public Iterator iterator();

    public ParallelIterator parallelIterator(int perItemWeight);

    public boolean hasNulls();

    int[] getMaxLengths();
}
