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
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.tempobject.LazyListAdaptor;
import com.gs.fw.common.mithra.tempobject.LazyTuple;

import java.io.ObjectStreamException;
import java.util.Iterator;
import java.util.List;


public class MithraDataHolderTupleSet implements MithraTupleSet
{
    private List dataHolders;
    private Extractor[] extractors;
    private transient int[] maxLengths;
    private ConcurrentFullUniqueIndex fullUniqueIndex;
    private int hashCode;

    public MithraDataHolderTupleSet(List dataHolders, Extractor[] extractors)
    {
        this.extractors = extractors;
        populateIndex(dataHolders, extractors);
    }

    public MithraDataHolderTupleSet(List dataHolders, Extractor[] extractors, boolean doNotHoldList)
    {
        this.extractors = extractors;
        populateIndex(dataHolders, extractors);
        this.dataHolders = null;
    }

    @Override
    public boolean hasNulls()
    {
        return false;
    }

    private void populateIndex(List dataHolders, Extractor[] extractors)
    {
        this.fullUniqueIndex = ConcurrentFullUniqueIndex.parallelConstructIndexWithoutNulls(dataHolders, extractors);
        if (fullUniqueIndex.size() == dataHolders.size())
        {
            this.dataHolders = dataHolders;
        }
    }

    public void markAsReadOnly()
    {
        // nothing to do
    }

    @Override
    public int[] getMaxLengths()
    {
        if (maxLengths == null)
        {
            for(int i=0;i<extractors.length;i++)
            {
                Extractor e = extractors[i];
                if (e instanceof StringExtractor)
                {
                    if (maxLengths == null)
                    {
                        maxLengths = new int[this.extractors.length];
                    }
                    populateMaxLength(i, (StringExtractor) e);
                }
            }
        }
        return maxLengths;
    }

    private void populateMaxLength(int index, StringExtractor e)
    {
        int max = 0;
        for(int i=0;i<dataHolders.size();i++)
        {
            String s = e.stringValueOf(dataHolders.get(i));
            if (s != null && s.length() > max)
            {
                max = s.length();
            }
        }
        maxLengths[index] = max;
    }

    public ConcurrentFullUniqueIndex getAsIndex()
    {
        return this.fullUniqueIndex;
    }

    public Extractor[] getExtractors()
    {
        return this.extractors;
    }

    public List getTupleList()
    {
        return  new LazyListAdaptor(getDataHolders(), LazyTuple.createFactory(this.extractors));
    }

    private List getDataHolders()
    {
        if (this.dataHolders == null)
        {
            this.dataHolders = this.fullUniqueIndex.getAll();
        }
        return this.dataHolders;
    }

    public boolean contains(Object valueHolder, Extractor[] attributes)
    {
        return this.fullUniqueIndex.get(valueHolder, attributes) != null;
    }

    @Override
    public boolean doUntil(DoUntilProcedure procedure)
    {
        return this.fullUniqueIndex.doUntil(procedure);
    }

    public Object getFirstDataHolder()
    {
        return this.fullUniqueIndex.getFirst();
    }

    public Iterator iterator()
    {
        return this.fullUniqueIndex.iterator();
    }

    public ParallelIterator parallelIterator(int perItemWeight)
    {
        return this.fullUniqueIndex.parallelIterator(perItemWeight);
    }

    public void add(Object... value)
    {
        throw new RuntimeException("not supported");
    }

    public int size()
    {
        return this.fullUniqueIndex.size();
    }

    @Override
    public int hashCode()
    {
        if (this.hashCode == 0)
        {
            hashCode = fullUniqueIndex.roughHashCode();
        }
        return this.hashCode;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj instanceof MithraTupleSet)
        {
            MithraTupleSet other = (MithraTupleSet) obj;
            if (this.size() == other.size())
            {
                return this.fullUniqueIndex.equalsByExtractedValues(other.getAsIndex());
            }
        }
        return false;
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return new MithraArrayTupleTupleSet(getDataHolders(), this.extractors);
    }
}
