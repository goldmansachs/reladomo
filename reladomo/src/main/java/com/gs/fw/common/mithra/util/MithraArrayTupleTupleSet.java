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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.tempobject.ArrayTuple;
import com.gs.fw.common.mithra.tempobject.IntIntTuple;
import com.gs.fw.common.mithra.tempobject.Tuple;
import com.gs.fw.common.mithra.tempobject.tupleextractor.*;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;


public class MithraArrayTupleTupleSet implements MithraTupleSet, Externalizable
{
    private static final long serialVersionUID = 826563429092366120L;

    private ConcurrentFullUniqueIndex fullUniqueIndex;
    private Extractor[] extractors;
    private transient int[] maxLengths;
    private transient List dataHoldersForWriteReplacement;
    private boolean readOnly = false;
    private int hashCode;
    private boolean hasNulls;

    public MithraArrayTupleTupleSet()
    {
    }

    public MithraArrayTupleTupleSet(Extractor[] dataExtractors, List dataHolders)
    {
        populateNotIgnoringNulls(dataExtractors, dataHolders);
    }

    public void addAll(Extractor[] dataExtractors, List dataHolders)
    {
        for(Object dataHolder : dataHolders)
        {
            Object[] values = getExtractedValues(dataExtractors, dataHolder);
            add(values);
        }
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
                    populateMaxLength(i);
                }
            }
        }
        return maxLengths;
    }

    private void populateMaxLength(final int index)
    {
        final int[] max = new int[1];
        this.fullUniqueIndex.doUntil(new DoUntilProcedure()
        {
            @Override
            public boolean execute(Object object)
            {
                Tuple tuple = (Tuple) object;
                String s = ((String) tuple.getAttribute(index));
                if (s != null && s.length() > max[0])
                {
                    max[0] = s.length();
                }
                return false;
            }
        });
        maxLengths[index] = max[0];
    }

    private Object[] getExtractedValues(Extractor[] dataExtractors, Object dataHolder)
    {
        Object[] values = new Object[dataExtractors.length];
        for(int i=0; i< dataExtractors.length;i++)
        {
            values[i] = dataExtractors[i].valueOf(dataHolder);
        }
        return values;
    }

    @Override
    public boolean hasNulls()
    {
        return hasNulls;
    }

    private void populateNotIgnoringNulls(Extractor[] dataExtractors, List dataHolders)
    {
        if (dataHolders.size() > 0)
        {
            Object data = dataHolders.get(0);
            Object[] values = getExtractedValues(dataExtractors, data);
            populateFromPrototypeValues(dataExtractors, dataHolders, values, 1, false);
        }
    }

    private void populateFromPrototypeValues(Extractor[] dataExtractors, List dataHolders, Object[] values, int startIndex, boolean ignoreNulls)
    {
        hasNulls = !ignoreNulls;
        determineExtractors(values);
        this.fullUniqueIndex = new ConcurrentFullUniqueIndex(this.extractors);
        if (values.length == 2 && values[0] instanceof Integer && values[1] instanceof Integer)
        {
            this.fullUniqueIndex.putIfAbsent(new IntIntTuple((Integer) values[0], (Integer) values[1]));
            IntExtractor firstExtractor = (IntExtractor) dataExtractors[0];
            IntExtractor secondExtractor = (IntExtractor) dataExtractors[1];
            for(int i=startIndex;i<dataHolders.size();i++)
            {
                Object data = dataHolders.get(i);
                if (firstExtractor.isAttributeNull(data) || secondExtractor.isAttributeNull(data))
                {
                    addArrayTuple(dataExtractors, dataHolders, i, ignoreNulls);
                }
                else
                {
                    this.fullUniqueIndex.putIfAbsent(new IntIntTuple(firstExtractor.intValueOf(data), secondExtractor.intValueOf(data)));
                }
            }
        }
        else
        {
            this.fullUniqueIndex.putIfAbsent(new ArrayTuple(values));
            for(int i=startIndex;i<dataHolders.size();i++)
            {
                addArrayTuple(dataExtractors, dataHolders, i, ignoreNulls);
            }
        }
    }

    private void addArrayTuple(Extractor[] dataExtractors, List dataHolders, int index, boolean ignoreNulls)
    {
        Object data = dataHolders.get(index);
        Object[] values = new Object[dataExtractors.length];
        for(int j=0;j<dataExtractors.length;j++)
        {
            values[j] = dataExtractors[j].valueOf(data);
            if (ignoreNulls && values[j] == null)
            {
                return;
            }
        }
        this.fullUniqueIndex.putIfAbsent(new ArrayTuple(values));
    }

    protected MithraArrayTupleTupleSet(List dataHolders, Extractor[] extractors)
    {
        this.dataHoldersForWriteReplacement = dataHolders;
        this.extractors = extractors;
    }

    public MithraArrayTupleTupleSet(Extractor[] dataExtractors, List dataHolders, boolean ignoreNulls)
    {
        if (ignoreNulls)
        {
            for (int k=0;k<dataHolders.size();k++)
            {
                Object data = dataHolders.get(k);
                Object[] values = new Object[dataExtractors.length];
                boolean hasNull = false;
                for(int i=0;i<dataExtractors.length && !hasNull;i++)
                {
                    values[i] = dataExtractors[i].valueOf(data);
                    hasNull = values[i] == null;
                }
                if (!hasNull)
                {
                    populateFromPrototypeValues(dataExtractors, dataHolders, values, k, ignoreNulls);
                    break;
                }
            }
        }
        else
        {
            populateNotIgnoringNulls(dataExtractors, dataHolders);
        }
    }

    public void markAsReadOnly()
    {
        this.readOnly = true;
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
        return this.fullUniqueIndex.getAll();
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
        if (this.readOnly) throw new RuntimeException("A tuple set cannot be reused after it has been put into an operation. Create a new one instead.");
        if (this.extractors == null)
        {
            determineExtractors(value);
            this.fullUniqueIndex = new ConcurrentFullUniqueIndex(this.extractors);
        }
        if (value.length != this.extractors.length)
        {
            throw new RuntimeException("wrong number of arguments. was expecting "+this.extractors.length+" but got "+value.length);
        }
        this.fullUniqueIndex.putIfAbsent(new ArrayTuple(value));
    }

    private void determineExtractors(Object[] value)
    {
        this.extractors = new Extractor[value.length];
        for(int i=0;i<value.length;i++)
        {
            this.extractors[i] = determineExtractor(value[i], i);
        }
    }

    private Extractor determineExtractor(Object o, int pos)
    {
        if (o == null)
        {
            throw new MithraBusinessException("nulls are not allowed in tuples");
        }
        if (o instanceof Boolean)
        {
            return new BooleanTupleExtractor(pos);
        }
        if (o instanceof Character)
        {
            return new CharTupleExtractor(pos);
        }
        if (o instanceof Byte)
        {
            return new ByteTupleExtractor(pos);
        }
        if (o instanceof Short)
        {
            return new ShortTupleExtractor(pos);
        }
        if (o instanceof Integer)
        {
            return new IntTupleExtractor(pos);
        }
        if (o instanceof Long)
        {
            return new LongTupleExtractor(pos);
        }
        if (o instanceof Float)
        {
            return new FloatTupleExtractor(pos);
        }
        if (o instanceof Double)
        {
            return new DoubleTupleExtractor(pos);
        }
        if (o instanceof String)
        {
            return new StringTupleExtractor(pos);
        }
        if (o instanceof Timestamp)
        {
            return new TimestampTupleExtractor(pos);
        }
        if (o instanceof Date)
        {
            return new DateTupleExtractor(pos);
        }
        if (o instanceof Time)
        {
            return new TimeTupleExtractor(pos);
        }
        if (o instanceof BigDecimal)
        {
            return new BigDecimalTupleExtractor(pos);
        }
        if (o instanceof byte[])
        {
            return new ByteArrayTupleExtractor(pos);
        }
        throw new MithraBusinessException("unsupported object type in tuple: "+o.getClass().getName());
    }

    public int size()
    {
        if (this.fullUniqueIndex == null) return 0;
        return this.fullUniqueIndex.size();
    }

    @Override
    public int hashCode()
    {
        if (this.hashCode == 0 && this.fullUniqueIndex != null)
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

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(this.extractors.length);
        if (this.dataHoldersForWriteReplacement != null)
        {
            writeForDataHolders(out);
        }
        else
        {
            out.writeInt(this.size());
            this.fullUniqueIndex.doUntil(new ArrayTupleExternalizerDoUntil(out));
        }
    }

    private void writeForDataHolders(ObjectOutput out) throws IOException
    {
        out.writeInt(dataHoldersForWriteReplacement.size());
        for(int i=0;i<dataHoldersForWriteReplacement.size();i++)
        {
            for(Extractor e: this.extractors)
            {
                out.writeObject(e.valueOf(dataHoldersForWriteReplacement.get(i)));
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int extractorSize = in.readInt();
        int size = in.readInt();
        Object[] first = readArray(in, extractorSize);
        this.determineExtractors(first);
        this.fullUniqueIndex = new ConcurrentFullUniqueIndex(this.extractors, size);
        this.fullUniqueIndex.putIfAbsent(new ArrayTuple(first));
        for(int i=1;i<size;i++)
        {
            this.fullUniqueIndex.putIfAbsent(new ArrayTuple(this.readArray(in, extractorSize)));
        }
        this.readOnly = true;
    }

    private Object[] readArray(ObjectInput in, int extractorSize)
            throws ClassNotFoundException, IOException
    {
        Object[] first = new Object[extractorSize];
        for(int k=0;k<extractorSize;k++)
        {
            first[k] = in.readObject();
        }
        return first;
    }

    private static class ArrayTupleExternalizerDoUntil implements DoUntilProcedure
    {
        private ObjectOutput out;

        private ArrayTupleExternalizerDoUntil(ObjectOutput out)
        {
            this.out = out;
        }

        public boolean execute(Object object)
        {
            Tuple t = (Tuple) object;
            try
            {
                t.writeToStream(out);
            }
            catch (IOException e)
            {
                Exceptions.throwCheckedException(e);
            }
            return false;
        }
    }

    private static class Exceptions
    {
        private static Throwable throwable;

        private Exceptions() throws Throwable {
            throw throwable;
        }

        public static synchronized void throwCheckedException(Throwable throwable) {
            Exceptions.throwable = throwable;
            try {
                Exceptions.class.newInstance();
            } catch(InstantiationException e) {
            } catch(IllegalAccessException e) {
            } finally {
                Exceptions.throwable = null;
            }
        }
    }

}
