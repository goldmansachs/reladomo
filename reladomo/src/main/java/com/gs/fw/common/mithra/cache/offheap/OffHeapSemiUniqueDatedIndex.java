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

package com.gs.fw.common.mithra.cache.offheap;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;



@SuppressWarnings({"unchecked"})
public class OffHeapSemiUniqueDatedIndex implements SemiUniqueDatedIndex
{

    private static final int FREE = 0;

    private static final int UPPER_BIT_MASK = 0xC0000000;
    private static final int MULTI_ENTRY_PATTERN = 0x80000000;
    private static final int CHAINED_BUCKET_PATTERN = 0x40000000;

    private OffHeapDataStorage dataStorage;
    private OffHeapIntArrayStorage storage = new FastUnsafeOffHeapIntArrayStorage();
    private int nonDatedArrayRef;
    private int datedArrayRef;

    private int getDatedTableAt(int index)
    {
        return storage.getInt(datedArrayRef, index);
    }

    private void setDatedTableAt(int index, int value)
    {
        storage.setInt(datedArrayRef, index, value);
    }

    private int getNonDatedTableAt(int index)
    {
        return storage.getInt(nonDatedArrayRef, index);
    }

    private void setNonDatedTableAt(int index, int value)
    {
        storage.setInt(nonDatedArrayRef, index, value);
    }

    private void setDatedChainAt(int index, int chainRef)
    {
        storage.setInt(datedArrayRef, index, chainRef | CHAINED_BUCKET_PATTERN);
    }

    private void setNonDatedChainAt(int index, int chainRef)
    {
        storage.setInt(nonDatedArrayRef, index, chainRef | CHAINED_BUCKET_PATTERN);
    }

    private void setNonDatedMultiEntryAt(int index, int chainRef)
    {
        storage.setInt(nonDatedArrayRef, index, chainRef | MULTI_ENTRY_PATTERN);
    }

    private int getDatedTableLength()
    {
        return storage.getLength(datedArrayRef);
    }

    private int getNonDatedTableLength()
    {
        return storage.getLength(nonDatedArrayRef);
    }

    private boolean isChainedBucket(int value)
    {
        return (value & UPPER_BIT_MASK) == CHAINED_BUCKET_PATTERN;
    }

    private static boolean isMultiEntry(int value)
    {
        return (value & UPPER_BIT_MASK) == MULTI_ENTRY_PATTERN;
    }


    // semiUnique is a synonym for nonDated in this file.

    protected ExtractorBasedOffHeapHashStrategy datedHashStrategy;
    protected ExtractorBasedOffHeapHashStrategy asOfAttributeHashStrategy;
    private Extractor[] onHeapDatedExtractors;
    private ExtractorBasedOffHeapHashStrategy nonDatedHashStrategy;
    private Extractor[] onHeapNonDatedExtractors;
    private AsOfAttribute[] asOfAttributes;
    //private TimestampAttribute[] fromAttributes;
    /**
     * The default initial capacity -- MUST be a power of two.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private int nonDatedSize;
    private int datedSize;

    private byte datedRightShift;
    private byte nonDatedRightShift;

    public OffHeapSemiUniqueDatedIndex(Extractor[] onHeapNonDatedExtractors, AsOfAttribute[] asOfAttributes, OffHeapDataStorage dataStorage)
    {
        this.dataStorage = dataStorage;
        this.onHeapNonDatedExtractors = onHeapNonDatedExtractors;
        this.asOfAttributes = asOfAttributes;
        this.populateExtractors();
        nonDatedHashStrategy = ExtractorBasedOffHeapHashStrategy.create(this.onHeapNonDatedExtractors);
        this.allocateDated(DEFAULT_INITIAL_CAPACITY);
        this.allocateNonDated(DEFAULT_INITIAL_CAPACITY);
    }

    public OffHeapSemiUniqueDatedIndex(ExtractorBasedHashStrategy nonDatedHashStrategy, Extractor[] onHeapNonDatedExtractors,
                                       AsOfAttribute[] asOfAttributes, Extractor[] pkExtractors, ExtractorBasedHashStrategy datedHashStrategy, OffHeapDataStorage dataStorage)
    {
        this(onHeapNonDatedExtractors, asOfAttributes, dataStorage);
    }

    protected void populateExtractors()
    {
        this.asOfAttributeHashStrategy = ExtractorBasedOffHeapHashStrategy.create(this.getFromAttributes());
        this.onHeapDatedExtractors = createDatedExtractors();
        this.datedHashStrategy = ExtractorBasedOffHeapHashStrategy.create(this.onHeapDatedExtractors);
    }

    protected Extractor[] createDatedExtractors()
    {
        TimestampAttribute[] fromAttributes = this.getFromAttributes();
        Extractor[] result = new Extractor[this.onHeapNonDatedExtractors.length + fromAttributes.length];
        System.arraycopy(this.onHeapNonDatedExtractors, 0, result, 0, this.onHeapNonDatedExtractors.length);
        System.arraycopy(fromAttributes, 0, result, this.onHeapNonDatedExtractors.length, fromAttributes.length);
        return result;

    }

    private TimestampAttribute[] getFromAttributes()
    {
        TimestampAttribute[] fromAttributes = new TimestampAttribute[this.asOfAttributes.length];
        for (int i = 0; i < this.asOfAttributes.length; i++)
        {
            fromAttributes[i] = this.asOfAttributes[i].getFromAttribute();
        }
        return fromAttributes;
    }

    public Extractor[] getExtractors()
    {
        return this.onHeapDatedExtractors;
    }

    public Extractor[] getNonDatedExtractors()
    {
        return this.onHeapNonDatedExtractors;
    }

    private int indexFor(int h, int length, byte aRightShift)
    {
        return (h ^ (h >>> aRightShift)) & (length - 1);
    }

    public int size()
    {
        if (datedSize == 0)
            return 0;
        return datedSize;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        return dataStorage.forAll(procedure);
    }

    public Object getFromData(Object data, int nonDatedHashCode)
    {
        int hash = this.asOfAttributeHashStrategy.computeOnHeapCombinedHashCode(data, nonDatedHashCode);
        int length = getDatedTableLength();
        int index = indexFor(hash, length, this.datedRightShift);
        int cur = getDatedTableAt(index);
        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return ChainedBucket.getFromData(storage, cur & ~UPPER_BIT_MASK, datedHashStrategy, dataStorage, data);
        }
        if (this.datedHashStrategy.equals(dataStorage, cur, data))
        {
            return dataStorage.getDataAsObject(cur);
        }
        return null;
    }

    public Object get(Object valueHolder, List extractors)
    {
        int hash = this.datedHashStrategy.computeHashCode(valueHolder, extractors);
        int length = getDatedTableLength();
        int index = indexFor(hash, length, this.datedRightShift);
        int cur = getDatedTableAt(index);
        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return ChainedBucket.getFromData(storage, cur & ~UPPER_BIT_MASK, datedHashStrategy, dataStorage, valueHolder, extractors);
        }
        if (this.datedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
        {
            return dataStorage.getDataAsObject(cur);
        }
        return null;
    }

    public Object get(Object valueHolder, Extractor[] extractors)
    {
        int hash = this.datedHashStrategy.computeHashCode(valueHolder, extractors);
        int length = getDatedTableLength();
        int index = indexFor(hash, length, this.datedRightShift);
        int cur = getDatedTableAt(index);
        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return ChainedBucket.getFromData(storage, cur & ~UPPER_BIT_MASK, datedHashStrategy, dataStorage, valueHolder, extractors);
        }
        if (this.datedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
        {
            return dataStorage.getDataAsObject(cur);
        }
        return null;
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        int hash = this.datedHashStrategy.computeHashCode(keyHolder, extractors);
        int length = getDatedTableLength();
        int index = indexFor(hash, length, this.datedRightShift);
        int cur = getDatedTableAt(index);
        if (cur == FREE) return false;
        if (isChainedBucket(cur))
        {
            return ChainedBucket.contains(storage, cur & ~UPPER_BIT_MASK, datedHashStrategy, dataStorage, keyHolder, extractors, filter);
        }
        return this.datedHashStrategy.equals(dataStorage, cur, keyHolder, extractors) &&
                (filter == null || filter.matches(dataStorage.getDataAsObject(cur), keyHolder));
    }

    public boolean containsInSemiUnique(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(keyHolder, extractors);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);
        int cur = getNonDatedTableAt(index);

        if (cur == FREE) return false;
        if (isChainedBucket(cur))
        {
            return containsInNonDatedChained(cur & ~UPPER_BIT_MASK, keyHolder, extractors, filter);
        }
        else if (isMultiEntry(cur))
        {
            return containsInNonDatedMultiEntry(cur & ~UPPER_BIT_MASK, keyHolder, extractors, filter);
        }
        else if (nonDatedHashStrategy.equals(dataStorage, cur, keyHolder, extractors))
        {
            return containsInNonDatedIfMatchAsOfDates(dataStorage.getDataAsObject(cur), keyHolder, extractors, filter);
        }
        return false;
    }

    private boolean containsInNonDatedIfMatchAsOfDates(Object e, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        AsOfExtractor extractor = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length];
        if (!extractor.dataMatches(e, extractor.timestampValueOf(keyHolder), asOfAttributes[0]))
        {
            return false;
        }
        if (asOfAttributes.length == 2)
        {
            extractor = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length + 1];
            if (!extractor.dataMatches(e, extractor.timestampValueOf(keyHolder), asOfAttributes[1]))
            {
                return false;
            }
        }
        return filter == null || filter.matches(e, keyHolder);
    }

    private boolean containsInNonDatedChained(int chainRef, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length];
        AsOfExtractor extractorTwo = null;
        if (asOfAttributes.length > 1)
        {
            extractorTwo = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length + 1];
        }

        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                if (containsInNonDatedMultiEntry(cur & ~UPPER_BIT_MASK, keyHolder, extractorOne, extractorTwo, extractors, filter))
                {
                    return true;
                }
            }
            else if (nonDatedHashStrategy.equals(dataStorage, cur, keyHolder, extractors))
            {
                Object candidate = getFromNonDatedIfMatchAsOfDates(dataStorage.getDataAsObject(cur), keyHolder, extractors);
                if (candidate != null && (filter == null || filter.matches(candidate, keyHolder)))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean containsInNonDatedMultiEntry(int multiEntryRef, Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        if (nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), keyHolder, extractors))
        {
            AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length];
            AsOfExtractor extractorTwo = null;
            if (asOfAttributes.length > 1)
            {
                extractorTwo = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length + 1];
            }
            return containsInNonDatedMultiEntry(multiEntryRef, keyHolder, extractorOne, extractorTwo, extractors, filter);
        }
        return false;
    }

    private boolean containsInNonDatedMultiEntry(int multiEntryRef, Object keyHolder,
                                                 AsOfExtractor extractorOne, AsOfExtractor extractorTwo, Extractor[] extractors, Filter2 filter)
    {
        int cur = MultiEntry.getFirst(storage, multiEntryRef);
        if (nonDatedHashStrategy.equals(dataStorage, cur, keyHolder, extractors))
        {
            int size = MultiEntry.getSize(storage, multiEntryRef);
            for (int i = 0; i < size; i++)
            {
                Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
                if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(keyHolder), asOfAttributes[0]))
                {
                    continue;
                }
                if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(keyHolder), asOfAttributes[1]))
                {
                    continue;
                }
                if (filter == null || filter.matches(e, keyHolder))
                {
                    return true;
                }
            }
        }
        return false;
    }

    public Object put(Object key, int nonDatedHashCode)
    {
        return put(((MithraOffHeapDataObject) key).zGetOffset(), nonDatedHashCode);
    }

    private Object put(int data, int nonDatedHashCode)
    {
        int removed = this.putInDatedTable(data, nonDatedHashCode);
        if (removed != FREE)
        {
            this.removeNonDatedEntry(removed);
        }
        putInNonDatedTable(data, nonDatedHashCode);
        return removed == FREE ? null : dataStorage.getDataAsObject(removed);
    }

    public Object putSemiUnique(Object key)
    {
        return this.putSemiUnique(((MithraOffHeapDataObject) key).zGetOffset());
    }

    private Object putSemiUnique(int data)
    {
        return put(data, this.nonDatedHashStrategy.computeHashCode(dataStorage, data));
    }

    private int putInDatedTable(int data, int nonDatedHashCode)
    {
        int removed = FREE;
        int hash = this.asOfAttributeHashStrategy.computeCombinedHashCode(dataStorage, data, nonDatedHashCode);
        int length = getDatedTableLength();
        int index = indexFor(hash, length, this.datedRightShift);

        int cur = getDatedTableAt(index);

        if (cur == FREE)
        {
            setDatedTableAt(index, data);
        }
        else if (isChainedBucket(cur))
        {
            setDatedChainAt(index, ChainedBucket.addDated(storage, cur & ~UPPER_BIT_MASK, data));
        }
        else if (datedHashStrategy.equals(dataStorage, cur, data))
        {
            setDatedTableAt(index, data);
            removed = cur;
        }
        else
        {
            int chainRef = ChainedBucket.allocateWithTwoElements(storage, cur, data);
            setDatedChainAt(index, chainRef);
        }
        if (removed == FREE)
        {
            int datedThreshold = length >> 1;
            datedThreshold += (datedThreshold >> 1); // threshold = length * 0.75 = length/2 + length/4
            if (++this.datedSize > datedThreshold)
            {
                this.resizeDatedTable();
            }
        }
        return removed;
    }

    protected int allocateDated(int capacity)
    {
        this.datedArrayRef = storage.allocate(capacity);
        this.computeDatedRightShift(capacity);

        return capacity;
    }

    protected void computeDatedRightShift(int capacity)
    {
        this.datedRightShift = (byte) (Integer.numberOfTrailingZeros(capacity) + 1);
    }

    private void resizeDatedTable()
    {
        int newCapacity = storage.getLength(datedArrayRef) << 1;
        resizeDatedTable(newCapacity);
    }

    private void resizeDatedTable(int newCapacity)
    {
        computeDatedRightShift(newCapacity);

        int newDatedArrayRef = storage.allocate(newCapacity);
        transferDatedTable(datedArrayRef, newDatedArrayRef);
        storage.free(datedArrayRef);
        datedArrayRef = newDatedArrayRef;
        if (storage.isFragmented())
        {
            //todo: rehash both the dated and non-dated tables into a new storage area
        }
    }

    private void transferDatedTable(int srcArrayRef, int destArrayRef)
    {
        int length = storage.getLength(srcArrayRef);
        int newLength = storage.getLength(destArrayRef);
        for (int j = 0; j < length; ++j)
        {
            int cur = getDatedTableAt(j);
            if (cur == FREE)
            {
                continue;
            }
            if (isChainedBucket(cur))
            {
                int chainRef = cur & ~UPPER_BIT_MASK;
                ChainedBucket.transferDated(storage, chainRef, destArrayRef, newLength, this);
                storage.free(chainRef);
            }
            else
            {
                transferDated(destArrayRef, cur, newLength);
            }
        }
    }

    private void transferDated(int destArrayRef, int data, int newLength)
    {
        int hash = this.datedHashStrategy.computeHashCode(dataStorage, data);
        int index = indexFor(hash, newLength, this.datedRightShift);
        int cur = storage.getInt(destArrayRef, index);
        if (cur == FREE)
        {
            storage.setInt(destArrayRef, index, data);
        }
        else if (isChainedBucket(cur))
        {
            int chainRef = ChainedBucket.addDated(storage, cur & ~UPPER_BIT_MASK, data);
            storage.setInt(destArrayRef, index, chainRef | CHAINED_BUCKET_PATTERN);
        }
        else
        {
            int chainRef = ChainedBucket.allocateWithTwoElements(storage, cur, data);
            storage.setInt(destArrayRef, index, chainRef | CHAINED_BUCKET_PATTERN);
        }
    }

    public Object remove(Object key)
    {
        int removed = this.removeFromDatedTable(key);
        if (removed != FREE)
        {
            this.removeNonDatedEntry(removed);
            return dataStorage.getDataAsObject(removed);
        }
        return null;
    }

    public Object removeUsingUnderlying(Object businessObject)
    {
        return this.remove(businessObject);
    }

    private void removeRefFromDatedTable(int toRemove)
    {
        int hash = this.datedHashStrategy.computeHashCode(dataStorage, toRemove);
        int length = getDatedTableLength();
        int index = indexFor(hash, length, this.datedRightShift);
        int cur = getDatedTableAt(index);
        if (cur == FREE)
        {
            throw new RuntimeException("should not get here");
        }
        if (isChainedBucket(cur))
        {
            int chainRef = cur & ~UPPER_BIT_MASK;
            int removed = ChainedBucket.removeDatedByIdentity(storage, chainRef, toRemove);
            if (removed != FREE)
            {
                datedSize--;
                if (ChainedBucket.getSize(storage, chainRef) == 0)
                {
                    storage.free(chainRef);
                    setDatedTableAt(index, FREE);
                }
            }
        }
        else if (this.datedHashStrategy.equals(dataStorage, cur, toRemove))
        {
            datedSize--;
            setDatedTableAt(index, FREE);
        }
    }

    private int removeFromDatedTable(Object underlying)
    {
        int hash = this.datedHashStrategy.computeHashCode(underlying);
        int length = getDatedTableLength();
        int index = indexFor(hash, length, this.datedRightShift);
        int cur = getDatedTableAt(index);
        if (cur == FREE)
        {
            return FREE;
        }
        if (isChainedBucket(cur))
        {
            int chainRef = cur & ~UPPER_BIT_MASK;
            int removed = ChainedBucket.removeDatedByEquality(storage, chainRef, dataStorage, datedHashStrategy, underlying);
            if (removed != FREE)
            {
                datedSize--;
                if (ChainedBucket.getSize(storage, chainRef) == 0)
                {
                    storage.free(chainRef);
                    setDatedTableAt(index, FREE);
                }
            }
            return removed;
        }
        if (this.datedHashStrategy.equals(dataStorage, cur, underlying))
        {
            datedSize--;
            setDatedTableAt(index, FREE);
            return cur;
        }
        return FREE;
    }

    public List removeAll(Filter filter)
    {
        FastList result = new FastList();
        int length = getDatedTableLength();
        for (int i = 0; i < length; i++)
        {
            int cur = getDatedTableAt(i);
            if (isChainedBucket(cur))
            {
                int chainRef = cur & ~UPPER_BIT_MASK;
                int sizeBefore = result.size();
                ChainedBucket.removeDatedByFilter(storage, chainRef, dataStorage, filter, result, this);
                datedSize -= (result.size() - sizeBefore);
                if (ChainedBucket.getSize(storage, chainRef) == 0)
                {
                    storage.free(chainRef);
                    setDatedTableAt(i, FREE);
                }
            }
            else if (cur != FREE && filter.matches(dataStorage.getDataAsObject(cur)))
            {
                result.add(dataStorage.getDataAsObject(cur));
                datedSize--;
                setDatedTableAt(i, FREE);
                removeNonDatedEntry(cur);
            }
        }
        return result;
    }

    public boolean evictCollectedReferences()
    {
        return false;
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return false;
    }

    public CommonExtractorBasedHashingStrategy getNonDatedPkHashStrategy()
    {
        return this.nonDatedHashStrategy;
    }

    public void forAllInParallel(final ParallelProcedure procedure)
    {
        this.dataStorage.forAllInParallel(procedure);
    }

    public void ensureExtraCapacity(int extraCapacity)
    {
        int datedSize = extraCapacity + this.datedSize;
        this.storage.ensureCapacity(datedSize * 20L);
        int length = getDatedTableLength();
        int datedThreshold = length >> 1;
        datedThreshold += (datedThreshold >> 1); // threshold = length * 0.75 = length/2 + length/4
        if (datedSize > datedThreshold)
        {
            int capacity = length;
            while (datedSize > datedThreshold)
            {
                capacity = capacity << 1;
                datedThreshold = capacity >> 1;
                datedThreshold += (datedThreshold >> 1); // threshold = length * 0.75 = length/2 + length/4
            }
            this.resizeDatedTable(capacity);
        }
        int nonDatedSize = this.nonDatedSize + extraCapacity / 8;
        int nonDatedLength = getNonDatedTableLength();
        int nonDatedThreshold = nonDatedLength >> 1;
        nonDatedThreshold += (nonDatedThreshold >> 1);
        if (nonDatedSize > nonDatedThreshold)
        {
            int capacity = nonDatedLength;
            while (nonDatedSize > nonDatedThreshold)
            {
                capacity = capacity << 1;
                nonDatedThreshold = capacity >> 1;
                nonDatedThreshold += (nonDatedThreshold >> 1);
            }
            this.resizeNonDated(capacity);
        }

    }

    public void clear()
    {
        clearDatedTable();
        clearNonDatedTable();
    }

    private void clearDatedTable()
    {
        int length = getDatedTableLength();
        for (int i = 0; i < length; i++)
        {
            int cur = getDatedTableAt(i);
            if (isChainedBucket(cur))
            {
                storage.free(cur & ~UPPER_BIT_MASK);
            }
            setDatedTableAt(i, FREE);
        }
        datedSize = 0;
    }

    public PrimaryKeyIndex copy()
    {
        final FullUniqueIndex result = new FullUniqueIndex(this.getExtractors(), this.datedSize);
        this.forAll(new DoUntilProcedure()
        {
            public boolean execute(Object object)
            {
                result.put(object);
                return false;
            }
        });
        return result;
    }

    /////////////////////////////////// non dated methods

    public Object getFromSemiUnique(Object valueHolder, List extractors)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(valueHolder, extractors);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);
        int cur = getNonDatedTableAt(index);

        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return getFromNonDatedChained(cur & ~UPPER_BIT_MASK, valueHolder, extractors);
        }
        else if (isMultiEntry(cur))
        {
            return getFromNonDatedMultiEntry(cur & ~UPPER_BIT_MASK, valueHolder, extractors);
        }
        else if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
        {
            return getFromNonDatedIfMatchAsOfDates(dataStorage.getDataAsObject(cur), valueHolder, extractors);
        }
        return null;
    }

    private Object getFromNonDatedIfMatchAsOfDates(Object e, Object valueHolder, List extractors)
    {
        AsOfExtractor extractor = (AsOfExtractor) extractors.get(this.onHeapNonDatedExtractors.length);
        if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[0]))
        {
            return null;
        }
        if (asOfAttributes.length == 2)
        {
            extractor = (AsOfExtractor) extractors.get(this.onHeapNonDatedExtractors.length + 1);
            if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[1]))
            {
                return null;
            }
        }
        return e;
    }

    private Object getFromNonDatedChained(int chainRef, Object valueHolder, List extractors)
    {
        AsOfExtractor extractorOne = (AsOfExtractor) extractors.get(this.onHeapNonDatedExtractors.length);
        AsOfExtractor extractorTwo = null;
        boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
        if (asOfAttributes.length > 1)
        {
            extractorTwo = (AsOfExtractor) extractors.get(this.onHeapNonDatedExtractors.length + 1);
            matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
        }

        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            Object result = null;
            if (isMultiEntry(cur))
            {
                result = getFromNonDatedMultiEntry(cur & ~UPPER_BIT_MASK, valueHolder, extractorOne, extractorTwo, matchMoreThanOne, extractors);
            }
            else if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
            {
                result = getFromNonDatedIfMatchAsOfDates(dataStorage.getDataAsObject(cur), valueHolder, extractors);
            }
            if (result != null) return result;
        }
        return null;
    }

    private Object getFromNonDatedMultiEntry(int multiEntryRef, Object valueHolder, List extractors)
    {
        if (nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), valueHolder, extractors))
        {
            AsOfExtractor extractorOne = (AsOfExtractor) extractors.get(this.onHeapNonDatedExtractors.length);
            AsOfExtractor extractorTwo = null;
            boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
            if (asOfAttributes.length > 1)
            {
                extractorTwo = (AsOfExtractor) extractors.get(this.onHeapNonDatedExtractors.length + 1);
                matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
            }
            return getFromNonDatedMultiEntry(multiEntryRef, valueHolder, extractorOne, extractorTwo, matchMoreThanOne, extractors);
        }
        return null;
    }

    private Object getFromNonDatedMultiEntry(int multiEntryRef, Object valueHolder,
                                             AsOfExtractor extractorOne, AsOfExtractor extractorTwo, boolean matchMoreThanOne, List extractors)
    {
        int cur = MultiEntry.getFirst(storage, multiEntryRef);
        if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
        {
            int size = MultiEntry.getSize(storage, multiEntryRef);
            if (matchMoreThanOne)
            {
                FastList result = new FastList(size);
                for (int i = 0; i < size; i++)
                {
                    Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
                    if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(valueHolder), asOfAttributes[0]))
                    {
                        continue;
                    }
                    if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(valueHolder), asOfAttributes[1]))
                    {
                        continue;
                    }
                    result.add(e);
                }
                return result.size() > 0 ? result : null;
            }
            else
            {
                for (int i = size - 1; i >= 0; i--)
                {
                    Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
                    if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(valueHolder), asOfAttributes[0]))
                    {
                        continue;
                    }
                    if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(valueHolder), asOfAttributes[1]))
                    {
                        continue;
                    }
                    return e;
                }
                return null;
            }
        }
        return null;
    }

    public Object getFromSemiUnique(Object valueHolder, Extractor[] extractors)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(valueHolder, extractors);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);
        int cur = getNonDatedTableAt(index);

        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return getFromNonDatedChained(cur & ~UPPER_BIT_MASK, valueHolder, extractors);
        }
        else if (isMultiEntry(cur))
        {
            return getFromNonDatedMultiEntry(cur & ~UPPER_BIT_MASK, valueHolder, extractors);
        }
        else if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
        {
            return getFromNonDatedIfMatchAsOfDates(dataStorage.getDataAsObject(cur), valueHolder, extractors);
        }
        return null;
    }

    private Object getFromNonDatedIfMatchAsOfDates(Object e, Object valueHolder, Extractor[] extractors)
    {
        AsOfExtractor extractor = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length];
        if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[0]))
        {
            return null;
        }
        if (asOfAttributes.length == 2)
        {
            extractor = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length + 1];
            if (!extractor.dataMatches(e, extractor.timestampValueOf(valueHolder), asOfAttributes[1]))
            {
                return null;
            }
        }
        return e;
    }

    private Object getFromNonDatedChained(int chainRef, Object valueHolder, Extractor[] extractors)
    {
        AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length];
        AsOfExtractor extractorTwo = null;
        boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
        if (asOfAttributes.length > 1)
        {
            extractorTwo = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length + 1];
            matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
        }

        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            Object result = null;
            if (isMultiEntry(cur))
            {
                result = getFromNonDatedMultiEntry(cur & ~UPPER_BIT_MASK, valueHolder, extractorOne, extractorTwo, matchMoreThanOne, extractors);
            }
            else if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
            {
                result = getFromNonDatedIfMatchAsOfDates(dataStorage.getDataAsObject(cur), valueHolder, extractors);
            }
            if (result != null) return result;
        }
        return null;
    }

    private Object getFromNonDatedMultiEntry(int multiEntryRef, Object valueHolder, Extractor[] extractors)
    {
        if (nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), valueHolder, extractors))
        {
            AsOfExtractor extractorOne = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length];
            AsOfExtractor extractorTwo = null;
            boolean matchMoreThanOne = extractorOne.matchesMoreThanOne();
            if (asOfAttributes.length > 1)
            {
                extractorTwo = (AsOfExtractor) extractors[this.onHeapNonDatedExtractors.length + 1];
                matchMoreThanOne = matchMoreThanOne || extractorTwo.matchesMoreThanOne();
            }
            return getFromNonDatedMultiEntry(multiEntryRef, valueHolder, extractorOne, extractorTwo, matchMoreThanOne, extractors);
        }
        return null;
    }

    private Object getFromNonDatedMultiEntry(int multiEntryRef, Object valueHolder,
                                             AsOfExtractor extractorOne, AsOfExtractor extractorTwo, boolean matchMoreThanOne, Extractor[] extractors)
    {
        int cur = MultiEntry.getFirst(storage, multiEntryRef);
        if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors))
        {
            int size = MultiEntry.getSize(storage, multiEntryRef);
            if (matchMoreThanOne)
            {
                FastList result = new FastList(size);
                for (int i = 0; i < size; i++)
                {
                    Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
                    if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(valueHolder), asOfAttributes[0]))
                    {
                        continue;
                    }
                    if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(valueHolder), asOfAttributes[1]))
                    {
                        continue;
                    }
                    result.add(e);
                }
                return result.size() > 0 ? result : null;
            }
            else
            {
                for (int i = size - 1; i >= 0; i--)
                {
                    Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
                    if (!extractorOne.dataMatches(e, extractorOne.timestampValueOf(valueHolder), asOfAttributes[0]))
                    {
                        continue;
                    }
                    if (extractorTwo != null && !extractorTwo.dataMatches(e, extractorTwo.timestampValueOf(valueHolder), asOfAttributes[1]))
                    {
                        continue;
                    }
                    return e;
                }
                return null;
            }
        }
        return null;
    }

    public Object getSemiUniqueFromData(Object data, Timestamp[] asOfDates)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);
        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return getNonDatedFromDataChained(cur & ~UPPER_BIT_MASK, data, asOfDates);
        }
        else if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
            {
                return getNonDatedFromMultiEntry(multiEntryRef, asOfDates);
            }
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            return getFromNonDatedIfMatchesAsOfDates(asOfDates, dataStorage.getDataAsObject(cur));
        }
        return null;
    }

    private Object getNonDatedFromDataChained(int chainRef, Object data, Timestamp[] asOfDates)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                int multiEntryRef = cur & ~UPPER_BIT_MASK;
                if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
                {
                    return getNonDatedFromMultiEntry(multiEntryRef, asOfDates);
                }
            }
            else if (nonDatedHashStrategy.equals(dataStorage, cur, data))
            {
                return getFromNonDatedIfMatchesAsOfDates(asOfDates, dataStorage.getDataAsObject(cur));
            }
        }
        return null;
    }

    private Object getFromNonDatedIfMatchesAsOfDates(Timestamp[] asOfDates, Object e)
    {
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            if (!asOfAttributes[i].dataMatches(e, asOfDates[i])) return null;
        }
        return e;
    }

    private Object getNonDatedFromMultiEntry(int multiEntryRef, Timestamp[] asOfDates)
    {
        int size = MultiEntry.getSize(storage, multiEntryRef);
        for (int i = size - 1; i >= 0; i--)
        {
            Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
            boolean matches = true;
            for (int j = 0; j < asOfAttributes.length && matches; j++)
            {
                if (!asOfAttributes[j].dataMatches(e, asOfDates[j])) matches = false;
            }
            if (matches) return e;
        }
        return null;
    }

    public boolean addSemiUniqueToContainer(Object data, TemporalContainer container)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);
        if (cur == FREE) return false;
        if (isChainedBucket(cur))
        {
            return addNonDatedToContainerChained(cur & ~UPPER_BIT_MASK, data, container);
        }
        else if (isMultiEntry(cur))
        {
            return addMultiEntryToContainer(cur & ~UPPER_BIT_MASK, data, container);
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            container.addCommittedData(dataStorage.getData(cur));
            return true;
        }
        return false;
    }

    private boolean addNonDatedToContainerChained(int chainRef, Object data, TemporalContainer container)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                if (addMultiEntryToContainer(cur & ~UPPER_BIT_MASK, data, container))
                {
                    return true;
                }
            }
            else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
            {
                container.addCommittedData(dataStorage.getData(cur));
                return true;
            }
        }
        return false;
    }

    private boolean addMultiEntryToContainer(int multiEntryRef, Object data, TemporalContainer container)
    {
        if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
        {
            int size = MultiEntry.getSize(storage, multiEntryRef);
            for (int i = 0; i < size; i++)
            {
                container.addCommittedData(dataStorage.getData(MultiEntry.getAt(storage, multiEntryRef, i)));
            }
            return true;
        }
        return false;
    }

    public List getFromDataForAllDatesAsList(Object data)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);
        if (cur == FREE) return ListFactory.EMPTY_LIST;
        if (isChainedBucket(cur))
        {
            return getFromDataForAllDatesAsListChained(cur & ~UPPER_BIT_MASK, data);
        }
        if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
            {
                return convertMultiEntryToList(multiEntryRef);
            }
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            return ListFactory.create(dataStorage.getDataAsObject(cur));
        }
        return ListFactory.EMPTY_LIST;
    }

    private List getFromDataForAllDatesAsListChained(int chainRef, Object data)
    {
        int size = ChainedBucket.getSize(storage, chainRef);

        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                int multiEntryRef = cur & ~UPPER_BIT_MASK;
                if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
                {
                    return convertMultiEntryToList(multiEntryRef);
                }
            }
            else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
            {
                return ListFactory.create(dataStorage.getDataAsObject(cur));
            }
        }
        return ListFactory.EMPTY_LIST;
    }

    private List convertMultiEntryToList(int multiEntryRef)
    {
        int size = MultiEntry.getSize(storage, multiEntryRef);
        FastList result = new FastList(size);
        for (int i = 0; i < size; i++)
        {
            result.add(dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i)));
        }
        return result;
    }

    public Object getSemiUniqueAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int index = indexFor(nonDatedHash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);

        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return getNonDatedChained(cur & ~UPPER_BIT_MASK, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        if (isMultiEntry(cur))
        {
            return getNonDatedMulti(cur & ~UPPER_BIT_MASK, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        Object data = dataStorage.getDataAsObject(cur);
        if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, data, asOfDate0, asOfDate1))
        {
            return data;
        }
        return null;
    }

    private Object getNonDatedChained(int chainRef, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                Object result = getNonDatedMulti(cur & ~UPPER_BIT_MASK, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
                if (result != null) return result;
            }
            else
            {
                Object data = dataStorage.getDataAsObject(cur);
                if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, data, asOfDate0, asOfDate1))
                {
                    return data;
                }
            }
        }
        return null;
    }

    private Object getNonDatedMulti(int multiEntryRef, Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        int size = MultiEntry.getSize(storage, multiEntryRef);
        for (int i = size - 1; i >= 0; i--)
        {
            Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
            if (relationshipHashStrategy.equalsForRelationship(srcObject, srcData, e, asOfDate0, asOfDate1))
            {
                return e;
            }
        }
        return null;
    }

    public Object getSemiUniqueAsOneWithDates(Object valueHolder, Extractor[] extractors, Timestamp[] asOfDates, int nonDatedHash)
    {
        int index = indexFor(nonDatedHash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);
        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return getNonDatedAsOneWithDatesChained(cur & ~UPPER_BIT_MASK, valueHolder, extractors, asOfDates);
        }
        if (isMultiEntry(cur))
        {
            return getNonDatedAsOneWithDatesMulti(cur & ~UPPER_BIT_MASK, valueHolder, extractors, asOfDates);
        }
        Object data = dataStorage.getDataAsObject(cur);
        if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors) && asOfAttributes[0].dataMatches(data, asOfDates[0]) &&
                (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(data, asOfDates[1])))
        {
            return data;
        }
        return null;
    }

    private Object getNonDatedAsOneWithDatesChained(int chainRef, Object valueHolder, Extractor[] extractors, Timestamp[] asOfDates)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                int multiEntryRef = cur & ~UPPER_BIT_MASK;
                if (nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), valueHolder, extractors))
                {
                    return matchAsOfDates(multiEntryRef, asOfDates);
                }
            }
            else
            {
                Object data = dataStorage.getDataAsObject(cur);
                if (nonDatedHashStrategy.equals(dataStorage, cur, valueHolder, extractors) && asOfAttributes[0].dataMatches(data, asOfDates[0]) &&
                        (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(data, asOfDates[1])))
                {
                    return data;
                }
            }
        }
        return null;
    }

    private Object getNonDatedAsOneWithDatesMulti(int multiEntryRef, Object valueHolder, Extractor[] extractors, Timestamp[] asOfDates)
    {
        if (nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), valueHolder, extractors))
        {
            return matchAsOfDates(multiEntryRef, asOfDates);
        }
        return null;
    }

    private Object matchAsOfDates(int multiEntryRef, Timestamp[] asOfDates)
    {
        int size = MultiEntry.getSize(storage, multiEntryRef);
        for (int i = size - 1; i >= 0; i--)
        {
            Object e = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
            if (asOfAttributes[0].dataMatches(e, asOfDates[0]) &&
                    (asOfAttributes.length == 1 || asOfAttributes[1].dataMatches(e, asOfDates[1])))
            {
                return e;
            }
        }
        return null;
    }

    public int getSemiUniqueSize()
    {
        return this.nonDatedSize;
    }

    public List removeOldEntryForRange(Object data)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);

        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);

        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return removeOldEntryForRangeChained(cur & ~UPPER_BIT_MASK, data, index);
        }
        if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
            {
                FastList result = removeOldEntryForRangeMulti(data, multiEntryRef);
                if (MultiEntry.getSize(storage, multiEntryRef) == 0)
                {
                    setNonDatedTableAt(index, FREE);
                    storage.free(multiEntryRef);
                    this.nonDatedSize--;
                }
                return result;
            }
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            if (hasOverlap(cur, data))
            {
                setNonDatedTableAt(index, FREE);
                this.nonDatedSize--;
                removeRefFromDatedTable(cur);
                return ListFactory.create(dataStorage.getDataAsObject(cur));
            }
        }
        return null;
    }

    private List removeOldEntryForRangeChained(int chainRef, Object data, int index)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        List result = null;
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                int multiEntryRef = cur & ~UPPER_BIT_MASK;
                if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
                {
                    result = removeOldEntryForRangeMulti(data, multiEntryRef);
                    if (MultiEntry.getSize(storage, multiEntryRef) == 0)
                    {
                        ChainedBucket.removeAt(storage, chainRef, i);
                        storage.free(multiEntryRef);
                        this.nonDatedSize--;
                    }
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
            {
                if (hasOverlap(cur, data))
                {
                    ChainedBucket.removeAt(storage, chainRef, i);
                    this.nonDatedSize--;
                    removeRefFromDatedTable(cur);
                    result = ListFactory.create(dataStorage.getDataAsObject(cur));
                    break;
                }
            }
        }

        if (ChainedBucket.getSize(storage, chainRef) == 0)
        {
            setNonDatedTableAt(index, FREE);
            storage.free(chainRef);
        }
        return result;
    }

    private FastList removeOldEntryForRangeMulti(Object data, int multiEntryRef)
    {
        FastList result = null;
        for (int i = 0; i < MultiEntry.getSize(storage, multiEntryRef); )
        {
            int cur = MultiEntry.getAt(storage, multiEntryRef, i);
            if (hasOverlap(cur, data))
            {
                if (result == null) result = new FastList(MultiEntry.getSize(storage, multiEntryRef));
                result.add(dataStorage.getDataAsObject(cur));
                removeRefFromDatedTable(cur);
                MultiEntry.removeAt(storage, multiEntryRef, i);
            }
            else
            {
                i++;
            }
        }
        return result;
    }

    private boolean hasOverlap(int cur, Object data)
    {
        boolean fullMatch = true;
        for (int i = 0; fullMatch && i < asOfAttributes.length; i++)
        {
            fullMatch = asOfAttributes[i].hasRangeOverlap(dataStorage.getData(cur),
                    asOfAttributes[i].getFromAttribute().timestampValueOfAsLong(data),
                    asOfAttributes[i].getToAttribute().timestampValueOfAsLong(data));
        }
        return fullMatch;
    }

    public boolean removeAllIgnoringDate(Object data, DoProcedure procedure)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);

        if (cur == FREE) return false;
        if (isChainedBucket(cur))
        {
            return removeAllIgnoringDateChained(cur & ~UPPER_BIT_MASK, data, procedure, index);
        }
        if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
            {
                removeMultiAndExecute(procedure, index, multiEntryRef);
                return true;
            }
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            removeSingleAndExecute(procedure, index, cur);
            return true;
        }
        return false;
    }

    private boolean removeAllIgnoringDateChained(int chainRef, Object data, DoProcedure procedure, int index)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        boolean result = false;
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (removeFromBucket(cur, chainRef, data, procedure, i))
            {
                result = true;
                break;
            }
        }
        if (ChainedBucket.getSize(storage, chainRef) == 0)
        {
            setNonDatedTableAt(index, FREE);
            storage.free(chainRef);
        }
        return result;
    }

    private boolean removeFromBucket(int cur, int chainRef, Object data, DoProcedure procedure, int pos)
    {
        if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
            {
                removeMultiFromBucketAndExecute(procedure, chainRef, pos, multiEntryRef);
                return true;
            }
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            removeSingleFromBucketAndExecute(procedure, chainRef, pos, cur);
            return true;
        }
        return false;
    }

    private void removeSingleFromBucketAndExecute(DoProcedure procedure, int chainRef, int pos, int cur)
    {
        this.nonDatedSize--;
        ChainedBucket.removeAt(storage, chainRef, pos);
        removeRefFromDatedTable(cur);
        procedure.execute(dataStorage.getDataAsObject(cur));
    }

    private void removeMultiFromBucketAndExecute(DoProcedure procedure, int chainRef, int pos, int multiEntryRef)
    {
        this.nonDatedSize--;
        removeMultiAndExecute(procedure, multiEntryRef);
        ChainedBucket.removeAt(storage, chainRef, pos);
    }

    private void removeSingleAndExecute(DoProcedure procedure, int index, int cur)
    {
        setNonDatedTableAt(index, FREE);
        this.nonDatedSize--;
        removeRefFromDatedTable(cur);
        procedure.execute(dataStorage.getDataAsObject(cur));
    }

    private void removeMultiAndExecute(DoProcedure procedure, int index, int multiEntryRef)
    {
        setNonDatedTableAt(index, FREE);
        removeMultiAndExecute(procedure, multiEntryRef);
    }

    private void removeMultiAndExecute(DoProcedure procedure, int multiEntryRef)
    {
        this.nonDatedSize--;
        int size = MultiEntry.getSize(storage, multiEntryRef);
        for (int i = 0; i < size; i++)
        {
            int cur = MultiEntry.getAt(storage, multiEntryRef, i);
            removeRefFromDatedTable(cur);
            procedure.execute(dataStorage.getDataAsObject(cur));
        }
        storage.free(multiEntryRef);
    }

    public Object removeOldEntry(Object data, Timestamp[] asOfDates)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(data);

        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);
        if (cur == FREE) return null;
        if (isChainedBucket(cur))
        {
            return removeOldEntryChained(cur & ~UPPER_BIT_MASK, index, data, asOfDates);
        }
        if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
            {
                Object result = removeOldEntryMulti(multiEntryRef, asOfDates);
                if (MultiEntry.getSize(storage, multiEntryRef) == 0)
                {
                    setNonDatedTableAt(index, FREE);
                    storage.free(multiEntryRef);
                    this.nonDatedSize--;
                }
                return result;
            }
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            Object o = dataStorage.getDataAsObject(cur);
            if (matchesAsOfDates(o, asOfDates))
            {
                setNonDatedTableAt(index, FREE);
                this.nonDatedSize--;
                removeRefFromDatedTable(cur);
                return o;
            }
        }
        return null;
    }

    private Object removeOldEntryChained(int chainRef, int index, Object data, Timestamp[] asOfDates)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        Object result = null;
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                int multiEntryRef = cur & ~UPPER_BIT_MASK;
                if (this.nonDatedHashStrategy.equals(dataStorage, MultiEntry.getFirst(storage, multiEntryRef), data))
                {
                    result = removeOldEntryMulti(multiEntryRef, asOfDates);
                    if (MultiEntry.getSize(storage, multiEntryRef) == 0)
                    {
                        ChainedBucket.removeAt(storage, chainRef, i);
                        storage.free(multiEntryRef);
                        this.nonDatedSize--;
                    }
                    break;
                }
            }
            else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
            {
                Object o = dataStorage.getDataAsObject(cur);
                if (matchesAsOfDates(o, asOfDates))
                {
                    ChainedBucket.removeAt(storage, chainRef, i);
                    this.nonDatedSize--;
                    removeRefFromDatedTable(cur);
                    result = o;
                    break;
                }
            }
        }
        if (ChainedBucket.getSize(storage, chainRef) == 0)
        {
            setNonDatedTableAt(index, FREE);
            storage.free(chainRef);
        }
        return result;
    }

    private Object removeOldEntryMulti(int multiEntryRef, Timestamp[] asOfDates)
    {
        int size = MultiEntry.getSize(storage, multiEntryRef);
        for (int i = size - 1; i >= 0; i--)
        {
            int cur = MultiEntry.getAt(storage, multiEntryRef, i);
            Object o = dataStorage.getDataAsObject(cur);
            if (matchesAsOfDates(o, asOfDates))
            {
                removeRefFromDatedTable(cur);
                MultiEntry.removeAt(storage, multiEntryRef, i);
                return o;
            }
        }
        return null;
    }

    private boolean matchesAsOfDates(Object entry, Timestamp[] asOfDates)
    {
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            if (!asOfAttributes[i].dataMatches(entry, asOfDates[i])) return false;
        }
        return true;
    }

    protected int allocateNonDated(int capacity)
    {
        this.nonDatedArrayRef = storage.allocate(capacity);

        this.nonDatedRightShift = (byte) (Integer.numberOfTrailingZeros(capacity) + 1);

        return capacity;
    }

    private void putInNonDatedTable(int data, int hash)
    {
        int length = getNonDatedTableLength();
        int index = indexFor(hash, length, this.nonDatedRightShift);

        int cur = getNonDatedTableAt(index);
        boolean newEntry = false;
        if (cur == FREE)
        {
            setNonDatedTableAt(index, data);
            newEntry = true;
        }
        else if (isChainedBucket(cur))
        {
            int chainRef = cur & ~UPPER_BIT_MASK;
            int chainedSize = ChainedBucket.getSize(storage, chainRef);
            chainRef = ChainedBucket.addNonDated(storage, chainRef, nonDatedHashStrategy, dataStorage, data);
            if (chainedSize < ChainedBucket.getSize(storage, chainRef))
            {
                this.setNonDatedChainAt(index, chainRef);
                newEntry = true;
            }
        }
        else if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            int first = MultiEntry.getFirst(storage, multiEntryRef);
            if (this.nonDatedHashStrategy.equals(dataStorage, data, first))
            {
                this.setNonDatedMultiEntryAt(index, MultiEntry.add(storage, multiEntryRef, data));
            }
            else
            {
                int chainRef = ChainedBucket.allocateWithTwoElements(storage, cur, data);
                this.setNonDatedChainAt(index, chainRef);
                newEntry = true;
            }
        }
        else if (this.nonDatedHashStrategy.equals(dataStorage, cur, data))
        {
            int multiEntryRef = MultiEntry.allocateWithTwoElements(storage, cur, data);
            this.setNonDatedMultiEntryAt(index, multiEntryRef);
        }
        else
        {
            int chainRef = ChainedBucket.allocateWithTwoElements(storage, cur, data);
            this.setNonDatedChainAt(index, chainRef);
            newEntry = true;
        }
        int nonDatedThreshold = length >> 1;
        nonDatedThreshold += (nonDatedThreshold >> 1); // threshold = length * 0.75 = length/2 + length/4
        if (newEntry && ++this.nonDatedSize > nonDatedThreshold)
        {
            this.resizeNonDatedTable();
        }
    }

    private void resizeNonDatedTable()
    {
        int newCapacity = getNonDatedTableLength() << 1;
        resizeNonDated(newCapacity);
    }

    private void resizeNonDated(int newCapacity)
    {
        this.nonDatedRightShift = (byte) (Integer.numberOfTrailingZeros(newCapacity) + 1);

        int newNonDatedArrayRef = storage.allocate(newCapacity);

        transferNonDatedTable(nonDatedArrayRef, newNonDatedArrayRef);
        storage.free(nonDatedArrayRef);
        nonDatedArrayRef = newNonDatedArrayRef;
        if (storage.isFragmented())
        {
            //todo: rehash both the dated and non-dated tables into a new storage area
        }
    }

    private void transferNonDatedTable(int srcArrayRef, int destArrayRef)
    {
        int length = storage.getLength(srcArrayRef);
        int newLength = storage.getLength(destArrayRef);
        for (int j = 0; j < length; ++j)
        {
            int cur = getNonDatedTableAt(j);
            if (cur == FREE)
            {
                continue;
            }
            if (isChainedBucket(cur))
            {
                int chainRef = cur & ~UPPER_BIT_MASK;
                ChainedBucket.transferNonDated(storage, chainRef, destArrayRef, newLength, this);
                storage.free(chainRef);
            }
            else if (isMultiEntry(cur))
            {
                transferNonDated(destArrayRef, newLength, cur, nonDatedHashStrategy.computeHashCode(dataStorage, MultiEntry.getFirst(storage, cur & ~UPPER_BIT_MASK)));
            }
            else
            {
                transferNonDated(destArrayRef, newLength, cur, nonDatedHashStrategy.computeHashCode(dataStorage, cur));
            }
        }
    }

    private void transferNonDated(int destArrayRef, int destLength, int entry, int hash)
    {
        int index = indexFor(hash, destLength, this.nonDatedRightShift);
        int cur = storage.getInt(destArrayRef, index);
        if (cur == FREE)
        {
            storage.setInt(destArrayRef, index, entry);
        }
        else if (isChainedBucket(cur))
        {
            storage.setInt(destArrayRef, index, ChainedBucket.add(storage, cur & ~UPPER_BIT_MASK, entry) | CHAINED_BUCKET_PATTERN);
        }
        else
        {
            storage.setInt(destArrayRef, index, ChainedBucket.allocateWithTwoElements(storage, cur, entry) | CHAINED_BUCKET_PATTERN);
        }
    }

    protected void removeNonDatedEntry(int toRemove)
    {
        int hash = this.nonDatedHashStrategy.computeHashCode(dataStorage, toRemove);
        int index = indexFor(hash, getNonDatedTableLength(), this.nonDatedRightShift);

        int cur = this.getNonDatedTableAt(index);

        if (cur == toRemove)
        {
            this.nonDatedSize--;
            this.setNonDatedTableAt(index, FREE);
        }
        else if (isChainedBucket(cur))
        {
            removeNonDatedChained(cur & ~UPPER_BIT_MASK, index, toRemove);
        }
        else if (isMultiEntry(cur))
        {
            int multiEntryRef = cur & ~UPPER_BIT_MASK;
            int size = MultiEntry.getSize(storage, multiEntryRef);
            for (int i = 0; i < size; i++)
            {
                int ref = MultiEntry.getAt(storage, multiEntryRef, i);
                if (ref == toRemove)
                {
                    MultiEntry.removeAt(storage, multiEntryRef, i);
                    break;
                }
            }
            if (MultiEntry.getSize(storage, multiEntryRef) == 0)
            {
                this.nonDatedSize--;
                setNonDatedTableAt(index, FREE);
                storage.free(multiEntryRef);
            }
        }
    }

    private void removeNonDatedChained(int chainRef, int index, int toRemove)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (cur == toRemove)
            {
                this.nonDatedSize--;
                ChainedBucket.removeAt(storage, chainRef, i);
                break;
            }
            else if (isMultiEntry(cur))
            {
                boolean found = false;
                int multiEntryRef = cur & ~UPPER_BIT_MASK;
                int multiSize = MultiEntry.getSize(storage, multiEntryRef);
                for (int j = 0; j < multiSize; j++)
                {
                    int ref = MultiEntry.getAt(storage, multiEntryRef, j);
                    if (ref == toRemove)
                    {
                        found = true;
                        MultiEntry.removeAt(storage, multiEntryRef, j);
                        break;
                    }
                }
                if (MultiEntry.getSize(storage, multiEntryRef) == 0)
                {
                    this.nonDatedSize--;
                    ChainedBucket.removeAt(storage, chainRef, i);
                    storage.free(multiEntryRef);
                }
                if (found) break;
            }
        }

        if (ChainedBucket.getSize(storage, chainRef) == 0)
        {
            setNonDatedTableAt(index, FREE);
            storage.free(chainRef);
        }
    }

    private void clearNonDatedTable()
    {
        int size = getNonDatedTableLength();
        for (int i = 0; i < size; i++)
        {
            int cur = getNonDatedTableAt(i);
            if (isMultiEntry(cur) || isChainedBucket(cur))
            {
                storage.free(cur & ~UPPER_BIT_MASK);
            }
            setNonDatedTableAt(i, FREE);
        }
        nonDatedSize = 0;
    }

    private static class MultiEntry extends OffHeapIntList
    {
    }

    private static final class ChainedBucket extends OffHeapIntList
    {
        public static int addDated(OffHeapIntArrayStorage storage, int chainRef, int value)
        {
            return add(storage, chainRef, value);
        }

        public static int addNonDated(OffHeapIntArrayStorage storage, int chainRef, ExtractorBasedOffHeapHashStrategy nonDatedHashStrategy, OffHeapDataStorage dataStorage, int data)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = getAt(storage, chainRef, i);
                if (isMultiEntry(cur))
                {
                    int multiEntryRef = cur & ~UPPER_BIT_MASK;
                    int first = MultiEntry.getFirst(storage, multiEntryRef);
                    if (nonDatedHashStrategy.equals(dataStorage, data, first))
                    {
                        setMultiEntryAt(storage, chainRef, i, MultiEntry.add(storage, multiEntryRef, data));
                        return chainRef;
                    }
                }
                else if (nonDatedHashStrategy.equals(dataStorage, cur, data))
                {
                    int multiEntryRef = MultiEntry.allocateWithTwoElements(storage, cur, data);
                    setMultiEntryAt(storage, chainRef, i, multiEntryRef);
                    return chainRef;
                }
            }
            return add(storage, chainRef, data);
        }

        public static void setMultiEntryAt(OffHeapIntArrayStorage storage, int chainRef, int pos, int value)
        {
            setAt(storage, chainRef, pos, value | MULTI_ENTRY_PATTERN);
        }

        public static Object getFromData(OffHeapIntArrayStorage storage, int chainRef, ExtractorBasedOffHeapHashStrategy datedHashStrategy, OffHeapDataStorage dataStorage, Object data)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                if (datedHashStrategy.equals(dataStorage, cur, data))
                {
                    return dataStorage.getDataAsObject(cur);
                }
            }
            return null;
        }

        public static Object getFromData(OffHeapIntArrayStorage storage, int chainRef, ExtractorBasedOffHeapHashStrategy datedHashStrategy,
                                         OffHeapDataStorage dataStorage, Object data, List extractors)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                if (datedHashStrategy.equals(dataStorage, cur, data, extractors))
                {
                    return dataStorage.getDataAsObject(cur);
                }
            }
            return null;
        }

        public static Object getFromData(OffHeapIntArrayStorage storage, int chainRef, ExtractorBasedOffHeapHashStrategy datedHashStrategy,
                                         OffHeapDataStorage dataStorage, Object data, Extractor[] extractors)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                if (datedHashStrategy.equals(dataStorage, cur, data, extractors))
                {
                    return dataStorage.getDataAsObject(cur);
                }
            }
            return null;
        }

        public static boolean contains(OffHeapIntArrayStorage storage, int chainRef, ExtractorBasedOffHeapHashStrategy datedHashStrategy,
                                       OffHeapDataStorage dataStorage, Object keyHolder, Extractor[] extractors, Filter2 filter)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                if (datedHashStrategy.equals(dataStorage, cur, keyHolder, extractors) &&
                        (filter == null || filter.matches(dataStorage.getDataAsObject(cur), keyHolder)))
                {
                    return true;
                }
            }
            return false;
        }

        public static void transferDated(OffHeapIntArrayStorage storage, int chainRef, int destArrayRef, int newLength, OffHeapSemiUniqueDatedIndex offHeapSemiUniqueDatedIndex)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                offHeapSemiUniqueDatedIndex.transferDated(destArrayRef, cur, newLength);
            }
        }

        public static void transferNonDated(OffHeapIntArrayStorage storage, int chainRef, int destArrayRef, int newLength, OffHeapSemiUniqueDatedIndex offHeapSemiUniqueDatedIndex)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                int hash;
                if (isMultiEntry(cur))
                {
                    hash = offHeapSemiUniqueDatedIndex.nonDatedHashStrategy.computeHashCode(offHeapSemiUniqueDatedIndex.dataStorage, MultiEntry.getFirst(storage, cur & ~UPPER_BIT_MASK));
                }
                else
                {
                    hash = offHeapSemiUniqueDatedIndex.nonDatedHashStrategy.computeHashCode(offHeapSemiUniqueDatedIndex.dataStorage, cur);
                }
                offHeapSemiUniqueDatedIndex.transferNonDated(destArrayRef, newLength, cur, hash);
            }
        }

        public static int removeDatedByIdentity(OffHeapIntArrayStorage storage, int chainRef, int toRemove)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                if (cur == toRemove)
                {
                    removeAt(storage, chainRef, i);
                    return cur;
                }
            }
            return FREE;
        }

        public static int removeDatedByEquality(OffHeapIntArrayStorage storage, int chainRef, OffHeapDataStorage dataStorage, ExtractorBasedOffHeapHashStrategy datedHashStrategy, Object underlying)
        {
            int used = getSize(storage, chainRef);
            for (int i = 0; i < used; i++)
            {
                int cur = storage.getInt(chainRef, i + 1);
                if (datedHashStrategy.equals(dataStorage, cur, underlying))
                {
                    removeAt(storage, chainRef, i);
                    return cur;
                }
            }
            return FREE;
        }

        public static void removeDatedByFilter(OffHeapIntArrayStorage storage, int chainRef, OffHeapDataStorage dataStorage, Filter filter, FastList result,
                                               OffHeapSemiUniqueDatedIndex offHeapSemiUniqueDatedIndex)
        {
            for (int i = 0; i < getSize(storage, chainRef); i++)
            {
                int cur = getAt(storage, chainRef, i);
                if (filter.matches(dataStorage.getDataAsObject(cur)))
                {
                    removeAt(storage, chainRef, i);
                    i--;
                    result.add(dataStorage.getDataAsObject(cur));
                    offHeapSemiUniqueDatedIndex.removeNonDatedEntry(cur);
                }
            }
        }
    }

    /*
      Task that calculates duplicates on data segment assigned to it.
      Result of such tasks are merged to get consolidated list
     */
    private class DetectDuplicateTask extends CpuTask
    {
        private final ArrayBasedQueue queue;
        private List<Object> duplicates;

        public DetectDuplicateTask(ArrayBasedQueue queue)
        {
            this.queue = queue;
            this.duplicates = FastList.newList();
        }

        public List<Object> getDuplicates()
        {
            return duplicates;
        }

        @Override
        public void execute()
        {
            ArrayBasedQueue.Segment segment = queue.borrow(null);
            while (segment != null)
            {
                for (int i = segment.getStart(); i < segment.getEnd(); i++)
                {
                    int cur = getNonDatedTableAt(i);
                    delegateByType(cur, duplicates);
                }
                segment = queue.borrow(segment);
            }
        }
    }

    @Override
    public List<Object> collectMilestoningOverlaps()
    {
        if (asOfAttributes.length == 1 || asOfAttributes.length == 2)
        {
            return parallelCollectMilestoneOverlap();
        }
        else
        {
            throw new MithraException("Unsupported number of asOfAttributes");
        }
    }

    private List<Object> parallelCollectMilestoneOverlap()
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), this.getNonDatedTableLength(), 1);
        final ArrayBasedQueue queue = new ArrayBasedQueue(this.getNonDatedTableLength(), threadChunkSize.getChunkSize());
        int threads = threadChunkSize.getThreads();

        final List<DetectDuplicateTask> tasks = FastList.newList(threads);
        CooperativeCpuTaskFactory taskFactory = new CooperativeCpuTaskFactory(pool, threads)
        {
            @Override
            protected CpuTask createCpuTask()
            {
                DetectDuplicateTask detectDuplicateTask = new DetectDuplicateTask(queue);
                synchronized (tasks)
                {
                    tasks.add(detectDuplicateTask);
                }
                return detectDuplicateTask;
            }
        };
        taskFactory.startAndWorkUntilFinished();
        List<Object> mergedList = new FastList<Object>();
        for (int i = 0; i < tasks.size(); i++)
        {
            mergedList.addAll(tasks.get(i).getDuplicates());
        }
        return mergedList;
    }

    private void delegateByType(int cur, List<Object> duplicateData)
    {
        if (isMultiEntry(cur))
        {
            collectMilestoningOverlapsMulti(cur & ~UPPER_BIT_MASK, duplicateData);
        }
        else if (isChainedBucket(cur))
        {
            collectMilestoningOverlapsChained(cur & ~UPPER_BIT_MASK, duplicateData);
        }
    }

    private void collectMilestoningOverlapsChained(int chainRef, List<Object> duplicateData)
    {
        int size = ChainedBucket.getSize(storage, chainRef);
        for (int i = 0; i < size; i++)
        {
            int cur = ChainedBucket.getAt(storage, chainRef, i);
            if (isMultiEntry(cur))
            {
                collectMilestoningOverlapsMulti(cur & ~UPPER_BIT_MASK, duplicateData);
            }
        }
    }

    private Object checkForMilestoningOverlap(int multiEntryRef, int currentIndex, AsOfAttribute[] asOfAttributes, Object data)
    {
        int lstSize = MultiEntry.getSize(storage, multiEntryRef);
        for (int index = currentIndex + 1; index < lstSize; index++)
        {
            Object secondData = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, index));
            if (AsOfAttribute.isMilestoningOverlap(data, secondData, asOfAttributes))
            {
                return secondData;
            }
        }
        return null;
    }

    private void collectMilestoningOverlapsMulti(int multiEntryRef, List duplicateList)
    {
        int itrSize = MultiEntry.getSize(storage, multiEntryRef);
        for (int i = 0; i < itrSize; i++)
        {
            Object data = dataStorage.getDataAsObject(MultiEntry.getAt(storage, multiEntryRef, i));
            Object duplicateObject = checkForMilestoningOverlap(multiEntryRef, i, this.asOfAttributes, data);
            if (duplicateObject != null)
            {
                duplicateList.add(data);
                duplicateList.add(duplicateObject);
            }
        }
    }

    public void destroy()
    {
        this.storage.destroy();
        this.storage = null;
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {
        String msg = className + " index on ";
        for (Extractor e : onHeapDatedExtractors)
        {
            msg += ((Attribute) e).getAttributeName() + ", ";
        }
        this.storage.reportSpaceUsage(logger, msg);
    }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return this.storage.getAllocatedSize();
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return this.storage.getUsedSize();
    }
}