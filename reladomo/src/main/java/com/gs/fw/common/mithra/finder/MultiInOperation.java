
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

package com.gs.fw.common.mithra.finder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfTimestampEqualityMapper;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.*;

public class MultiInOperation implements Operation, SqlParameterSetter
{
    private Attribute[] attributes;
    private MithraTupleSet mithraTupleSet;
    private transient IndexReference bestIndexRef;
    private transient int hashCode;
    private transient List attributeList;

    private transient EqualityOperation equalityOperation; // only used for full cache evaluation

    public MultiInOperation(Attribute[] attributes, List dataHolders, Extractor[] extractors)
    {
        this.attributes = attributes;
        this.mithraTupleSet = new MithraDataHolderTupleSet(dataHolders, extractors);
        this.mithraTupleSet.markAsReadOnly();
    }

    public MultiInOperation(Attribute[] attributes, MithraTupleSet mithraTupleSet)
    {
        this.attributes = attributes;
        mithraTupleSet.markAsReadOnly();
        this.mithraTupleSet = mithraTupleSet;
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return null;
    }

    public Operation setExtraOperationAndReturnLeftOver(Operation extraOperationOnResult)
    {
        if (extraOperationOnResult != null)
        {
            equalityOperation = extraOperationOnResult.zExtractEqualityOperations();
            if (equalityOperation == extraOperationOnResult)
            {
                return null;
            }
        }
        return extraOperationOnResult; // todo: optimize this
    }

    protected Cache getCache()
    {
        return this.getResultObjectPortal().getCache();
    }

    protected void findBestIndex()
    {
        createAttributeList();
        if (this.bestIndexRef == null || !this.bestIndexRef.isForCache(this.getCache()))
        {
            this.bestIndexRef = this.getCache().getBestIndexReference(this.attributeList);
        }
    }

    private void createAttributeList()
    {
        if (this.attributeList == null)
        {
            if (this.equalityOperation == null)
            {
                this.attributeList = Arrays.asList(this.attributes);
            }
            else
            {
                this.attributeList = FastList.newList(this.attributes.length + this.equalityOperation.getEqualityOpCount());
                for(int i=0;i<this.attributes.length;i++)
                {
                    this.attributeList.add(this.attributes[i]);
                }
                this.equalityOperation.addEqAttributes(this.attributeList);
            }
        }
    }

    public boolean usesUniqueIndex()
    {
        this.findBestIndex();
        Cache cache = this.getCache();
        return this.bestIndexRef.isValid()  && cache.isUnique(bestIndexRef.indexReference);
    }

    public boolean usesImmutableUniqueIndex()
    {
        this.findBestIndex();
        Cache cache = this.getCache();
        return this.bestIndexRef.isValid() && cache.isUniqueAndImmutable(bestIndexRef.indexReference);
    }

    public boolean usesNonUniqueIndex()
    {
        this.findBestIndex();
        Cache cache = this.getCache();
        return this.bestIndexRef.isValid() && bestIndexRef.indexReference != IndexReference.AS_OF_PROXY_INDEX_ID && !cache.isUnique(bestIndexRef.indexReference);
    }

    public int zEstimateReturnSize()
    {
        this.findBestIndex();
        Cache cache = this.getCache();
        if (this.bestIndexRef.isValid())
        {
            return cache.getAverageReturnSize(this.bestIndexRef.indexReference, this.mithraTupleSet.size());
        }
        return cache.estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        this.findBestIndex();
        Cache cache = this.getCache();
        if (this.bestIndexRef.isValid())
        {
            return cache.getMaxReturnSize(this.bestIndexRef.indexReference, this.mithraTupleSet.size());
        }
        return cache.estimateQuerySize();
    }

    @Override
    public boolean zIsEstimatable()
    {
        MithraObjectPortal portal = this.getResultObjectPortal();
        return portal.isFullyCached() && !portal.isForTempObject();
    }

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        if (this.equalityOperation != null)
        {
            this.equalityOperation.zRegisterEqualitiesAndAtomicOperations(transitivePropagator);
        }
    }

    public boolean zHazTriangleJoins()
    {
        return false;
    }

    public void zToString(ToStringContext toStringContext)
    {
        if (this.equalityOperation != null)
        {
            this.equalityOperation.zToString(toStringContext);
            toStringContext.append("and");
        }
        toStringContext.append("(");
        for(int i=0;i<attributes.length;i++)
        {
            if (i > 0) toStringContext.append(", ");
            attributes[i].zAppendToString(toStringContext);
        }
        toStringContext.append(") in (<tuple set with").append(mithraTupleSet.size()).append("elements>)");
    }

    public void addDependentPortalsToSet(Set set)
    {
        if (this.equalityOperation != null)
        {
            this.equalityOperation.addDependentPortalsToSet(set);
        }

        set.add(getFirstLeftAttribute().getOwnerPortal());
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        if (this.equalityOperation != null)
        {
            this.equalityOperation.addDepenedentAttributesToSet(set);
        }
        for (int i = 0; i < attributes.length; i++)
        {
            set.add(attributes[i]);
        }
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return false;
    }

    protected int getAttributeIndex(Attribute attribute)
    {
        for (int i = 0; i < attributes.length; i++)
        {
            if (attribute.equals(attributes[i]))
            {
                return i;
            }
        }
        return -1;
    }

    public List applyOperationToFullCache()
    {
        return this.applyOperation(false);
    }

    public List applyOperationToPartialCache()
    {
        if (this.usesUniqueIndex())
        {
            return this.applyOperation(true);
        }
        return null;
    }

    protected List applyOperation(boolean returnNullIfNotFoundInIndex)
    {
        this.findBestIndex();
        Cache cache = this.getCache();
        Extractor[] extractors = mithraTupleSet.getExtractors();
        if (this.bestIndexRef.isValid())
        {
            Attribute[] bestAttributes = cache.getIndexAttributes(this.bestIndexRef.indexReference);
            Extractor[] sortedRightIndexAttributes = new Extractor[bestAttributes.length];
            int extractorPos = 0;

            int usedTupleAttributes = 0;
            int usedEqualityAttributes = 0;
            for (int i = 0; i < bestAttributes.length; i++)
            {
                int attributeIndex = this.getAttributeIndex(bestAttributes[i]);
                if (attributeIndex >= 0)
                {
                    sortedRightIndexAttributes[extractorPos] = extractors[attributeIndex];
                    extractorPos++;
                    usedTupleAttributes++;
                }
                else
                {
                    sortedRightIndexAttributes[extractorPos] = this.equalityOperation.getParameterExtractorFor(bestAttributes[i]);
                    extractorPos++;
                    usedEqualityAttributes++;
                }
            }

            List result;
            if (usedTupleAttributes < this.attributes.length)
            {
                Extractor[] unusedLeft = new Extractor[attributes.length - usedTupleAttributes];
                Extractor[] unusedRight = new Extractor[attributes.length - usedTupleAttributes];
                int count = 0;
                for(int i=0;i<attributes.length;i++)
                {
                    boolean add = true;
                    for(int k=0;k<bestAttributes.length;k++)
                    {
                        if (attributes[i].equals(bestAttributes[k]))
                        {
                            add = false;
                            break;
                        }
                    }
                    if (add)
                    {
                        unusedLeft[count] = attributes[i];
                        unusedRight[count] = extractors[i];
                        count++;
                    }
                }
                result = getFromCacheAndFilter(returnNullIfNotFoundInIndex, cache, sortedRightIndexAttributes, unusedLeft, unusedRight);
            }
            else
            {
                result = cache.getMany(this.bestIndexRef.indexReference, mithraTupleSet, sortedRightIndexAttributes, returnNullIfNotFoundInIndex);
            }
            if (result != null && this.equalityOperation != null && usedEqualityAttributes < this.equalityOperation.getEqualityOpCount())
            {
                result = this.equalityOperation.applyOperation(result); // todo optimize this by just applying the left over
            }
            return result;
        }
        else
        {
            return this.applyOperation(cache.getAll());
        }
    }

    private List getFromCacheAndFilter(boolean returnNullIfNotFoundInIndex, Cache cache,
            Extractor[] sortedRightIndexAttributes, Extractor[] unusedLeft, Extractor[] unusedRight)
    {
        if (mithraTupleSet.size() < 6 || unusedLeft == null)
        {
            return getFromCacheAndFilterSmall(returnNullIfNotFoundInIndex, cache, sortedRightIndexAttributes, unusedLeft, unusedRight);
        }
        else
        {
            return getFromCacheAndFilterLarge(returnNullIfNotFoundInIndex, cache, sortedRightIndexAttributes, unusedLeft, unusedRight);
        }
    }

    private List getFromCacheAndFilterSmall(final boolean returnNullIfNotFoundInIndex, final Cache cache, final Extractor[] sortedRightIndexAttributes,
            final Extractor[] unusedLeft, final Extractor[] unusedRight)
    {
        final Extractor[] extractors = mithraTupleSet.getExtractors();
        final FullUniqueIndex identitySet = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY, cache.getAverageReturnSize(this.bestIndexRef.indexReference, this.mithraTupleSet.size()));
        boolean notFound = mithraTupleSet.doUntil(new DoUntilProcedure()
        {
            public boolean execute(Object dataHolder)
            {
                if (!dataHasNull(dataHolder, extractors))
                {
                    List list = cache.get(bestIndexRef.indexReference, dataHolder, sortedRightIndexAttributes, true);
                    if (returnNullIfNotFoundInIndex && list.size() == 0)
                    {
                        return true;
                    }
                    for (int j = 0; j < list.size(); j++)
                    {
                        Object foundObject = list.get(j);
                        if (unusedLeft != null)
                        {
                            foundObject = matchesWithUnused(dataHolder, foundObject, unusedLeft, unusedRight);
                        }
                        if (foundObject != null)
                        {
                            identitySet.put(foundObject);
                        }
                    }
                }
                return false;
            }
        });
        if (notFound) return null;
        return createListFromSet(identitySet);
    }

    private List getFromCacheAndFilterLarge(final boolean returnNullIfNotFoundInIndex, final Cache cache, final Extractor[] sortedRightAttributes,
            final Extractor[] unusedLeft, final Extractor[] unusedRight)
    {
        final Extractor[] extractors = mithraTupleSet.getExtractors();
        final NonUniqueIndex nui = new NonUniqueIndex(null, unusedRight, sortedRightAttributes);
        mithraTupleSet.doUntil(new DoUntilProcedure()
        {
            public boolean execute(Object dataHolder)
            {
                if (!dataHasNull(dataHolder, extractors))
                {
                    nui.put(dataHolder);
                }
                return false;
            }
        });

        int avgHits =  cache.getAverageReturnSize(this.bestIndexRef.indexReference, nui.size());
        int nominalSize = Math.min(1000, avgHits);
        final MithraFastList result = new MithraFastList(nominalSize);
        boolean aborted = nui.nonUniqueDoUntil(new DoUntilProcedure()
        {
            public boolean execute(Object o)
            {
                if (o instanceof FullUniqueIndex)
                {
                    FullUniqueIndex uniqueIndex = (FullUniqueIndex) o;
                    Object dataHolder = uniqueIndex.getFirst();
                    List list = cache.get(bestIndexRef.indexReference, dataHolder, sortedRightAttributes, true);
                    if (returnNullIfNotFoundInIndex && list.size() == 0)
                    {
                        return true;
                    }
                    for (int j = 0; j < list.size(); j++)
                    {
                        Object foundObject = list.get(j);
                        if (uniqueIndex.get(foundObject, unusedLeft) != null)
                        {
                            result.add(foundObject);
                        }
                    }
                }
                else
                {
                    List list = cache.get(bestIndexRef.indexReference, o, sortedRightAttributes, true);
                    if (returnNullIfNotFoundInIndex && list.size() == 0)
                    {
                        return true;
                    }
                    for (int j = 0; j < list.size(); j++)
                    {
                        Object foundObject = matchesWithUnused(o, list.get(j), unusedLeft, unusedRight);
                        if (foundObject != null)
                        {
                            result.add(foundObject);
                        }
                    }
                }
                return false;
            }
        });
        if (aborted) return null;
        return result;
    }

    private boolean dataHasNull(Object dataHolder, Extractor[] extractors)
    {
        if (mithraTupleSet.hasNulls())
        {
            for (int j = 0; j < extractors.length; j++)
            {
                if (extractors[j].isAttributeNull(dataHolder))
                {
                    // in a relationship, null does not match anything, including another null
                    return true;
                }
            }
        }
        return false;
    }

    private Object matchesWithUnused(Object o, Object foundObject, Extractor[] unusedLeft, Extractor[] unusedRight)
    {
        for (int k = 0; k < unusedLeft.length && foundObject != null; k++)
        {
            Extractor extractor = unusedRight[k];
            if (!extractor.valueEquals(o, foundObject, unusedLeft[k]))
            {
                foundObject = null;
            }
        }
        return foundObject;
    }

    private List createListFromSet(FullUniqueIndex identitySet)
    {
        final MithraFastList arrayList = new MithraFastList(identitySet.size());
        identitySet.forAll(new DoUntilProcedure()
        {
            public boolean execute(Object object)
            {
                arrayList.add(object);
                return false;
            }
        });
        return arrayList;
    }

    public List applyOperation(List list)
    {
        if (this.equalityOperation != null)
        {
            list = this.equalityOperation.applyOperation(list);
        }
        MithraFastList result = new MithraFastList(list.size());
        for(int i=0;i<list.size();i++)
        {
            Object valueHolder = list.get(i);
            if (mithraTupleSet.contains(valueHolder, attributes))
            {
                result.add(valueHolder);
            }
        }
        return result;
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        return new AndOperation(this, op);
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        return OrOperation.or(this, op);
    }

    /*
    returns the combined and operation. Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return null;
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return getFirstLeftAttribute().getOwnerPortal();
    }

    public String zGetResultClassName()
    {
        return getFirstLeftAttribute().zGetTopOwnerClassName();
    }

    public boolean zIsNone()
    {
        return false;
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        for(Attribute a: attributes) result.add(a);
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        for(int i=0;i<this.attributes.length;i++)
        {
            if (attributes[i].equals(asOfAttribute))
            {
                Object o = this.mithraTupleSet.getFirstDataHolder();
                final TimestampExtractor timestampExtractor = (TimestampExtractor) this.mithraTupleSet.getExtractors()[i];
                final Timestamp value = timestampExtractor.timestampValueOf(o);
                this.mithraTupleSet.doUntil(new DoUntilProcedure()
                {
                    public boolean execute(Object object)
                    {
                        final Timestamp other = timestampExtractor.timestampValueOf(object);
                        if (other != null && !other.equals(value))
                        {
                            throw new RuntimeException("only one as of attribute value is supported");
                        }
                        return false;
                    }
                });
                return asOfAttribute.eq(value);
            }
        }
        return null;
    }

    private Attribute getFirstLeftAttribute()
    {
        return this.attributes[0];
    }

    private boolean hasSourceAttribute()
    {
        for(Attribute a: attributes)
        {
            if (a.isSourceAttribute())
            {
                return true;
            }
        }
        return false;
    }

    public void generateSql(SqlQuery query)
    {
        if (this.equalityOperation != null)
        {
            throw new RuntimeException("should not get here");
        }
        if (hasSourceAttribute())
        {
            //todo: implement SourceOperation
            throw new RuntimeException("multi-in with source attribute not yet implemented");
        }
        if (isSmallAndHasNoAsOfAttributes())
        {
            query.appendWhereClause("(");
            for(int k=0;k<mithraTupleSet.size();k++)
            {
                if (k > 0) query.appendWhereClause(" or ");
                query.appendWhereClause("(");
                for(int i=0;i<this.attributes.length;i++)
                {
                    if (i > 0) query.appendWhereClause(" and ");
                    query.appendWhereClause(attributes[i].getFullyQualifiedLeftHandExpression(query));
                    query.appendWhereClause(" = ?");
                }
                query.appendWhereClause(")");
            }
            query.appendWhereClause(")");
            query.addSqlParameterSetter(this);
        }
        else
        {
            TupleTempContext tempContext = query.getMultiInTempContext(this);
            query.addTempContext(tempContext);
            Object source = null;
            if (this.attributes[0].getSourceAttribute() != null)
            {
                source = query.getSourceAttributeValue(query.getCurrentMapperList(), query.getCurrentSourceNumber());
            }
            tempContext.insert(this.attributes[0].getOwnerPortal(), 100, source, mithraTupleSet.getTupleList(), query.isParallel());
            Mapper mapper = createMapper(tempContext);
            query.registerTempTupleMapper(mapper);
            mapper.generateSql(query);
            if (source != null)
            {
                Attribute sourceAttribute = this.attributes[0].getSourceAttribute();
                query.setSourceOperation((SourceOperation) tempContext.getSourceOperation(source, sourceAttribute));
            }
            mapper.popMappers(query);
        }
    }

    private Mapper createMapper(TupleTempContext tempContext)
    {
        InternalList equalityMappers = new InternalList(this.attributes.length);
        for(int i=0;i<this.attributes.length;i++)
        {
            SingleColumnAttribute tempAttribute = tempContext.getPersistentTupleAttributes()[i];
            if (attributes[i] instanceof AsOfAttribute)
            {
                equalityMappers.add(new AsOfTimestampEqualityMapper((AsOfAttribute) attributes[i], (TimestampAttribute) tempAttribute));
            }
            else
            {
                equalityMappers.add(new EqualityMapper(attributes[i], (Attribute) tempAttribute));
            }
        }
        return new MultiEqualityMapper(equalityMappers);
    }

    public TupleTempContext createTempContext()
    {
        //todo: source attribute needs to be figured out correctly here
        return new TupleTempContext(this.attributes, this.mithraTupleSet.getMaxLengths(), true);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        if (isSmallAndHasNoAsOfAttributes())
        {
            TupleTempContext tempContext = createTempContext();
            SingleColumnAttribute[] tupleAttributes = tempContext.getPersistentTupleAttributes();
            List tuples = mithraTupleSet.getTupleList();
            for(int i=0;i<tuples.size();i++)
            {
                for(int k=0;k<tupleAttributes.length;k++)
                {
                    tupleAttributes[k].setSqlParameters(pstmt, tuples.get(i), startIndex++, query.getTimeZone(), query.getDatabaseType());
                }
            }
            tempContext.destroy();
            return tuples.size() * tupleAttributes.length;
        }
        //no parameters to set otherwise
        return 0;
    }

    public int getClauseCount(SqlQuery query)
    {
        if (isSmallAndHasNoAsOfAttributes())
        {
            return mithraTupleSet.size() * mithraTupleSet.getExtractors().length;
        }
        return mithraTupleSet.getExtractors().length;
    }

    private boolean isSmallAndHasNoAsOfAttributes()
    {
        return mithraTupleSet.size() * mithraTupleSet.getExtractors().length < 10 && !hasAsOfAttributes();
    }

    private boolean hasAsOfAttributes()
    {
        for (int i = 0; i < attributes.length; i++)
        {
            Attribute attribute = attributes[i];
            if (attribute instanceof AsOfAttribute) return true;
        }
        return false;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        if (this.equalityOperation != null)
        {
            this.equalityOperation.registerAsOfAttributesAndOperations(checker);
        }
        checker.registerAsOfAttributes(this.getFirstLeftAttribute().getAsOfAttributes());
        TupleTempContext tempContext = checker.getOrCreateMultiInTempContext(this);
        for (int i = 0; i < attributes.length; i++)
        {
            Attribute attribute = attributes[i];
            if (attribute instanceof AsOfAttribute)
            {
                AsOfAttribute asOfAttribute = (AsOfAttribute) attribute;
                ObjectWithMapperStack leftAsOfAttributeWithStack = checker.constructWithMapperStack(asOfAttribute);
                Mapper mapper = this.createMapper(tempContext);
                mapper.pushMappers(checker);
                checker.setAsOfOperation(leftAsOfAttributeWithStack,
                        new AsOfTimestampEqualityMapper(asOfAttribute, (TimestampAttribute) tempContext.getPersistentTupleAttributes()[i]));
                mapper.popMappers(checker);
            }
        }
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            return MultiEqualityOperation.createEqOperation(asOfEqOperations).and(this);
        }
        return null;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return transitivePropagator.constructAnd(insertPosition, this, toInsert);
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return this;
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        if (this.equalityOperation != null)
        {
            this.equalityOperation.registerOperation(extractor, registerEquality);
        }
        // todo: implement SourceOperation
        // todo: if we have a source attribute, we have to register here
    }

    public boolean zHasAsOfOperation()
    {
        if (this.equalityOperation != null)
        {
            return this.equalityOperation.zHasAsOfOperation();
        }
        return false;
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        return null;
    }

    public Operation zFindEquality(TimestampAttribute attr)
    {
        if (this.equalityOperation != null)
        {
            return this.equalityOperation.zFindEquality(attr);
        }
        return null;
    }

    public int hashCode()
    {
        if (hashCode == 0)
        {
            int hash = mithraTupleSet.hashCode();
            for(int i=0;i<attributes.length;i++)
            {
                hash = HashUtil.combineHashes(hash, attributes[i].hashCode());
            }
            if (this.equalityOperation != null)
            {
                hash = HashUtil.combineHashes(hash, this.equalityOperation.hashCode());
            }
            hashCode = hash;
        }
        return hashCode;
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof MultiInOperation)
        {
            MultiInOperation other = (MultiInOperation) obj;
            if (this.equalityOperation == other.equalityOperation || (this.equalityOperation != null && this.equalityOperation.equals(other.equalityOperation)))
            {
                return Arrays.equals(this.attributes, other.attributes) && this.mithraTupleSet.equals(other.mithraTupleSet);
            }
        }
        return false;
    }

    public Boolean matches(Object o)
    {
        if (this.equalityOperation != null && !this.equalityOperation.matches(o))
        {
            return false;
        }
        return mithraTupleSet.contains(o, attributes);
    }

    public boolean zPrefersBulkMatching()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return ToStringContext.createAndToString(this);
    }

    @Override
    public boolean zContainsMappedOperation()
    {
        return false;
    }

    @Override
    public boolean zHasParallelApply()
    {
        return false;
    }
}
