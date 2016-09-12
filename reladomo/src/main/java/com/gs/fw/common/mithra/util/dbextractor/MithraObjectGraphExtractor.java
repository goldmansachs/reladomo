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

package com.gs.fw.common.mithra.util.dbextractor;


import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import com.gs.collections.impl.block.factory.*;
import com.gs.collections.impl.list.mutable.*;
import com.gs.collections.impl.map.mutable.*;
import com.gs.collections.impl.map.strategy.mutable.*;
import com.gs.collections.impl.set.strategy.mutable.*;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.*;

/**
 * MithraObjectGraphExtractor takes one or more root operations and extracts the data returned by those operations, and
 * all related data to text files. These text files can be parsed by MithraTestDataParser and are commonly used for unit
 * testing Mithra based applications. A key feature of MithraObjectGraphExtractor is the ability to set an extract
 * threshold and to filter/ignore relationships to limit the amount of data extracted. A timeout can also be configured
 * to indicate relationships that take a long time to traverse - these can be ignored and additional root operations
 * added to extract this data.
 *
 * @see com.gs.fw.common.mithra.util.dbextractor.ExtractorConfig
 * @see com.gs.fw.common.mithra.util.dbextractor.MilestoneStrategy
 * @see com.gs.fw.common.mithra.util.dbextractor.OutputStrategy
 */
public class MithraObjectGraphExtractor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MithraObjectGraphExtractor.class);

    private final Map<RelatedFinder, Operation> filters = UnifiedMapWithHashingStrategy.newMap(HashingStrategies.identityStrategy(), 4);
    private final Set<RelatedFinder> ignored = UnifiedSetWithHashingStrategy.newSet(HashingStrategies.identityStrategy(), 4);
    private final List<Operation> operations = FastList.newList();
    private final ExtractorConfig extractorConfig;
    private final MilestoneStrategy milestoneStrategy;
    private final OutputStrategy outputStrategy;
    private final Queue<RelationshipTraverser> traverserQueue = new ArrayDeque<RelationshipTraverser>();
    private boolean warnIfMaxObjectThresholdExceeded = false;

    public MithraObjectGraphExtractor(ExtractorConfig extractorConfig, MilestoneStrategy milestoneStrategy, OutputStrategy outputStrategy)
    {
        this.extractorConfig = extractorConfig;
        this.milestoneStrategy = milestoneStrategy;
        this.outputStrategy = outputStrategy;
    }

    /**
     * Adds a root operation to be extracted. This operation should not include AsOfOperations as these will be added
     * by the MilestoneStrategy passed to the constructor.

     * @param operation - an operation without AsOfOperations
     * @see com.gs.fw.common.mithra.util.dbextractor.MilestoneStrategy
     */
    public void addRootOperation(Operation operation)
    {
        this.operations.add(operation);
    }

    /**
     * Add a filter to be applied to the result of a traversed relationship.
     *
     * @param relatedFinder - the relationship to apply the filter to
     * @param filter - the filter to apply
     */
    public void addRelationshipFilter(RelatedFinder relatedFinder, Operation filter)
    {
        Operation existing = this.filters.get(relatedFinder);
        this.filters.put(relatedFinder, existing == null ? filter : existing.or(filter));
    }

    /**
     * Add a relationship to be ignored during extraction.
     * @param relatedFinder - the relationship to be ignored
     */
    public void ignoreRelationship(RelatedFinder relatedFinder)
    {
        this.ignored.add(relatedFinder);
    }
    
    public <T extends MithraList> T evaluateDeepRelationship(MithraList leftList, AbstractRelatedFinder deepRelationship)
    {
        List<AbstractRelatedFinder> finderPath = FastList.<AbstractRelatedFinder>newList(8).with(deepRelationship);
        AbstractRelatedFinder parent = (AbstractRelatedFinder) deepRelationship.getParentDeepRelationshipAttribute();
        while (parent != null)
        {
            finderPath.add(parent);
            parent = (AbstractRelatedFinder) parent.getParentDeepRelationshipAttribute();
        }
        MithraList resultList = leftList;
        for (int i = finderPath.size() - 1; i >= 0; i--)
        {
            List<MithraList> resultLists = new RelationshipEvaluator(finderPath.get(i), null, resultList).evaluate();
            resultList = resultLists.get(resultLists.size() - 1);
        }
        return (T) resultList;
    }

    /**
     * Extract data and related data from the root operations added and write extracted data to text files as defined
     * by OutputStrategy.
     *
     * @see com.gs.fw.common.mithra.util.dbextractor.OutputStrategy
     */
    public void extract()
    {
        ExecutorService executor = Executors.newFixedThreadPool(this.extractorConfig.getThreadPoolSize());
        try
        {
            Map<Pair<RelatedFinder, Object>, List<MithraDataObject>> extract = extractData(executor);
            writeToFiles(executor, extract);
        }
        finally
        {
            executor.shutdown();
            try
            {
                executor.awaitTermination(extractorConfig.getTimeoutSeconds(), TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                //
            }
        }
    }

    private void writeToFiles(ExecutorService executor, Map<Pair<RelatedFinder, Object>, List<MithraDataObject>> extract)
    {
        UnifiedMap<File, UnifiedMap<RelatedFinder, List<MithraDataObject>>> dataByFile = UnifiedMap.newMap();
        for (Pair<RelatedFinder, Object> key : extract.keySet())
        {
            File outputFile = this.outputStrategy.getOutputFile(key.getOne(), key.getTwo());
            if (outputFile != null)
            {
                dataByFile.getIfAbsentPut(outputFile, UnifiedMap.<RelatedFinder, List<MithraDataObject>>newMap()).put(key.getOne(), extract.get(key));
            }
        }
        for (File file : dataByFile.keySet())
        {
            executor.submit(new FileWriterTask(file, dataByFile.get(file)));
        }
    }

    private Map<Pair<RelatedFinder, Object>, List<MithraDataObject>> extractData(ExecutorService executor)
    {
        ExtractResult result = new ExtractResult();
        for (Operation operation : operations)
        {
            RelatedFinder finder = operation.getResultObjectPortal().getFinder();
            MithraList mithraList = finder.findMany(this.milestoneStrategy.addAsOfOperations(operation));
            if (mithraList.notEmpty())
            {
                FullUniqueIndex pkIndex = createDatelessIndex(mithraList);
                ExtractResult operationResult = new ExtractResult();
                UnifiedSetWithHashingStrategy<RelatedFinder> toManyRelationships = UnifiedSetWithHashingStrategy.newSet(HashingStrategies.identityStrategy());
                this.traverserQueue.add(new RelationshipTraverser(mithraList, pkIndex, operationResult, toManyRelationships, executor, finder.getMithraObjectPortal().getBusinessClassName()));
                while (!this.traverserQueue.isEmpty())
                {
                    this.traverserQueue.poll().traverse();
                }
                result.merge(operationResult);
            }
        }
        return result.getDataObjectsByFinderAndSource();
    }

    private FullUniqueIndex createDatelessIndex(MithraList mithraList)
    {
        MithraObject mithraObject = (MithraObject) mithraList.get(0);
        RelatedFinder finder = mithraObject.zGetCurrentData().zGetMithraObjectPortal().getFinder();
        FullUniqueIndex pkIndex = new FullUniqueIndex(finder.getPrimaryKeyAttributes(), mithraList.size());
        pkIndex.addAll(mithraList);
        return pkIndex;
    }

    private static String getPath(String path, AbstractRelatedFinder relatedFinder)
    {
        return path + '.' + relatedFinder.getRelationshipPath() + (relatedFinder.isToOne() ? "()" : "()*");
    }

    public void setWarnIfMaxObjectThresholdExceeded(boolean warnIfMaxObjectThresholdExceeded)
    {
        this.warnIfMaxObjectThresholdExceeded = warnIfMaxObjectThresholdExceeded;
    }

    private class RelationshipEvaluatorTask implements Callable<List<MithraList>>
    {
        private final AbstractRelatedFinder relatedFinder;
        private final Operation relatedFilter;
        private final MithraList mithraList;
        private final String path;

        public RelationshipEvaluatorTask(AbstractRelatedFinder relatedFinder, Operation relatedFilter, MithraList mithraList, String path)
        {
            this.relatedFinder = relatedFinder;
            this.relatedFilter = relatedFilter;
            this.mithraList = mithraList;
            this.path = path;
        }

        @Override
        public List<MithraList> call() throws Exception
        {
            try
            {
                LOGGER.debug("Traversing {}", path);
                return new RelationshipEvaluator(relatedFinder, relatedFilter, mithraList).evaluate();
            }
            catch (MithraConfigurationException e)
            {
                LOGGER.info("Object not configured {} - {}", path, e.getMessage());
                return Collections.emptyList();
            }
            catch (MithraBusinessException e)
            {
                LOGGER.warn("Could not traverse " + path, e);
                return Collections.emptyList();
            }
        }
    }

    private class RelationshipEvaluator
    {
        private final AbstractRelatedFinder relatedFinder;
        private final Operation relatedFilter;
        private final MithraList mithraList;

        public RelationshipEvaluator(AbstractRelatedFinder relatedFinder, Operation relatedFilter, MithraList mithraList)
        {
            this.relatedFinder = relatedFinder;
            this.relatedFilter = relatedFilter;
            this.mithraList = mithraList;
        }

        public List<MithraList> evaluate()
        {
            List<MithraList> resultLists = FastList.newList(2);
            List<Mapper> unChainedMappers = this.relatedFinder.zGetMapper().getUnChainedMappers();
            MithraList leftList = this.mithraList;
            for (Mapper mapper : unChainedMappers)
            {
                Operation baseOperation = mapper.getFromPortal().getFinder().all();
                if (this.relatedFilter != null && this.relatedFilter.getResultObjectPortal().equals(baseOperation.getResultObjectPortal()))
                {
                    baseOperation = baseOperation.and(this.relatedFilter);
                }
                leftList = new MapperEvaluator(baseOperation).evaluate(leftList, mapper, resultLists);
            }
            return resultLists;
        }
    }

    private class MapperEvaluator
    {
        private final Operation baseOperation;

        private MapperEvaluator(Operation baseOperation)
        {
            this.baseOperation = baseOperation;
        }

        public MithraList evaluate(MithraList leftList, Mapper originalMapper, List<MithraList> resultLists)
        {
            return evaluateMapper(originalMapper, leftList, resultLists, this.baseOperation);
        }

        private MithraList evaluateMapper(Mapper mapper, MithraList leftList, List<MithraList> resultLists, Operation operation)
        {
            Operation rightFilter = null;
            if (mapper instanceof FilteredMapper)
            {
                FilteredMapper filteredMapper = FilteredMapper.class.cast(mapper);
                rightFilter = filteredMapper.getRightFilters();
                Operation leftFilter = filteredMapper.getLeftFilters();
                if (leftFilter != null)
                {
                    LOGGER.debug("Don't know what to do with left filter: {}", leftFilter);
                }
                mapper = filteredMapper.getUnderlyingMapper();
            }
            if (mapper instanceof EqualityMapper)
            {
                EqualityMapper equalityMapper = EqualityMapper.class.cast(mapper);
                operation = operation.and(equalityMapper.getRight().in(leftList, equalityMapper.getLeft()));
            }
            else if (mapper instanceof MultiEqualityMapper)
            {
                operation = addNonAsOfOperation(operation, leftList, (MultiEqualityMapper) mapper);
            }
            else
            {
                LOGGER.warn("Unsupported relationship mapper: {}", mapper.getClass().getSimpleName());
                return null;
            }
            if (rightFilter != null)
            {
                operation = operation.and(rightFilter);
            }
            Operation finderFilter = filters.get(operation.getResultObjectPortal().getFinder());
            if (finderFilter != null)
            {
                operation = operation.and(finderFilter);
            }
            operation = milestoneStrategy.addAsOfOperations(operation);
            MithraList result = operation.getResultObjectPortal().getFinder().findMany(operation);
            result.size();
            if (rightFilter instanceof MappedOperation)
            {
                MappedOperation mappedOperation = (MappedOperation) rightFilter;
                evaluateMapper(mappedOperation.getMapper(), result, resultLists, mappedOperation.getUnderlyingOperation());
            }
            resultLists.add(result);
            return result;
        }

        private Operation addNonAsOfOperation(Operation operation, MithraList mithraList, MultiEqualityMapper mapper)
        {
            InternalList equalityMappers = mapper.getEqualityMappers();
            List<Attribute> nonAsOfLeftAttributes = FastList.newList(equalityMappers.size());
            List<Attribute> nonAsOfRightAttributes = FastList.newList(equalityMappers.size());
            for (int i = 0; i < equalityMappers.size(); i++)
            {
                EqualityMapper equalityMapper = (EqualityMapper) equalityMappers.get(i);
                Attribute left = equalityMapper.getLeft();
                Attribute right = equalityMapper.getRight();
                boolean containsAsOfAttributes = left.isAsOfAttribute() || right.isAsOfAttribute();
                if (!containsAsOfAttributes)
                {
                    nonAsOfLeftAttributes.add(left);
                    nonAsOfRightAttributes.add(right);
                }
            }
            Attribute[] nonAsOfAttributesArray = nonAsOfRightAttributes.toArray(new Attribute[nonAsOfRightAttributes.size()]);
            if (nonAsOfAttributesArray.length > 1)
            {
                TupleAttribute tupleAttribute = new TupleAttributeImpl(nonAsOfAttributesArray);
                Extractor[] extractors = nonAsOfLeftAttributes.toArray(new Extractor[nonAsOfLeftAttributes.size()]);
                MithraArrayTupleTupleSet tupleSet = new MithraArrayTupleTupleSet(extractors, mithraList, true);
                return tupleSet.size() == 0 ? new None(nonAsOfAttributesArray[0]) : operation.and(tupleAttribute.inIgnoreNulls(mithraList, extractors));
            }
            if (nonAsOfAttributesArray.length == 1)
            {
                return operation.and(nonAsOfAttributesArray[0].in(mithraList, nonAsOfLeftAttributes.get(0)));
            }
            return operation;
        }
    }

    private class FileWriterTask implements Runnable
    {
        private final File outputFile;
        private final UnifiedMap<RelatedFinder, List<MithraDataObject>> dataByFinder;

        private FileWriterTask(File outputFile, UnifiedMap<RelatedFinder, List<MithraDataObject>> dataByFinder)
        {
            this.outputFile = outputFile;
            this.dataByFinder = dataByFinder;
        }

        @Override
        public void run()
        {
            LOGGER.info("Writing data to " + this.outputFile);
            this.outputFile.getParentFile().mkdirs();
            try
            {
                new DbExtractor(
                        this.outputFile.getPath(),
                        outputStrategy.overwriteOutputFile(),
                        outputStrategy.getOutputFileHeader()).addDataByFinder(this.dataByFinder);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private class RelationshipTraverser
    {
        private final MithraList leftList;
        private final FullUniqueIndex leftListDatelessIndex;
        private final ExtractResult extractResult;
        private final Set<RelatedFinder> toManyRelationships;
        private final ExecutorService executor;
        private final String currentPath;

        public RelationshipTraverser(MithraList leftList, FullUniqueIndex leftListDatelessIndex, ExtractResult extractResult, Set<RelatedFinder> toManyRelationships, ExecutorService executor, String currentPath)
        {
            this.leftList = leftList;
            this.leftListDatelessIndex = leftListDatelessIndex;
            this.extractResult = extractResult;
            this.toManyRelationships = toManyRelationships;
            this.executor = executor;
            this.currentPath = currentPath;
        }

        public void traverse()
        {
            RelatedFinder finder = MithraObject.class.cast(this.leftList.get(0)).zGetCurrentData().zGetMithraObjectPortal().getFinder();
            MithraList notTraversed = finder.constructEmptyList();
            extractResult.addMithraObjects(leftList, notTraversed);
            if (notTraversed.notEmpty())
            {
                List<RelatedFinder> relationships = this.getRelationships(finder, toManyRelationships);
                List<Pair<AbstractRelatedFinder, Future<List<MithraList>>>> results = FastList.newList(relationships.size());
                for (RelatedFinder rel : relationships)
                {
                    AbstractRelatedFinder relatedFinder = AbstractRelatedFinder.class.cast(rel);
                    Operation relatedFilter = filters.get(relatedFinder);
                    Future<List<MithraList>> relatedList = executor.submit(new RelationshipEvaluatorTask(relatedFinder, relatedFilter, notTraversed, getPath(currentPath, relatedFinder)));
                    results.add(Pair.of(relatedFinder, relatedList));
                }
                for (Pair<AbstractRelatedFinder, Future<List<MithraList>>> finderAndResult : results)
                {
                    AbstractRelatedFinder relatedFinder = finderAndResult.getOne();
                    String nextPath = getPath(currentPath, relatedFinder);
                    try
                    {
                        List<MithraList> resultAndJoinObjects = finderAndResult.getTwo().get(extractorConfig.getTimeoutSeconds(), TimeUnit.SECONDS);
                        for (MithraList resultOrJoinObjects : resultAndJoinObjects)
                        {
                            if (resultOrJoinObjects.notEmpty())
                            {
                                FullUniqueIndex resultOrJoinDatelessIndex = createDatelessIndex(resultOrJoinObjects);
                                if (resultOrJoinDatelessIndex != null && resultOrJoinDatelessIndex.size() / leftListDatelessIndex.size() > extractorConfig.getExtractThresholdRatio())
                                {
                                    String message = String.format("Too many objects (%s/%d) extracted via %s", resultOrJoinObjects.getClass().getSimpleName(), resultOrJoinDatelessIndex.size(), nextPath);
                                    if (warnIfMaxObjectThresholdExceeded)
                                    {
                                        LOGGER.warn(message);
                                    }
                                    else
                                    {
                                        LOGGER.error(message);
                                        executor.shutdownNow();
                                        throw new IllegalStateException(message);
                                    }
                                }
                                else
                                {
                                    Set<RelatedFinder> toManyRelationshipsCopy = UnifiedSetWithHashingStrategy.newSet(HashingStrategies.identityStrategy(), toManyRelationships);
                                    if (!relatedFinder.isToOne())
                                    {
                                        toManyRelationshipsCopy.add(relatedFinder);
                                    }
                                    if (resultOrJoinObjects.notEmpty())
                                    {
                                        traverserQueue.add(new RelationshipTraverser(resultOrJoinObjects, resultOrJoinDatelessIndex, extractResult, toManyRelationshipsCopy, executor, nextPath + "[" + resultOrJoinObjects.size() + "]"));
                                    }
                                }
                            }
                        }
                    }
                    catch (InterruptedException e)
                    {
                        //
                    }
                    catch (ExecutionException e)
                    {
                        LOGGER.error("Error traversing " + nextPath, e.getCause());
                        executor.shutdownNow();
                        throw new RuntimeException(e);
                    }
                    catch (TimeoutException e)
                    {
                        executor.shutdownNow();
                        LOGGER.error("Timed out traversing " + nextPath);
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private List<RelatedFinder> getRelationships(RelatedFinder relatedFinder, Set<RelatedFinder> toManyRelationships)
        {
            List allRelationships = relatedFinder.getRelationshipFinders();
            List<RelatedFinder> interestingRelationships = FastList.newList(allRelationships.size());
            for (Object object : allRelationships)
            {
                AbstractRelatedFinder relationship = AbstractRelatedFinder.class.cast(object);
                boolean notIgnored = !ignored.contains(relationship);
                boolean isToOneOrFirstToMany = relationship.isToOne() || toManyRelationships.isEmpty();
                if (notIgnored && isToOneOrFirstToMany)
                {
                    interestingRelationships.add(relationship);
                }
            }
            return interestingRelationships;
        }
    }
}
