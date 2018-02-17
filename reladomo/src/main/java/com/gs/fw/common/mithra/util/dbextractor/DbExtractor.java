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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.util.dbextractor;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.NonUniqueIndex;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import com.gs.fw.common.mithra.util.fileparser.AbstractMithraDataFileParser;
import com.gs.fw.common.mithra.util.fileparser.AttributeReaderState;
import com.gs.fw.common.mithra.util.fileparser.BeginningOfLineState;
import com.gs.fw.common.mithra.util.fileparser.BinaryCompressor;
import com.gs.fw.common.mithra.util.fileparser.ClassReaderState;
import com.gs.fw.common.mithra.util.fileparser.DataReaderState;
import com.gs.fw.common.mithra.util.fileparser.MithraParsedData;
import com.gs.fw.common.mithra.util.fileparser.ParserState;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;



/**
 * This class will generate a file in order to add class data to it.  Default constructor will use mithraTestData
 * file formatting.  Only the data returned by the supplied Operation will be written to the file.
 */
public class DbExtractor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DbExtractor.class);

    private static final int DEFAULT_INDEX_SIZE = 10000;
    private final Function<Object, String> rowFormatter;
    private final Function<Class, String> headerFormatter;
    private final String delimiter;
    private final String fileName;
    private final boolean overwrite;
    private final String fileHeader;
    private MithraDataTransformer transformer = new MithraDataTransformer()
    {
        @Override
        public Map<RelatedFinder, List<MithraDataObject>> transform(Map<RelatedFinder, List<MithraDataObject>> allMergedData)
        {
            return allMergedData;
        }
    };
    private boolean endPointsInclusive;
    private List<MithraParsedData> mergedData = FastList.newList();
    private boolean saveMergedDataInMemory = false;

    public DbExtractor(String fileName, boolean overwrite)
    {
        this(fileName, overwrite, null);
    }

    public DbExtractor(String fileName, boolean overwrite, String fileHeader)
    {
        this(fileName, new MithraTestDataRowFormatter(), new MithraTestDataHeaderFormatter(), ",", overwrite, fileHeader);
    }

    public List<MithraParsedData> getMergedData()
    {
        return mergedData;
    }

    public DbExtractor(String fileName, Function<Object, String> rowFormatter,
                       Function<Class, String> headerFormatter, String delimiter, boolean overwrite)
    {
        this(fileName, rowFormatter, headerFormatter, delimiter, overwrite, null);
    }

    public void saveMergedDataInMemory()
    {
        this.saveMergedDataInMemory = true;
    }

    private DbExtractor(String fileName, Function<Object, String> rowFormatter,
                        Function<Class, String> headerFormatter, String delimiter, boolean overwrite, String fileHeader)
    {
        this.rowFormatter = rowFormatter;
        this.headerFormatter = headerFormatter;
        this.delimiter = delimiter;
        this.fileName = fileName;
        this.overwrite = overwrite;
        this.fileHeader = fileHeader;
    }

    public void setEndPointsInclusive(boolean endPointsInclusive)
    {
        this.endPointsInclusive = endPointsInclusive;
    }

    public void addClassToFile(RelatedFinder finder, Operation op) throws IOException
    {
        this.addClassAndRelatedToFile(finder, op, Collections.EMPTY_LIST);
    }

    public void addClassAndRelatedToFile(RelatedFinder finder, Operation op, List<DeepRelationshipAttribute> deepFetchAttributes) throws IOException
    {
        Map<RelatedFinder, List<MithraDataObject>> map = UnifiedMap.newMap();
        this.addClassAndRelatedToMap(finder, op, deepFetchAttributes, map);
        this.addDataByFinder(map);
    }

    public void addData(List<MithraParsedData> mergedData) throws IOException
    {
        this.addNonUniqueIndicesToOutputFile(this.loadData(mergedData));
    }

    public void addDataFrom(String filename) throws IOException
    {
        UnifiedMap<RelatedFinder, NonUniqueIndex> sourceData = this.loadDataFromFile(filename);
        this.addNonUniqueIndicesToOutputFile(sourceData);
    }

    public void addDataByFinder(Map<RelatedFinder, List<MithraDataObject>> dataObjectsByFinder) throws IOException
    {
        Map<RelatedFinder, NonUniqueIndex> allNewData = UnifiedMap.newMap();
        for (RelatedFinder finder : dataObjectsByFinder.keySet())
        {
            List<MithraDataObject> dataObjects = dataObjectsByFinder.get(finder);
            NonUniqueIndex newData = new NonUniqueIndex("", getSourcelessPrimaryKeyWithFromAttributes(finder), getPrimaryKeyAttributesWithNoSource(finder), DEFAULT_INDEX_SIZE);
            for (int i = 0; i < dataObjects.size(); i++)
            {
                newData.put(dataObjects.get(i));
            }
            allNewData.put(finder, newData);
        }
        this.addNonUniqueIndicesToOutputFile(allNewData);
    }

    private void addNonUniqueIndicesToOutputFile(Map<RelatedFinder, NonUniqueIndex> allNewData) throws IOException
    {
        Map<RelatedFinder, NonUniqueIndex> allExistingData = this.getExistingData();

        Map<RelatedFinder, List<MithraDataObject>> allMergedData = UnifiedMap.newMap();
        for (RelatedFinder finder : allNewData.keySet())
        {
            NonUniqueIndex newData = allNewData.get(finder);
            allMergedData.put(finder, this.mergeData(finder, newData, allExistingData.remove(finder)));
        }

        for (RelatedFinder finder : allExistingData.keySet())
        {
            allMergedData.put(finder, this.mergeData(finder, null, allExistingData.get(finder)));
        }

        this.writeToFile(allMergedData);
    }

    private void writeToFile(Map<RelatedFinder, List<MithraDataObject>> allMergedData) throws IOException
    {
        allMergedData = this.transformer.transform(allMergedData);

        LOGGER.info("Writing merged data to " + this.fileName);
        List<RelatedFinder> allRelatedFinders = FastList.newList(allMergedData.keySet());
        Collections.sort(allRelatedFinders, new Comparator<RelatedFinder>()
        {
            public int compare(RelatedFinder o1, RelatedFinder o2)
            {
                return o1.getClass().getName().compareTo(o2.getClass().getName());
            }
        });
        if (this.fileName.endsWith(".ccbf") || this.saveMergedDataInMemory)
        {
            writeColumnarFile(allMergedData, allRelatedFinders);
        }
        else
        {
            writeTextFile(allMergedData, allRelatedFinders);
        }
    }

    private void writeColumnarFile(Map<RelatedFinder, List<MithraDataObject>> allMergedData, List<RelatedFinder> allRelatedFinders) throws IOException
    {
        List<MithraParsedData> mithraParsedData = FastList.newList();
        for (RelatedFinder finder : allRelatedFinders)
        {
            MithraList data = finder.constructEmptyList();
            data.addAll(allMergedData.get(finder));
            Attribute[] orderByAttributes = getSourcelessPrimaryKeyWithFromAttributes(finder);
            data.setOrderBy(orderBy(orderByAttributes));
            MithraParsedData parsedData = new MithraParsedData();
            parsedData.setFinder(finder);
            parsedData.setDataObjects(new FastList(data));
            mithraParsedData.add(parsedData);
        }
        if (this.saveMergedDataInMemory)
        {
            this.mergedData = mithraParsedData;
        }
        else
        {
            writeToFile(mithraParsedData);
        }
    }

    public void writeMergedDataToColumnarFile() throws IOException
    {
        this.writeToFile(this.mergedData);
    }

    public void writeToFile(List<MithraParsedData> mithraParsedData) throws IOException
    {
        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(this.fileName);
            new BinaryCompressor().compressData(mithraParsedData, fos);
        }
        finally
        {
            closeOut(fos);
        }
    }

    private void writeTextFile(Map<RelatedFinder, List<MithraDataObject>> allMergedData, List<RelatedFinder> allRelatedFinders) throws IOException
    {
        BufferedWriter out = null;
        try
        {
            out = new BufferedWriter(new FileWriter(this.fileName));
            if (this.fileHeader != null)
            {
                out.write(this.fileHeader);
            }
            for (RelatedFinder finder : allRelatedFinders)
            {
                MithraList data = finder.constructEmptyList();
                data.addAll(allMergedData.get(finder));
                Attribute[] orderByAttributes = getSourcelessPrimaryKeyWithFromAttributes(finder);
                data.setOrderBy(orderBy(orderByAttributes));
                this.addClass(data, finder, out);
            }
        }
        finally
        {
            this.closeOut(out);
        }
    }

    private OrderBy orderBy(Attribute[] primaryKeyAttributes)
    {
        OrderBy result = primaryKeyAttributes[0].ascendingOrderBy();
        for(int i = 1; i < primaryKeyAttributes.length; i++)
        {
            result = result.and(primaryKeyAttributes[i].ascendingOrderBy());
        }
        return result;
    }

    private List<MithraDataObject> mergeData(final RelatedFinder finder, NonUniqueIndex newData, final NonUniqueIndex existingData)
    {
        LOGGER.info("Merging " + finder.getFinderClassName());
        final List<MithraDataObject> mergedData = FastList.newList();
        final List<Attribute> pkExtractors = Arrays.asList(finder.getPrimaryKeyAttributes());
        if (newData != null)
        {
            newData.forEachGroup(new DoUntilProcedure<Object>()
            {
                @Override
                public boolean execute(Object group)
                {
                    Object newObjectOrList = group instanceof FullUniqueIndex ? ((FullUniqueIndex)group).getAll() : group;
                    Object existingObjectOrList = null;
                    if (existingData != null)
                    {
                        Object existingKey = newObjectOrList;
                        if (newObjectOrList instanceof List)
                        {
                            existingKey = ((List) newObjectOrList).get(0);
                        }
                        existingObjectOrList = existingData.removeGroup(existingKey, pkExtractors);
                        if (existingObjectOrList instanceof FullUniqueIndex)
                        {
                            existingObjectOrList = ((FullUniqueIndex) existingObjectOrList).getAll();
                        }
                    }
                    MilestoneRectangle.merge(newObjectOrList, existingObjectOrList, finder, mergedData);
                    return false;
                }
            });
        }
        if (existingData != null)
        {
            existingData.forEachGroup(new DoUntilProcedure<Object>()
            {
                @Override
                public boolean execute(Object group)
                {
                    Object objectOrList = group instanceof FullUniqueIndex ? ((FullUniqueIndex)group).getAll() : group;
                    MilestoneRectangle.merge(objectOrList, null, finder, mergedData);
                    return false;
                }
            });
        }
        return mergedData;
    }

    private Map<RelatedFinder, NonUniqueIndex> getExistingData()
    {
        if (!this.overwrite)
        {
            if (this.saveMergedDataInMemory)
            {
                LOGGER.info("Loading merged data from memory. Size size of the list is " + this.mergedData.size());
                return this.loadData(this.mergedData);
            }
            else if (new File(this.fileName).exists())
            {
                LOGGER.info("Loading existing data from " + this.fileName);
                return this.loadDataFromFile(this.fileName);
            }
        }
        return UnifiedMap.newMap();
    }

    private UnifiedMap<RelatedFinder, NonUniqueIndex> loadDataFromFile(String filename)
    {
        List<MithraParsedData> results;
        if (filename.endsWith(".ccbf"))
        {
            results = new BinaryCompressor().decompress(filename);
        }
        else
        {
            results = new MithraTestDataParser(filename).getResults();
        }

        return loadData(results);
    }

    private UnifiedMap<RelatedFinder, NonUniqueIndex> loadData(List<MithraParsedData> results)
    {
        UnifiedMap<RelatedFinder, NonUniqueIndex> indexByFinder = UnifiedMap.newMap();
        for (MithraParsedData result : results)
        {
            List<MithraDataObject> dataObjects = result.getDataObjects();
            Map<Class, RelatedFinder> relatedFindersByDataClass = UnifiedMap.newMap();
            Map<RelatedFinder, List<MithraDataObject>> dataObjectsByFinder = UnifiedMap.newMap();
            for (MithraDataObject dataObject : dataObjects)
            {
                RelatedFinder finder = relatedFindersByDataClass.get(dataObject.getClass());
                if (finder == null)
                {
                    finder = finderFromDataObject(dataObject);
                    relatedFindersByDataClass.put(dataObject.getClass(), finder);
                }
                List<MithraDataObject> objects = dataObjectsByFinder.get(finder);
                if (objects == null)
                {
                    objects = FastList.newList();
                    dataObjectsByFinder.put(finder, objects);
                }
                objects.add(dataObject);
            }
            for (RelatedFinder finder : dataObjectsByFinder.keySet())
            {
                this.indexDataObjects(finder, dataObjectsByFinder.get(finder), indexByFinder);
            }
        }
        return indexByFinder;
    }

    private static RelatedFinder finderFromDataObject(MithraDataObject dataObject)
    {
        String name = dataObject.getClass().getName();
        int dataIndex = name.lastIndexOf("Data$");
        if (dataIndex < 0)
        {
            dataIndex = name.lastIndexOf("Data");
        }
        if (dataIndex < 0)
        {
            throw new IllegalStateException("Could not determine finder class name from " + name);
        }
        try
        {
            Class finderClass = Class.forName(name.substring(0, dataIndex) + "Finder");
            return (RelatedFinder) finderClass.getMethod("getFinderInstance").invoke(null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void indexDataObjects(RelatedFinder finder, List<MithraDataObject> dataObjects, UnifiedMap<RelatedFinder, NonUniqueIndex> indexByFinder)
    {
        NonUniqueIndex index = indexByFinder.get(finder);
        if (index == null)
        {
            index = new NonUniqueIndex("", getSourcelessPrimaryKeyWithFromAttributes(finder), getPrimaryKeyAttributesWithNoSource(finder), DEFAULT_INDEX_SIZE);
            indexByFinder.put(finder, index);
        }
        for (MithraDataObject data : dataObjects)
        {
            index.put(data);
        }
    }
    
    public void setTransformer(MithraDataTransformer transformer)
    {
        this.transformer = transformer;
    }

    private void addClassAndRelatedToMap(RelatedFinder finder, Operation op, List<DeepRelationshipAttribute> deepFetchAttributes, Map<RelatedFinder, List<MithraDataObject>> dataMap)
    {
        final MithraList data = finder.findMany(op);
        data.setNumberOfParallelThreads(5);

        for (DeepRelationshipAttribute deepFetchAttribute : deepFetchAttributes)
        {
            AbstractRelatedFinder attribute = (AbstractRelatedFinder) deepFetchAttribute;
            LOGGER.debug("Deep fetching data for : " + attribute.getRelationshipPath());
            data.deepFetch(deepFetchAttribute);
        }

        this.addClassToMap(data, finder, dataMap);
        for (DeepRelationshipAttribute deepFetchAttribute : deepFetchAttributes)
        {
            AbstractRelatedFinder attribute = (AbstractRelatedFinder) deepFetchAttribute;
            LOGGER.debug("Getting data for : " + attribute.getRelationshipPath());

            List relatedList = deepFetchAttribute.listValueOf(data);
            RelatedFinder rootFinder = ((RelatedFinder) deepFetchAttribute).getMithraObjectPortal().getFinder();
            addClassToMap(relatedList, rootFinder, dataMap);
            if (relatedList.size() > 10000)
            {
                LOGGER.warn("Relationship with lots of data (" + relatedList.size() + ") :" + attribute.getRelationshipPath() + ". Consider filtering it out.");
            }
        }
    }

    private void addClassToMap(List data, RelatedFinder finder, Map<RelatedFinder, List<MithraDataObject>> map)
    {
        if (!data.isEmpty())
        {
            List<MithraDataObject> existing = map.get(finder);
            if (existing == null)
            {
                existing = FastList.newList(data.size());
                map.put(finder, existing);
            }
            for (int i = 0; i < data.size(); i++)
            {
                existing.add(MithraObject.class.cast(data.get(i)).zGetCurrentData());
            }
        }
    }

    private static Attribute[] getSourcelessPrimaryKeyWithFromAttributes(RelatedFinder finder)
    {
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        Attribute sourceAttribute = finder.getSourceAttribute();
        Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        if (asOfAttributes == null && sourceAttribute == null)
        {
            return primaryKeyAttributes;
        }
        int asOfLength = asOfAttributes == null ? 0 : asOfAttributes.length;
        int sourceLength = sourceAttribute == null ? 0 : 1;
        Attribute[] attributes = new Attribute[primaryKeyAttributes.length + asOfLength - sourceLength];
        int index = 0;
        for (Attribute pkAttr : primaryKeyAttributes)
        {
            if (!pkAttr.equals(sourceAttribute))
            {
                attributes[index++] = pkAttr;
            }
        }
        if (asOfAttributes != null)
        {
            for (AsOfAttribute asOfAttr : asOfAttributes)
            {
                attributes[index++] = asOfAttr.getFromAttribute();
            }
        }
        return attributes;
    }

    private static Attribute[] getPrimaryKeyAttributesWithNoSource(RelatedFinder finder)
    {
        Attribute sourceAttribute = finder.getSourceAttribute();
        Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        if (sourceAttribute == null)
        {
            return primaryKeyAttributes;
        }
        int sourceLength = sourceAttribute == null ? 0 : 1;
        Attribute[] attributes = new Attribute[primaryKeyAttributes.length - sourceLength];
        int index = 0;
        for (Attribute pkAttr : primaryKeyAttributes)
        {
            if (!pkAttr.equals(sourceAttribute))
            {
                attributes[index++] = pkAttr;
            }
        }
        return attributes;
    }

    private void addClass(List data, RelatedFinder finder, BufferedWriter out) throws IOException
    {
        if (!data.isEmpty())
        {
            Attribute[] attributes = finder.getPersistentAttributes();
            this.addHeader(out, finder.getClass(), attributes);
            this.addData(out, attributes, data);
            out.newLine();
        }
    }

    private void addHeader(BufferedWriter out, Class finderClass, Attribute[] attributes) throws IOException
    {
        this.startLine(out);
        out.write(this.headerFormatter.valueOf(finderClass));
        this.endLine(out);

        this.startLine(out);
        for (int i = 0; i < attributes.length - 1; i++)
        {
            out.write(attributes[i].getAttributeName() + delimiter);
        }
        out.write(attributes[attributes.length -1].getAttributeName());
        this.endLine(out);
    }

    private void addData(BufferedWriter out, Attribute[] attributes, List data) throws IOException
    {
        for (Object object : data)
        {
            this.startLine(out);
            for (int i = 0; i < attributes.length - 1; i++)
            {
                Attribute attribute = attributes[i];
                out.write(rowFormatter.valueOf(getValueConvertIfNeeded(attribute, object)) + delimiter);
            }
            out.write(rowFormatter.valueOf(getValueConvertIfNeeded(attributes[attributes.length - 1], object)));
            this.endLine(out);
        }
    }

    private Object getValueConvertIfNeeded(Attribute attribute, Object object)
    {
        Object value = attribute.valueOf(object);
        if (attribute instanceof TimestampAttribute && value != null)
        {
            TimestampAttribute timestampAttribute = (TimestampAttribute) attribute;
            if (timestampAttribute.requiresConversionFromUtc() &&
                    (timestampAttribute.getAsOfAttributeInfinity() == null || !timestampAttribute.getAsOfAttributeInfinity().equals(value)))
            {
                return MithraTimestamp.zConvertTimeForReadingWithUtcCalendar(new Timestamp(((Timestamp) value).getTime()), TimeZone.getDefault());
            }
        }
        return value;
    }

    private void closeOut(Closeable out)
    {
        if (out != null)
        {
            try
            {
                out.close();
            }
            catch (IOException e)
            {
                LOGGER.error("could not close output stream", e);
            }
        }
    }

    private void endLine(BufferedWriter out) throws IOException
    {
        if(endPointsInclusive)
        {
            out.write(delimiter);
        }
        out.newLine();
    }

    private void startLine(BufferedWriter out) throws IOException
    {
        if(endPointsInclusive)
        {
            out.write(delimiter);
        }
    }

    public class MithraTestDataParser extends AbstractMithraDataFileParser
    {

        public MithraTestDataParser(String filename)
        {
            super(filename);
        }

        public MithraTestDataParser(URL streamLocation, InputStream is)
        {
            super(streamLocation, is);
        }

        protected ParserState createBeginningOfLineState()
        {
            return new BeginningOfLineState(this);
        }

        protected ParserState createClassReaderState()
        {
            return new ClassReaderState(this);
        }

        protected DataReaderState createDataReaderState()
        {
            return new DataReaderState(this);
        }

        protected AttributeReaderState createAttributeReaderState()
        {
            return new AttributeReaderState(this);
        }
    }

}