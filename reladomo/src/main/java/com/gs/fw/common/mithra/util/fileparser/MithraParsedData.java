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

package com.gs.fw.common.mithra.util.fileparser;

import java.io.StreamTokenizer;
import java.lang.reflect.InvocationTargetException;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import org.slf4j.LoggerFactory;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;



public class MithraParsedData
{

    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);

    private static transient org.slf4j.Logger logger = LoggerFactory.getLogger(MithraParsedData.class.getName());
    private String parsedClassName;
    private Class dataClass;
    private RelatedFinder finder;
    private List<MithraDataObject> dataObjects = new ArrayList<MithraDataObject>();
    private List<Attribute> attributes = new ArrayList<Attribute>();

    public RelatedFinder getFinder()
    {
        return finder;
    }

    public String getParsedClassName()
    {
        return this.parsedClassName;
    }

    public void setFinder(RelatedFinder finder)
    {
        this.finder = finder;
        ReladomoClassMetaData metaData = ReladomoClassMetaData.fromFinder(finder);
        this.parsedClassName = metaData.getBusinessOrInterfaceClassName();
        this.dataClass = metaData.getOnHeapDataClass();
    }

    public void setParsedClassName(String parsedClassName) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        this.parsedClassName = parsedClassName;
        ReladomoClassMetaData metaData = ReladomoClassMetaData.fromBusinessClassName(parsedClassName);
        dataClass = metaData.getOnHeapDataClass();
        finder = metaData.getFinderInstance();
    }

    public List<MithraDataObject> getDataObjects()
    {
        return dataObjects;
    }

    public void setDataObjects(List<MithraDataObject> dataObjects)
    {
        this.dataObjects = dataObjects;
    }

    public List getAttributes()
    {
        return attributes;
    }

    public void setAttributes(List<Attribute> attributes)
    {
        this.attributes = attributes;
    }

    public void addAttribute(Attribute attribute)
    {
        this.attributes.add(attribute);
    }

    public void addDataObject(MithraDataObject dataObject)
    {
        this.dataObjects.add(dataObject);
    }

    public MithraDataObject createAndAddDataObject(int lineNumber) throws ParseException
    {
        MithraDataObject currentData;
        try
        {
            currentData = (MithraDataObject) dataClass.newInstance();
        }
        catch (Exception e)
        {
            ParseException parseException = new ParseException("Could not instantiate data class "+dataClass.getName()+" for line "+lineNumber, lineNumber);
            parseException.initCause(e);
            throw parseException;
        }
        this.addDataObject(currentData);
        return currentData;
    }


    public void addAttribute(String attributeName, int lineNumber) throws ParseException
    {
        Attribute attr = this.finder.getAttributeByName(attributeName);
        if (attr == null)
        {
            throw new ParseException("Could not find attribute "+attributeName+" in class "+this.parsedClassName+" on line "+lineNumber
                    , lineNumber);
        }
        this.attributes.add(attr);
    }

    public void parseStringData(String input, int attributeNumber, Object currentData, int lineNumber, Format dateFormat) throws ParseException
    {
        if (attributeNumber >= this.attributes.size())
        {
            logger.debug("attribute number > attribute size!!!");
            throw new ParseException("extra data on line "+lineNumber, lineNumber);
        }
        Attribute attribute = this.attributes.get(attributeNumber);
        if (input.equals("null"))
        {
            attribute.parseWordAndSet(input, currentData, lineNumber);
            logger.debug("null parsed.");
        }
        else if (input.equals(""))
        {
            attribute.parseWordAndSet("null", currentData, lineNumber);
            logger.debug("empty string parsed.");
        }
        else
        {
            attribute.parseStringAndSet(input, currentData, lineNumber, dateFormat);
            logger.debug("string parsed.");
        }
    }

    public void parseData(StreamTokenizer st, int attributeNumber, Object currentData) throws ParseException
    {
        if (attributeNumber >= this.attributes.size())
        {
            throw new ParseException("extra data on line "+st.lineno(),st.lineno());
        }
        Attribute attribute = this.attributes.get(attributeNumber);
        int token = st.ttype;
        switch (token)
        {
            case StreamTokenizer.TT_NUMBER:
                //note: this can't parse large long values correctly
                attribute.parseNumberAndSet(st.nval, currentData, st.lineno());
                break;
            case StreamTokenizer.TT_WORD:
                String word = st.sval;
                if (!"null".equals(word) && attribute instanceof NumericAttribute)
                {
                    attribute.parseStringAndSet(st.sval, currentData, st.lineno(), null);
                }
                else
                {
                    attribute.parseWordAndSet(word, currentData, st.lineno());
                }
                break;
            case '"':
                attribute.parseStringAndSet(st.sval, currentData, st.lineno(), simpleDateFormat);
                break;
            case StreamTokenizer.TT_EOL:
                throw new RuntimeException("should never get here");
            case StreamTokenizer.TT_EOF:
                throw new ParseException("Unexpected end of file", st.lineno());
            default:
                char ch = (char)st.ttype;
                throw new ParseException("unexpected character "+ch+" or type "+st.ttype+" on line "+st.lineno()+" attribute count "+attributeNumber, st.lineno());
        }
    }
}
