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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.extractor.AbstractStringExtractor;
import com.gs.fw.common.mithra.extractor.NonPrimitiveExtractor;
import com.gs.fw.common.mithra.util.fileparser.MithraParsedData;

import java.net.URL;
import java.util.List;


public class TestDataFile
{

    private String filename;
    private Object sourceId;
    private List<MithraParsedData> parsedData;
    private boolean inserted;

    public TestDataFile(String filename, List<MithraParsedData> parsedData)
    {
        this.filename = filename;
        this.parsedData = parsedData;
    }

    public TestDataFile(URL fileLocation, List<MithraParsedData> parsedData)
    {
        if (fileLocation == null)
        {
            throw new IllegalArgumentException("file location must not be null");
        }
        this.filename = fileLocation.toString();
        this.parsedData = parsedData;
    }

    public TestDataFile(URL fileLocation, List<MithraParsedData> parsedData, Object sourceId)
    {
        if (fileLocation == null)
        {
            throw new IllegalArgumentException("file location must not be null");
        }
        this.filename = fileLocation.toString();
        this.parsedData = parsedData;
        this.sourceId = sourceId;
    }

    public TestDataFile(String filename, List<MithraParsedData> parsedData, Object sourceId)
    {
        this.filename = filename;
        this.parsedData = parsedData;
        this.sourceId = sourceId;
    }

    public List<MithraParsedData> getParsedData()
    {
        return this.parsedData;
    }

    public String getFilename()
    {
        return filename;
    }

    public boolean isInserted()
    {
        return inserted;
    }

    public void setInserted(boolean inserted)
    {
        this.inserted = inserted;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TestDataFile that = (TestDataFile) o;

        return filename.equals(that.filename) && (this.sourceId == null || this.sourceId.equals(that.sourceId));

    }

    public int hashCode()
    {
        return filename.hashCode();
    }

    public static class FilenameExtractor extends AbstractStringExtractor
    {
        public void setStringValue(Object o, String newValue)
        {
            throw new RuntimeException("not implemented");
        }

        public String stringValueOf(Object o)
        {
            return ((TestDataFile)o).getFilename();
        }
    }

    public static class SourceIdExtractor extends NonPrimitiveExtractor
    {
        public void setValue(Object o, Object newValue)
        {
        }

        public Object valueOf(Object object)
        {
            TestDataFile tdf = (TestDataFile) object;
            return tdf.sourceId;
        }
    }
}
