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

import com.gs.fw.common.mithra.util.fileparser.*;

import java.io.InputStream;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.net.URL;

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

    @Override
    protected StreamTokenizer createStreamTokenizer(Reader reader)
    {
        return createStreamTokenizerWithoutNumbers(reader);
    }
}
