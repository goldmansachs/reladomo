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

import com.gs.fw.common.mithra.MithraDataObject;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.text.ParseException;

public class DataReaderState extends ParserState
{
    private Class dataClass;

    public DataReaderState(AbstractMithraDataFileParser parser)
    {
        super(parser);
    }

    public void setClass(String parsedClass, int lineNumber) throws ParseException
    {
        try
        {
            //todo: offheap
            dataClass = Class.forName(parsedClass+"Data");
        }
        catch (ClassNotFoundException e)
        {
            throw new ParseException("Could not find data class for class "+parsedClass +" on line "+lineNumber, lineNumber);
        }
    }

    public ParserState parse(StreamTokenizer st) throws IOException, ParseException
    {
        if (dataClass == null)
        {
            throw new ParseException("no class name found before line "+st.lineno(), st.lineno());
        }
        MithraDataObject currentData;
        currentData = this.getParser().getCurrentParsedData().createAndAddDataObject(st.lineno());

        // parse the data
        int currentAttribute = 0;
        int token = st.ttype;

        boolean wantData = true;
        while(token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF)
        {
            if (wantData)
            {
                this.getParser().getCurrentParsedData().parseData(st, currentAttribute, currentData);
                currentAttribute++;
            }
            else
            {
                if (token != ',')
                {
                    throw new ParseException("Expected a comma on line "+st.lineno(), st.lineno());
                }
            }
            wantData = !wantData;
            token = st.nextToken();
        }

        return this.getParser().getBeginningOfLineState();
    }
}
