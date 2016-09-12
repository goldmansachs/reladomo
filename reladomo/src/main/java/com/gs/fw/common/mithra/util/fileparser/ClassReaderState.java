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

import java.io.IOException;
import java.io.StreamTokenizer;
import java.text.ParseException;

public class ClassReaderState extends ParserState
{

    public ClassReaderState(AbstractMithraDataFileParser parser)
    {
        super(parser);
    }

    public ParserState parse(StreamTokenizer st) throws IOException, ParseException
    {
        int token = st.ttype;
        if (token != StreamTokenizer.TT_WORD || !st.sval.equals(AbstractMithraDataFileParser.CLASS_IDENTIFIER))
        {
            throw new ParseException("expected line " + st.lineno() + " to begin with class", st.lineno());
        }
        token = st.nextToken();
        if (token != StreamTokenizer.TT_WORD)
        {
            throw new ParseException("expected a class name on line "+st.lineno(), st.lineno());
        }
        this.getParser().addNewMithraParsedData();
        String className = st.sval;
        try
        {
            this.getParser().getCurrentParsedData().setParsedClassName(className);
        }
        catch (Exception e)
        {
            ParseException parseException = new ParseException("no such class (or finder): "+className+" on line "+st.lineno(), st.lineno());
            parseException.initCause(e);
            throw parseException;
        }
        this.getParser().getDataReaderState().setClass(className, st.lineno());

        token = st.nextToken();
        if (token != StreamTokenizer.TT_EOL)
        {
            throw new ParseException("invalid data after the class name on line "+st.lineno(), st.lineno());
        }

        return this.getParser().getAttributeReaderState();
    }
}
