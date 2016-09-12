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

public class AttributeReaderState extends ParserState
{

    public AttributeReaderState(AbstractMithraDataFileParser parser)
    {
        super(parser);
    }

    public ParserState parse(StreamTokenizer st) throws IOException, ParseException
    {
        parseAttributes(st);
        return this.getParser().getBeginningOfLineState();
    }

    private void parseAttributes(StreamTokenizer st) throws IOException, ParseException
    {
        int token = st.nextToken();
        boolean wantAttribute = true;
        while(token != StreamTokenizer.TT_EOL)
        {
            if (wantAttribute)
            {
                if (token != StreamTokenizer.TT_WORD)
                {
                    throw new ParseException("expected an attribute name on line "+st.lineno(), st.lineno());
                }
                this.getParser().getCurrentParsedData().addAttribute(st.sval, st.lineno());
            }
            else
            {
                if (token != ',')
                {
                    throw new ParseException("Expected a comma on line "+st.lineno(), st.lineno());
                }
            }
            wantAttribute = !wantAttribute;
            token = st.nextToken();
        }
        if (wantAttribute)
        {
            throw new ParseException("extra comma at the end of line "+st.lineno(), st.lineno());
        }
    }
}
