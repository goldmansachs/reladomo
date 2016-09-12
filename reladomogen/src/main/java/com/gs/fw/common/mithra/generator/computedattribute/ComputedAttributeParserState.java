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

package com.gs.fw.common.mithra.generator.computedattribute;

import java.io.IOException;
import java.io.StreamTokenizer;

public abstract class ComputedAttributeParserState
{
    private ComputedAttributeParser parser;

    public ComputedAttributeParserState(ComputedAttributeParser parser)
    {
        this.parser = parser;
    }

    public ComputedAttributeParser getParser()
    {
        return this.parser;
    }

    public abstract ComputedAttributeParserState parse(StreamTokenizer st) throws IOException, ParseException;

    protected ParseException createUnexpectedCharacterException(char ch, String expected)
    {
        return new ParseException("unexpected character "+ch+" in expression "+this.getParser().getFormula()+" was expecting '"+expected+"' in "+this.getParser().getDiagnosticMessage());
    }
}
