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
import java.io.StringReader;

public class JavaExpressionParserState extends ComputedAttributeParserState
{
    public JavaExpressionParserState(ComputedAttributeParser parser)
    {
        super(parser);
    }

    @Override
    public ComputedAttributeParserState parse(StreamTokenizer st) throws IOException, ParseException
    {
        StringBuilder javaString = new StringBuilder();
        StringReader reader = this.getParser().getReader();
        int ch = reader.read();
        while (ch >= 0)
        {
            if (ch == '}')
            {
                this.getParser().getStateStack().add(new JavaExpression(javaString.toString()));
                return new ExpressionEndState(this.getParser());
            }
            javaString.append((char) ch);
            ch = reader.read();
        }
        throw new ParseException("did not find closing '}' after reading: "+javaString.toString());
    }
}
