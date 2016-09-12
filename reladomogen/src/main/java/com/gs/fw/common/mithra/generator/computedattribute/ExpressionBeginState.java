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
import java.util.ArrayList;

public class ExpressionBeginState extends ComputedAttributeParserState
{
    public ExpressionBeginState(ComputedAttributeParser computedAttributeParser)
    {
        super(computedAttributeParser);
    }

    @Override
    public ComputedAttributeParserState parse(StreamTokenizer st) throws IOException, ParseException
    {
        ComputedAttributeParserState nextState = null;
        while(nextState == null && st.ttype != StreamTokenizer.TT_EOF)
        {
            int nextToken = st.nextToken();
            if (nextToken != StreamTokenizer.TT_EOL && nextToken != StreamTokenizer.TT_EOF)
            {
                ArrayList<Expression> stack = this.getParser().getStateStack();
                switch(nextToken)
                {
                    case StreamTokenizer.TT_NUMBER:
                        stack.add(new NumberConstantExpression(st.nval));
                        nextState = new ExpressionEndState(this.getParser());
                        break;
                    case StreamTokenizer.TT_WORD:
                        if (st.sval.equals("null"))
                        {
                            stack.add(new NullExpression());
                        }
                        else if (st.sval.equals("true") || st.sval.equals("false"))
                        {
                            stack.add(new BooleanExpression(Boolean.valueOf(st.sval)));
                        }
                        else
                        {
                            stack.add(new AttributeExpression(st.sval));
                        }
                        nextState = new ExpressionEndState(this.getParser());
                        break;
                    case '"':
                        stack.add(new ConstantStringExpression(st.sval));
                        nextState = new ExpressionEndState(this.getParser());
                        break;
                    case '{':   //todo: is this necessary? how will type inference work with a java expression?
                        nextState = new JavaExpressionParserState(this.getParser());
                        break;
                    default:
                        char ch = (char)st.ttype;
                        throw createUnexpectedCharacterException(ch, "<number>|<null>|<boolean>|<attribute>|<string>");
                }
            }
        }
        return nextState;
    }
}
