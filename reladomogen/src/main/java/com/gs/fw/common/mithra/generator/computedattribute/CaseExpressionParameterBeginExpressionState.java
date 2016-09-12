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

public class CaseExpressionParameterBeginExpressionState extends ComputedAttributeParserState
{
    public CaseExpressionParameterBeginExpressionState(ComputedAttributeParser parser)
    {
        super(parser);
    }

    @Override
    public ComputedAttributeParserState parse(StreamTokenizer st) throws IOException, ParseException
    {
        ComputedAttributeParserState nextState = null;
        while(nextState == null && st.ttype != StreamTokenizer.TT_EOF)
        {
            ArrayList<Expression> stack = this.getParser().getStateStack();
            int nextToken = st.nextToken();
            if (nextToken != StreamTokenizer.TT_EOL && nextToken != StreamTokenizer.TT_EOF)
            {
                switch(nextToken)
                {
                    case '(':
                        Expression function = stack.get(stack.size() - 1);
                        ((CaseExpression) function).startNewKey();
                        nextState = new CaseSelectorBeginParserState(this.getParser());
                        break;
                    case StreamTokenizer.TT_NUMBER:
                        throw new ParseException("unexpected number "+st.nval+" in expression "+this.getParser().getFormula()+" in "+this.getParser().getDiagnosticMessage());
                    case StreamTokenizer.TT_WORD:
                        throw new ParseException("unexpected word "+st.sval+" in expression "+this.getParser().getFormula()+" in "+this.getParser().getDiagnosticMessage());
                    default:
                        char ch = (char)st.ttype;
                        throw createUnexpectedCharacterException(ch, "(");
                }
            }
        }
        return nextState;
    }
}
