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

public class CaseSelectorBeginParserState extends ComputedAttributeParserState
{
    public CaseSelectorBeginParserState(ComputedAttributeParser computedAttributeParser)
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
                CaseExpression caseExpression = (CaseExpression) stack.get(stack.size() - 1);
                switch(nextToken)
                {
                    case StreamTokenizer.TT_NUMBER:
                        caseExpression.addNumberKey(st.nval);
                        nextState = new CaseSelectorMiddleParserState(this.getParser());
                        break;
                    case StreamTokenizer.TT_WORD:
                        if (st.sval.equals("null"))
                        {
                            caseExpression.addNullKey();
                            stack.add(new NullExpression());
                        }
                        else if (st.sval.equals("true") || st.sval.equals("false"))
                        {
                            caseExpression.addBooleanKey(Boolean.valueOf(st.sval));
                            stack.add(new NullExpression());
                        }
                        else if (st.sval.equals("default"))
                        {
                            caseExpression.addDefaultKey();
                            stack.add(new NullExpression());
                        }
                        else
                        {
                            throw new ParseException("unexpected word "+st.sval+" in expression "+this.getParser().getFormula()+" in "+this.getParser().getDiagnosticMessage());
                        }
                        nextState = new CaseSelectorMiddleParserState(this.getParser());
                        break;
                    case '"':
                        caseExpression.addStringConstantKey(st.sval);
                        nextState = new CaseSelectorMiddleParserState(this.getParser());
                        break;
                    default:
                        char ch = (char)st.ttype;
                        throw createUnexpectedCharacterException(ch, "<number>|<null>|<boolean>|<default>|<string>");
                }
            }
        }
        return nextState;
    }
}
