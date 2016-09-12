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

public class ExpressionEndState extends ComputedAttributeParserState
{
    public ExpressionEndState(ComputedAttributeParser computedAttributeParser)
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
                    case '.':
                        Expression expression = stack.remove(stack.size() - 1);
                        stack.add(new FunctionExpression(expression));
                        nextState = new FunctionBeginExpressionState(this.getParser());
                        break;
                    case ',':
                        addParameterToFunction(st, stack);
                        nextState = new ExpressionBeginState(this.getParser());
                        break;
                    case ';':
                        endCaseValue(st, stack);
                        nextState = new CaseSelectorBeginParserState(this.getParser());
                        break;
                    case ')':
                        endFunctionOrCase(st, stack);
                        // nextState = new ExpressionEndState(this.getParser()); we can just loop here instead
                        break;
                    case StreamTokenizer.TT_NUMBER:
                        throw new ParseException("unexpected number "+st.nval+" in expression "+this.getParser().getFormula()+" in "+this.getParser().getDiagnosticMessage());
                    case StreamTokenizer.TT_WORD:
                        throw new ParseException("unexpected word "+st.sval+" in expression "+this.getParser().getFormula()+" in "+this.getParser().getDiagnosticMessage());
                    default:
                        char ch = (char)st.ttype;
                        throw createUnexpectedCharacterException(ch, ",;).");
                }
            }
        }
        return nextState;
    }

    private void endCaseValue(StreamTokenizer st, ArrayList<Expression> stack)
    {
        Expression lastExpression = stack.remove(stack.size() - 1);
        Expression caseExpression = stack.get(stack.size() - 1);
        if (!(caseExpression instanceof CaseExpression))
        {
            throw new ParseException("expecting ';' to appear only inside a case statement");
        }
        ((CaseExpression)caseExpression).setValue(lastExpression);
    }

    private void endFunctionOrCase(StreamTokenizer st, ArrayList<Expression> stack)
    {
        Expression lastExpression = stack.get(stack.size() - 1);
        FunctionExpression functionToFinish = null;
        if (lastExpression instanceof FunctionExpression)
        {
            FunctionExpression func = (FunctionExpression) lastExpression;
            if (!func.isFinished())
            {
                functionToFinish = func;
            }
        }
        if (functionToFinish == null)
        {
            Expression param = stack.remove(stack.size() - 1);
            // could be a case or a function
            Expression caseOrFunction = stack.get(stack.size() - 1);
            if (caseOrFunction instanceof FunctionExpression)
            {
                functionToFinish = (FunctionExpression) caseOrFunction;
                functionToFinish.addParameter(param);
            }
            else if (caseOrFunction instanceof CaseExpression)
            {
                ((CaseExpression)caseOrFunction).setValue(param);
                ((CaseExpression)caseOrFunction).markFinished();
            }
            else
            {
                throw new ParseException("the closing parenthesis is not parsable as a function or case statement end");
            }
        }
        if (functionToFinish != null)
        {
            functionToFinish.markFinished();
        }
    }

    private void addParameterToFunction(StreamTokenizer st, ArrayList<Expression> stack) throws ParseException
    {
        Expression parameter = stack.remove(stack.size() - 1);
        Expression function = stack.get(stack.size() - 1);
        if (!(function instanceof FunctionExpression))
        {
            throw new ParseException("The comma in the expression "+this.getParser().getFormula()+" was expecting a function to precede it, but instead got "+function.toString());
        }
        ((FunctionExpression)function).addParameter(parameter);
    }
}
