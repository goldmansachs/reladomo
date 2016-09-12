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
import java.util.ArrayList;
import java.util.List;

public class ComputedAttributeParser
{
    private ArrayList<Expression> stateStack = new ArrayList<Expression>();
    private String diagnosticMessage;
    private String formula;
    private StringReader reader;

    public String getDiagnosticMessage()
    {
        return diagnosticMessage;
    }

    public ArrayList<Expression> getStateStack()
    {
        return stateStack;
    }

    public String getFormula()
    {
        return formula;
    }

    public Expression parse(String formula, String diagnosticMessage, List<String> errors)
    {
        this.diagnosticMessage = diagnosticMessage;
        this.formula = formula;
        this.reader = new StringReader(formula);
        StreamTokenizer tokenizer = new StreamTokenizer(reader);
        tokenizer.resetSyntax();
        tokenizer.wordChars('a', 'z');
        tokenizer.wordChars('A', 'Z');
//        wordChars(128 + 32, 255);
        tokenizer.whitespaceChars(0, ' ');
        tokenizer.commentChar('/');
        tokenizer.quoteChar('"');
        tokenizer.quoteChar('\'');
        tokenizer.parseNumbers();
        tokenizer.ordinaryChar('.');
        tokenizer.wordChars('_', '_');
        
        ComputedAttributeParserState state = new ExpressionBeginState(this);
        while(state != null)
        {
            try
            {
//                System.out.println("Expression state "+state.getClass().getName());
                state = state.parse(tokenizer);
            }
            catch (IOException e)
            {
                throw new RuntimeException("shouldn't happen");
            }
            catch (ParseException e)
            {
                errors.add("Could not parse formula '"+formula+"' in "+diagnosticMessage + " "+e.getClass().getName()+": "+e.getMessage());
                return null;
            }
        }
        if (this.stateStack.isEmpty())
        {
            errors.add("Could not parse the formula '"+formula+"' in "+diagnosticMessage+" was it totally empty??");
            return null;
        }
        if (this.stateStack.size() > 1)
        {
            errors.add("Could not parse the formula '"+formula+"' in "+diagnosticMessage+" parsed multiple expressions "+this.stateStack);
            return null;
        }
        return this.stateStack.get(0);
    }

    public StringReader getReader()
    {
        return reader;
    }
}
