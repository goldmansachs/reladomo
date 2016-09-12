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

package com.gs.fw.common.mithra.generator;

import java.io.PrintWriter;


public class SourceFormatter
{
    private static final FormatState NORMAL_STATE = new NormalState();
    private static final FormatState LONG_COMMENT_STATE = new LongCommentState();
    private static final FormatState DOUBLE_QUOTE_STATE = new QuoteState('"');
    private static final FormatState SINGLE_QUOTE_STATE = new QuoteState('\'');

    private ProcessingState processingState = new ProcessingState();

    public SourceFormatter()
    {
        init();
    }

    public void init()
    {
        processingState.formatState = NORMAL_STATE;
        processingState.indent = 0;
        processingState.previousLineWasBlank = false;
    }

    // blank line rules: 1) no consecutive blank line 2) no blank line after an open brace 3) no blank line before a closing brace
    public void formatLine(String line, PrintWriter writer)
    {
        processingState.setLine(line);
        processingState.pwriter = writer;
        processingState = processingState.formatState.formatPartialLine(processingState);
        while (!processingState.isDoneWithLine())
        {
            processingState = processingState.formatState.formatPartialLine(processingState);
        }
    }

    private static class ProcessingState
    {
        private String line;
        private int offset;
        private int end;
        private int indent;
        private PrintWriter pwriter;
        private boolean currentLineIsBlank = true;
        private FormatState formatState = NORMAL_STATE;
        private boolean previousLineWasBlank;
        public boolean tempIndent;

        public void setLine(String line)
        {
            this.line = line;
            this.offset = 0;
            this.end = line.length();
            while(!this.isDoneWithLine() && Character.isWhitespace(this.line.charAt(this.end - 1)))
            {
                this.end--;
            }
            while(!this.isDoneWithLine() && Character.isWhitespace(this.line.charAt(this.offset)))
            {
                this.offset++;
            }
        }

        private void indentLine()
        {
            for(int i=0;i<indent;i++)
            {
                pwriter.append('\t');
            }
            if (tempIndent)
            {
                tempIndent = false;
                pwriter.append('\t');
            }
        }

        public void emitBlankLine()
        {
            previousLineWasBlank = true;
        }

        public void emitEndOfLine()
        {
            previousLineWasBlank = this.currentLineIsBlank;
            pwriter.write("\n");
            this.currentLineIsBlank = true;
        }

        public boolean isDoneWithLine()
        {
            return offset == this.end;
        }

        public void appendRemainingLineWithEndOfLine()
        {
            while(!this.isDoneWithLine() && Character.isWhitespace(this.line.charAt(this.offset)))
            {
                this.offset++;
            }
            if (this.offset == this.end)
            {
                if (currentLineIsBlank)
                {
                    this.previousLineWasBlank = true;
                }
                else
                {
                    this.emitEndOfLine();
                }
            }
            else
            {
                indentLineIfBlank();
                pwriter.append(this.line, this.offset, this.end);
                this.emitEndOfLine();
            }
            this.offset = this.end;
        }

        public void appendPartialLine(int appendEnd)
        {
            while(!this.isDoneWithLine() && Character.isWhitespace(this.line.charAt(this.offset)))
            {
                this.offset++;
            }
            if (this.offset != this.end)
            {
                indentLineIfBlank();
                pwriter.append(this.line, this.offset, appendEnd);
                this.offset = appendEnd;
            }
        }

        public void emitEndOfLineIfNotBlank()
        {
            if (!this.currentLineIsBlank)
            {
                this.emitEndOfLine();
            }
        }

        public void emitWhite()
        {
            if (!currentLineIsBlank)
            {
                pwriter.append(' ');
            }
        }

        public void emitChar(char c)
        {
            if (currentLineIsBlank && previousLineWasBlank && c != '}' && c != '}' && !line.regionMatches(false, this.offset, "else", 0, "else".length()))
            {
                emitEndOfLine();
            }
            indentLineIfBlank();
            pwriter.write(c);
            currentLineIsBlank = false;
        }

        public void indentLineIfBlank()
        {
            if (currentLineIsBlank)
            {
                this.indentLine();
                currentLineIsBlank = false;
            }
        }

        public void emitEndOfLineIfDone()
        {
            if (this.isDoneWithLine())
            {
                this.emitEndOfLineIfNotBlank();
            }
        }
    }

    private interface FormatState
    {
        public ProcessingState formatPartialLine(ProcessingState processingState);
    }

    private static class NormalState implements FormatState
    {
        @Override
        public ProcessingState formatPartialLine(ProcessingState processingState)
        {
            char lastNormalChar = 0;
            for(;processingState.offset < processingState.end;processingState.offset++)
            {
                //look for quotes, comments, braces
                char c = processingState.line.charAt(processingState.offset);
                boolean emitWhite = false;
                while(Character.isWhitespace(c) && processingState.offset < processingState.end)
                {
                    emitWhite = true;
                    c = processingState.line.charAt(++processingState.offset);
                }
                if (emitWhite)
                {
                    processingState.emitWhite();
                    if (processingState.isDoneWithLine())
                    {
                        processingState.emitEndOfLineIfNotBlank();
                        return processingState;
                    }
                }
                switch(c)
                {
                    case '\"':
                        processingState.emitChar(c);
                        processingState.offset++;
                        processingState.formatState = DOUBLE_QUOTE_STATE;
                        return processingState;
                    case '\'':
                        processingState.emitChar(c);
                        processingState.offset++;
                        processingState.formatState = SINGLE_QUOTE_STATE;
                        return processingState;
                    case '{':
                        processingState.emitEndOfLineIfNotBlank();
                        processingState.emitChar(c);
                        processingState.indent++;
                        processingState.emitEndOfLine();
                        break;
                    case '}':
                        processingState.emitEndOfLineIfNotBlank();
                        processingState.indent--;
                        processingState.emitChar(c);
                        processingState.emitEndOfLine();
                        processingState.emitBlankLine();
                        break;
                    case '/':
                        if (processingState.offset == processingState.end - 1)
                        {
                            processingState.emitChar(c);
                        }
                        else
                        {
                            char nextChar = processingState.line.charAt(processingState.offset + 1);
                            if (nextChar == '*')
                            {
                                processingState.emitChar('/');
                                processingState.emitChar('*');
                                processingState.offset += 2;
                                processingState.formatState = LONG_COMMENT_STATE;
                                return processingState;
                            }
                            else if (nextChar == '/')
                            {
                                processingState.appendRemainingLineWithEndOfLine();
                                return processingState;
                            }
                            else
                            {
                                processingState.emitChar(c);
                            }
                        }
                        break;
                    default:
                        processingState.emitChar(c);
                        lastNormalChar = c;
                        break;
                }
            }
            if (lastNormalChar == ',' || lastNormalChar == '(' || lastNormalChar == ':')
            {
                processingState.tempIndent = true;
            }
            processingState.emitEndOfLineIfNotBlank();
            return processingState;
        }
    }

    private static class LongCommentState implements FormatState
    {
        @Override
        public ProcessingState formatPartialLine(ProcessingState processingState)
        {
            if (processingState.isDoneWithLine())
            {
                processingState.emitBlankLine();
                return processingState;
            }
            int endComment = processingState.line.indexOf("*/", processingState.offset);
            if (endComment >=0 )
            {
                processingState.appendPartialLine(endComment + 2);
                processingState.offset = endComment + 2;
                processingState.emitEndOfLineIfDone();
                processingState.formatState = NORMAL_STATE;
                return processingState;
            }
            else
            {
                processingState.appendRemainingLineWithEndOfLine();
                return processingState;
            }
        }
    }

    private static class QuoteState implements FormatState
    {
        private char quoteChar;

        private QuoteState(char quoteChar)
        {
            this.quoteChar = quoteChar;
        }

        @Override
        public ProcessingState formatPartialLine(ProcessingState processingState)
        {
            for(;processingState.offset < processingState.end;processingState.offset++)
            {
                //look for quote, and escape char
                char c = processingState.line.charAt(processingState.offset);
                if (c == quoteChar)
                {
                    processingState.emitChar(c);
                    processingState.offset++;
                    processingState.emitEndOfLineIfDone();
                    processingState.formatState = NORMAL_STATE;
                    return processingState;
                }
                else if (c == '\\')
                {
                    processingState.emitChar(c);
                    if(processingState.offset < processingState.end - 1)
                    {
                        processingState.offset++;
                        processingState.emitChar(processingState.line.charAt(processingState.offset));
                    }
                }
                else
                {
                    processingState.emitChar(c);
                }
            }
            processingState.offset = processingState.end;
            processingState.emitEndOfLineIfNotBlank();
            processingState.formatState = NORMAL_STATE;
            return processingState;
        }
    }

}
