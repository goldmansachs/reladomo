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


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class TemplateMinifier
{

    private String templateDir;

    public TemplateMinifier(String templateDir)
    {
        this.templateDir = templateDir;
    }

    public void cleanTemplates() throws IOException
    {
        File dir = new File(templateDir);
        File[] files = dir.listFiles();
        ArrayList<File> list = new ArrayList(Arrays.asList(files));
        StringBuilder builder = new StringBuilder(200000);
        for(int i=0;i<list.size();i++)
        {
            File f = list.get(i);
            if (f.isDirectory())
            {
                list.addAll(Arrays.asList(f.listFiles()));
            }
            else if (f.getName().endsWith(".java"))
            {
                minify(f, builder);
            }
        }
    }

    private void minify(File f, StringBuilder builder) throws IOException
    {
        builder.setLength(0);
        ArrayList<BufferedWrite> lineBuffer = new ArrayList();
        ArrayList<WriteMethod> methodBuffer = new ArrayList();
        StringBuilder tempArg = new StringBuilder(200);
        LineNumberReader reader = new LineNumberReader(new FileReader(f));
        String line;
        while((line = reader.readLine()) != null)
        {
            line = line.trim();
            if (line.startsWith("out."))
            {
                int end = line.lastIndexOf(')');
                if (end > 0 && line.regionMatches(false, 4, "print(", 0, 6))
                {
                    lineBuffer.add(new BufferedWrite(true, line.substring(10, end)));
                }
                else if (end > 0 && line.regionMatches(false, 4, "write(", 0, 6))
                {
                    String argument = line.substring(10, end);
                    argument = cleanWrite(argument, tempArg, lineBuffer);
                    if (!argument.isEmpty())
                    {
                        lineBuffer.add(new BufferedWrite(false, argument));
                    }
                }
                else
                {
                    writeBuffered(lineBuffer, methodBuffer, builder);
                    builder.append(line).append("\n");
                }
            }
            else
            {
                line = replaceServletStuff(line);
                if (!line.isEmpty())
                {
                    writeBuffered(lineBuffer, methodBuffer, builder);
                    builder.append(line).append("\n");
                }
            }
        }
        reader.close();
        int lastBrace = builder.lastIndexOf("}");
        builder.setLength(lastBrace);
        addMethods(builder, methodBuffer);
        builder.append("}\n");
        FileOutputStream out = new FileOutputStream(f);
        PrintWriter writer = new PrintWriter(out);
        reader = new LineNumberReader(new StringReader(builder.toString()));
        SourceFormatter formatter = new SourceFormatter();
        while((line = reader.readLine()) != null)
        {
            formatter.formatLine(line, writer);
        }
        writer.close();
        out.close();
    }

    private void addMethods(StringBuilder builder, ArrayList<WriteMethod> methodBuffer)
    {
        for(int i=0;i<methodBuffer.size();i++)
        {
            WriteMethod method = methodBuffer.get(i);
            builder.append("private void writeMany").append(method.methodNumber).append("(JspWriter out");
            for(int j=0;j<method.bufferedWrites.size();j++)
            {
                BufferedWrite bufferedWrite = method.bufferedWrites.get(j);
                if (bufferedWrite.isPrint)
                {
                    builder.append(", Object a").append(bufferedWrite.argNumber);
                }
            }
            builder.append(")\n{\n");
            int multiCallCount = 0;
            int multiCountStart = -1;
            for(int j=0;j<method.bufferedWrites.size();j++)
            {
                BufferedWrite bufferedWrite = method.bufferedWrites.get(j);
                if (multiCallCount % 2 == 0 && bufferedWrite.isWrite())
                {
                    multiCallCount++;
                    if (multiCountStart == -1)
                    {
                        multiCountStart = j;
                    }
                }
                else if (multiCallCount % 2 == 1 && bufferedWrite.isPrint)
                {
                    multiCallCount++;
                }
                else
                {
                    writeMulti(builder, method, multiCallCount, multiCountStart);
                    multiCallCount = 0;
                    multiCountStart = -1;
                    writeOne(builder, bufferedWrite);
                }
                if (multiCallCount == 6)
                {
                    writeMulti(builder, method, multiCallCount, multiCountStart);
                    multiCallCount = 0;
                    multiCountStart = -1;
                }
            }
            writeMulti(builder, method, multiCallCount, multiCountStart);
            builder.append("}");
        }
    }

    private void writeOne(StringBuilder builder, BufferedWrite bufferedWrite)
    {
        builder.append("out.");
        if (bufferedWrite.isPrint)
        {
            builder.append("print(").append('a').append(bufferedWrite.argNumber);
        }
        else
        {
            builder.append("write(").append(bufferedWrite.buffer);
        }
        builder.append(");\n");
    }

    private void writeMulti(StringBuilder builder, WriteMethod method, int multiCallCount, int multiCountStart)
    {
        if (multiCallCount > 1)
        {
            builder.append("out.writeMany").append(multiCallCount).append('(').append(method.bufferedWrites.get(multiCountStart).buffer);
            for (int k = multiCountStart + 1; k < multiCallCount + multiCountStart; k++)
            {
                builder.append(",\n");
                BufferedWrite argWrite = method.bufferedWrites.get(k);
                if (argWrite.isPrint)
                {
                    builder.append('a').append(argWrite.argNumber);
                }
                else
                {
                    builder.append(argWrite.buffer);
                }
            }
            builder.append(");\n");
        }
        else if (multiCallCount == 1)
        {
            writeOne(builder, method.bufferedWrites.get(multiCountStart));
        }
    }

    private void writeBuffered(ArrayList<BufferedWrite> lineBuffer, ArrayList<WriteMethod> methodBuffer, StringBuilder builder)
    {
        if (lineBuffer.size() > 1)
        {
            WriteMethod method = new WriteMethod();
            method.methodNumber = methodBuffer.size();
            methodBuffer.add(method);
            method.bufferedWrites = new ArrayList<BufferedWrite>(lineBuffer);
            builder.append("writeMany").append(method.methodNumber).append("(out");
            int argNumber = 0;
            for (int i = 0; i < lineBuffer.size(); i++)
            {
                BufferedWrite bufferedWrite = lineBuffer.get(i);
                if (bufferedWrite.isPrint)
                {
                    bufferedWrite.argNumber = argNumber++;
                    builder.append(",\n");
                    builder.append(bufferedWrite.buffer);
                }
            }
            builder.append(");\n");
        }
        else if (lineBuffer.size() == 1)
        {
            BufferedWrite bufferedWrite = lineBuffer.get(0);
            if (bufferedWrite.isPrint)
            {
                builder.append("out.print(").append(bufferedWrite.buffer).append(");\n");
            }
            else
            {
                if (bufferedWrite.buffer.equals("\"\\n\""))
                {
                    builder.append("out.writeEndOfLine();\n");
                }
                else
                {
                    builder.append("out.write(").append(bufferedWrite.buffer).append(");\n");
                }
            }
        }
        lineBuffer.clear();
    }

    private String cleanWrite(String argument, StringBuilder tempArg, ArrayList<BufferedWrite> lineBuffer)
    {
        argument = argument.trim();
        if (argument.equals("'\r'"))
        {
            return "";
        }
        if (argument.equals("'\"'"))
        {
            argument = "\"\\\"\"";
        }
        tempArg.setLength(0);
        boolean isAfterReturn = false;
        for(int i=0;i<argument.length();i++)
        {
            char c = argument.charAt(i);
            switch(c)
            {
                case '\\':
                    if (i < argument.length() - 1)
                    {
                        i++;
                        c = argument.charAt(i);
                        switch (c)
                        {
                            case 'r':
                                break;
                            case 'n':
                                if (!isAfterReturn)
                                {
                                    isAfterReturn = true;
                                    tempArg.append('\\').append('n');
                                }
                                break;
                            default:
                                isAfterReturn = false;
                                tempArg.append('\\').append(c);
                                break;
                        }
                    }
                    break;
                case ' ':
                    if (!isAfterReturn)
                    {
                        tempArg.append(' ');
                    }
                    break;
                default:
                    tempArg.append(c);
                    isAfterReturn = false;
                    break;
            }
        }
        if (tempArg.charAt(0) == '\'')
        {
            tempArg.setCharAt(0, '"');
        }
        if (tempArg.charAt(tempArg.length() - 1) == '\'')
        {
            tempArg.setCharAt(tempArg.length() - 1, '"');
        }
        if(!lineBuffer.isEmpty() && lineBuffer.get(lineBuffer.size() - 1).isWrite())
        {
            // two consecutive writes, let's see if we should combine them
            BufferedWrite lastWrite = lineBuffer.get(lineBuffer.size() - 1);
            if (lastWrite.buffer.length() < 100)
            {
                tempArg.deleteCharAt(0);
                tempArg.insert(0, lastWrite.buffer, 0, lastWrite.buffer.length() - 1);
                lastWrite.buffer = tempArg.toString();
                tempArg.setLength(0);
            }
        }
        return tempArg.toString();
    }

    private String replaceServletStuff(String line)
    {
        line = line.replace("import javax.servlet.*;", "import java.io.*;");
        line = line.replace("implements org.apache.jasper.runtime.JspSourceDependent", "implements MithraTemplate");
        line = remove(line, "extends org.apache.jasper.runtime.HttpJspBase");
        line = remove(line, "HttpSession session = null;");
        line = remove(line, "ServletContext application = null;");
        line = remove(line, "ServletConfig config = null;");
        line = remove(line, "application = pageContext.getServletContext();");
        line = remove(line, "config = pageContext.getServletConfig();");
        line = remove(line, "session = pageContext.getSession();");
        line = remove(line, ", ServletException");
        if (line.contains(" javax.servlet"))
        {
            line = "";
        }
        else if (line.contains("_el_expressionfactory"))
        {
            line = "";
        }
        else if (line.contains("_jsp_annotationprocessor"))
        {
            line = "";
        }
        else if (line.contains("_jspx_dependants"))
        {
            if (line.contains("return"))
            {
                line = "return null;";
            }
            else
            {
                line = "";
            }
        }
        return line;
    }

    private String remove(String line, String toRemove)
    {
        line = line.replace(toRemove, "");
        return line;
    }

    private static class BufferedWrite
    {
        private boolean isPrint;
        private String buffer;
        private int argNumber;

        private BufferedWrite(boolean isPrint, String buffer)
        {
            this.isPrint = isPrint;
            this.buffer = buffer;
        }

        public boolean isWrite()
        {
            return !isPrint;
        }

        public String getArgumentType()
        {
            if (isWrite()) return "String";
            return "Object";
        }
    }

    private static class WriteMethod
    {
        private int methodNumber;
        ArrayList<BufferedWrite> bufferedWrites;

    }

    public static void main(String[] args) throws IOException
    {
        TemplateMinifier templateMinifier = new TemplateMinifier(args[0]);
        templateMinifier.cleanTemplates();
    }
}
