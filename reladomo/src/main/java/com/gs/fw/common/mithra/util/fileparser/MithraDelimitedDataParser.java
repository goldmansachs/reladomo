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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;

import java.io.*;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class MithraDelimitedDataParser
{
    private String bcpFilename;
    private String delimiter;
    private List<MithraParsedData> results;
    private List<Attribute>attributes;
    private Format dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public MithraDelimitedDataParser(String bcpFilename, String delimiter, List<Attribute> attributes, String dateFormatString)
    {
        this.bcpFilename = bcpFilename;
        this.delimiter = delimiter;
        this.attributes = attributes;
        if(dateFormatString != null && !dateFormatString.equals(""))
        {
            this.dateFormat = new SimpleDateFormat(dateFormatString);
        }
        this.validateAttributes();
    }

    public MithraDelimitedDataParser(String bcpFilename, String delimiter, List<Attribute> attributes, Format dateFormat)
    {
        this.bcpFilename = bcpFilename;
        this.delimiter = delimiter;
        this.attributes = attributes;
        if(dateFormat != null)
        {
            this.dateFormat = dateFormat;
        }
        this.validateAttributes();
    }

    public String getBcpFilename()
    {
        return bcpFilename;
    }

    public String getDelimeter()
    {
        return delimiter;
    }

    public void validateAttributes()
    {
        MithraObjectPortal initialPortal = attributes.get(0).getOwnerPortal();
        for(int i = 1 ; i < attributes.size(); i++)
        {
            if(attributes.get(i).getOwnerPortal() != initialPortal)
            {
                throw new RuntimeException("Invalid attribute "+attributes.get(i).getAttributeName()+" does not" +
                        " belong to the same class "+initialPortal.getFinder().getClass().getName());
            }
        }
    }

    private void close(Closeable c)
    {
        try
        {
            c.close();
        }
        catch (IOException e)
        {
            //ignore
        }
    }

    public void parse()
    {
        String line;
        this.results = new ArrayList<MithraParsedData>();
        BufferedReader reader;

        InputStream is = this.getClass().getClassLoader().getResourceAsStream(this.getBcpFilename());
        if (is == null)
        {
            File file = new File(this.getBcpFilename());
            if(file.exists() && file.canRead()){
                try
                {
                    is = new FileInputStream(file);
                }
                catch (FileNotFoundException e)
                {
                    String msg = "could not find file "+this.getBcpFilename()+" on the classpath nor is "
                                 + this.getBcpFilename() + " a valid full path file name";
                    throw new RuntimeException(msg,e);
                }
            }
        }
        reader = new BufferedReader(new InputStreamReader(is));
        MithraParsedData parsedData = new MithraParsedData();
        this.results.add(parsedData);

        try
        {
           parsedData.setParsedClassName(this.getFullyQualifiedClassname());
        }
        catch(Exception e)
        {
            throw new RuntimeException("could not initialize class "+this.getFullyQualifiedClassname(), e);
        }

        parsedData.setAttributes(attributes);

        int lineNo = 1;
        try
        {
            while ((line = reader.readLine()) != null)
            {
                int attributeNo = 0;
                MithraDataObject mithraDataObject = parsedData.createAndAddDataObject(lineNo);

                StringTokenizer tokenizer = new StringTokenizer(line, this.getDelimeter(), true);
                String token = null;
                String previousToken = null;
                while (tokenizer.hasMoreElements())
                {
                    previousToken = token;
                    token = tokenizer.nextToken();
                    if(!token.equals(this.getDelimeter()))
                    {
                        parsedData.parseStringData(token, attributeNo, mithraDataObject, lineNo, this.dateFormat);
                        attributeNo++;
                    }
                    else if (this.getDelimeter().equals(previousToken) || previousToken == null)
                    {
                        parsedData.parseStringData("", attributeNo, mithraDataObject, lineNo, this.dateFormat);
                        attributeNo++;
                    }
                }
                if(token.equals(this.getDelimeter()))
                {
                    parsedData.parseStringData("", attributeNo, mithraDataObject, lineNo, this.dateFormat);
                    attributeNo++;
                }
                lineNo++;
            }
        }
        catch (ParseException e)
        {
            throw new RuntimeException("error while parsing "+this.getBcpFilename()+" on line: "+lineNo, e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("I/O error while reading "+this.getBcpFilename()+" line: "+lineNo, e);
        }
        finally
        {
            close(reader);
            close(is);
        }
    }

    private String getFullyQualifiedClassname()
    {
        return attributes.get(0).getOwnerPortal().getClassMetaData().getBusinessOrInterfaceClassName();
    }

    public List<MithraParsedData> getResults()
    {
        this.parse();
        return results;
    }
}
