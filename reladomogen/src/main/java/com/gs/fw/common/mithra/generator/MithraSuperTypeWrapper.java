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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;


public class MithraSuperTypeWrapper implements CommonWrapper
{

    private static Map<String, StringBuilder> abstractTemplateCache = new HashMap<String, StringBuilder>();

    private String packageName;
    private String superClass;
    private String className;
    private boolean isDated;

    private Set<String> imports;

    private boolean isWritten;

    public String getPackageName()
    {
        return packageName;
    }

    public boolean hasSuperClass()
    {
        return this.superClass != null;
    }

    public String getClassName()
    {
        return className;
    }

    public String getFullyQualifiedSuperClassType()
    {
        return superClass;
    }

    public String getMithraInterfaceName()
    {
        if (this.isDated)
        {
            return "MithraDatedTransactionalObject";
        }
        return "MithraTransactionalObject";
    }

    public void writeAbstractTemplate(com.gs.fw.common.mithra.generator.JspWriter out, String templateSection) throws IOException
    {
        StringBuilder template = new StringBuilder(getCachedAbstractTemplate(templateSection));
        String toReplace = "MithraTransactionalObjectImpl";
        if (isDated)
        {
            toReplace = "MithraDatedTransactionalObjectImpl";
        }
        int pos = template.indexOf(toReplace);
        while(pos >= 0)
        {
            template.replace(pos, toReplace.length() + pos, this.getClassName());
            pos = template.indexOf(toReplace);
        }
        out.write(template.toString());
    }

    private StringBuilder getCachedAbstractTemplate(String templateSection)
    {
        String templateName = "com/gs/fw/common/mithra/generator/";
        if (this.isDated)
        {
            templateName += "Dated";
        }
        templateName += "Transactional";
        templateName += ".tmpl."+templateSection;
        StringBuilder builder = abstractTemplateCache.get(templateName);
        if (builder == null)
        {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(templateName);
            builder = new StringBuilder();
            byte[] buf = new byte[1024];
            if (is != null)
            {
                int read;
                try
                {
                    while((read = is.read(buf)) >= 0)
                    {
                        builder.append(new String(buf, 0, read));
                    }
                    is.close();
                }
                catch (IOException e)
                {
                    throw new RuntimeException("could not read template "+templateName, e);
                }
            }
            else
            {
                throw new RuntimeException("The template "+templateName+" is missing from the classpath");
            }
            abstractTemplateCache.put(templateName, builder);
        }
        return builder;
    }


    public String getFullyQualifiedClassName()
    {
        return this.packageName+"."+this.className;
    }

    public void setClassName(String className)
    {
        if (className.indexOf('.') >= 0)
        {
            this.className = className.substring(className.lastIndexOf('.')+1);
            this.packageName = className.substring(0, className.lastIndexOf('.'));
        }
        else
        {
            this.className = className;
        }
    }

    public void setSuperClass(String fullyQualifiedSuperClassType)
    {
        this.superClass = fullyQualifiedSuperClassType;
    }

    public void setDated(boolean isDated)
    {
        this.isDated = isDated;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MithraSuperTypeWrapper that = (MithraSuperTypeWrapper) o;

        if (!className.equals(that.className)) return false;
        if (packageName != null ? !packageName.equals(that.packageName) : that.packageName != null) return false;

        return true;
    }

    public int hashCode()
    {
        int result;
        result = (packageName != null ? packageName.hashCode() : 0);
        result = 31 * result + className.hashCode();
        return result;
    }

    public void setPackageName(String packageName)
    {
        this.packageName = packageName;
    }

    public boolean isWritten()
    {
        return isWritten;
    }

    public void setWritten(boolean written)
    {
        isWritten = written;
    }

    public Set<String> getImports()
    {
        return imports;
    }

    public void setImports(Set<String> imports)
    {
        this.imports = imports;
    }
}
