

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

package com.gs.fw.common.mithra.generator.dbgenerator;

import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.Cardinality;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.RelationshipAttribute;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;
import com.gs.fw.common.mithra.generator.mapper.Join;
import com.gs.fw.common.mithra.generator.mapper.RelationshipConversionVisitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;

public abstract class AbstractGeneratorDatabaseType
{
    protected final int DEFAULT_VARCHAR_MAX_LENGTH = 255;

    private final HashMap<String, HashSet> foreignKeysByTable = new HashMap<String, HashSet>();

    protected AbstractGeneratorDatabaseType()
    {
    }

    protected abstract DatabaseType getDatabaseType();

    protected abstract CommonDatabaseType getGeneratorDatabaseType();

    public abstract void generateDdlFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException;

    public abstract void generateIdxFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException;

    public abstract void generateFkFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException;

    public abstract String getStatementTerminator();

    protected PrintWriter getDdlPrintWriter(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        File outDdl = new File(outDir, wrapper.getDefaultTable() + ".ddl");
        return new PrintWriter(new BufferedWriter(new FileWriter(outDdl)));
    }

    protected PrintWriter getIdxPrintWriter(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        File outIdx = new File(outDir, wrapper.getDefaultTable() + ".idx");
        return new PrintWriter(new BufferedWriter(new FileWriter(outIdx)));
    }

    protected void generateDdlColumnList(Attribute[] attributes, PrintWriter writer)
    {
        for (int i = 0; i < attributes.length; i++)
        {
            String attributeSqlType = attributes[i].getType().getSqlDataType(this.getGeneratorDatabaseType());

            if (attributes[i].isIdentity())
            {
                attributeSqlType += this.getDatabaseType().getIdentityTableCreationStatement();
            }
            else if (attributeSqlType.equalsIgnoreCase("varchar"))
            {
                int attributeMaxLength = attributes[i].getMaxLength();

                if (attributeMaxLength <= 0)
                {
                    attributeMaxLength = DEFAULT_VARCHAR_MAX_LENGTH;
                }

                attributeSqlType += "(" + attributeMaxLength + ')';
            }
            else if ( isNumericDataType(attributeSqlType)){
                int scale = attributes[i].getScale();
                int precision = attributes[i].getPrecision();
                attributeSqlType += "(" + precision + "," + scale + ')';
            }
            this.generateNullStatement(writer, attributes, attributeSqlType, i);
        }
    }

    protected boolean isNumericDataType(String attributeSqlType ){
        return  attributeSqlType.equalsIgnoreCase("numeric") ||
                attributeSqlType.equalsIgnoreCase("number") ||
                attributeSqlType.equalsIgnoreCase("decimal");

    }

    protected abstract void generateNullStatement(PrintWriter writer, Attribute[] attributes, String attributeSqlType, int i);

    protected void printFkFile(MithraObjectTypeWrapper wrapper, File outDir, AbstractGeneratorDatabaseType generatorDatabaseType)
            throws IOException
    {
        RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
        if (wrapper.isPure()) return;
        for (int i = 0; i < relationshipAttributes.length; i++)
        {
            /* Determine if we want to put the foreign key in the 'from' or the 'to' object */
            boolean isFkInFrom = true;
            File outFk;

            MithraObjectTypeWrapper relatedObject = relationshipAttributes[i].getRelatedObject();
            if (relatedObject.isPure()) continue;
            RelationshipConversionVisitor visitor = relationshipAttributes[i].getMapperVisitor();

            /* Must be joined directly and the relationship attribute cannot be parameterized or have filters */
            if (visitor.isNextObjectInJoinRelatedObject() && !relationshipAttributes[i].hasParameters() && ! relationshipAttributes[i].hasFilters())
            {
                Cardinality cardinality = relationshipAttributes[i].getCardinality();
                if ((cardinality.isFromMany() && !cardinality.isToMany()) || (!cardinality.isFromMany() && cardinality.isToMany()))
                {
                    boolean doAppend;

                    HashSet relationships;

                    /* We cannot create a foreign key in a dated object */
                    if (relatedObject.hasAsOfAttributes())
                    {
                        continue;
                    }

                    if (cardinality.isToMany())
                    {
                        isFkInFrom = false;

                        outFk = new File(outDir, relatedObject.getDefaultTable() + ".fk");
                        doAppend = !relatedObject.isFirstFkFileWrite();
                        relatedObject.setNotFirstFkFileWrite();
                        relationships = this.foreignKeysByTable.get(relatedObject.getDefaultTable());
                        if (relationships == null)
                        {
                            relationships = new HashSet();
                            this.foreignKeysByTable.put(relatedObject.getDefaultTable(), relationships);
                        }
                    }
                    else
                    {
                        outFk = new File(outDir, wrapper.getDefaultTable() + ".fk");
                        doAppend = !wrapper.isFirstFkFileWrite();
                        wrapper.setNotFirstFkFileWrite();
                        relationships = this.foreignKeysByTable.get(wrapper.getDefaultTable());
                        if (relationships == null)
                        {
                            relationships = new HashSet();
                            this.foreignKeysByTable.put(wrapper.getDefaultTable(), relationships);
                        }
                    }

                    Attribute[] attributesFk;
                    Attribute[] attributesReference;

                    Join join = visitor.getJoinedToThis();

                    if (isFkInFrom)
                    {
                        attributesFk = join.getLeftRelationshipAttributesAsArray();
                        attributesReference = join.getRightRelationshipAttributesAsArray();
                    }
                    else
                    {
                        attributesFk = join.getRightRelationshipAttributesAsArray();
                        attributesReference = join.getLeftRelationshipAttributesAsArray();
                    }

                    if (attributesFk.length > 0)
                    {
                        boolean illegalFk = false;
                        for (int j = 0; j < attributesReference.length; j++)
                        {
                            /* Check if attributes are dated -- fk are illegal for such tables */
                            if (attributesReference[j].isAsOfAttribute())
                            {
                                illegalFk = true;
                                break;
                            }
                            if (attributesFk[j].isAsOfAttribute())
                            {
                                illegalFk = true;
                                break;
                            }
                            if (attributesReference[j].isSourceAttribute() && !attributesFk[j].isSourceAttribute())
                            {
                                illegalFk = true;
                                break;
                            }
                            if (!attributesReference[j].isSourceAttribute() && attributesFk[j].isSourceAttribute())
                            {
                                illegalFk = true;
                                break;
                            }
                        }
                        if (illegalFk) continue;

                        ForeignKey fk = null;

                        if (isFkInFrom)
                        {
                            fk = new ForeignKey(relatedObject, wrapper);
                        }
                        else
                        {
                            fk = new ForeignKey(wrapper, relatedObject);
                        }

                        for (int j = 0; j < attributesFk.length; j++)
                        {
                            if (!attributesFk[j].isSourceAttribute())
                            {
                                fk.addFromColumn(attributesFk[j].getColumnName());
                            }
                        }

                        for (int j = 0; j < attributesReference.length; j++)
                        {
                            if (!attributesReference[j].isSourceAttribute())
                            {
                                fk.addToColumn(attributesReference[j].getColumnName());
                            }
                        }
                        if (!relationships.contains(fk))
                        {
                            relationships.add(fk);
                            /* Append because this file may have foreign keys for a number of different objects */
                            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outFk, doAppend)));
                            fk.printAlterTable(writer, generatorDatabaseType);
                            writer.println(" foreign key (");
                            fk.printFromColumns(writer);
                            writer.println("\n)");
                            fk.printReferences(writer);
                            fk.printToColumns(writer);
                            writer.print("\n)");
                            writer.println(generatorDatabaseType.getStatementTerminator());
                            writer.println();

                            writer.close();
                        }
                    }
                }
            }
        }
    }

    protected StringBuilder fixStringLength(String name, int targetLength)
    {
        StringBuilder builder = new StringBuilder(name);
        for(int i=0;i<builder.length() && builder.length() > targetLength;)
        {
            char c = builder.charAt(i);
            if (isVowel(c) && i != 0)
            {
                builder.deleteCharAt(i);
            }
            else
            {
                i++;
            }
        }
        for(int i=0;i<builder.length() && builder.length() > targetLength;)
        {
            char c = builder.charAt(i);
            if (c == '_')
            {
                builder.deleteCharAt(i);
            }
            else
            {
                i++;
            }
        }
        if (builder.length() > targetLength && targetLength > 8)
        {
            targetLength -= 8;
            String soFar = builder.toString();
            builder.setLength(0);
            int stair = soFar.length()/targetLength;
            if (soFar.length() % targetLength > 0) stair++;
            for(int i=0;i<soFar.length();i++)
            {
                if (builder.length()*stair <= i)
                {
                    builder.append(soFar.charAt(i));
                }
            }
            if (builder.length() < targetLength)
            {
                builder.append(soFar.charAt(soFar.length() - 1));
            }
            builder.append(Integer.toHexString(name.hashCode()).toUpperCase());
        }
        return builder;
    }

    private boolean isVowel(char c)
    {
        c = Character.toUpperCase(c);
        return c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U';
    }

    private String fixConstraint(String name, int extrasNeeded)
    {
        int target = this.getMaxConstraintLength() - extrasNeeded;
        if (name.length() < target) return name;
        return this.fixStringLength(name, target).toString();
    }

    protected abstract int getMaxConstraintLength();

    private static class ForeignKey
    {
        private MithraObjectTypeWrapper fromObject;
        private MithraObjectTypeWrapper toObject;
        private LinkedHashSet fromColumns;
        private LinkedHashSet toColumns;

        private ForeignKey(MithraObjectTypeWrapper fromObject, MithraObjectTypeWrapper toObject)
        {
            this.fromObject = fromObject;
            this.toObject = toObject;
            this.fromColumns = new LinkedHashSet();
            this.toColumns = new LinkedHashSet();
        }

        public void addFromColumn(String col)
        {
            this.fromColumns.add(col);
        }

        public void addToColumn(String col)
        {
            this.toColumns.add(col);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ForeignKey that = (ForeignKey) o;

            if (!toObject.getDefaultTable().equals(that.toObject.getDefaultTable())) return false;
            if (!fromColumns.equals(that.fromColumns)) return false;
            if (!toColumns.equals(that.toColumns)) return false;

            return true;
        }

        public int hashCode()
        {
            int result;
            result = toObject.getDefaultTable().hashCode();
            result = 31 * result + fromColumns.hashCode();
            result = 31 * result + toColumns.hashCode();
            return result;
        }

        public void printAlterTable(PrintWriter writer, AbstractGeneratorDatabaseType generatorDatabaseType)
        {
            writer.print("alter table " + toObject.getDefaultTable());
            String fk = "_fk_" + toObject.getFkCounterAndIncrement();
            writer.print(" add constraint "+generatorDatabaseType.fixConstraint(toObject.getDefaultTable(), fk.length()) + fk);
        }

        public void printColumns(PrintWriter writer, LinkedHashSet columns)
        {
            writer.print("    ");
            boolean wrote = false;
            for(Iterator it = columns.iterator();it.hasNext();)
            {
                if (wrote) writer.print(",");
                writer.print(it.next());
                wrote = true;
            }
        }

        public void printToColumns(PrintWriter writer)
        {
            printColumns(writer, toColumns);
        }

        public void printFromColumns(PrintWriter writer)
        {
            printColumns(writer, fromColumns);
        }

        public void printReferences(PrintWriter writer)
        {
            writer.println("references " + fromObject.getDefaultTable() + '(');
        }
    }
}
