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

import com.gs.fw.common.mithra.generator.mapper.Join;
import com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.*;

public class CoreMithraGraphGenerator extends BaseMithraGenerator
{
    //todo: tobinj: does not properly handle some many to many relationships like this one:
    // on the EntOrganisations class
    // <Relationship name="organizations" relatedObject="EntOrganizations" cardinality="many-to-many" reverseRelationshipName="countries">
    //        CountryToOrganization.cntryIsoCD=this.isoCode and
    //        EntOrganizations.orgnzEntityID = CountryToOrganization.orgnzEntityID
    //    </Relationship>
    // It labels the relationship wrongly.  Also, many to many relationships between 2 classes without join classes
    // in between have the arrow head at the wrong end of the line

    private boolean executed = false;
    private String outputFile = null;

    private ArrayList includeListArray = null;
    private boolean collapseRelationships = true;
    private ArrayList relationships = new ArrayList();
    private ArrayList objectsToDraw = new ArrayList();
    private ArrayList nonMithraSuperClasses = new ArrayList();
    private ArrayList<MithraInterfaceType> mithraInterfacesList = new ArrayList<MithraInterfaceType>();
    private int followRelationshipDepth = 1;
    private String showAttributes = "none";

    public String getOutputFile()
    {
        return outputFile;
    }

    public ArrayList getObjectsToDraw()
    {
        return objectsToDraw;
    }

    public ArrayList<MithraInterfaceType> getMithraInterfacesList()
    {
        return mithraInterfacesList;
    }

    public ArrayList getRelationships()
    {
        return relationships;
    }

    public ArrayList getNonMithraSuperClasses()
    {
        return nonMithraSuperClasses;
    }

    public void setIncludeList(String includeList)
    {
        if (includeList != null)
        {
            String[] buildListString = includeList.split(",");
            this.includeListArray = new ArrayList();

            for (int i = 0; i < buildListString.length; i++)
            {
                buildListString[i] = buildListString[i].trim();
                this.includeListArray.add(buildListString[i]);
            }
        }
        else
        {
            includeListArray = null;
        }
    }

    public void setShowAttributes(String showAttributes)
    {
        this.showAttributes = showAttributes;
    }

    public List getIncludeListArray()
    {
        return includeListArray;
    }

    public void setOutputFile(String outputFile)
    {
        this.outputFile = outputFile;
    }

    public void setFollowRelationshipDepth(int followRelationshipDepth)
    {
        this.followRelationshipDepth = followRelationshipDepth;
    }

    public void setCollapseRelationships(boolean collapseRelationships)
    {
        this.collapseRelationships = collapseRelationships;
    }

    public void printObjectSuperClass(PrintWriter writer, MithraObjectTypeWrapper mithraObjectTypeWrapper, int edgeCount)
    {
        String superClass = mithraObjectTypeWrapper.getFullyQualifiedSuperClassType();
        if (superClass != null)
        {
            if (mithraObjectTypeWrapper.getSuperClassWrapper() != null)
            {
                superClass = mithraObjectTypeWrapper.getSuperClassWrapper().getClassName();
            }
            String sourceArrow = "none";
            String color = "#000000";
            writer.println("<edge id=\"s"+edgeCount+"\" source=\""+mithraObjectTypeWrapper.getClassName()+
                    "\" target=\""+superClass+"\">");
            writer.println("<data key=\"d2\" >");
            writer.println("        <y:PolyLineEdge >\n" +
                    "          <y:Path sx=\"0.0\" sy=\"0.0\" tx=\"0.0\" ty=\"0.0\"/>");
            writer.println("<y:LineStyle type=\"line\" width=\"1.0\" color=\""+color+"\" />\n" +
                    "          <y:Arrows source=\""+sourceArrow+"\" target=\"white_delta\"/>\n" +
                    "          <y:BendStyle smoothed=\"false\"/>\n" +
                    "        </y:PolyLineEdge>\n" +
                    "      </data></edge>");
        }
    }


    public void printObjectInterface(PrintWriter writer, MithraObjectTypeWrapper mithraObjectTypeWrapper, int edgeCount)
    {
        MithraInterfaceType[] implementingInterfaces = mithraObjectTypeWrapper.getMithraInterfaces();
        if (implementingInterfaces.length > 0)
        {
            for (int i=0;i<implementingInterfaces.length;i++)
            {
                MithraInterfaceType interfaceType = implementingInterfaces[i];
                String sourceArrow = "none";
                String color = "#000000";
                writer.println("<edge id=\"ea"+edgeCount+"x"+i+"\" source=\""+mithraObjectTypeWrapper.getClassName()+
                        "\" target=\""+interfaceType.getClassName()+"\">");
                writer.println("<data key=\"d2\" >");
                writer.println("        <y:PolyLineEdge >\n" +
                        "          <y:Path sx=\"0.0\" sy=\"0.0\" tx=\"0.0\" ty=\"0.0\"/>");
                writer.println("<y:LineStyle type=\"line\" width=\"1.0\" color=\""+color+"\" />\n" +
                        "          <y:Arrows source=\""+sourceArrow+"\" target=\"standard\"/>\n");
                writer.println(
                        "          <y:BendStyle smoothed=\"false\"/>\n" +
                                "        </y:PolyLineEdge>\n" +
                                "      </data></edge>");
            }
        }
    }

    public void printNonMithraClass(PrintWriter writer, String superClass)
    {
        int width = 10 * superClass.length();
        int height = 60;
        String color = "#00CC00";
        writer.println("    <node id=\""+superClass+"\">\n" +
                "      <data key=\"d0\" >\n" +
                "        <y:UMLClassNode >\n" +
                "          <y:Geometry  x=\"218.5\" y=\"205.0\" width=\""+width+"\" height=\""+height+"\"/>\n" +
                "          <y:Fill color=\""+color+"\"  transparent=\"false\"/>\n" +
                "          <y:BorderStyle type=\"line\" width=\"1.0\" color=\"#000000\" />\n" +
                "          <y:NodeLabel x=\"23.0\" y=\"3.0\" width=\"68.0\" height=\"19.92626953125\" visible=\"true\" alignment=\"center\" fontFamily=\"Dialog\" fontSize=\"13\" fontStyle=\"bold\" textColor=\"#000000\" hasBackgroundColor=\"false\" hasLineColor=\"false\" modelName=\"internal\" modelPosition=\"c\" autoSizePolicy=\"content\">");
        writer.println(justClassName(superClass));
        writer.println("</y:NodeLabel>\n" +
                "          <y:UML clipContent=\"true\" omitDetails=\"false\" use3DEffect=\"true\" stereotype=\"\" constraint=\"\">\n");
        writer.println("            <y:MethodLabel></y:MethodLabel>\n" +
                "          </y:UML>\n" +
                "        </y:UMLClassNode>\n" +
                "      </data>");
        writer.println("    </node>");
    }

    public void printMithraInterface(PrintWriter writer, MithraInterfaceType mithraInterfaceType)
    {
        List attributes = new ArrayList();
        if (this.showAttributes.equalsIgnoreCase("all"))
        {
            if (mithraInterfaceType.hasSourceAttribute())
            {
                attributes.add(mithraInterfaceType.getSourceAttribute());
            }
            if (mithraInterfaceType.getAsOfAttributes().size() > 0)
            {
                attributes.addAll(Arrays.asList(mithraInterfaceType.getAsOfAttributes()));
            }
            attributes.addAll(Arrays.asList(mithraInterfaceType.getAttributes()));
        }

        int totalAttributes = attributes.size();
        int width = 10 * mithraInterfaceType.getClassName().length();
        int height = 60 + totalAttributes;
        if (attributes.size() > 0)
        {
            height += 15;
        }

        for (int i = 0; i < attributes.size(); i++)
        {
            int attrWidth = 7 * (getFullAttributeName((AbstractAttribute) attributes.get(i))).length();
            if (attrWidth > width) width = attrWidth;
        }
        String color = "#00C000";
        writer.println("    <node id=\"" + mithraInterfaceType.getClassName() + "\">\n" +
                "      <data key=\"d0\" >\n" +
                "        <y:UMLClassNode >\n" +
                "          <y:Geometry  x=\"218.5\" y=\"205.0\" width=\"" + width + "\" height=\"" + height + "\"/>\n" +
                "          <y:Fill color=\"" + color + "\"  transparent=\"false\"/>\n" +
                "          <y:BorderStyle type=\"line\" width=\"1.0\" color=\"#000000\" />\n" +
                "          <y:NodeLabel x=\"23.0\" y=\"3.0\" width=\"68.0\" height=\"19.92626953125\" visible=\"true\" alignment=\"center\" fontFamily=\"Dialog\" fontSize=\"13\" fontStyle=\"bold\" textColor=\"#000000\" hasBackgroundColor=\"false\" hasLineColor=\"false\" modelName=\"internal\" modelPosition=\"c\" autoSizePolicy=\"content\">");
        writer.println(mithraInterfaceType.getClassName());
        writer.println("</y:NodeLabel>\n" +
                "          <y:UML clipContent=\"true\" omitDetails=\"false\" use3DEffect=\"true\" stereotype=\"\" constraint=\"\">\n" +
                "            <y:AttributeLabel>");
        for (int i = 0; i < attributes.size(); i++)
        {
            writer.println(getFullAttributeName((AbstractAttribute) attributes.get(i)));
        }
        writer.println("</y:AttributeLabel>\n" +
                "            <y:MethodLabel></y:MethodLabel>\n" +
                "          </y:UML>\n" +
                "        </y:UMLClassNode>\n" +
                "      </data>");
        writer.println("    </node>");
    }


    public void printMithraSuperInterfacesEdges(PrintWriter writer, MithraInterfaceType mithraInterfaceType, int edgeCount)
    {
        List<String> superInterfaces = mithraInterfaceType.getSuperInterfaces();
        if (superInterfaces.size() > 0)
        {
            for (int i = 0; i < superInterfaces.size(); i++)
            {
                String currInterfaceName = superInterfaces.get(i);
                MithraInterfaceType currSuperInterface = getMithraInterfaces().get(currInterfaceName);

                String sourceArrow = "none";
                String color = "#C00000";
                writer.println("<edge id=\"s" + edgeCount + "x" + i + "\" source=\"" + mithraInterfaceType.getClassName() +
                        "\" target=\"" + currSuperInterface.getClassName() + "\">");
                writer.println("<data key=\"d2\" >");
                writer.println("        <y:PolyLineEdge >\n" +
                        "          <y:Path sx=\"0.0\" sy=\"0.0\" tx=\"0.0\" ty=\"0.0\"/>");
                writer.println("<y:LineStyle type=\"line\" width=\"1.0\" color=\"" + color + "\" />\n" +
                        "          <y:Arrows source=\"" + sourceArrow + "\" target=\"white_delta\"/>\n" +
                        "          <y:BendStyle smoothed=\"false\"/>\n" +
                        "        </y:PolyLineEdge>\n" +
                        "      </data></edge>");

            }
        }
    }

    private String justClassName(String superClass)
    {
        if (superClass.indexOf(".") > 0)
        {
            superClass = superClass.substring(superClass.lastIndexOf(".")+1,superClass.length());
        }
        return superClass;
    }

    public void processNonMithraSuperClasses()
    {
        HashSet nonMithraSuperClasses = new HashSet();
        for(int i=0;i<this.objectsToDraw.size();i++)
        {
            MithraObjectTypeWrapper mithraObjectTypeWrapper = (MithraObjectTypeWrapper) this.objectsToDraw.get(i);
            String fullyQualifiedSuperClassType = mithraObjectTypeWrapper.getFullyQualifiedSuperClassType();
            if (fullyQualifiedSuperClassType != null)
            {
                MithraObjectTypeWrapper superClassWrapper = mithraObjectTypeWrapper.getSuperClassWrapper();
                if (superClassWrapper == null)
                {
                    nonMithraSuperClasses.add(fullyQualifiedSuperClassType);
                }
            }
        }
        this.nonMithraSuperClasses.addAll(nonMithraSuperClasses);
    }

    public void processMithraInterfaces()
    {
        HashSet mithraInterfaceSet = new HashSet();
        for (int i = 0; i < this.objectsToDraw.size(); i++)
        {
            MithraObjectTypeWrapper mithraObjectTypeWrapper = (MithraObjectTypeWrapper) this.objectsToDraw.get(i);
            if (mithraObjectTypeWrapper.hasMithraInterfaces())
            {
                MithraInterfaceType[] interfaces = mithraObjectTypeWrapper.getMithraInterfaces();
                if (interfaces.length > 0)
                {
                    for (int j = 0; j < interfaces.length; j++)
                    {
                        MithraInterfaceType currInter = interfaces[j];
                        mithraInterfaceSet.add(currInter);
                        mithraInterfaceSet.addAll(currInter.getAllSuperInterfaces());

                    }
                }

            }

        }
        this.mithraInterfacesList.addAll(mithraInterfaceSet);
        Collections.sort(this.mithraInterfacesList, new Comparator<MithraInterfaceType>()
        {
            @Override
            public int compare(MithraInterfaceType interfaceType1, MithraInterfaceType interfaceType2)
            {
                return interfaceType1.getClassName().compareTo(interfaceType2.getClassName());
            }
        });
    }

    public void createFullIncludeList()
    {
        if (includeListArray != null)
        {
            ArrayList toDo = new ArrayList();
            HashSet done = new HashSet();
            Set<MithraObjectTypeWrapper> neededForManyToManyRelationships = new HashSet<MithraObjectTypeWrapper>();
            for(int i=0;i<includeListArray.size();i++)
            {
                String className = (String) includeListArray.get(i);
                MithraObjectTypeWrapper obj = this.getMithraObjects().get(className);
                if (obj == null)
                {
                    throw new MithraGeneratorException("no such object: "+className);
                }
                toDo.add(obj);
            }
            int depthToDo = this.followRelationshipDepth;
            while(!toDo.isEmpty())
            {
                ArrayList nextTodo = new ArrayList();
                done.addAll(toDo);
                depthToDo--;
                for(int i=0;i<toDo.size();i++)
                {
                    MithraObjectTypeWrapper obj = (MithraObjectTypeWrapper) toDo.get(i);
                    MithraObjectTypeWrapper superClass = obj.getSuperClassWrapper();
                    if (superClass != null)
                    {
                        if (!done.contains(superClass))
                        {
                            if (depthToDo > 0)
                            {
                                nextTodo.add(superClass);
                            }
                            else
                            {
                                done.add(superClass);
                            }
                        }
                    }
                    if (depthToDo >= 0)
                    {
                        RelationshipAttribute[] relationshipAttributes = obj.getRelationshipAttributes();
                        for(int r=0;r<relationshipAttributes.length;r++)
                        {
                            RelationshipAttribute relationshipAttribute = relationshipAttributes[r];
                            MithraObjectTypeWrapper related = relationshipAttribute.getRelatedObject();
                            if (!done.contains(related))
                            {
                                nextTodo.add(related);
                            }
                            Join join = relationshipAttribute.getMapperVisitor().getJoinedToThis();
                            if ( join != null )
                            {
                                MithraObjectTypeWrapper right = join.getRight();
                                if ( ! done.contains(right) )
                                {
                                    neededForManyToManyRelationships.add( right );
                                }
                            }
                        }
                    }
                }
                toDo = nextTodo;
            }
            this.objectsToDraw.addAll(done);
            for (MithraObjectTypeWrapper joinWrapper : neededForManyToManyRelationships)
            {
                this.addToObjectsToDrawIfAvailable( joinWrapper);
            }
        }
        else
        {
            this.objectsToDraw.addAll(this.getMithraObjects().values());
        }
    }

    public void processRelationships()
    {
        HashMap uniqueRelationships = new HashMap();
        for (Iterator iterator = getMithraObjects().values().iterator(); iterator.hasNext();)
        {
            MithraObjectTypeWrapper mithraObjectTypeWrapper = (MithraObjectTypeWrapper) iterator.next();

            if (isExcluded(mithraObjectTypeWrapper))
            {
                continue;
            }
            RelationshipAttribute[] relationshipAttributes = mithraObjectTypeWrapper.getRelationshipAttributes();
            for(int i=0;i<relationshipAttributes.length;i++)
            {
                boolean drawRelationship = true;
                RelationshipAttribute currentAttribute = relationshipAttributes[i];
                if (currentAttribute.isBidirectional())
                {
                    drawRelationship = ! currentAttribute.isReverseRelationship();
                }
                if (drawRelationship)
                {
                    drawRelationship = !isExcluded(currentAttribute.getRelatedObject());
                }
                if ( drawRelationship )
                {
                    Join join = currentAttribute.getMapperVisitor().getJoinedToThis();
                    if ( join != null )
                    {
                        MithraObjectTypeWrapper rightSide = join.getRight();
                        if ( this.isExcluded(rightSide ) )
                        {
                            boolean successfullyAdded = this.addToObjectsToDrawIfAvailable( rightSide );
                            drawRelationship = successfullyAdded;
                        }
                    }
                }
                if (drawRelationship)
                {
                    if ( currentAttribute.getCardinality().isManyToMany() && !relationshipAttributes[i].dependsOnlyOnFromToObjects())
                    {
                        MithraObjectTypeWrapper targetObject = currentAttribute.getRelatedObject();
                        MithraObjectTypeWrapper joinObject = currentAttribute.getMapperVisitor().getRight();
                        SimplifiedRelationship back = new SimplifiedRelationship(joinObject, targetObject, currentAttribute, true );
                        uniqueRelationships.put(back, back );
                    }
                    else
                    {
                        SimplifiedRelationship rel = new SimplifiedRelationship( currentAttribute );
                        if (this.collapseRelationships)
                        {
                            SimplifiedRelationship existing = (SimplifiedRelationship) uniqueRelationships.get(rel);
                            if (existing != null)
                            {
                                existing.setCollapsed(true);
                                if (existing.from != currentAttribute.getFromObject() || currentAttribute.isBidirectional())
                                {
                                    existing.setBidirectional(true);
                                }
                            }
                            else
                            {
                                uniqueRelationships.put(rel, rel);
                            }
                        }
                        else
                        {
                            this.relationships.add(rel);
                        }
                    }
                }
            }
        }
        if (collapseRelationships)
        {
            this.relationships.addAll(uniqueRelationships.values());
        }
    }

    private boolean addToObjectsToDrawIfAvailable(MithraObjectTypeWrapper rightSide)
    {
        if (this. isExcluded( rightSide ) )
        {
            if ( this.getMithraObjects().containsValue( rightSide ))
            {
                this.objectsToDraw.add(rightSide);
                return true;
            }
        }
        return false;
    }

    private boolean isExcluded(MithraObjectTypeWrapper mithraObjectTypeWrapper)
    {
        return (includeListArray != null) && (!objectsToDraw.contains(mithraObjectTypeWrapper));
    }

    public void printObject(PrintWriter writer, MithraObjectTypeWrapper mithraObjectTypeWrapper)
    {
        List attributes = getRelevantAttributes(mithraObjectTypeWrapper);
        int totalAttributes = attributes.size();
        int width = 10 * mithraObjectTypeWrapper.getClassName().length();
        int height = 60 + totalAttributes * 15;
        if (attributes.size() > 0 && attributes.size() < this.getAllAttributes(mithraObjectTypeWrapper).size())
        {
            height += 15;
        }

        for(int i=0;i<attributes.size();i++)
        {
            int attrWidth = 7* (getFullAttributeName((AbstractAttribute)attributes.get(i))).length();
            if (attrWidth > width) width = attrWidth;
        }
        String color = "#FFCC00";
        if (mithraObjectTypeWrapper.isAbstractTablePerSubclass() || mithraObjectTypeWrapper.isTablePerSubclassSuperClass())
        {
            color = "#CCFFFF";
        }
        writer.println("    <node id=\""+mithraObjectTypeWrapper.getClassName()+"\">\n" +
                "      <data key=\"d0\" >\n" +
                "        <y:UMLClassNode >\n" +
                "          <y:Geometry  x=\"218.5\" y=\"205.0\" width=\""+width+"\" height=\""+height+"\"/>\n" +
                "          <y:Fill color=\""+color+"\"  transparent=\"false\"/>\n" +
                "          <y:BorderStyle type=\"line\" width=\"1.0\" color=\"#000000\" />\n" +
                "          <y:NodeLabel x=\"23.0\" y=\"3.0\" width=\"68.0\" height=\"19.92626953125\" visible=\"true\" alignment=\"center\" fontFamily=\"Dialog\" fontSize=\"13\" fontStyle=\"bold\" textColor=\"#000000\" hasBackgroundColor=\"false\" hasLineColor=\"false\" modelName=\"internal\" modelPosition=\"c\" autoSizePolicy=\"content\">");
        writer.println(mithraObjectTypeWrapper.getClassName());
        writer.println("</y:NodeLabel>\n" +
                "          <y:UML clipContent=\"true\" omitDetails=\"false\" use3DEffect=\"true\" stereotype=\"\" constraint=\"\">\n" +
                "            <y:AttributeLabel>");
        for(int i=0;i<attributes.size();i++)
        {
            writer.println(getFullAttributeName((AbstractAttribute)attributes.get(i)));
        }
        if (attributes.size() > 0 && attributes.size() < this.getAllAttributes(mithraObjectTypeWrapper).size())
        {
            writer.println("...");
        }
        writer.println("</y:AttributeLabel>\n" +
                "            <y:MethodLabel></y:MethodLabel>\n" +
                "          </y:UML>\n" +
                "        </y:UMLClassNode>\n" +
                "      </data>");
        writer.println("    </node>");
    }

    private List getAllAttributes(MithraObjectTypeWrapper mithraObjectTypeWrapper)
    {
        List attributes = new ArrayList();
        if (mithraObjectTypeWrapper.hasSourceAttribute())
        {
            attributes.add(mithraObjectTypeWrapper.getSourceAttribute());
        }
        if (mithraObjectTypeWrapper.hasAsOfAttributes())
        {
            attributes.addAll(Arrays.asList(mithraObjectTypeWrapper.getAsOfAttributes()));
        }
        attributes.addAll(Arrays.asList(mithraObjectTypeWrapper.getAttributes()));
        return attributes;
    }

    private List getRelevantAttributes(MithraObjectTypeWrapper mithraObjectTypeWrapper)
    {
        List attributes = new ArrayList();
        if (this.showAttributes.equalsIgnoreCase("none"))
        {
            // do nothing
        }
        else if (this.showAttributes.equalsIgnoreCase("all"))
        {
            if (mithraObjectTypeWrapper.hasSourceAttribute())
            {
                attributes.add(mithraObjectTypeWrapper.getSourceAttribute());
            }
            if (mithraObjectTypeWrapper.hasAsOfAttributes())
            {
                attributes.addAll(Arrays.asList(mithraObjectTypeWrapper.getAsOfAttributes()));
            }
            attributes.addAll(Arrays.asList(mithraObjectTypeWrapper.getAttributes()));
        }
        else if (this.showAttributes.equalsIgnoreCase("primaryKey"))
        {
            if (mithraObjectTypeWrapper.hasSourceAttribute())
            {
                attributes.add(mithraObjectTypeWrapper.getSourceAttribute());
            }
            if (mithraObjectTypeWrapper.hasAsOfAttributes())
            {
                attributes.addAll(Arrays.asList(mithraObjectTypeWrapper.getAsOfAttributes()));
            }
            attributes.addAll(Arrays.asList(mithraObjectTypeWrapper.getPrimaryKeyAttributes()));
        }
        else
        {
            try
            {
                int maxToShow = Integer.parseInt(this.showAttributes);
                int count = 0;
                if (mithraObjectTypeWrapper.hasSourceAttribute())
                {
                    attributes.add(mithraObjectTypeWrapper.getSourceAttribute());
                    count++;
                }
                if (mithraObjectTypeWrapper.hasAsOfAttributes())
                {
                    attributes.addAll(Arrays.asList(mithraObjectTypeWrapper.getAsOfAttributes()));
                    count+= mithraObjectTypeWrapper.getAsOfAttributes().length;
                }
                Attribute[] primaryKeyAttributes = mithraObjectTypeWrapper.getPrimaryKeyAttributes();
                for(int i=0;i<primaryKeyAttributes.length && count < maxToShow; i++, count++)
                {
                    attributes.add(primaryKeyAttributes[i]);
                }
                Attribute[] otherAttributes = mithraObjectTypeWrapper.getAttributes();
                for(int i=0;i<otherAttributes.length && count < maxToShow; i++)
                {
                    if (!otherAttributes[i].isPrimaryKey())
                    {
                        attributes.add(otherAttributes[i]);
                        count++;
                    }
                }
            }
            catch (NumberFormatException e)
            {
                throw new MithraGeneratorException("showAttributes must be one of :none, all, primaryKey or an integer");
            }
        }

        return attributes;
    }

    private String getFullAttributeName(AbstractAttribute attribute)
    {
        String result = attribute.getName()+" : ";
        if (attribute.isAsOfAttribute())
        {
            if (((AsOfAttribute)attribute).isProcessingDate())
            {
                result += "[Processing] ";
            }
            else
            {
                result += "[Business] ";
            }
        }
        if (attribute.isSourceAttribute())
        {
            result += "[Source] ";
        }
        if (attribute.isPrimaryKey())
        {
            result += "[PK] ";
        }
        result += attribute.getTypeAsString();
        return result;
    }

    public static class SimplifiedRelationship
    {
        private MithraObjectTypeWrapper from;
        private MithraObjectTypeWrapper to;
        private boolean isBidirectional;
        private boolean isCollapsed;
        private boolean isCompatible = true;
        private RelationshipAttribute originalRel;
        private int startingAssociationCount = 0;
        private boolean writeReverseRelationshipName = false;

        private static int globalAssociationCount = 0;

        public SimplifiedRelationship(RelationshipAttribute rel)
        {
            this.from = rel.getFromObject();
            this.to = rel.getRelatedObject();
            this.isBidirectional = rel.isBidirectional();
            this.originalRel = rel;
        }

        public SimplifiedRelationship(
                MithraObjectTypeWrapper from,
                MithraObjectTypeWrapper to,
                RelationshipAttribute originalRelationship,
                boolean writeReverseRelationshipName )
        {
            this.from = from;
            this.to = to;
            this.originalRel = originalRelationship;
            this.isBidirectional = false;
            this.writeReverseRelationshipName = writeReverseRelationshipName;
        }

        public void collapse(RelationshipAttribute rel)
        {
            this.setCollapsed(true);
            if (this.from != rel.getFromObject() || rel.isBidirectional())
            {
                this.setBidirectional(true);
            }
            List originalJoins = this.originalRel.getMapperVisitor().getFurtherJoins();
            List newJoins = rel.getMapperVisitor().getFurtherJoins();
            if (originalJoins.size() != newJoins.size())
            {
                this.isCompatible = false;
            }
            else
            {
                if (this.originalRel.getMapperVisitor().getJoinedToThis().getRight() != rel.getMapperVisitor().getJoinedToThis().getRight())
                {
                    this.isCompatible = false;
                }
                for(int i=0;i<originalJoins.size();i++)
                {
                    Join orig = (Join) originalJoins.get(i);
                    Join newJoin = (Join) newJoins.get(i);
                    if (orig.getRight() != newJoin.getRight())
                    {
                        this.isCompatible = false;
                        break;
                    }
                }
            }
        }

        public boolean isBidirectional()
        {
            return isBidirectional;
        }

        public void setBidirectional(boolean bidirectional)
        {
            isBidirectional = bidirectional;
        }

        public MithraObjectTypeWrapper getFrom()
        {
            return from;
        }

        public MithraObjectTypeWrapper getTo()
        {
            return to;
        }

        public boolean isCollapsed()
        {
            return isCollapsed;
        }

        public void writeCardinalityLabels(PrintWriter writer, boolean writeRelationshipName )
        {
            if (!this.isCollapsed())
            {
                writer.println("          <y:EdgeLabel x=\"0\" y=\"0\" width=\"35.0\" height=\"18.701171875\" visible=\"true\" alignment=\"center\" fontFamily=\"Dialog\" fontSize=\"12\" fontStyle=\"plain\" textColor=\"#000000\" hasBackgroundColor=\"false\" hasLineColor=\"false\" modelName=\"six_pos\" modelPosition=\"thead\" preferredPlacement=\"anywhere\" distance=\"2.0\" ratio=\"0.5\">"
                        +(originalRel.isToMany() ? "*" : "1") + "</y:EdgeLabel>");
                writer.println("          <y:EdgeLabel x=\"0\" y=\"0\" width=\"35.0\" height=\"18.701171875\" visible=\"true\" alignment=\"center\" fontFamily=\"Dialog\" fontSize=\"12\" fontStyle=\"plain\" textColor=\"#000000\" hasBackgroundColor=\"false\" hasLineColor=\"false\" modelName=\"six_pos\" modelPosition=\"shead\" preferredPlacement=\"anywhere\" distance=\"2.0\" ratio=\"0.5\">"
                        +(originalRel.isFromMany() ? "*" : "1") + "</y:EdgeLabel>");
                if ( writeRelationshipName )
                {
                    writer.println("          <y:EdgeLabel x=\"0\" y=\"0\" width=\"35.0\" height=\"18.701171875\" visible=\"true\" alignment=\"center\" fontFamily=\"Dialog\" fontSize=\"12\" fontStyle=\"plain\" textColor=\"#000000\" hasBackgroundColor=\"false\" hasLineColor=\"false\" modelName=\"six_pos\" modelPosition=\"ttail\" preferredPlacement=\"anywhere\" distance=\"2.0\" ratio=\"0.7\">"
                            +this.getRelationshipNameForDisplay() + "</y:EdgeLabel>");
                }
            }
        }

        public String getRelationshipNameForDisplay()
        {
            if ( this.writeReverseRelationshipName )
            {
                return originalRel.getReverseName() != null ? originalRel.getReverseName() : originalRel.getName();
            }
            return originalRel.getName();
        }

        public int writeRelationship(PrintWriter writer, int edgeCount)
        {
            String color = "#000000";
            if (this.isCollapsed())
            {
                color = "#0000FF";
            }
            List furtherJoins = new ArrayList(this.originalRel.getMapperVisitor().getFurtherJoins());
            if (this.isCompatible && !furtherJoins.isEmpty() && this.originalRel.getFromObject() != this.originalRel.getRelatedObject())
            {
                furtherJoins.add(0, this.originalRel.getMapperVisitor().getJoinedToThis());
                Join lastJoin = (Join) furtherJoins.get(furtherJoins.size() - 1);
                if (!lastJoin.getRight().equals(this.originalRel.getRelatedObject()))
                {
                    for(int i=furtherJoins.size() - 2;i>=0;i--)
                    {
                        lastJoin = (Join) furtherJoins.get(i);
                        if (lastJoin.getRight().equals(this.originalRel.getRelatedObject()))
                        {
                            break;
                        }
                    }
                }
                int count = 0;
                for(int i=0;i<furtherJoins.size();i++)
                {
                    Join join = (Join) furtherJoins.get(i);
                    if (join != lastJoin)
                    {
                        writeAssociationEdge(writer, edgeCount, count, join, color);
                        count++;
                        edgeCount+=2;
                    }
                }
                writer.println("<edge id=\"e"+edgeCount+"\" source=\"a"+(this.startingAssociationCount+furtherJoins.size()-2)+
                        "\" target=\""+this.getTo().getClassName()+"\">");
                writer.println("<data key=\"d2\" >");
                writer.println("        <y:PolyLineEdge >\n" +
                        "          <y:Path sx=\"0.0\" sy=\"0.0\" tx=\"0.0\" ty=\"0.0\"/>");
                writer.println("<y:LineStyle type=\"line\" width=\"1.0\" color=\""+color+"\" />\n" +
                        "          <y:Arrows source=\"none\" target=\"standard\"/>\n");
                this.writeCardinalityLabels(writer, this.shouldWriteRelationshipName() );
                writer.println(
                        "          <y:BendStyle smoothed=\"false\"/>\n" +
                                "        </y:PolyLineEdge>\n" +
                                "      </data></edge>");
                edgeCount++;
            }
            else
            {
                String sourceArrow = "none";
                if ( this.isBidirectional() || this.originalRel.getCardinality().isManyToMany() )
                {
                    sourceArrow = "standard";
                }
                writer.println("<edge id=\"e"+edgeCount+"\" source=\""+this.getFrom().getClassName()+
                        "\" target=\""+this.getTo().getClassName()+"\">");
                writer.println("<data key=\"d2\" >");
                writer.println("        <y:PolyLineEdge >\n" +
                        "          <y:Path sx=\"0.0\" sy=\"0.0\" tx=\"0.0\" ty=\"0.0\"/>");
                writer.println("<y:LineStyle type=\"line\" width=\"1.0\" color=\""+color+"\" />\n" +
                        "          <y:Arrows source=\""+sourceArrow+"\" target=\"standard\"/>\n");
                this.writeCardinalityLabels(writer, this.shouldWriteRelationshipName());
                writer.println(
                        "          <y:BendStyle smoothed=\"false\"/>\n" +
                                "        </y:PolyLineEdge>\n" +
                                "      </data></edge>");
                edgeCount++;
            }
            return edgeCount;
        }

        private boolean shouldWriteRelationshipName() {
            if ( this.getTo().equals(this.getFrom()))
            {
                return true;
            }

            boolean writeRelationshipNames =
                    this.originalRel.getCardinality().isToMany();

            if ( this.originalRel.getCardinality(). isManyToMany() )
            {
                writeRelationshipNames = this.getTo().equals( originalRel.getFromObject());
            }
            return writeRelationshipNames;
        }

        private void writeAssociationEdge(PrintWriter writer, int edgeCount, int i, Join join, String color)
        {
            String source = this.originalRel.getFromObject().getClassName();
            String sourceArrow = "none";
            if (i == 0 && this.isBidirectional)
            {
                sourceArrow = "standard";
            }
            if (i > 0)
            {
                source = "a"+(startingAssociationCount + i - 1);
            }
            writer.println("<edge id=\"e"+edgeCount+"\" source=\""+source+
                    "\" target=\"a"+(startingAssociationCount+i)+"\">");
            writer.println("<data key=\"d2\" >");
            writer.println("        <y:PolyLineEdge >\n" +
                    "          <y:Path sx=\"0.0\" sy=\"0.0\" tx=\"0.0\" ty=\"0.0\"/>");
            writer.println("<y:LineStyle type=\"line\" width=\"1.0\" color=\""+color+"\" />\n" +
                    "          <y:Arrows source=\""+sourceArrow+"\" target=\"none\"/>\n");
            this.writeCardinalityLabels(writer, true);
            writer.println(
                    "          <y:BendStyle smoothed=\"false\"/>\n" +
                            "        </y:PolyLineEdge>\n" +
                            "      </data></edge>");

            source = join.getRight().getClassName();
            writer.println("<edge id=\"e"+(edgeCount+1)+"\" source=\""+source+
                    "\" target=\"a"+(startingAssociationCount+i)+"\">");
            writer.println("<data key=\"d2\" >");
            writer.println("        <y:PolyLineEdge >\n" +
                    "          <y:Path sx=\"0.0\" sy=\"0.0\" tx=\"0.0\" ty=\"0.0\"/>");
            writer.println("<y:LineStyle type=\"dashed\" width=\"1.0\" color=\""+color+"\" />\n" +
                    "          <y:Arrows source=\"none\" target=\"none\"/>\n");
            this.writeCardinalityLabels(writer, true);
            writer.println(
                    "          <y:BendStyle smoothed=\"false\"/>\n" +
                            "        </y:PolyLineEdge>\n" +
                            "      </data></edge>");

        }

        public void writeAssociationLinks(PrintWriter writer)
        {
            if (this.isCompatible && this.originalRel.getFromObject() != this.originalRel.getRelatedObject())
            {
                List furtherJoins = this.originalRel.getMapperVisitor().getFurtherJoins();
                if (!furtherJoins.isEmpty())
                {
                    this.startingAssociationCount = globalAssociationCount;
                    String associationName = this.originalRel.getName();
                    globalAssociationCount += furtherJoins.size();
                    for(int i=0;i<furtherJoins.size();i++)
                    {
                        writer.println("    <node id=\"a"+(this.startingAssociationCount+i)+"\">\n" +
                                "      <data key=\"d0\" >\n" +
                                "        <y:ShapeNode >\n" +
                                "          <y:Geometry  x=\"84.5\" y=\"-71.0\" width=\"2.0\" height=\"2.0\"/>\n" +
                                "          <y:Fill color=\"#FFFFFF\"  transparent=\"false\"/>\n" +
                                "          <y:BorderStyle type=\"line\" width=\"1.0\" color=\"#000000\" />\n" +
                                "          <y:NodeLabel x=\"2.5\" y=\"2.5\" width=\"4.0\" height=\"4.0\" visible=\"true\" alignment=\"center\" fontFamily=\"Dialog\" fontSize=\"13\" fontStyle=\"plain\" textColor=\"#000000\" hasBackgroundColor=\"false\" hasLineColor=\"false\" modelName=\"internal\" modelPosition=\"c\" autoSizePolicy=\"content\">" +
                                associationName + "</y:NodeLabel>\n" +
                                "          <y:Shape type=\"roundrectangle\"/>\n" +
                                "        </y:ShapeNode>\n" +
                                "      </data>\n" +
                                "    </node>");
                    }
                }
            }
        }

        public boolean equals(Object o)
        {
            final SimplifiedRelationship that = (SimplifiedRelationship) o;

            boolean intermediateEquals = this.from == that.from && this.to == that.to;
            if ( ! intermediateEquals )
            {
                return false;
            }
            intermediateEquals = this.from == that.to && this .to == that.from;
            if ( ! intermediateEquals )
            {
                return false;
            }
            intermediateEquals = this.originalRel.getName().equals(that.originalRel.getName());
            return intermediateEquals;
        }

        public int hashCode()
        {
            return from.hashCode() ^ to.hashCode();
        }

        public void setCollapsed(boolean collapsed)
        {
            isCollapsed = collapsed;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.
                    append("Simplified Relationship from: ").
                    append(this.from.getInterfaceName()).
                    append(" to: ").
                    append(this.to.getInterfaceName()).
                    append("  ").
                    append(this.originalRel.getName());
            return builder.toString();
        }
    }

    public void execute() 
    {
        if (!executed)
        {
            try
            {
                if (this.getOutputFile() == null)
                {
                    throw new MithraGeneratorException("you must provide an outputFile");
                }
                File file = new File(this.getXml());
                parseMithraXml(file.getName(), null, new MithraGeneratorImport.DirectoryFileProvider(file.getParent()));
                parseImportedMithraXml();
                validateXml();
                this.createFullIncludeList();
                this.processRelationships();
                this.processNonMithraSuperClasses();
                this.processMithraInterfaces();
                PrintWriter writer = new PrintWriter(new FileOutputStream(this.getOutputFile()));
                writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                writer.println("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns/graphml\" xmlns:y=\"http://www.yworks.com/xml/graphml\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns/graphml http://www.yworks.com/xml/schema/graphml/1.0/ygraphml.xsd\">");
                writer.println("<key id=\"d0\" for=\"node\" yfiles.type=\"nodegraphics\"/>");
                writer.println("<key id=\"d2\" for=\"edge\" yfiles.type=\"edgegraphics\"/>");
                writer.println("<graph id=\"G\" edgedefault=\"directed\">");
                for (int i=0;i<this.getObjectsToDraw().size();i++)
                {
                    MithraObjectTypeWrapper mithraObjectTypeWrapper = (MithraObjectTypeWrapper) this.getObjectsToDraw().get(i);
                    this.printObject(writer, mithraObjectTypeWrapper);
                }
                for (int i=0;i<this.getMithraInterfacesList().size();i++)
                {
                    MithraInterfaceType mithraInterfaceType =  this.getMithraInterfacesList().get(i);
                    this.printMithraInterface(writer, mithraInterfaceType);
                }
                for (int i=0;i<this.getMithraInterfacesList().size();i++)
                {
                    MithraInterfaceType mithraInterfaceType = this.getMithraInterfacesList().get(i);
                    this.printMithraSuperInterfacesEdges(writer, mithraInterfaceType, i);
                }
                for(int i=0;i<this.getNonMithraSuperClasses().size();i++)
                {
                    String superClass = (String) this.getNonMithraSuperClasses().get(i);
                    this.printNonMithraClass(writer, superClass);
                }
                for (int i=0;i<this.getObjectsToDraw().size();i++)
                {
                    MithraObjectTypeWrapper mithraObjectTypeWrapper = (MithraObjectTypeWrapper) this.getObjectsToDraw().get(i);
                    this.printObjectSuperClass(writer, mithraObjectTypeWrapper, i);
                    this.printObjectInterface(writer, mithraObjectTypeWrapper, i);
                }
                for (int i=0;i<this.getRelationships().size();i++)
                {
                    CoreMithraGraphGenerator.SimplifiedRelationship rel = (CoreMithraGraphGenerator.SimplifiedRelationship) this.getRelationships().get(i);
                    rel.writeAssociationLinks(writer);
                }
                int edgeCount = 0;
                for (int i=0;i<this.getRelationships().size();i++)
                {
                    CoreMithraGraphGenerator.SimplifiedRelationship rel = (CoreMithraGraphGenerator.SimplifiedRelationship) this.getRelationships().get(i);
                    edgeCount = rel.writeRelationship(writer, edgeCount);
                }
                writer.println("</graph>\n" +"</graphml>");
                writer.close();
                executed = true;
            }
            catch (Exception e)
            {
                throw new MithraGeneratorException("Exception in mithra code generation", e);
            }
        }
        else
        {
            this.logger.info("skipped");
        }
    }

    public static void main(String[] args)
    {
        CoreMithraGraphGenerator gen = new CoreMithraGraphGenerator();
        gen.setLogger(new StdOutLogger());
        
        long startTime = System.currentTimeMillis();
        gen.setXml("H:/projects/glew/xml/mithra/GlewMithraClassList.xml");
        MithraGeneratorImport xmlImport = new MithraGeneratorImport();
        xmlImport.setDir("H:/projects/glew/xml/mithra/");
        xmlImport.setFilename("GlewMithraRefDataClassList.xml");
        gen.addConfiguredMithraImport(xmlImport);
        gen.setOutputFile("c:/temp/glew.graphml");
        gen.setShowAttributes("none");
//        gen.setIncludeList("LewAgreement");
//        gen.setFollowRelationshipDepth(1);
        gen.execute();
        System.out.println("time: "+(System.currentTimeMillis() - startTime));
    }
}
