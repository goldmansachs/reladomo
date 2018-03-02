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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.util.serializer;

import com.gs.fw.common.mithra.DeepFetchTree;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.MappedAttribute;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class SerializationNode
{
    private final SerializationNode parent; // null at root
    private List<SerializationNode> children = ListFactory.EMPTY_LIST;
    private List<SerializationNode> links = ListFactory.EMPTY_LIST;

    private List<Attribute> attributes;
    private final AbstractRelatedFinder relatedFinder;
    private final boolean isLink;
    private Attribute[] linkAttributes;
    private Class relatedClass;

    protected SerializationNode(RelatedFinder relatedFinder)
    {
       this(null, relatedFinder, false);
    }

    protected SerializationNode(SerializationNode parent, RelatedFinder relatedFinder)
    {
        this(parent, relatedFinder, false);
    }

    protected SerializationNode(SerializationNode parent, RelatedFinder relatedFinder, boolean isLink)
    {
        this.parent = parent;
        this.relatedFinder = ((AbstractRelatedFinder) relatedFinder).zWithoutParentSelector();
        this.isLink = isLink;
        if (this.isLink)
        {
            populateLinkAttributes();
        }
    }

    private void populateLinkAttributes()
    {
        Set<Attribute> allLeftAttributes = this.relatedFinder.zGetMapper().getAllLeftAttributes();
        this.linkAttributes = new Attribute[allLeftAttributes.size()];
        allLeftAttributes.toArray(this.linkAttributes);
        Arrays.sort(this.linkAttributes, new Comparator<Attribute>()
        {
            @Override
            public int compare(Attribute o1, Attribute o2)
            {
                return o1.getAttributeName().compareTo(o2.getAttributeName());
            }
        });
    }

    public static SerializationNode withDefaultAttributes(RelatedFinder finder)
    {
        SerializationNode result = new SerializationNode(finder);
        fillDefaultAttributes(finder, result);
        return result;
    }

    public static SerializationNode withDefaultAttributesAndDeepFetchesFromList(RelatedFinder finder, MithraList list)
    {
        SerializationNode result = withDefaultAttributes(finder);
        DeepFetchTree deepFetchTree = list.getDeepFetchTree();
        List<DeepFetchTree> children = deepFetchTree.getChildren();
        List<AbstractRelatedFinder> fullList = FastList.newList();
        for(int i=0;i<children.size(); i++)
        {
            addToList(fullList, children.get(i));
        }
        result.addChildren(fullList);
        return result;
    }

    private static void fillDefaultAttributes(RelatedFinder finder, SerializationNode result)
    {
        FastList<Attribute> attributes = unwrapAttributes(finder.getPersistentAttributes());
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        if (asOfAttributes != null)
        {
            for(AsOfAttribute a: asOfAttributes)
            {
                attributes.add(unwrapAttribute(a));
            }
        }
        result.attributes = attributes;
    }

    private static FastList<Attribute> unwrapAttributes(Attribute[] persistentAttributes)
    {
        FastList<Attribute> list = FastList.newList(persistentAttributes.length);
        for(Attribute a: persistentAttributes)
        {
            list.add(unwrapAttribute(a));
        }
        return list;
    }

    private static Attribute unwrapAttribute(Attribute a)
    {
        while (a instanceof MappedAttribute)
        {
            a = ((MappedAttribute)a).getWrappedAttribute();
        }
        return a;
    }

    protected static SerializationNode withDefaultAttributes(SerializationNode parent, RelatedFinder finder)
    {
        SerializationNode result = new SerializationNode(parent, finder);
        fillDefaultAttributes(finder, result);
        return result;
    }

    public List<Attribute> getAttributes()
    {
        return attributes;
    }

    public List<SerializationNode> getChildren()
    {
        return children;
    }

    public AbstractRelatedFinder getRelatedFinder()
    {
        return relatedFinder;
    }

    public SerializationNode getParent()
    {
        return parent;
    }

    public List<SerializationNode> getLinks()
    {
        return links;
    }

    public Attribute[] getLinkAttributes()
    {
        return linkAttributes;
    }

    public SerializationNode withoutTheseAttributes(Attribute... attributes)
    {
        SerializationNode result = new SerializationNode(this.relatedFinder);
        result.attributes = this.attributes;
        result.children = this.children;
        result.links = this.links;

        for(Attribute a: attributes)
        {
            if (a instanceof MappedAttribute)
            {
                MappedAttribute mappedAttribute = (MappedAttribute) a;
                if (result.children == this.children)
                {
                    result.children = FastList.newList(this.children);
                }
                removeDeepAttributeFromChildren(result, mappedAttribute);
            }
            else
            {
                if (result.attributes == this.attributes)
                {
                    result.attributes = FastList.newList(this.attributes);
                }
                result.attributes.remove(a);
            }
        }
        return result;
    }

    private void removeDeepAttributeFromChildren(SerializationNode result, MappedAttribute mappedAttribute)
    {
        for(int i=0;i<result.children.size();i++)
        {
            SerializationNode child = result.children.get(i);
            SerializationNode newChild = child.removeDeepAttribute(mappedAttribute);
            if (newChild != null)
            {
                result.children.set(i, newChild);
                break;
            }
        }
    }

    private SerializationNode removeDeepAttribute(MappedAttribute mappedAttribute)
    {
        if (isMatchingMapper(mappedAttribute))
        {
            SerializationNode result = new SerializationNode(this.parent, this.relatedFinder);
            if (mappedAttribute.getMapper().equals(relatedFinder.zGetMapper()))
            {
                result.attributes = FastList.newList(this.attributes);
                result.attributes.remove(mappedAttribute.getWrappedAttribute());
                result.children = this.children;
            }
            else
            {
                result.attributes = this.attributes;
                result.children = FastList.newList(this.children);
                removeDeepAttributeFromChildren(result, mappedAttribute);
            }
            return result;
        }
        return null;
    }

    private boolean isMatchingMapper(MappedAttribute mappedAttribute)
    {
        Mapper curMapper = mappedAttribute.getMapper();
        while (curMapper != null)
        {
            if (curMapper.equals(relatedFinder.zGetMapper()))
            {
                return true;
            }
            curMapper = curMapper.getParentMapper();
        }
        return false;
    }

    public SerializationNode withDeepFetches(DeepRelationshipAttribute... relationships)
    {
        SerializationNode result = new SerializationNode(this.relatedFinder);
        result.attributes = this.attributes;
        result.children = FastList.newList(this.children);
        result.links = this.links;

        for(DeepRelationshipAttribute dra : relationships)
        {
            AbstractRelatedFinder relatedFinder = (AbstractRelatedFinder) dra;
            DeepRelationshipAttribute parentAttribute = relatedFinder.getParentDeepRelationshipAttribute();
            List<AbstractRelatedFinder> fullList = new FastList<AbstractRelatedFinder>(parentAttribute == null ? 1 : 6);
            fullList.add(relatedFinder);
            while(parentAttribute != null)
            {
                fullList.add((AbstractRelatedFinder) parentAttribute);
                parentAttribute = parentAttribute.getParentDeepRelationshipAttribute();
            }
            result.addChildren(fullList);
        }
        if (!this.links.isEmpty())
        {
            result = result.withLinks();
        }
        return result;
    }

    private boolean addChildren(List<AbstractRelatedFinder> fullList)
    {
        int end = fullList.size() - 1;
        SerializationNode cur = this;
        boolean added = false;
        while(end >= 0)
        {
            AbstractRelatedFinder toAdd = fullList.get(end);
            SerializationNode found = null;
            if (cur.children != null)
            {
                for(int i=0;i<cur.children.size();i++)
                {
                    SerializationNode o = cur.children.get(i);
                    if (o.equalsRelatedFinder(toAdd))
                    {
                        found = o;
                        break;
                    }
                }
            }
            if (found == null)
            {
                added = true;
                found = SerializationNode.withDefaultAttributes(cur, toAdd);
                cur.addChild(found);
            }
            end--;
            cur = found;
        }
        return added;
    }

    private void addChild(SerializationNode child)
    {
        if (this.children.isEmpty())
        {
            this.children = FastList.newList();
        }
        this.children.add(child);
    }

    private boolean equalsRelatedFinder(AbstractRelatedFinder toAdd)
    {
        return this.relatedFinder.equals(toAdd);
    }

    private static void addToList(List<AbstractRelatedFinder> fullList, DeepFetchTree deepFetchTree)
    {
        fullList.add((AbstractRelatedFinder) deepFetchTree.getRelationshipAttribute());
        List<DeepFetchTree> children = deepFetchTree.getChildren();
        for(int i=0;i<children.size(); i++)
        {
            addToList(fullList, children.get(i));
        }
    }

    public SerializationNode withLinks()
    {
        return withLinks(null);
    }

    protected SerializationNode withLinks(SerializationNode parent)
    {
        SerializationNode result = new SerializationNode(parent, this.relatedFinder);
        result.attributes = this.attributes;

        Set<String> navigatedSet = UnifiedSet.newSet();
        for(int i=0;i<children.size();i++)
        {
            navigatedSet.add(children.get(i).getRelatedFinder().getRelationshipName());
        }

        List<RelatedFinder> allRelationships = this.relatedFinder.getRelationshipFinders();

        List<SerializationNode> newLinks = FastList.newList();

        for(int i=0;i<allRelationships.size();i++)
        {
            AbstractRelatedFinder rf = (AbstractRelatedFinder) allRelationships.get(i);
            if (!navigatedSet.contains(rf.getRelationshipName()))
            {
                SerializationNode link = new SerializationNode(result, rf, true);
                newLinks.add(link);
            }
        }
        List<SerializationNode> newChildren = FastList.newList(this.children.size());
        for(int i=0;i<children.size();i++)
        {
            newChildren.add(this.children.get(i).withLinks(result));
        }
        result.children = newChildren;
        result.links = newLinks;
        return result;
    }

    public SerializationNode withDeepDependents()
    {
        return this.withDeepDependents(null);
    }

    private SerializationNode withDeepDependents(SerializationNode parent)
    {
        SerializationNode result = new SerializationNode(parent, this.relatedFinder);
        result.attributes = this.attributes;
        result.children = FastList.newList(this.children);
        result.links = this.links;

        List<RelatedFinder> dependentRelationshipFinders = this.relatedFinder.getDependentRelationshipFinders();

        List<SerializationNode> fullList = FastList.newList(dependentRelationshipFinders.size());
        for(int i=0;i<dependentRelationshipFinders.size();i++)
        {
            RelatedFinder relatedFinder = dependentRelationshipFinders.get(i);
            int childIndex = childIndex(result.children, relatedFinder);
            if (childIndex >= 0)
            {
                result.children.set(childIndex, result.children.get(childIndex).withDeepDependents());
            }
            else
            {
                SerializationNode child = SerializationNode.withDefaultAttributes(result, relatedFinder);
                child = child.withDeepDependents(result);
                fullList.add(child);
            }
        }

        result.children.addAll(fullList);

        if (!this.links.isEmpty())
        {
            result = result.withLinks();
        }
        return result;
    }

    private int childIndex(List<SerializationNode> children, RelatedFinder relatedFinder)
    {
        for(int i=0;i<children.size();i++)
        {
            if (children.get(i).relatedFinder.equals(relatedFinder))
            {
                return i;
            }
        }
        return -1;
    }

    public Class getRelatedClass()
    {
        Class result = relatedClass;
        if (result == null)
        {
            result = ReladomoClassMetaData.fromFinder(relatedFinder).getBusinessOrInterfaceClass();
            relatedClass = result;
        }
        return result;
    }
}
