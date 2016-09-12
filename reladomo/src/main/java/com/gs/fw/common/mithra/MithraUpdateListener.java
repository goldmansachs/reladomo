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

package com.gs.fw.common.mithra;


/**
 * This interface must be implemented by any class that is listed in the <UpdateListener> tag of the mithra object xml.
 * The object is instantiated via reflection. It must have an empty constructor.
 * There is only one instance instantiated per Mithra class.
 * Context information is usually passed into the listener via a thread local.
 * See MithraUpdateListenerIAbstract for a useful abstract class to extend. 
 */
public interface MithraUpdateListener<T extends MithraTransactionalObject>
{
    /**
     * This method is called if an UpdateListener has been specified for a Mithra transactional object (via
     * the <UpdateListener> tag in the object xml) and an attribute is changed by calling a set method.
     * The updatedObject can be changed during this call. Any changes to the updatedObject will result in
     * further calls to this method.
     *
     * This method is not called for non-persistent objects (detached or newly created).
     *
     * This method is only called on the object on with the set method was called. It is not called if
     * changes are made because of ripple effect for dated object (e.g. when the increment
     * method changes multiple segments, only the first segment receives this callback).
     *
     * A typical use case for this class might be to set the updating user on an object. The code for this
     * method might look like:
     *
     * if (!updateInfo.getAttribute().equals(FooFinder.updatedUser()))
     * {
     *      ((Foo)updatedObject).setUpdatedUser(someUser);
     * }
     *
     * The someUser value is typically passed here via a thread local context.
     *
     * If this callback is used to set a timestamp attribute, it's best to set it to the processing date
     * value for the current transaction instead of System.currentTimeMillis():
     * MithraManagerProvider.getMithraManager().getCurrentTransaction().getProcessingStartTime()
     *
     * @param updatedObject the object that was changed
     * @param updateInfo the information about the update
     */
    public void handleUpdate(T updatedObject, UpdateInfo updateInfo);

    /**
     * This method is called when the object is modified by copying another object.
     * This method gets called when calling detachedObject.copyValuesToOriginalOrInsertIfNew()
     * with the original (persistent, not detached) object passed in.
     * It also gets called when calling mithraObject.copyNonPrimaryKeyAttributesFrom()
     *
     * @param updatedObject The persistent object that was just updated via a copy operation.
     */
    public void handleUpdateAfterCopy(T updatedObject);
}
