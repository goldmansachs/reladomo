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



public interface TransactionalCommand<R>
{

    /**
     * The code inside this method will be executed as a unit of work. The transaction is demarcated
     * when the method begins and ends when the method ends. To rollback, the method must throw an
     * exception.
     * @param tx The running transaction.
     * @return Whatever the application requires. Typically null.
     * @throws Throwable If the exception is a RuntimeException, it is rethrown after rollback.
     */
    R executeTransaction(MithraTransaction tx) throws Throwable;
}

