/*
 *    Copyright 2017 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.querying;

import com.google.common.base.Optional;

public class SubscriptionTicket {

  ExecutableNodeBase subscriber;

  Optional<Integer> channel = Optional.<Integer>absent();

  public SubscriptionTicket(ExecutableNodeBase subscriber) {
    this.subscriber = subscriber;
  }

  public SubscriptionTicket(ExecutableNodeBase subscriber, int channel) {
    this.subscriber = subscriber;
    this.channel = Optional.of(channel);
  }

  public ExecutableNodeBase getSubscriber() {
    return subscriber;
  }

  public Optional<Integer> getChannel() {
    return channel;
  }
}
