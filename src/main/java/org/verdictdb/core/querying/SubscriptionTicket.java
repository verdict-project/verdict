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
