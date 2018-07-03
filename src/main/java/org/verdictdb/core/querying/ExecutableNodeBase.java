package org.verdictdb.core.querying;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class ExecutableNodeBase implements ExecutableNode {

  List<ExecutableNodeBase> subscribers = new ArrayList<>();

  List<Pair<ExecutableNodeBase, Integer>> sources = new ArrayList<>();

  Map<Integer, ExecutionTokenQueue> channels = new TreeMap<>();
  
  final private String uniqueId;
  
  public ExecutableNodeBase() {
    uniqueId = UUID.randomUUID().toString();
  }

  public static ExecutableNodeBase create() {
    return new ExecutableNodeBase();
  }

  // setup method
  public SubscriptionTicket createSubscriptionTicket() {
    return new SubscriptionTicket(this);
  }

  public void registerSubscriber(SubscriptionTicket ticket) {
    if (ticket.getChannel().isPresent()) {
      ticket.getSubscriber().subscribeTo(this, ticket.getChannel().get());
    } else {
      ticket.getSubscriber().subscribeTo(this);
    }
  }

  public void subscribeTo(ExecutableNodeBase node) {
    for (int channel = 0; ; channel++) {
      if (!channels.containsKey(channel)) {
        subscribeTo(node, channel);
        break;
      }
    }
  }

  public void subscribeTo(ExecutableNodeBase node, int channel) {
//    node.getSubscribers().add(this);
    node.addSubscriber(this);
    sources.add(Pair.of(node, channel));
    if (!channels.containsKey(channel)) {
      channels.put(channel, new ExecutionTokenQueue());
    }
  }
  
  void addSubscriber(ExecutableNodeBase node) {
    subscribers.add(node);
  }

  public void cancelSubscriptionTo(ExecutableNodeBase node) {
//    node.subscribers.remove(node);
    List<Pair<ExecutableNodeBase, Integer>> newSources = new ArrayList<>();
    Set<Integer> leftChannels = new HashSet<>();
    for (Pair<ExecutableNodeBase, Integer> s : sources) {
      if (!s.getLeft().equals(node)) {
        newSources.add(s);
        leftChannels.add(s.getRight());
        continue;
      }
    }
    sources = newSources;
    
    // if there are no other nodes broadcasting to this channel, remove the queue
    for (Integer c : leftChannels) {
      if (!channels.containsKey(c)) {
        channels.remove(c);
      }
    }
  }

  public void clearSubscribers() {
    for (ExecutableNodeBase s : subscribers) {
      s.cancelSubscriptionTo(this);
    }
  }

  // runner methods
  @Override
  public void getNotified(ExecutableNode source, ExecutionInfoToken token) {
//    System.out.println("get notified: " + source + " " + token);
    for (Pair<ExecutableNodeBase, Integer> a : sources) {
      int channel = a.getRight();
  //    System.out.println("channel: " + channel);
      channels.get(channel).add(token);
  //    System.out.println("get notified: " + token);
    }
  }

  @Override
  public List<ExecutionTokenQueue> getSourceQueues() {
    return new ArrayList<ExecutionTokenQueue>(channels.values());
  }

  @Override
  public List<ExecutableNode> getSubscribers() {
    List<ExecutableNode> nodes = new ArrayList<>();
    for (ExecutableNodeBase s : subscribers) {
      nodes.add(s);
    }
    return nodes;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return null;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return null;
  }

  @Override
  public int getDependentNodeCount() {
    return sources.size();
  }

  // Helpers
  public List<ExecutableNodeBase> getSources() {
    List<ExecutableNodeBase> ss = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Integer> s : sources) {
      ss.add(s.getKey());
    }
    return ss;
  }
  
  public Integer getChannelForSource(ExecutableNodeBase node) {
    for (Pair<ExecutableNodeBase, Integer> s : sources) {
      if (s.getLeft().equals(node)) {
        return s.getRight();
      }
    }
    return null;
  }
  
  public List<Pair<ExecutableNodeBase, Integer>> getSourcesAndChannels() {
    List<Pair<ExecutableNodeBase, Integer>> sourceAndChannel = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Integer> s : sources) {
      sourceAndChannel.add(Pair.of(s.getKey(), s.getValue()));
    }
    return sourceAndChannel;
  }

  public List<ExecutableNodeBase> getExecutableNodeBaseParents() {
    List<ExecutableNodeBase> parents = new ArrayList<>();
    for (ExecutableNode node : subscribers) {
      parents.add((ExecutableNodeBase) node);
    }
    return parents;
  }

  public List<ExecutableNodeBase> getExecutableNodeBaseDependents() {
    return getSources();
//    List<ExecutableNodeBase> deps = new ArrayList<>();
//    for (ExecutableNode node : sources.keySet()) {
//      deps.add((ExecutableNodeBase) node);
//    }
//    return deps;
  }

  public ExecutableNodeBase getExecutableNodeBaseDependent(int idx) {
    return getExecutableNodeBaseDependents().get(idx);
  }

  public ExecutableNodeBase deepcopy() {
    ExecutableNodeBase node = ExecutableNodeBase.create();
    copyFields(this, node);
    return node;
  }

  protected void copyFields(ExecutableNodeBase from, ExecutableNodeBase to) {
    to.subscribers = new ArrayList<>(from.subscribers);
    to.sources = new ArrayList<>(from.sources);
    to.channels = new TreeMap<>(from.channels);
  }

  public void print() {
    print(0);
  }

  void print(int indentSpace) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < indentSpace; i++) {
      builder.append(" ");
    }
    builder.append(this.toString());
    System.out.println(builder.toString());

    for (ExecutableNodeBase dep : getExecutableNodeBaseDependents()) {
      dep.print(indentSpace + 2);
    }
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(uniqueId).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    ExecutableNodeBase rhs = (ExecutableNodeBase) obj;
    return new EqualsBuilder()
                  .appendSuper(super.equals(obj))
                  .append(uniqueId, rhs.uniqueId)
                  .isEquals();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("sources", sources)
        .append("channels", channels)
//        .append("subscribers", subscribers)
//        .append("channels", channels)
        .toString();
  }

}
