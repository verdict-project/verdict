# Architecture

## Deployment

VerdictDB is a thin Java library placed between the user (or an application) and the backend database. Even when its Python library is used (i.e., pyverdict), pyverdict relies on the core Java library. The user can make a connection to VerdictDB either using the standard JDBC or the public API provided by VerdictDB. VerdictDB communicates with the backend database also using JDBC except for special cases (e.g., Apache Spark's SparkSession).

<img width="400px" class="img-responsive" style="margin: 0 auto;" src="http://verdictdb.org/image/verdict-architecture.png" />


## Internal Architecture

VerdictDB has the following hierarchical structure. An item that comes earlier in the following list depends (only) on the item that comes after the item. For example, JDBC Wrapper depends on VerdictContext. However, JDBC Wrapper does not directly rely on ExecutionContext.

1. JDBC Wrapper (public interface)
1. VerdictContext (public interface)
1. ExecutionContext
1. QueryCoordinator
1. ExecutionPlan
1. ExecutionNode
