# Query Processing

On this page, we describe how VerdictDB could speed up query processing. Note that for query processing, VerdictDB internally creates directed acyclic graph (DAG) representations (as described on [this page](/how_it_works/architecture)) and use it for processing queries. The key to VerdictDB's faster query processing is how the DAG is constructed, which we describe below.


## DAG Construction

For description, we use the following example query:

```sql
select product, avg(sales_price) as avg_price
from (
  select product, price * (1 - discount) as sales_price
  from sales_table
  where order_date between date '2018-01-01' and date '2018-01-31'
) t
group by product
order by avg_price desc
```

In the above example, the inner query (projection) computes the price after discount, i.e., `sales_price`, and then the outer query (aggregation) computes the average of `sales_price`. Although this example query may be flattened into a simpler form, we intentionally use this nested form to make our description more general. Also, although VerdictDB internally parses the query (in String format) into its internal Java objects, our description will keep using the query string for easier understanding.

We suppose that a scramble `sales_table_scramble` has been created for the `sales_table` table, and `sales_table_scramble` contains *three* blocks. As described on [this page](/how_it_works/basics), each block of the scramble amounts to a random sample of the original table, i.e., `sales_table`.


### Step 1: regular DAG construction

The given query is decomposed into multiple queries, each of which is a flat query. Except for the root node, all other nodes include `create table as select ...` queries. The query for a parent node depends on its children.

Below we depict the DAG and the queries for those nodes.

<!-- <div class="img-center">
  <img src="/images/dag1.png" class="img-center" />
</div> -->
![zoomify](/images/dag1.png){: .center}


```sql
-- Q1
select *
from temp_table2
```


```sql
-- Q2
create table temp_table2
select product, avg(sales_price) as avg_price
from temp_table1 t
group by product
order by avg_price desc
```


```sql
-- Q3
create table temp_table1
select product, price * (1 - discount) as sales_price
from sales_table_scramble
where order_date between date '2018-01-01' and date '2018-01-31'
```



### Step 2: progressive aggregation DAG construction


```sql
create table temp_table2
select product, sum_price / count_price as avg_price
from (
  select product, sum(sales_price) as sum_price, count(sales_price) as count_price
  from temp_table1 t
  group by product
) verdictdb_inner
order by avg_price desc
```


### Step 3: plan simplification


### Step 4: Execution / Cleaning

