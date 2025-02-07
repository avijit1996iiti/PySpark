Here are some common **PySpark interview questions and answers** categorized by difficulty level:  

---

## **Beginner-Level PySpark Interview Questions**  

### **1. What is PySpark?**
**Answer:**  
PySpark is the Python API for Apache Spark, an open-source distributed computing framework. It allows users to leverage Spark’s capabilities using Python for big data processing, machine learning, and real-time analytics.  

---

### **2. What are the main components of PySpark?**  
**Answer:**  
- **RDD (Resilient Distributed Dataset):** Immutable distributed collection of objects.  
- **DataFrame:** Distributed collection of data organized in columns, similar to a table in SQL.  
- **Dataset:** Strongly-typed DataFrame with JVM objects (not available in PySpark).  
- **Spark SQL:** Module for structured data processing using SQL-like queries.  
- **MLlib:** Machine learning library in Spark.  
- **GraphX:** Graph processing library (not available in PySpark, but GraphFrames can be used).  

---

### **3. How do you create a SparkSession in PySpark?**  
**Answer:**  
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```
**Explanation:** `SparkSession` is the entry point for DataFrame API and Spark SQL.  

---

### **4. What is the difference between an RDD and a DataFrame?**  
**Answer:**  

| Feature       | RDD                        | DataFrame |
|--------------|----------------|------------|
| API Type      | Low-level API  | High-level API |
| Optimization | No optimization | Optimized using Catalyst Optimizer |
| Schema       | No schema enforcement | Schema-based |
| Performance  | Slower due to manual optimization | Faster due to optimizations |

---

### **5. What are transformations and actions in PySpark?**
**Answer:**  
- **Transformations:** Lazy operations that return a new RDD/DataFrame. Examples: `map()`, `filter()`, `groupBy()`, `select()`.  
- **Actions:** Trigger execution and return results. Examples: `count()`, `collect()`, `show()`, `saveAsTextFile()`.  

Example:
```python
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
transformed_rdd = rdd.map(lambda x: x * 2)  # Transformation (lazy)
print(transformed_rdd.collect())  # Action (triggers execution)
```

---

## **Intermediate-Level PySpark Interview Questions**  

### **6. What is lazy evaluation in PySpark?**  
**Answer:**  
Lazy evaluation means Spark does not execute transformations immediately. Instead, it builds a DAG (Directed Acyclic Graph) and executes transformations only when an action is called.  

---

### **7. What is the difference between `cache()` and `persist()`?**  
**Answer:**  
- **`cache()`** stores RDD/DataFrame in memory.  
- **`persist(storageLevel)`** allows specifying storage levels (memory, disk, both).  

Example:
```python
df.cache()  # Stores in memory
df.persist(storageLevel="DISK_ONLY")  # Stores only on disk
```

---

### **8. How do you read a CSV file in PySpark?**  
**Answer:**  
```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.show()
```
**Explanation:**  
- `header=True` reads the first row as column names.  
- `inferSchema=True` automatically detects data types.  

---

### **9. How do you handle missing values in PySpark?**  
**Answer:**  
```python
df = df.na.fill({"age": 25})  # Fill missing values in 'age' column with 25
df = df.dropna()  # Drop rows with null values
df = df.fillna(0)  # Replace all NaN with 0
```

---

### **10. What is the difference between `groupBy()` and `reduceByKey()`?**  
**Answer:**  
| Feature       | `groupBy()` | `reduceByKey()` |
|--------------|------------|----------------|
| Type         | DataFrame operation | RDD operation |
| Aggregation  | Requires explicit aggregation | Performs aggregation automatically |
| Performance  | More shuffle operations | More optimized for large datasets |

Example:
```python
# groupBy()
df.groupBy("department").agg({"salary": "avg"}).show()

# reduceByKey()
rdd = spark.sparkContext.parallelize([("A", 1), ("B", 2), ("A", 3)])
reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)
print(reduced_rdd.collect())
```

---

## **Advanced-Level PySpark Interview Questions**  

### **11. How does PySpark optimize queries?**  
**Answer:**  
- **Catalyst Optimizer:** Optimizes query plans.  
- **Tungsten Execution Engine:** Improves performance using code generation and memory management.  
- **Predicate Pushdown:** Pushes filters to the data source.  

Example:
```python
df.filter(df["age"] > 30).explain(True)  # Shows query plan optimization
```

---

### **12. What is a Broadcast variable in PySpark?**  
**Answer:**  
Broadcast variables allow efficient sharing of read-only data across worker nodes to avoid redundant copies.  

Example:
```python
from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
broadcast_var = sc.broadcast([1, 2, 3])

print(broadcast_var.value)  # Access broadcasted data
```

---

### **13. What is the difference between repartition() and coalesce()?**  
**Answer:**  
- **`repartition(n)`** increases/decreases partitions and performs a full shuffle.  
- **`coalesce(n)`** reduces partitions without full shuffle.  

Example:
```python
df.repartition(10)  # Increase partitions
df.coalesce(2)  # Reduce partitions efficiently
```

---

### **14. How do you implement window functions in PySpark?**  
**Answer:**  
Window functions allow operations like ranking, aggregation, and running totals.  

Example:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

window_spec = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", rank().over(window_spec)).show()
```

---

### **15. How does PySpark handle data skew?**  
**Answer:**  
- **Salting:** Add random keys to distribute data.  
- **Repartitioning:** Increase partitions.  
- **Broadcast Join:** Avoid shuffling large tables by broadcasting smaller tables.  

Example:
```python
df1.join(df2.hint("broadcast"), "id").show()  # Broadcast join
```

---

## **Conclusion**  
These **15 PySpark interview questions** cover all levels, from **basic** (Spark concepts, transformations, actions) to **advanced** (optimizations, performance tuning, and joins). Practicing these will help you ace your PySpark interview! 🚀