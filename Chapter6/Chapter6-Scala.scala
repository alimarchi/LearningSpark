// Databricks notebook source
// MAGIC %md
// MAGIC #### Spark SQL and Datasets
// MAGIC 
// MAGIC Datasets: strongly typed distributed collections. 
// MAGIC Only Scala and Java are strongly typed, Python and R support only the untyped DataFrame API. 
// MAGIC 
// MAGIC In order to create Dataset[T], where T is your typed object in Scala, you need a case class that defines the object.

// COMMAND ----------

// To create a distributed Dataset[Bloggers], we must first define a Scala case class that defines each individual field that comprises a Scala object.

case class Bloggers(id:BigInt, first:String, last:String, url:String, hits: BigInt, campaigns:Array[String])

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We can now read the file from the data source:
// MAGIC 
// MAGIC ```
// MAGIC val bloggers = "../data/bloggers.json"
// MAGIC val bloggersDS = spark
// MAGIC  .read
// MAGIC  .format("json")
// MAGIC  .option("path", bloggers)
// MAGIC  .load()
// MAGIC  .as[Bloggers]
// MAGIC  ```

// COMMAND ----------

val bloggers = "/FileStore/tables/blogs2.json"
val bloggersDS = spark
 .read
 .format("json")
 .option("path", bloggers)
 .load()
 .as[Bloggers]

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### Working with Datasets, creating sample data

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC You have to know all the individual column names and types for the rows you are reading. Unlike with DataFrames, where you can optionally let Spark infer the schema, the Dataset API requires that you define your data types ahead of time and that your case class or JavaBean class matches your schema.
// MAGIC 
// MAGIC One simple and dynamic way to create a sample Dataset is using a SparkSession
// MAGIC instance. 

// COMMAND ----------

// In Scala
import scala.util.Random._

// Our case class for the Dataset
case class Usage(uid:Int, uname:String, usage: Int)
val r = new scala.util.Random(42)

// Create 1000 instances of scala Usage class 
// This generates data on the fly
val data = for (i <- 0 to 1000)
 yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
 r.nextInt(1000)))

// Create a Dataset of Usage typed data
val dsUsage = spark.createDataset(data)

dsUsage.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC ##### Transforming
// MAGIC ##### Higher-order functions and functional programming
// MAGIC 
// MAGIC Recall that Datasets are strongly typed collections of domain-specific objects. These objects can be transformed in parallel using functional or relational operations.
// MAGIC Examples of these transformations include map(), reduce(), filter(), select(), and aggregate(). 

// COMMAND ----------

import org.apache.spark.sql.functions._

dsUsage
 .filter(d => d.usage > 900)
 .orderBy(desc("usage"))
 .show(5, false)


// COMMAND ----------

import org.apache.spark.sql.functions._

dsUsage
 .filter(d => d.usage > 900) // We use a lambda expression as an argument to the filter() method
 .orderBy(desc("usage")) // We defined a Scala function
 .show(5, false)

// In both cases, the filter() method iterates over each row of the Usage object in the distributed Dataset and applies the expression or executes the function, returning a new Dataset 

// COMMAND ----------

// Use an if-then-else lambda expression and compute a value
dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
 .show(5, false)

// Define a function to compute the usage
def computeCostUsage(usage: Int): Double = {
 if (usage > 750) usage * 0.15 else usage * 0.50
}

// Use the function as an argument to map()
dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

// COMMAND ----------

// Create a new case class with an additional field, cost

case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)

// Compute the usage cost with Usage as a parameter
// Return a new object, UsageCost
def computeUserCostUsage(u: Usage): UsageCost = {
 val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
 UsageCost(u.uid, u.uname, u.usage, v)
}

// Use map() on our original Dataset
dsUsage.map(u => {computeUserCostUsage(u)}).show(5)


// COMMAND ----------

// MAGIC %md
// MAGIC Recall that a DataFrame is a Dataset[Row], where Row is a generic untyped JVM object that can hold different types of fields. 

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Converting DataFrames to Datasets
// MAGIC 
// MAGIC 
// MAGIC To convert an existing DataFrame df to a Dataset of type SomeCaseClass, simply use the df.as[SomeCaseClass] notation.
// MAGIC 
// MAGIC ``` val bloggersDS = spark
// MAGIC  .read
// MAGIC  .format("json")
// MAGIC  .option("path", "/data/bloggers/bloggers.json")
// MAGIC  .load()
// MAGIC  .as[Bloggers]
// MAGIC  ````
// MAGIC  
// MAGIC spark.read.format("json") returns a DataFrame<Row>, which in Scala is a type alias for Dataset[Row]. 

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Cost of using Datasets and strategies to mitigate costs
// MAGIC 
// MAGIC when Datasets are passed to higher-order functions such as filter(), map(), or flatMap() that take lambdas and functional arguments, there is a cost associated with deserializing from Spark’s internal Tungsten format into the JVM object.
// MAGIC 
// MAGIC One strategy to mitigate excessive serialization and deserialization is to use DSL expressions in your queries and avoid excessive use of lambdas as anonymous functions as arguments to higher-order functions. 
// MAGIC 
// MAGIC The second strategy is to chain your queries together in such a way that serialization and deserialization is minimized. 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ```
// MAGIC Person(id: Integer, firstName: String, middleName: String, lastName: String, gender: String, birthDate: String, ssn: String, salary: String)
// MAGIC 
// MAGIC import java.util.Calendar
// MAGIC val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40
// MAGIC personDS
// MAGIC  // Everyone above 40: lambda-1
// MAGIC  .filter(x => x.birthDate.split("-")(0).toInt > earliestYear)
// MAGIC  
// MAGIC  // Everyone earning more than 80K
// MAGIC  .filter($"salary" > 80000)
// MAGIC  
// MAGIC import java.util.Calendar
// MAGIC val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40
// MAGIC personDS
// MAGIC  // Everyone above 40: lambda-1
// MAGIC  .filter(x => x.birthDate.split("-")(0).toInt > earliestYear)
// MAGIC  
// MAGIC  // Everyone earning more than 80K
// MAGIC  .filter($"salary" > 80000)
// MAGIC 
// MAGIC ```
// MAGIC 
// MAGIC We incur the cost of serializing and deserializing the Person JVM object.
// MAGIC 
// MAGIC By contrast, the following query uses only DSL and no lambdas. As a result, it’s much more efficient—no serialization/deserialization is required for the entire composed and chained query:
// MAGIC 
// MAGIC ```
// MAGIC personDS
// MAGIC  .filter(year($"birthDate") > earliestYear) // Everyone above 40
// MAGIC  .filter($"salary" > 80000) // Everyone earning more than 80K
// MAGIC  .filter($"lastName".startsWith("J")) // Last name starts with J
// MAGIC  .filter($"firstName".startsWith("D")) // First name starts with D
// MAGIC  .count()
// MAGIC  
// MAGIC ```
