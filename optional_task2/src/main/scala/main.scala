import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BookSalesNormalization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Normalize Book Sales Data")
      .master("local[*]")  // Use this for local execution
      .getOrCreate()

    import spark.implicits._


    val filePath = "sample_dataset.csv"
    val df = spark.read.option("header", "true").csv(filePath)
    
    val salesDF = df.withColumn("quantity", $"quantity".cast("int"))
      .withColumn("price", $"price".cast("double"))
      .withColumn("sale_date", to_date($"sale_date", "yyyy-MM-dd"))
      .withColumn("publish_date", to_date($"publish_date", "yyyy-MM-dd"))


    val booksDF = salesDF.select("book_id", "title", "author_id", "author_name", "genre", "publish_date").distinct()

 
    val authorsDF = salesDF.select("author_id", "author_name").distinct()


    val salesTableDF = salesDF.select("sale_id", "book_id", "sale_date", "quantity", "price")

    // Aggregate sales by book
    val salesByBookDF = salesTableDF.groupBy("book_id")
      .agg(sum("quantity").as("total_quantity"), sum("price").as("total_price"))

    // Aggregate sales by title
    val salesByTitleDF = salesDF.groupBy("title", "author_name")
      .agg(sum("quantity").as("total_quantity"), sum("price").as("total_price"))

    // Aggregate sales by month
    val salesByMonthDF = salesTableDF.withColumn("sales_month", month($"sale_date"))
      .withColumn("sales_year", year($"sale_date"))
      .groupBy("sales_year", "sales_month")
      .agg(sum("quantity").as("total_quantity"), sum("price").as("total_price"))

    // Show results (for debugging purposes)
    booksDF.show()
    authorsDF.show()
    salesTableDF.show()
    salesByBookDF.show()
    salesByTitleDF.show()
    salesByMonthDF.show()

    // Write the resulting dataframes to CSV files (for example)
    booksDF.write.option("header", "true").csv("output/books")
    authorsDF.write.option("header", "true").csv("output/authors")
    salesTableDF.write.option("header", "true").csv("output/sales")
    salesByBookDF.write.option("header", "true").csv("output/sales_by_book")
    salesByTitleDF.write.option("header", "true").csv("output/sales_by_title")
    salesByMonthDF.write.option("header", "true").csv("output/sales_by_month")

    spark.stop()
  }
}
