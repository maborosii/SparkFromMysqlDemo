import org.apache.spark.sql.SparkSession
import java.util.Properties
object SparkDemo4Mysql {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("MysqlSupport")
        .master("local[2]")
        .getOrCreate()

    method1(spark)
    // method2(spark)
    // method3(spark)
    // method4(spark)
    // method5(spark)
  }

  /** 方式一：不指定查询条件 所有的数据由RDD的一个分区处理，如果你这个表很大，很可能会出现OOM
    *
    * @param spark
    */
  def method1(spark: SparkSession): Unit = {
    val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
    val prop = new Properties()
    val df = spark.read.jdbc(url, "t_score", prop)

    println(df.count())
    println(df.rdd.partitions.size)
    df.createOrReplaceTempView("t_score")
    import spark.sql
    sql("select * from t_score where score=98").show()

  }

  /** 方式二：指定数据库字段的范围 通过lowerBound和upperBound 指定分区的范围 通过columnName 指定分区的列（只支持整形）
    * 通过numPartitions 指定分区数量 （不宜过大）
    *
    * @param spark
    */
  def method2(spark: SparkSession): Unit = {
    val lowerBound = 1
    val upperBound = 100000
    val numPartitions = 5
    val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
    val prop = new Properties()
    val df = spark.read.jdbc(
      url,
      "t_score",
      "id",
      lowerBound,
      upperBound,
      numPartitions,
      prop
    )

    println(df.count())
    println(df.rdd.partitions.size)
  }

  /** 方式三：根据任意字段进行分区 通过predicates将数据根据score分为2个区
    *
    * @param spark
    */
  def method3(spark: SparkSession): Unit = {
    val predicates = Array[String]("score <= 97", "score > 97 and score <= 100")
    val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
    val prop = new Properties()
    val df = spark.read.jdbc(url, "t_score", predicates, prop)

    println(df.count())
    println(df.rdd.partitions.size)
    import spark.sql
    df.createOrReplaceTempView("t_score")
    sql("select * from t_score").show()
  }

  /** 方式四：通过load获取，和方式二类似
    * @param spark
    */
  def method4(spark: SparkSession): Unit = {
    val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
    val df = spark.read
      .format("jdbc")
      .options(Map("url" -> url, "dbtable" -> "t_score"))
      .load()

    println(df.count())
    println(df.rdd.partitions.size)
    import spark.sql
    df.createOrReplaceTempView("t_score")
    sql("select * from t_score").show()
  }

  /** 方式五：加载条件查询后的数据
    * @param spark
    */
  def method5(spark: SparkSession): Unit = {
    val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
    val df = spark.read
      .format("jdbc")
      .options(
        Map(
          "url" -> url,
          "dbtable" -> "(SELECT s.*,u.name FROM t_score s JOIN t_user u ON s.id=u.score_id) t_score"
        )
      )
      .load()

    println(df.count())
    println(df.rdd.partitions.size)
    import spark.sql
    df.createOrReplaceTempView("t_score")
    sql("select * from t_score").show()

    Thread.sleep(60 * 1000)
  }
}
