
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => F}

import java.util.Properties

//1. 分析每年生产的电影总数 输出格式 1888,1

object CountYear {
  def main(args: Array[String]): Unit = {
    //1、创建配置文件对象
    val conf = new SparkConf().setMaster("local").setAppName("CountMovie")
    //2、创建SparkSession对象
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 3. 配置数据库连接参数
    val jdbcUrl = "jdbc:mysql://localhost:3306/movie?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
    val dbTable = "newmovieInfo" // 替换为数据库中的表名
    val dbProperties = new Properties()
    dbProperties.put("user", "root")          // 数据库用户名
    dbProperties.put("password", "12345678") // 数据库密码
    dbProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // 4. 使用 JDBC 读取数据库表数据
    val df = spark.read.jdbc(jdbcUrl, dbTable, dbProperties)

    // 5. 数据展示
    df.show(false)

    // 6. 分析每年电影总数
    val result = df.withColumn("date", F.year(F.to_date(F.col("date"), "yyyy-MM-dd")))
      .groupBy("date")
      .agg(F.count("movie_name").alias("movie_count"))
      .orderBy("date")

    // 7. 输出结果
    println("每年生产的电影总数:")
    result.collect().foreach(println)

    /*
    [1888,1]
    [1895,2]
    [1896,1]
    [1902,1]
    [1903,1]
     */
  }
}

