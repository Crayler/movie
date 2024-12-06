
//清洗date列年份数据统一改为yyyy-MM-dd的格式
//regions 列，types 列和date列都取第一个值，且regions 列中将 中国大陆、中国香港 和 中国台湾 替换为 中国
/*
    movie_name|      date|                  regions|              types|                            actors|actor_count|score|vote_count|
+--------------+----------+-------------------------+-------------------+----------------------------------+-----------+-----+----------+
|  肖申克的救赎|1994-09-10|                     美国|          犯罪|剧情|  蒂姆·罗宾斯|摩根·弗里曼|鲍勃·...|         25|  9.7|   3065926|
|      霸王别姬|1993-07-26|        中国大陆|中国香港|     剧情|爱情|同性|  张国荣|张丰毅|巩俐|葛优|英达|...|         28|  9.6|   2264084|

 */



import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => F}
import java.util.Properties

object CleanMovieLoad {
  def main(args: Array[String]): Unit = {
    // 1.配置 Spark
    val conf = new SparkConf().setMaster("local").setAppName("CleanMovieLoad")
    // 2. 创建SparkSession对象
    val spark = SparkSession.builder()
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  // 使用严格模式处理无效日期
      .config("spark.master", "local")
      .appName("MovieLoad")
      .getOrCreate()
    // 3.读取 CSV 数据
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true") // 自动推断字段类型
      .load("data/film_info.csv")
    // 4. 清洗release_date字段为yyyy-MM-dd格式
    // 4. 清洗数据：处理 regions 列，types 列和date列
    val cleanedDf = df
      .withColumn("regions",
        F.when(F.col("regions").contains("中国大陆") || F.col("regions").contains("中国香港") || F.col("regions").contains("中国台湾"), "中国")
          .otherwise(F.split(F.col("regions"), "\\|").getItem(0))
      ) // 按分隔符拆分并取第一个值，同时替换特定区域为中国
      .withColumn("types", F.split(F.col("types"), "\\|").getItem(0)) // 处理 types 列，取第一个值
      .withColumn("actors", F.split(F.col("actors"), "\\|").getItem(0)) // 处理 actors 列，取第一个值
      .withColumn(
        "date",
        F.when(
          F.length(F.col("date")) === 4, // 如果是年份
          F.concat(F.col("date"), F.lit("-01-01")) // 补充为 yyyy-MM-dd 格式
        ).otherwise(F.col("date")) // 保留其他格式
      )
    // 5. 显示清洗后的数据
    cleanedDf.show()
    // 6 加载MYSQL配置文件
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password","12345678")
    prop.put("driver", "com.mysql.cj.jdbc.Driver")
    //7 写入数据库
    cleanedDf.write.mode("append").jdbc("jdbc:mysql://localhost:3306/movie?"+
      "userSSL=false&userUnicode=true&"+
      "characterEncoding=utf8&serverTimezone=UTC","newmovieInfo",prop)
    println("数据写入成功！")
  }
}
