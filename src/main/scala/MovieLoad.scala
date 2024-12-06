import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => F}
import java.util.Properties
//数据写入
object MovieLoad {
  def main(args: Array[String]): Unit = {
    //1、创建配置文件对象
    val conf = new SparkConf().setMaster("local").setAppName("MovieLoad")
    //2、创建SparkSession对象
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //3、读CSV文件
    val df = spark.read.format("csv").option("header", "true").load("data/film_info.csv")
    //df.show()
    //4、加载DE到MYSQL中
    // 4.1 加载MYSQL配置文件
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password","12345678")
    prop.put("driver", "com.mysql.cj.jdbc.Driver")
    //4.2 写入数据库
    df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/movie?"+
      "userSSL=false&userUnicode=true&"+
      "characterEncoding=utf8&serverTimezone=UTC","movieInfo",prop)
    println("写入成功")
  }
}
