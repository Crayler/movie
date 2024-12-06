import java.sql.DriverManager
import scala.collection.mutable

//统计电影类型types数量，并输出前5

object CountType {
  def main(args: Array[String]): Unit = {
    // 设置数据库连接参数
    val url = "jdbc:mysql://localhost:3306/movie?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"// 替换为实际数据库地址
    val username = "root" // 替换为实际用户名
    val password = "12345678" // 替换为实际密码
    // 创建一个 Map 来统计电影类型
    val typeCount = mutable.Map[String, Int]()
    // 数据库连接和查询
    val connection = DriverManager.getConnection(url, username, password)
    try {
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT types FROM newmovieInfo") // 替换为实际表名和字段名
      // 遍历查询结果
//      while (resultSet.next()) {
//        val types = resultSet.getString("types") // 假设类型存储为逗号分隔的字符串，例如 "Action, Comedy"
//        if (types != null) {
//          types.split(",").map(_.trim).foreach { t =>
//            typeCount(t) = typeCount.getOrElse(t, 0) + 1
//          }电影类型统计结果： 纪录片: 149 剧情: 116 剧情|爱情: 115 动画|短片: 93
//        }
//      }
//      // 输出所有类型及统计数量
//      println("电影类型统计结果：")
//      typeCount.toSeq.sortBy(-_._2).foreach { case (movieType, count) =>
//        println(s"$movieType: $count")
//      }
      //
      while (resultSet.next()) {
        val types = resultSet.getString("types")
        if (types != null) {
          // 仅取每行类型的第一个值
          val firstType = types.split(",|\\|").map(_.trim).headOption.getOrElse("")
          if (firstType.nonEmpty) {
            typeCount(firstType) = typeCount.getOrElse(firstType, 0) + 1
          }
        }
      }
      println("电影类型统计结果（前5名）：")
      typeCount.toSeq
        .sortBy(-_._2) // 按数量降序排序
        .take(5)       // 取前5名
        .foreach { case (movieType, count) =>
          println(s"$movieType: $count")
        }
    } finally {
      connection.close()
    }
  }
}


