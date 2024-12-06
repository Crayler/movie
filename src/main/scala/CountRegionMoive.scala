import java.sql.{Connection, DriverManager, ResultSet} // 导入 JDBC 相关类

//统计每个地区region的电影数量

object CountRegionMoive {
  def main(args: Array[String]): Unit = {
    // 设置数据库连接参数
    val url = "jdbc:mysql://localhost:3306/movie?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"// 替换为实际数据库地址
    val username = "root" // 替换为实际用户名
    val password = "12345678" // 替换为实际密码
    // 加载 MySQL 驱动程序
    Class.forName("com.mysql.cj.jdbc.Driver")

    var connection: Connection = null

    try {
      // 建立数据库连接
      connection = DriverManager.getConnection(url, username, password)

      // 创建 SQL 查询
      val query = "SELECT regions, COUNT(*) AS movie_count FROM newmovieinfo GROUP BY regions"

      // 创建 Statement 对象
      val statement = connection.createStatement()

      // 执行查询
      val resultSet: ResultSet = statement.executeQuery(query)

      // 处理查询结果
      println("地区电影数:")
      while (resultSet.next()) {
        val regions = resultSet.getString("regions")
        val movieCount = resultSet.getInt("movie_count")
        println(s" $regions： $movieCount")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // 关闭数据库连接
      if (connection != null) connection.close()
    }

  }
}