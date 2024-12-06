import java.sql.{Connection, DriverManager, ResultSet}
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
import javax.swing.{JFrame, SwingUtilities}

//将地区电影数前十绘制一个柱状图

object DrawRegionMovie {
  def main(args: Array[String]): Unit = {
    // 设置数据库连接参数
    val url = "jdbc:mysql://localhost:3306/movie?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=UTC" // 替换为实际数据库地址
    val username = "root" // 替换为实际用户名
    val password = "12345678" // 替换为实际密码
    // 加载 MySQL JDBC 驱动
    Class.forName("com.mysql.cj.jdbc.Driver")

    var connection: Connection = null

    // 创建一个地区中文到英文的映射
    val regionMap: Map[String, String] = Map(
      "美国" -> "USA",
      "中国" -> "China",
      "意大利" -> "Italy",
      "法国" -> "France",
      "日本" -> "Japan",
      "韩国" -> "South Korea",
      "英国" -> "United Kingdom",
      "德国" -> "Germany",
      "印度" -> "India",
      "伊朗" -> "Iran",
      "黎巴嫩" -> "Lebanon",
      "新西兰" -> "New Zealand",
      "丹麦" -> "Denmark",
      "澳大利亚" -> "Australia",
      "巴西" -> "Brazil",
      "瑞典" -> "Sweden",
      "爱尔兰" -> "Ireland",
      "西班牙" -> "Spain",
      "阿根廷" -> "Argentina",
      "泰国" -> "Thailand",
      "南斯拉夫" -> "Yugoslavia",
      "苏联" -> "Soviet Union",
      "西德" -> "West Germany",
      "希腊" -> "Greece",
      "奥地利" -> "Austria",
      "加拿大" -> "Canada",
      "俄罗斯" -> "Russia",
      "墨西哥" -> "Mexico",
      "荷兰" -> "Netherlands",
      "哥伦比亚" -> "Colombia",
      "波兰" -> "Poland",
      "冰岛" -> "Iceland",
      "南非" -> "South Africa",
      "挪威" -> "Norway",
      "芬兰" -> "Finland",
      "比利时" -> "Belgium",
      "罗马尼亚" -> "Romania",
      "马来西亚" -> "Malaysia",
      "印度尼西亚" -> "Indonesia",
      "越南" -> "Vietnam",
      "土耳其" -> "Turkey",
      "匈牙利" -> "Hungary",
      "智利" -> "Chile",
      "捷克斯洛伐克" -> "Czechoslovakia",
      "北马其顿" -> "North Macedonia",
      "以色列" -> "Israel",
      "尼泊尔" -> "Nepal",
      "USA" -> "USA",    // USA 是缩写，直接映射
      "瑞士" -> "Switzerland",
      "菲律宾" -> "Philippines",
      "Austria" -> "Austria", // Austria 是英文名，直接映射
      "奥地利 Austria" -> "Austria", // 处理重复的条目
      "新加坡" -> "Singapore",
      "爱沙尼亚" -> "Estonia",
      "波黑" -> "Bosnia and Herzegovina",
      "马其顿" -> "Macedonia"
    )

    try {
      // 建立数据库连接
      connection = DriverManager.getConnection(url, username, password)

      // 查询前10个地区电影数量最多的地区
      val query = "SELECT regions, COUNT(*) AS movie_count FROM newmovieinfo GROUP BY regions ORDER BY movie_count DESC LIMIT 10"

      // 创建 Statement 对象
      val statement = connection.createStatement()

      // 执行查询
      val resultSet: ResultSet = statement.executeQuery(query)

      // 创建柱状图的数据集
      val dataset = new DefaultCategoryDataset()

      // 处理查询结果，将数据添加到数据集，并将中文地区名替换为英文
      while (resultSet.next()) {
        val region = resultSet.getString("regions")
        val movieCount = resultSet.getInt("movie_count")

        // 替换中文地区名为英文
        val englishRegion = regionMap.getOrElse(region, region) // 如果没有对应的英文名，则保持中文名

        dataset.addValue(movieCount, "Movies", englishRegion) // 向数据集中添加数据
      }

      // 创建柱状图
      val chart = ChartFactory.createBarChart(
        "Top 10 Movie Regions", // 图表标题
        "Region",               // X轴标签
        "Movie Count",          // Y轴标签
        dataset,                // 数据集
        PlotOrientation.VERTICAL,
        true,                   // 是否显示图例
        true,                   // 是否显示提示信息
        false                   // 不使用URL链接
      )

      // 显示图表
      SwingUtilities.invokeLater(new Runnable {
        def run(): Unit = {
          val frame = new JFrame("地区电影数前十统计表")
          frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
          frame.getContentPane.add(new ChartPanel(chart))
          frame.pack()
          frame.setVisible(true)
        }
      })

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // 关闭数据库连接
      if (connection != null) connection.close()
    }
  }
}