import java.awt.{Color, Font, Graphics2D, RenderingHints}
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO
import java.sql.DriverManager
import scala.collection.mutable
import scala.util.Random

//电影类型词云图生成

object CountTypeWithWordCloud {
  def main(args: Array[String]): Unit = {
    // 数据库连接参数
    val url = "jdbc:mysql://localhost:3306/movie?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
    val username = "root"
    val password = "12345678"
    val typeCount = mutable.Map[String, Int]()

    val connection = DriverManager.getConnection(url, username, password)
    try {
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT types FROM newmovieInfo")

      while (resultSet.next()) {
        val types = resultSet.getString("types")
        if (types != null) {
          val firstType = types.split(",|\\|").map(_.trim).headOption.getOrElse("")
          if (firstType.nonEmpty) {
            typeCount(firstType) = typeCount.getOrElse(firstType, 0) + 1
          }
        }
      }

      // 生成词云图
      generateWordCloud(typeCount.toMap)
    } finally {
      connection.close()
    }
  }

  def generateWordCloud(typeCount: Map[String, Int]): Unit = {
    val width = 800
    val height = 600
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    val graphics = image.createGraphics()

    // 设置绘图属性
    graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
    graphics.setColor(Color.WHITE)
    graphics.fillRect(0, 0, width, height)

    val random = new Random()
    val maxFontSize = 140
    val minFontSize = 60

    // 找到最大和最小计数值
    val maxCount = typeCount.values.max
    val minCount = typeCount.values.min

    // 绘制每个单词
    typeCount.foreach { case (word, count) =>
      val fontSize = minFontSize + ((count - minCount).toDouble / (maxCount - minCount) * (maxFontSize - minFontSize)).toInt
      val font = new Font("楷体", Font.BOLD, fontSize)
      graphics.setFont(font)

      // 随机位置和颜色
      val x = random.nextInt(width - fontSize * word.length)
      val y = random.nextInt(height - fontSize) + fontSize
      graphics.setColor(new Color(random.nextInt(256), random.nextInt(256), random.nextInt(256)))
      graphics.drawString(word, x, y)
    }

    graphics.dispose()

    // 保存词云图像
    val outputFile = new File("data/wordcloud.png")
    ImageIO.write(image, "png", outputFile)
    println(s"词云图已保存为 ${outputFile.getAbsolutePath}")
  }
}
