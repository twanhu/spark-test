package sql.hotel

import java.io.File

/**
 * 创建工具类，判断目录是否存在，删除目录
 */
object DeleteDirUtils {

  def apply(dir: File): Unit = {
    if (dir.exists()) { //判断传入的File对象是否存在
      val files: Array[File] = dir.listFiles() //创建File数组
      for (file <- files) { //遍历所有的子目录和文件
        if (file.isDirectory) {
          apply(file) //如果是目录，递归调用apply()
        } else {
          file.delete() // 如果是文件，直接删除
        }
      }
      dir.delete() //删除完一个目录里的所有文件后，就删除这个目录
    }
  }

}
