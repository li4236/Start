package util

import akka.util.ByteString

/**
  * 处理数据返回函数
  */
class DealData {

  def speed_value(bs: ByteString): Double = {
    val x: Float = hex2decimal(byte2String(bs(3)) + byte2String(bs(2)))
      .asInstanceOf[Float] / 1000
    f"$x%2.2f".toDouble
  }

  def height_value(bs: ByteString): Double = {
    val x: Float  = hex2decimal(byte2String(bs(5)) + byte2String(bs(4)))
      .asInstanceOf[Float] / 100
    f"$x%2.2f".toDouble
  }

  def err_speed_value(bs: ByteString): Double = {
    val x: Float = hex2decimal(byte2String(bs(8)) + byte2String(bs(9)))
      .asInstanceOf[Float] / 1000
    f"$x%2.2f".toDouble
  }

  def err_height_value(bs: ByteString): Double = {
    val x: Float  = hex2decimal(byte2String(bs(6)) + byte2String(bs(7)))
      .asInstanceOf[Float] / 100
    f"$x%2.2f".toDouble
  }

  def byte2String(byte: Byte): String = {
    val jsb = new java.lang.StringBuilder();
    val sb = new StringBuilder(jsb)
    val hexstr = Integer.toHexString(0xFF & byte)
    if (hexstr.length() == 1) {
      sb.append('0')
    }
    sb.append(hexstr)
    return sb.toString()
  }

  def hex2decimal(str: String): Int ={
    val digits = "0123456789ABCDEF"
    val string = str.toUpperCase
    var `val` = 0
    var i = 0
    while ( {
      i < string.length
    }) {
      val c = string.charAt(i)
      val d = digits.indexOf(c)
      `val` = 16 * `val` + d

      {
        i += 1; i - 1
      }
    }
    return `val`
  }

}
