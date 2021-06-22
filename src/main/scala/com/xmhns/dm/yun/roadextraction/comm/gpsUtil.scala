package com.xmhns.dm.yun.roadextraction.comm

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{trunc, udf}

import math.{Pi, abs, asin, atan, cos, exp, log, sin, sqrt, toDegrees, toRadians}

object gpsUtil {

  val jw_mx: UserDefinedFunction = udf(lonLat2MercatorX _)
  val jw_my: UserDefinedFunction = udf(lonLat2MercatorY _)

  val x_to_lon: UserDefinedFunction = udf(WebMercator2lonLatX _)
  val y_to_lat: UserDefinedFunction = udf(WebMercator2lonLatY _)

  val lon_to_x: UserDefinedFunction = udf(lonLat2MercatorX _)
  val lat_to_y: UserDefinedFunction = udf(lonLat2MercatorY _)

  val to_r: UserDefinedFunction = udf(math.toRadians _)
  val to_degress: UserDefinedFunction = udf(math.toDegrees _)

  val get_relative_x: UserDefinedFunction = udf(getRelativeX _)
  val get_relative_y: UserDefinedFunction = udf(getRelativeY _)
  val get_p: UserDefinedFunction = udf(getP _)

  val get_distance: UserDefinedFunction = udf(getDistance _)
  val get_drc: UserDefinedFunction = udf(getDrc _)

  val cal_move_x: UserDefinedFunction = udf(calMoveX _)
  val cal_move_y: UserDefinedFunction = udf(calMoveY _)
  // wgs84togcj02
  val wgs_to_gjc: UserDefinedFunction = udf(wgs84togcj02 _)

  val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0
  val a = 6378245.0
  val ee = 0.00669342162296594323

  def lonLat2MercatorX(lon:Double):(Double)={
    lon * 20037508.34 / 180
  }

  def lonLat2MercatorY(lat:Double):(Double)={
    val y = math.log(math.tan((90 + lat) * math.Pi / 360.0)) / (math.Pi / 180.0)
    y * 20037508.34 / 180
  }

  def WebMercator2lonLatX(x:Double):Double={
    x / 20037508.34 * 180.0
  }

  def WebMercator2lonLatY(y:Double):Double={
    val lat = y / 20037508.34 * 180.0
    180.0 / Pi * (2.0 * atan(exp(lat * Pi / 180.0)) - Pi / 2.0)
  }

  def getRelativeX(xa:Double, ya:Double, x0:Double, y0:Double, theta:Double):Double={
    cos(theta) * (xa - x0) - sin(theta) * (ya - y0)
  }

  def getRelativeY(xa:Double, ya:Double, x0:Double, y0:Double, theta:Double):Double={
    sin(theta) * (xa - x0) + cos(theta) * (ya - y0)
  }

  def getP(xa:Double, ya:Double):Double={
    sqrt(xa*xa + ya*ya)
  }

  def calMoveX(x:Double, d:Double, theta:Double):Double={
    cos(theta) * d + x
  }

  def calMoveY(y:Double, d:Double, theta:Double):Double={
    -sin(theta) * d + y
  }

  /**
   *大地坐标系资料WGS-84 长半径a=6378137 短半径b=6356752.3142 扁率f=1/298.2572236
   * @author bigdata_dc
   * @param angle 角度(顺时针，与正北方向的夹角)；
   * @param dist 距离长度；
   * @param lon 经度；
   * @param lat 纬度；
   * @return 从改点的经纬度位置出发，朝angle方向角行驶dist距离后的经纬度点。
   */
  def getNextPoint(angle: Double, dist:Double, lon:Double, lat: Double): (Double, Double) ={
    // 大地坐标系资料WGS-84 长半径a=6378137 短半径b=6356752.3142 扁率f=1/298.2572236
    val a = 6378137
    val b = 6356752.3142
    val f = 1 / 298.2572236

    val alpha1 = math.toRadians(angle)
    val sinAlpha1 = math.sin(alpha1)
    val cosAlpha1 = math.cos(alpha1)

    val tanU1 = (1-f) * math.tan(math.toRadians(lat))
    val cosU1 = 1 / math.sqrt((1 + tanU1 * tanU1))
    val sinU1 = tanU1 * cosU1
    val sigma1 = math.atan2(tanU1, cosAlpha1)
    val sinAlpha = cosU1 * sinAlpha1
    val cosSqAlpha = 1 - sinAlpha * sinAlpha

    val uSq = cosSqAlpha * (a * a - b * b) / (b * b)
    val A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)))
    val B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)))
    var cos2SigmaM = 0.0
    var sinSigma = 0.0
    var cosSigma = 0.0
    var sigma = dist / (b * A)
    var sigmaP = 2 * math.Pi

    while (math.abs(sigma - sigmaP) > 1e-12) {
      cos2SigmaM = math.cos(2 * sigma1 + sigma)
      sinSigma = math.sin(sigma)
      cosSigma = math.cos(sigma)

      val deltaSigma = B * sinSigma * (cos2SigmaM + B / 4 * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)
        - B / 6 * cos2SigmaM * (-3 + 4 * sinSigma * sinSigma) * (
        -3 + 4 * cos2SigmaM * cos2SigmaM)))
      sigmaP = sigma
      sigma = dist / (b * A) + deltaSigma
    }

    val tmp = sinU1 * sinSigma - cosU1 * cosSigma * cosAlpha1

    val lat2 = math.atan2(sinU1 * cosSigma + cosU1 * sinSigma * cosAlpha1,
      (1 - f) * math.sqrt(sinAlpha * sinAlpha + tmp * tmp))

    val lambda_v = math.atan2(sinSigma * sinAlpha1, cosU1 *cosSigma - sinU1 * sinSigma * cosAlpha1)

    val C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha))

    val L = lambda_v - (1 - C) * f * sinAlpha * (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)))
    val revAz = math.atan2(sinAlpha, -tmp)
    (lon + math.toDegrees(L), math.toDegrees(lat2))
  }

  /**
   * @author bigdata_dc
   * @param lon0 第一个点的经度；
   * @param lat0 第一个点的纬度；
   * @param lon1 第二个点的经度；
   * @param lat1 第二个点的纬度；
   * @return 两个点的夹角, resDrc (0, 180)。
   */
  def computeDrc(lon0:Double, lat0: Double, lon1:Double, lat1: Double): Double = {
    var res = 0.0
    val ilat0 = (0.50 + lat0 * 360000.0)
    val ilat1 = (0.50 + lat1 * 360000.0)
    val ilon0 = (0.50 + lon0 * 360000.0)
    val ilon1 = (0.50 + lon1 * 360000.0)

    val rlat0 = math.toRadians(lat0)
    val rlon0 = math.toRadians(lon0)
    val rlat1 = math.toRadians(lat1)
    val rlon1 = math.toRadians(lon1)

    if (ilat0 == ilat1 && ilon0 == ilon1)
      return res
    else if (ilon0 == ilon1) {
      if (ilat0 > ilat1)
        return 180.0
    }
    else {
      val c = math.acos(math.sin(rlat1) * math.sin(rlat0) + math.cos(rlat1) * math.cos(rlat0) * math.cos((rlon1 - rlon0)))
      val A = math.asin(math.cos(rlat1) * math.sin((rlon1 - rlon0)) / math.sin(c))
      res = math.toDegrees(A)
      if (ilat1 > ilat0 && ilon1 > ilon0)
        1
      else if (ilat1 < ilat0 && ilon1 < ilon0)
        res = 180.0 - res
      else if (ilat1 < ilat0 && ilon1 > ilon0)
        res = 180.0 - res
      else if (ilat1 > ilat0 && ilon1 < ilon0)
        res = res + 360.0
    }
    res
  }

  def getDegree(latA:Double, lonA:Double, latB:Double, lonB:Double):Double = {
    val radLatA = math.toRadians(latA)
    val radLonA = math.toRadians(lonA)
    val radLatB = math.toRadians(latB)
    val radLonB = math.toRadians(lonB)
    val dLon = radLonB - radLonA
    val y = sin(dLon) * cos(radLatB)
    val x = cos(radLatA) * sin(radLatB) - sin(radLatA) * cos(radLatB) * cos(dLon)
    val brng = toDegrees(math.atan2(y, x))
    (brng + 360) % 360
  }


  /**
   * @author bigdata_dc
   * @param lon0 第一个点的经度；
   * @param lat0 第一个点的纬度；
   * @param lon1 第二个点的经度；
   * @param lat1 第二个点的纬度；
   * @return 两个点的距离(m)。
   */
  def getDistance(lon0:Double, lat0: Double, lon1:Double, lat1: Double): Double ={
    val er = 6371  // 地球平均半径，6371km
    val vlat0 = toRadians(lat0)
    val vlat1 = toRadians(lat1)
    val vlon0 = toRadians(lon0)
    val vlon1 = toRadians(lon1)

    val dlon = abs(vlon0 - vlon1)
    val dlat = abs(vlat0 - vlat1)
    val h = hav(dlat) + cos(vlat0) * cos(vlat1) * hav(dlon)
    2 * er * asin(sqrt(h))*1000
  }

  def getDrc(drc1:Double, drc2:Double):Double={
    val temp = abs(drc1-drc2)
    if(temp>180.0) 360.0-temp
    else temp
  }

  /**
   * 从整个路网数据中搜索与GPS点匹配度最佳的道路和道路上的最近点
   * @author bigdata_dc
   * @param lon 经度；
   * @param lat 维度；
   * @param drc 行驶方向；
   * @param path 所在地的路网数据；
   * @return 若存在符合条件的，返回匹配点的经度、维度、点到线的距离、匹配到的road_id。
   */
  def getClostPointInLine2(lon:Double, lat:Double, drc:Double, path:DataFrame): List[Double] ={
    // pathv 为线路文件的信息，()
    val pathv = path.collect()
    var clostPathNum:Int = 0
    var minDist = 999999.9
    var info = List(0.0, 0.0, 99999.9)
    var clost_point = List(lon ,lat)
    var clost_road_id = -1
    var drcDiff = 0.0
    var line_drc = 0.0
    var f = 999999.9
    for(p <- pathv){
      var (x, y, dist) = (0.0, 0.0, 99999.9)
      val (xp, yp, x1, y1, x2, y2, road_id) =(lon, lat, p(2).toString.toDouble, p(3).toString.toDouble,
        p(4).toString.toDouble, p(5).toString.toDouble, p(1).toString.toInt)

      line_drc = computeDrc(x1, y1, x2, y2)

      drcDiff = math.abs(line_drc-drc)
      if(drcDiff>180.0){
        drcDiff = 360.0 - drcDiff
      }
      if(drcDiff<=90){
        if(y1 == y2){
          x = xp
          y = y1
          dist = getDistance(x, y, xp, yp)
        }
        else if(x1 == x2){
          x = x1
          y = yp
          dist = getDistance(x, y, xp, yp)
        }
        else{
          val k = (x2 - x1) / (y1 - y2)
          val b = yp - (k * xp)
          val k1 = (y2 - y1) / (x2 - x1)
          val b1 = y1 - (k1 * x1)
          x = (b1 - b) / (k - k1)
          y = k * x + b
          if(math.min(x1, x2) < x && x < math.max(x1, x2) && math.min(y1, y2) < y && y < math.max(y1, y2))
            dist = getDistance(x, y, xp, yp)
          else if(getDistance(xp, yp, x1, y1) < getDistance(xp, yp, x2, y2)){
            x = x1
            y = y1
            dist = getDistance(x, y, xp, yp)
          }
          else {
            x = x2
            y = y2
            dist = getDistance(x, y, xp, yp)
          }
        }
        if(dist < 100){
          if(dist <25 && drcDiff<45)
            clostPathNum = clostPathNum + 1
          val temp_f = dist*0.45 + drcDiff*0.55
          if(temp_f<f){
            minDist = dist
            f = temp_f
            clost_point = List(x, y)
            clost_road_id = road_id
          }
        }
      }
    }
    info = List(clost_point.head, clost_point(1), minDist, clost_road_id, lon, lat, clostPathNum)
    info
  }

  def hav(theta:Double): Double ={
    val s = math.sin(theta/2)
    s*s
  }

  def getClostPointInLine(lon:Double, lat:Double, path:DataFrame): List[Double] ={
    val pathv = path.collect()
    var minDist = 999999.9
    var info = List(0.0, 0.0, 99999.9)
    var clost_point = List(0.0 ,0.0)
    for(i <- Range(0, pathv.length-1)){
      var (x, y, dist) = (0.0, 0.0, 99999.9)
      val (xp, yp, x1, y1, x2, y2) =(lon, lat, pathv(i)(0).toString.toDouble, pathv(i)(1).toString.toDouble,
        pathv(i+1)(0).toString.toDouble, pathv(i+1)(1).toString.toDouble)

      if(y1 == y2){
        x = xp
        y = y1
        dist = getDistance(x, y, xp, yp)
      }
      else if(x1 == x2){
        x = x1
        y = yp
        dist = getDistance(x, y, xp, yp)
      }
      else{

        val k = (x2 - x1) / (y1 - y2)
        val b = yp - (k * xp)
        val k1 = (y2 - y1) / (x2 - x1)
        val b1 = y1 - (k1 * x1)
        x = (b1 - b) / (k - k1)
        y = k * x + b
        if(math.min(x1, x2) < x && x < math.max(x1, x2) && math.min(y1, y2) < y && y < math.max(y1, y2))
          dist = getDistance(x, y, xp, yp)
        else if(getDistance(xp, yp, x1, y1) < getDistance(xp, yp, x2, y2)){
          x = x1
          y = y1
          dist = getDistance(x, y, xp, yp)
        }
        else {
          x = x2
          y = y2
          dist = getDistance(x, y, xp, yp)
        }
      }
      if(dist < minDist){
        minDist = dist
        clost_point = List(x, y)
      }
    }
    info = List(clost_point.head, clost_point(1), minDist)
    info
  }
  def out_of_china(lng:Double, lat:Double):Boolean={
    if (lng < 72.004 || lng > 137.8347) return true
    if (lat < 0.8293 || lat > 55.8271)  return true
    false
  }

  def transformlat(lng:Double, lat:Double):Double={
    var ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(math.abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * Pi) + 20.0 *
      math.sin(2.0 * lng * Pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * Pi) + 40.0 *
      math.sin(lat / 3.0 * Pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * Pi) + 320 *
      math.sin(lat * Pi / 30.0)) * 2.0 / 3.0
    ret
  }



  def transformlng(lng:Double, lat:Double):Double={
    var ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(math.abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * Pi) + 20.0 *
      math.sin(2.0 * lng * Pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * Pi) + 40.0 *
      math.sin(lng / 3.0 * Pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * Pi) + 300.0 *
      math.sin(lng / 30.0 * Pi)) * 2.0 / 3.0
    ret
  }

  def wgs84togcj02(lng:Double, lat:Double):List[Double]={
    if (out_of_china(lng, lat)) return List(lng, lat)
    var dlat = transformlat(lng - 105.0, lat - 35.0)
    var dlng = transformlng(lng - 105.0, lat - 35.0)
    val radlat = lat / 180.0 * Pi
    var magic = sin(radlat)
    magic = 1 - ee * magic * magic
    val sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * Pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * Pi)
    List(lat + dlat, lng + dlng)
  }
}
