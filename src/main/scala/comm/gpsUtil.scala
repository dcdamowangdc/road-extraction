package comm

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.math._

object gpsUtil {

  val jw_mx: UserDefinedFunction = udf(lonLat2MercatorX _)
  val jw_my: UserDefinedFunction = udf(lonLat2MercatorY _)

  val x_to_lon: UserDefinedFunction = udf(WebMercator2lonLatX _)
  val y_to_lat: UserDefinedFunction = udf(WebMercator2lonLatY _)

  val lon_to_x: UserDefinedFunction = udf(lonLat2MercatorX _)
  val lat_to_y: UserDefinedFunction = udf(lonLat2MercatorY _)

  val to_r: UserDefinedFunction = udf(toRadians _)
  val to_degress: UserDefinedFunction = udf(toDegrees _)

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

  def lonLat2MercatorX(lon:Double):Double = lon * 20037508.34 / 180

  def lonLat2MercatorY(lat:Double):Double = log(tan((90 + lat) * Pi / 360.0)) / (Pi / 180.0) * 20037508.34 / 180


  def WebMercator2lonLatX(x:Double):Double = x / 20037508.34 * 180.0

  def WebMercator2lonLatY(y:Double):Double=180.0 / Pi * (2.0 * atan(exp((y / 20037508.34 * 180.0) * Pi / 180.0)) - Pi / 2.0)

  def getRelativeX(xa:Double, ya:Double, x0:Double, y0:Double, theta:Double):Double=cos(theta) * (xa - x0) - sin(theta) * (ya - y0)


  def getRelativeY(xa:Double, ya:Double, x0:Double, y0:Double, theta:Double):Double=sin(theta) * (xa - x0) + cos(theta) * (ya - y0)

  def getP(xa:Double, ya:Double):Double= sqrt(xa*xa + ya*ya)

  def calMoveX(x:Double, d:Double, theta:Double):Double= cos(theta) * d + x

  def calMoveY(y:Double, d:Double, theta:Double):Double= -sin(theta) * d + y

  /**
   *?????????????????????WGS-84 ?????????a=6378137 ?????????b=6356752.3142 ??????f=1/298.2572236
   * @author bigdata_dc
   * @param angle ??????(????????????????????????????????????)???
   * @param dist ???????????????
   * @param lon ?????????
   * @param lat ?????????
   * @return ???????????????????????????????????????angle???????????????dist???????????????????????????
   */
  def getNextPoint(angle: Double, dist:Double, lon:Double, lat: Double): (Double, Double) ={
    // ?????????????????????WGS-84 ?????????a=6378137 ?????????b=6356752.3142 ??????f=1/298.2572236
    val (a, b, f, alpha1) = (6378137, 6356752.3142, 1 / 298.2572236, toRadians(angle))
    val (sinAlpha1, cosAlpha1) = (sin(alpha1), cos(alpha1))

    val tanU1 = (1-f) * tan(toRadians(lat))
    val cosU1 = 1 / sqrt((1 + tanU1 * tanU1))
    val sinU1 = tanU1 * cosU1
    val sigma1 = atan2(tanU1, cosAlpha1)
    val sinAlpha = cosU1 * sinAlpha1
    val cosSqAlpha = 1 - sinAlpha * sinAlpha

    val uSq = cosSqAlpha * (a * a - b * b) / (b * b)
    val A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)))
    val B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)))
    var (cos2SigmaM, sinSigma, cosSigma, sigma, sigmaP) = (0.0, 0.0, 0.0, dist/(b*A), 2*Pi)

    while (abs(sigma - sigmaP) > 1e-12) {
      cos2SigmaM = cos(2 * sigma1 + sigma)
      sinSigma = sin(sigma)
      cosSigma = cos(sigma)

      val deltaSigma = B * sinSigma * (cos2SigmaM + B / 4 * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)
        - B / 6 * cos2SigmaM * (-3 + 4 * sinSigma * sinSigma) * (
        -3 + 4 * cos2SigmaM * cos2SigmaM)))
      sigmaP = sigma
      sigma = dist / (b * A) + deltaSigma
    }

    val tmp = sinU1 * sinSigma - cosU1 * cosSigma * cosAlpha1

    val lat2 = atan2(sinU1 * cosSigma + cosU1 * sinSigma * cosAlpha1,
      (1 - f) * sqrt(sinAlpha * sinAlpha + tmp * tmp))

    val lambda_v = atan2(sinSigma * sinAlpha1, cosU1 *cosSigma - sinU1 * sinSigma * cosAlpha1)

    val C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha))

    val L = lambda_v - (1 - C) * f * sinAlpha * (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)))
    val revAz = atan2(sinAlpha, -tmp)
    (lon + toDegrees(L), toDegrees(lat2))
  }

  /**
   * @author bigdata_dc
   * @param lon0 ????????????????????????
   * @param lat0 ????????????????????????
   * @param lon1 ????????????????????????
   * @param lat1 ????????????????????????
   * @return ??????????????????, resDrc (0, 180)???
   */
  def computeDrc(lon0:Double, lat0: Double, lon1:Double, lat1: Double): Double = {
    var res = 0.0
    val (ilat0, ilat1, ilon0, ilon1) = (0.50 + lat0 * 360000.0, 0.50 + lat1 * 360000.0, 0.50 + lon0 * 360000.0, 0.50 + lon1 * 360000.0)
    val (rlat0, rlon0, rlat1, rlon1) = (toRadians(lat0), toRadians(lon0), toRadians(lat1), toRadians(lon1))

    if (ilat0 == ilat1 && ilon0 == ilon1) return res
    else if (ilon0 == ilon1) {
      if (ilat0 > ilat1)
        return 180.0
    }
    else {
        val c = acos(sin(rlat1) * sin(rlat0) + cos(rlat1) * cos(rlat0) * cos(rlon1 - rlon0))
        val A = asin(cos(rlat1) * sin((rlon1 - rlon0)) / sin(c))
        res = toDegrees(A)
        if (ilat1 > ilat0 && ilon1 > ilon0)
          1
        else if (ilat1 < ilat0 && ilon1 < ilon0)
          res = 180.0 - res
        else if (ilat1 < ilat0 && ilon1 > ilon0)
          res = 180.0 - res
        else if (ilat1 > ilat0 && ilon1 < ilon0)
          res = res + 360.0
      }
    if(res<0) res += 360.0
    res
    }

  def getDegree(latA:Double, lonA:Double, latB:Double, lonB:Double):Double = {
    val radLatA = toRadians(latA)
    val radLonA = toRadians(lonA)
    val radLatB = toRadians(latB)
    val radLonB = toRadians(lonB)
    val dLon = radLonB - radLonA
    val y = sin(dLon) * cos(radLatB)
    val x = cos(radLatA) * sin(radLatB) - sin(radLatA) * cos(radLatB) * cos(dLon)
    val brng = toDegrees(atan2(y, x))
    (brng + 360) % 360
  }


  /**
   * @author bigdata_dc
   * @param lon0 ????????????????????????
   * @param lat0 ????????????????????????
   * @param lon1 ????????????????????????
   * @param lat1 ????????????????????????
   * @return ??????????????????(m)???
   */
  def getDistance(lon0:Double, lat0: Double, lon1:Double, lat1: Double): Double ={
    val (er, rlat0, rlon0, rlat1, rlon1) = (6371, toRadians(lat0), toRadians(lon0), toRadians(lat1), toRadians(lon1))
    val (dlon, dlat) = (abs(rlon0 - rlon1), abs(rlat0 - rlat1))
    val h = hav(dlat) + cos(rlat0) * cos(rlat1) * hav(dlon)
    2 * er * asin(sqrt(h))*1000
  }

  def getDrc(drc1:Double, drc2:Double):Double={
    val temp = abs(drc1-drc2)
    if(temp>180.0) 360.0-temp
    else temp
  }

  def hav(theta:Double): Double ={ sin(theta/2) * sin(theta/2) }

  def out_of_china(lng:Double, lat:Double):Boolean={
    if (lng < 72.004 || lng > 137.8347) return true
    if (lat < 0.8293 || lat > 55.8271)  return true
    false
  }

  def transformlat(lng:Double, lat:Double):Double={
    var ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * sqrt(abs(lng))
    ret += (20.0 * sin(6.0 * lng * Pi) + 20.0 *
      sin(2.0 * lng * Pi)) * 2.0 / 3.0
    ret += (20.0 * sin(lat * Pi) + 40.0 *
      sin(lat / 3.0 * Pi)) * 2.0 / 3.0
    ret += (160.0 * sin(lat / 12.0 * Pi) + 320 *
      sin(lat * Pi / 30.0)) * 2.0 / 3.0
    ret
  }

  def transformlng(lng:Double, lat:Double):Double={
    var ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * sqrt(abs(lng))
    ret += (20.0 * sin(6.0 * lng * Pi) + 20.0 *
      sin(2.0 * lng * Pi)) * 2.0 / 3.0
    ret += (20.0 * sin(lng * Pi) + 40.0 *
      sin(lng / 3.0 * Pi)) * 2.0 / 3.0
    ret += (150.0 * sin(lng / 12.0 * Pi) + 300.0 *
      sin(lng / 30.0 * Pi)) * 2.0 / 3.0
    ret
  }

  def wgs84togcj02(lng:Double, lat:Double):List[Double]={
    if (out_of_china(lng, lat)) return List(lng, lat)
    var dlat = transformlat(lng - 105.0, lat - 35.0)
    var dlng = transformlng(lng - 105.0, lat - 35.0)
    val radlat = lat / 180.0 * Pi
    var magic = sin(radlat)
    magic = 1 - ee * magic * magic
    val sqrtmagic = sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * Pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * cos(radlat) * Pi)
    List(lat + dlat, lng + dlng)
  }
}
