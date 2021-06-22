package com.xmhns.dm.yun.roadextraction.comm

class Path(vpath_id: Int, vstart_lon: Double, vstart_lat: Double, vend_lon: Double, vend_lat: Double) {
  var path_id: Int = vpath_id
  var start_lon: Double = vstart_lon
  var start_lat: Double = vstart_lat
  var end_lon: Double = vend_lon
  var end_lat: Double = vend_lat

  def show() {
    println ("path_id: " + path_id)
    println ("start_lon: " + start_lon)
    println ("start_lat: " + start_lat)
    println ("end_lon: " + end_lon)
    println ("end_lat: " + end_lat)
  }

}