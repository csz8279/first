package com.dahua.util

object AppAvaUtil {

  def appAna(imei: String, mac: String, idfa:String,openudid:String, androidid:String):String ={
    if(!imei.isEmpty){
      imei
    }else if(!mac.isEmpty){
      mac
    }else if(!idfa.isEmpty){
      idfa
    }else if(!openudid.isEmpty){
      openudid
    }else
      androidid
  }

  def gglx(adspacetype:Int):String ={
    if(adspacetype<10){
      "LC0"+adspacetype+"->1"
    }else{
      "LC"+adspacetype+"->1"
    }
  }
}
