//package com.jim.calcite.sql.model;
//
///**
// * 数据类型
// *
// * @author Jim Chen
// * @date 2022-06-27
// */
//public enum DataTypeEnum {
//
//  STRING("string","字符串"),
//  TEXT("longtext","长字符串"),
//  CLOB("clob","clob"),
//  BLOB("blob","blob"),
//  CHAR("char","字符"),
//  NUMBER("number","数值型"),
//  DECIMAL("decimal","浮点型"),
//  DATE("date","日期类型"),
//  TIMESTAMP("timestamp","时间戳类型"),
//  INTEGER("integer","常用的整数")
//  ;
//
//
//  private String type;
//
//  private String name;
//
//  DataTypeEnum(String type, String name) {
//    this.name =name;
//    this.type = type ;
//  }
//
//  public String getName() {
//    return name;
//  }
//
//  public String getType() {
//    return type;
//  }
//
//  public static DataTypeEnum getByCode(String code) {
//    for (DataTypeEnum obj : DataTypeEnum.values()) {
//      if (obj.getType().equals(code)) {
//        return obj;
//      }
//    }
//    return null;
//  }
//
//}
