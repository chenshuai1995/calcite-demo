//package com.jim.calcite.sql.model;
//
//import java.io.Serializable;
//import java.util.List;
//import lombok.Data;
//
///**
// * 表信息
// *
// * @author Jim Chen
// * @date 2022-06-27
// */
//@Data
//public class Table implements Serializable {
//  private static final long serialVersionUID = -2971832417447325046L;
//
//  /**
//   * 表前缀
//   **/
//  private String schema;
//
//  /**
//   * 表名称
//   **/
//  private String table;
//
//  /**
//   * 字段信息
//   **/
//  private List<Field> fields;
//
//  public Table() {
//
//  }
//
//  public Table(String schema, String table, List<Field> fields) {
//    this.schema = schema;
//    this.table = table;
//    this.fields = fields;
//  }
//
//  @Data
//  public static class Field implements Serializable {
//    private static final long serialVersionUID = 5728717173174364365L;
//
//    /**
//     * 字段名
//     **/
//    private String name;
//
//    /**
//     * 字段类型
//     **/
//    private String type;
//
//    public Field() {
//    }
//
//    public Field(String name, String type) {
//      this.name = name;
//      this.type = type;
//    }
//  }
//}
