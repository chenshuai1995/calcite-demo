//package com.jim.calcite.sql.table;
//
//import com.jim.calcite.sql.function.SqlToDateFunction;
//import org.apache.calcite.sql.SqlFunction;
//import org.apache.calcite.sql.fun.SqlStdOperatorTable;
//
///**
// * 重写SqlStdOperatorTable添加to_date函数
// *
// * @author Jim Chen
// * @date 2022-06-27
// */
//public class SqlMyOperatorTable extends SqlStdOperatorTable {
//
//
//  private static SqlMyOperatorTable instance;
//
//  public static final SqlFunction TO_DATE = new SqlToDateFunction();
//
//  public static synchronized SqlMyOperatorTable instance() {
//    if (instance == null) {
//      // Creates and initializes the standard operator table.
//      // Uses two-phase construction, because we can't initialize the
//      // table until the constructor of the sub-class has completed.
//      instance = new SqlMyOperatorTable();
//      instance.init();
//    }
//
//    return instance;
//  }
//
//
//}
