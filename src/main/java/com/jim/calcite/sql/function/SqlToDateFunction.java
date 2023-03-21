//package com.jim.calcite.sql.function;
//
//import org.apache.calcite.sql.SqlCall;
//import org.apache.calcite.sql.SqlFunction;
//import org.apache.calcite.sql.SqlFunctionCategory;
//import org.apache.calcite.sql.SqlKind;
//import org.apache.calcite.sql.SqlOperandCountRange;
//import org.apache.calcite.sql.SqlOperatorBinding;
//import org.apache.calcite.sql.SqlSyntax;
//import org.apache.calcite.sql.type.OperandTypes;
//import org.apache.calcite.sql.type.ReturnTypes;
//import org.apache.calcite.sql.type.SqlOperandCountRanges;
//import org.apache.calcite.sql.type.SqlOperandTypeChecker;
//import org.apache.calcite.sql.validate.SqlMonotonicity;
//import org.apache.calcite.sql.validate.SqlValidator;
//
///**
// * 自定义to_date函数
// *
// * @author Jim Chen
// * @date 2022-06-27
// */
//public class SqlToDateFunction extends SqlFunction {
//
//  //~ Constructors -----------------------------------------------------------
//
//  public SqlToDateFunction() {
//    super(
//        "TO_DATE",
//        SqlKind.OTHER_FUNCTION,
//        ReturnTypes.DATE,
//        null,
//        OperandTypes.ANY,
//        SqlFunctionCategory.TIMEDATE);
//  }
//
//  //~ Methods ----------------------------------------------------------------
//
//  public SqlSyntax getSyntax() {
//    return SqlSyntax.FUNCTION_ID;
//  }
//
//  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
//    return SqlMonotonicity.INCREASING;
//  }
//
//  // Plans referencing context variables should never be cached
//  public boolean isDynamicFunction() {
//    return true;
//  }
//
//  @Override
//  public SqlOperandCountRange getOperandCountRange() {
//    /**
//     * 返回参数个数信息
//     **/
//    return SqlOperandCountRanges.of(2);
//  }
//
//  @Override
//  protected void checkOperandCount(SqlValidator validator, SqlOperandTypeChecker argType, SqlCall call) {
//    /**
//     * 参数个数校验
//     **/
//    assert call.operandCount() == 2;
//  }
//
//}
