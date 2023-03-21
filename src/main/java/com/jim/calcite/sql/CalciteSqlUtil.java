//package com.jim.calcite.sql;
//
//import com.jim.calcite.sql.model.DataTypeEnum;
//import com.jim.calcite.sql.model.Table;
//import com.jim.calcite.sql.table.SqlMyOperatorTable;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.calcite.config.CalciteConnectionConfig;
//import org.apache.calcite.config.CalciteConnectionConfigImpl;
//import org.apache.calcite.jdbc.CalciteSchema;
//import org.apache.calcite.plan.Context;
//import org.apache.calcite.plan.ConventionTraitDef;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.plan.RelOptTable;
//import org.apache.calcite.plan.hep.HepPlanner;
//import org.apache.calcite.plan.hep.HepProgramBuilder;
//import org.apache.calcite.prepare.CalciteCatalogReader;
//import org.apache.calcite.rel.RelDistributionTraitDef;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.RelRoot;
//import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
//import org.apache.calcite.rel.rel2sql.SqlImplementor;
//import org.apache.calcite.rel.rules.PruneEmptyRules;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.rel.type.RelDataTypeSystem;
//import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
//import org.apache.calcite.rex.RexBuilder;
//import org.apache.calcite.schema.SchemaPlus;
//import org.apache.calcite.schema.impl.AbstractTable;
//import org.apache.calcite.sql.SqlDialect;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.dialect.OracleSqlDialect;
//import org.apache.calcite.sql.parser.SqlParser;
//import org.apache.calcite.sql.type.BasicSqlType;
//import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
//import org.apache.calcite.sql.type.SqlTypeName;
//import org.apache.calcite.sql.validate.SqlConformance;
//import org.apache.calcite.sql.validate.SqlConformanceEnum;
//import org.apache.calcite.sql.validate.SqlValidator;
//import org.apache.calcite.sql.validate.SqlValidatorUtil;
//import org.apache.calcite.sql2rel.RelDecorrelator;
//import org.apache.calcite.sql2rel.SqlToRelConverter;
//import org.apache.calcite.tools.FrameworkConfig;
//import org.apache.calcite.tools.Frameworks;
//import org.apache.calcite.tools.RelBuilder;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.stream.Collectors;
//
///**
// * calcite的sql优化
// *
// * @author Jim Chen
// * @date 2022-06-27
// */
//@Slf4j
//public class CalciteSqlUtil {
//
//
//  /***
//   * 根据Calcite的RBO，对sql进行简单优化
//   * @author test
//   * @date 2021/3/1 19:15
//   * @param sql sql语句
//   * @param type 数据库类型
//   * @param tables 表信息
//   * @return java.lang.String
//   **/
//  public static String optimizationSql(String sql, String type, List<Table> tables) throws Exception {
//    SchemaPlus rootSchema = registerRootSchema(tables);
//
//    final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
//        .parserConfig(SqlParser.Config.DEFAULT)
//        .defaultSchema(rootSchema)
//        .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
//        .build();
//
//    // HepPlanner RBO优化
//    HepProgramBuilder builder = new HepProgramBuilder();
//    builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);
//    HepPlanner planner = new HepPlanner(builder.build());
//
//    try {
//      SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
//
//      // sql解析器
//      SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
//      SqlNode parsed = parser.parseStmt();
//
//      CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
//          CalciteSchema.from(rootSchema),
//          CalciteSchema.from(rootSchema).path(null),
//          factory,
//          new CalciteConnectionConfigImpl(new Properties())
//      );
//
//      // sql validate
//      SqlValidator validator = SqlValidatorUtil.newValidator(
//          SqlMyOperatorTable.instance(),
//          calciteCatalogReader,
//          factory,
//          SqlValidator.Config.DEFAULT
//      );
//      SqlNode validated = validator.validate(parsed);
//
//      final RexBuilder rexBuilder = new RexBuilder(factory);
//      final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
//
//      // init SqlToRelConverter config
//      final SqlToRelConverter.Config config = SqlToRelConverter.config();
//
//      // SqlNode转换为RelNode
//      final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
//          new ViewExpanderImpl(),
//          validator,
//          calciteCatalogReader,
//          cluster,
//          fromworkConfig.getConvertletTable(),
//          config
//      );
//
//      RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);
//
//      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
//      final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
//      root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
//      RelNode relNode = root.rel;
//
//      // 寻找优化路径
//      planner.setRoot(relNode);
//      relNode = planner.findBestExp();
//
//      // 转换为需要的数据库类型的sql
//      RelToSqlConverter relToSqlConverter = new RelToSqlConverter(getSqlDialect(type));
//      SqlImplementor.Result visit = relToSqlConverter.visitRoot(relNode);
//      SqlNode sqlNode = visit.asStatement();
//
//      return sqlNode.toSqlString(getSqlDialect(type)).getSql();
//    } catch (Exception e) {
//      log.error("SQL优化失败!", e);
//      throw new Exception("SQL优化失败");
//    }
//  }
//
//  /***
//   * 注册表的字段信息
//   * @author test
//   * @date 2021/3/1 19:17
//   * @param tables 表名
//   * @return org.apache.calcite.schema.SchemaPlus
//   **/
//  private static SchemaPlus registerRootSchema(List<Table> tables) {
//    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
//
//    List<Table> noSchemaTables = tables.stream().filter(t -> StringUtils.isEmpty(t.getSchema())).collect(Collectors.toList());
//    for (Table table : noSchemaTables) {
//      rootSchema.add(table.getTable().toUpperCase(), new AbstractTable() {
//        @Override
//        public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
//          RelDataTypeFactory.Builder builder = relDataTypeFactory.builder();
//
//          table.getFields().forEach(field -> {
//            builder.add(
//                field.getName().toUpperCase(),
//                new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.VARCHAR)
//            );
//          });
//
//          return builder.build();
//        }
//      });
//    }
//
//    Map<String, List<Table>> map = tables.stream().filter(t -> StringUtils.isNotEmpty(t.getSchema())).collect(Collectors.groupingBy(Table::getSchema));
//    for (String key : map.keySet()) {
//      List<Table> tableList = map.get(key);
//      SchemaPlus schema = Frameworks.createRootSchema(true);
//      for (Table table : tableList) {
//        schema.add(table.getTable().toUpperCase(), new AbstractTable() {
//          @Override
//          public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
//            RelDataTypeFactory.Builder builder = relDataTypeFactory.builder();
//
//            table.getFields().forEach(field -> {
//              builder.add(
//                  field.getName().toUpperCase(),
//                  new BasicSqlType(new RelDataTypeSystemImpl() {}, getSqlTypeName(field.getType()))
//              );
//            });
//
//            return builder.build();
//          }
//        });
//      }
//
//      rootSchema.add(key.toUpperCase(), schema);
//    }
//
//    return rootSchema;
//  }
//
//  private static SqlConformance conformance(FrameworkConfig config) {
//    final Context context = config.getContext();
//    if (null == context) {
//      return SqlConformanceEnum.DEFAULT;
//    }
//
//    final CalciteConnectionConfig connectionConfig = context.unwrap(CalciteConnectionConfig.class);
//    if (null == connectionConfig) {
//      return SqlConformanceEnum.DEFAULT;
//    }
//
//    return connectionConfig.conformance();
//  }
//
//  private static class ViewExpanderImpl implements RelOptTable.ViewExpander {
//    public ViewExpanderImpl() {
//    }
//
//    @Override
//    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
//      return null;
//    }
//  }
//
//  /***
//   * 获取sql类型
//   * @author test
//   * @date 2021/3/3 10:27
//   * @param type 类型
//   * @return org.apache.calcite.sql.SqlDialect
//   **/
//  private static SqlDialect getSqlDialect(String type) {
//    return OracleSqlDialect.DEFAULT;
//  }
//
//  /***
//   * 根据DataTypeEnum获取对应的SqlTypeName
//   * @author test
//   * @date 2021/3/6 10:55
//   * @param type DataTypeEnum的类型
//   * @return org.apache.calcite.sql.type.SqlTypeName
//   **/
//  private static SqlTypeName getSqlTypeName(String type) {
//    DataTypeEnum dataType = DataTypeEnum.getByCode(type);
//    if (null == dataType) {
//      return SqlTypeName.VARCHAR;
//    }
//
//    switch (dataType) {
//      case CHAR:
//      case CLOB:
//      case TEXT:
//      case BLOB:
//      case STRING: return SqlTypeName.VARCHAR;
//      case DECIMAL:
//      case NUMBER: return SqlTypeName.DECIMAL;
//      case INTEGER: return SqlTypeName.INTEGER;
//      case DATE: return SqlTypeName.DATE;
//      case TIMESTAMP: return SqlTypeName.DATE;
//      default: return SqlTypeName.VARCHAR;
//    }
//  }
//
//  public static void main(String[] args) throws Exception {
//    String sql = "select t1.id, t1.name " +
//        "from (" +
//        "  select student.id,student.name from school.student " +
//        "  where student.create_time > to_date('2021-03-04 12:00:00', 'yyyy-mm-dd hh24:mi:ss')" +
//        ") t1";
//    List<Table> tables = new ArrayList<>();
//    List<Table.Field> fields = new ArrayList<>();
//    fields.add(new Table.Field("id", DataTypeEnum.NUMBER.getType()));
//    fields.add(new Table.Field("name", DataTypeEnum.STRING.getType()));
//    fields.add(new Table.Field("create_time", DataTypeEnum.DATE.getType()));
//    tables.add(new Table("school", "student", fields));
//
//    sql = CalciteSqlUtil.optimizationSql(sql, "ORACLE", tables);
//    System.out.println(sql);
//  }
//
//}
