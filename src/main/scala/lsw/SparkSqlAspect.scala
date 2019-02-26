package lsw

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{Around, Aspect}

@Aspect
class SparkSqlAspect {

  @Around("execution(public org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> org.apache.spark.sql.SparkSession.sql(java.lang.String)) && args(sqlRaw)")
  def around(pjp: ProceedingJoinPoint, sqlRaw: String): Dataset[Row] = {
    val sql = sqlRaw.trim
    val spark = pjp.getThis.asInstanceOf[SparkSession]
    val userId = spark.sparkContext.sparkUser
    val tables = getTables(sql, spark)
    if (accessControl(userId, tables)) {
      pjp.proceed(pjp.getArgs).asInstanceOf[Dataset[Row]]
    } else {
      throw new IllegalAccessException("access failed")
    }
  }

  def getTables(query: String,
                spark: SparkSession): Seq[String] = {
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName }
  }

  def accessControl(user: String,
                    table: Seq[String]): Boolean = {
    println("userId: " + user, "tableName: " + table.mkString(","))
    true
  }
}
