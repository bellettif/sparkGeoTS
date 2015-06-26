package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.joda.time.Period

/**
 * Created by Francois Belletti on 6/22/15.
 */
class TsDataFrame(
  @transient override val sqlContext: SQLContext,
  logicalPlan: LogicalPlan,
  timeIndex: String)
  extends DataFrame(sqlContext, logicalPlan){

  val minTS = this.agg(Map(timeIndex -> "min")).head()(0)
  val maxTS = this.agg(Map(timeIndex -> "max")).head()(0)

  def this(dataFrame: DataFrame, timeIndex: String) = {
    this(dataFrame.sqlContext, dataFrame.logicalPlan, timeIndex)
  }

  def applyBy(period: Period, rolling: Boolean): Any = {

  }



}
