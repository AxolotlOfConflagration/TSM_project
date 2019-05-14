object TupleUDFs {
  import org.apache.spark.sql.functions.udf
  import scala.reflect.runtime.universe.{TypeTag, typeTag}

  def toTuple2[S: TypeTag, T: TypeTag] =
    udf[(S, T), S, T]((x: S, y: T) => (x, y))
}