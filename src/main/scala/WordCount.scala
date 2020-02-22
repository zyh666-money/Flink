import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WordCount {

  def main(args: Array[String]): Unit = {

    //创建环境
    val env:ExecutionEnvironment=ExecutionEnvironment.getExecutionEnvironment;

    //读取文件
    val input="F:\\xh.txt";
    val dataset: DataSet[String] = env.readTextFile(input)
    //空格切割
    import org.apache.flink.api.scala._
    val aggDs :AggregateDataSet[(String,Int)]=dataset.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1);
    aggDs.print();
  }
}
