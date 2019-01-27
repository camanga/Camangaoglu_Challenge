import java.io.InputStream

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.types.Row

object Trendyol_Challenge {

        def main(args: Array[String]) {

        val env = ExecutionEnvironment.getExecutionEnvironment
        val tableEnv = TableEnvironment.getTableEnvironment(env)
        val stream: InputStream = getClass.getResourceAsStream("/input")
        print(getClass.getResource("/input/case.csv").getPath)

        print(getClass.getResource("/input/case.csv").getPath)
        val csvtable = CsvTableSource
        .builder
        .path(getClass.getResource("/input/case.csv").getPath)
        .ignoreFirstLine
        .fieldDelimiter("|")
        .field("date", Types.STRING)
        .field("productID", Types.STRING)
        .field("eventName", Types.STRING)
        .field("userID", Types.STRING)
        .build


        tableEnv.registerTableSource("Challenge_Csv", csvtable)

        //result 1
        val result1 = tableEnv.sqlQuery("select productID,count(1) from Challenge_Csv where eventName='view' group by productID")
        //result 1 v.2
        //val result1=tableEnv.scan("Challenge_Csv").where("eventName='view'").groupBy("productID").select("productID,count(1)")
        result1.toDataSet[(String, Long)].writeAsCsv("/output/result1.txt", fieldDelimiter = "|", writeMode = WriteMode.OVERWRITE).setParallelism(1)

        //result 2
        val result2 = tableEnv.sqlQuery("select eventName,count(1) from Challenge_Csv group by eventName ")
        //result 2 v.2
        //val result2=tableEnv.scan("Challenge_Csv").groupBy("eventName").select("eventName,count(1)")
        result2.toDataSet[(String, Long)].writeAsCsv("/output/result2.txt", fieldDelimiter = "|", writeMode = WriteMode.OVERWRITE).setParallelism(1)

        //result 3
        val result3 = tableEnv.sqlQuery("select userID,count(1) from Challenge_Csv group by userID having count(distinct eventName)=4 order by 2 desc limit 5")
        //val result32 = tableEnv.sqlQuery("select userID,(select case when count(*)=0 then 0 else 1 end from Challenge_Csv a where a.userID=e.userID and eventName='remove' group by userID),(select case when count(*)=0 then 0 else 1 end from Challenge_Csv b where b.userID=e.userID and  eventName='view'),(select case when count(*)=0 then 0 else 1 end from Challenge_Csv c where c.userID=e.userID and eventName='click'),(select case when count(*)=0 then 0 else 1 end from Challenge_Csv d where d.userID=e.userID and eventName='add' ) from Challenge_Csv as e group by userID ")
        //result32.toDataSet[Row].writeAsText("C:/Users/Okan/Downloads/result32.txt").setParallelism(1)

        result3.toDataSet[(String, Long)].writeAsCsv("/output/result3.txt", fieldDelimiter = "|", writeMode = WriteMode.OVERWRITE).setParallelism(1)

        //result 4
        val result4 = tableEnv.sqlQuery("select eventName,count(1) from Challenge_Csv where userID='47' group by  eventName")
        //result 4 v2
        //val result4=tableEnv.scan("Challenge_Csv").where("userID='47'").groupBy("eventName").select("eventName,count(1)")
        result4.toDataSet[(String, Long)].writeAsCsv("/output/result4.txt", fieldDelimiter = "|", writeMode = WriteMode.OVERWRITE).setParallelism(1)

        //reult 5
        val result5 = tableEnv.sqlQuery("select distinct productID from Challenge_Csv where userID='47' ")
        //result 5 v2
        //val result5=tableEnv.scan("Challenge_Csv").where("userID='47'").groupBy("productID").select(" productID")
        result5.toDataSet[Row].writeAsText("/output/result5.txt", writeMode = WriteMode.OVERWRITE).setParallelism(1)


        env.execute("Flink Batch Scala API Skeleton")
        }