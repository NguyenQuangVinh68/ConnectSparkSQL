using Microsoft.Spark.Sql;
namespace demoSpark
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string query = "select top 10 * from tco_emp";
            Console.WriteLine("Hello World!");
            SparkSession spark = SparkSession
                .Builder()
                .AppName("Spark MySQL Example")
                // .Config("spark.driver.extraClassPath", "E:\\BigData\\spark-3.0.0-bin-hadoop2.7\\jars\\mssql-jdbc-8.4.1.jre8.jar")
                .GetOrCreate();
            // Define JDBC URL and connection properties
            // Read data from MySQL using JDBC
            string jdbcUrl = "jdbc:sqlserver://192.168.1.81:56578;instanceName=MSSQL2019;databaseName=leasing_13122023"; //yes
            DataFrame df = spark.Read()
                .Format("jdbc")
                .Option("url", jdbcUrl)
                .Option("query", query)
                .Option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .Option("user", "sa").Option("password", "123456a@")
                .Load();
            // Show the DataFrame
            df.Show();
            // Stop the Spark session
            spark.Stop();


        }
    }
}