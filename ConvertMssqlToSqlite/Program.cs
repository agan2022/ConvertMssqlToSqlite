using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;

class Program
{
    static void Main(string[] args)
    {
        string mssql_conn_str = "Data Source=.;Initial Catalog=CPSDB;Integrated Security=True";
        string sqlite_conn_str = "Data Source=C:\\sqlite\\test\\CSPDB.db;Version=3;";

        // Connect to the MSSQL database
        using (SqlConnection mssql_conn = new SqlConnection(mssql_conn_str))
        {
            mssql_conn.Open();

            // Connect to the SQLite database
            using (SQLiteConnection sqlite_conn = new SQLiteConnection(sqlite_conn_str))
            {
                sqlite_conn.Open();

                // Get a list of tables in the MSSQL database
                DataTable tables = mssql_conn.GetSchema("Tables");
                foreach (DataRow table in tables.Rows)
                {
                    string table_name = table[2].ToString();

                    // Create a corresponding table in the SQLite database
                    SqlCommand cmd = new SqlCommand($"SELECT TOP 0 * FROM {table_name}", mssql_conn);
                    using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                    {
                        DataTable schema = reader.GetSchemaTable();
                        string column_definitions = "";
                        foreach (DataRow row in schema.Rows)
                        {
                            string column_name = row["ColumnName"].ToString();
                            string data_type = row["DataTypeName"].ToString();
                            column_definitions += $"`{column_name}` {MapDataType(data_type)}, ";
                        }
                        column_definitions = column_definitions.TrimEnd(',', ' ');
                        string create_query = $"CREATE TABLE `{table_name}` ({column_definitions})";
                        using (SQLiteCommand sqlite_cmd = new SQLiteCommand(create_query, sqlite_conn))
                        {
                            sqlite_cmd.ExecuteNonQuery();
                        }
                    }

                    // Insert the data from the MSSQL table into the SQLite table
                    cmd = new SqlCommand($"SELECT * FROM {table_name}", mssql_conn);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string placeholders = "";
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                placeholders += "?, ";
                            }
                            placeholders = placeholders.TrimEnd(',', ' ');
                            string insert_query = $"INSERT INTO `{table_name}` VALUES ({placeholders})";
                            using (SQLiteCommand insert_cmd = new SQLiteCommand(insert_query, sqlite_conn))
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    insert_cmd.Parameters.AddWithValue($"@{i}", reader[i]);
                                }
                                insert_cmd.ExecuteNonQuery();
                            }
                        }
                    }
                }
                Console.WriteLine("Conversion complete!");
            }
        }
    }

    static string MapDataType(string data_type)
    {
        switch (data_type.ToLower())
        {
            case "bigint":
            case "bit":
            case "decimal":
            case "float":
            case "int":
            case "money":
            case "numeric":
            case "smallint":
            case "tinyint":
                return "INTEGER";
            case "binary":
            case "image":
            case "timestamp":
            case "varbinary":
                return "BLOB";
            case "datetime":
            case "datetime2":
            case "date":
            case "smalldatetime":
            case "time":
                return "TEXT";
            case "char":
            case "nchar":
            case "ntext":
            case "nvarchar":
            case "text":
            case "varchar":
                return "TEXT";
            default:
                return "TEXT";
        }
    }
}
