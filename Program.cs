using System;
using System.Data;
using System.Data.OleDb;
using Microsoft.Data.SqlClient;

namespace DSCGImport
{
    class Program
    {
        static string connString;
        static int importType;
        static string file;

        static string tempTable;
        static int colNumber;

        static void Main(string[] args)
        {
            connString = args[0];
            importType = int.Parse(args[1]);
            file = args[2];

            CreateTempTables(importType);

            if (importType <= 4)
                BulkCopy();
            //TestGit
        }

        private static void BulkCopy()
        {
            string excelConnString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + file +
                ";Extended Properties=Excel 12.0;Persist Security Info=False";

            using (var excelConn = new OleDbConnection(excelConnString))
            {
                excelConn.Open();
                var cmd = GetCommand(excelConn);
                var reader = cmd.ExecuteReader();

                using (var loader = new SqlBulkCopy(connString))
                {
                    SetColumnMappings(loader);
                    loader.DestinationTableName = tempTable;
                    loader.WriteToServer(reader);
                }
            }
        }


        private static OleDbCommand GetCommand(OleDbConnection excelConn)
        {
            DataTable schema = excelConn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
            string sheetName = schema.Rows[0]["TABLE_NAME"].ToString();
            var cmd = new OleDbCommand("select * from [" + sheetName + "]", excelConn);
            return cmd;
        }

        static void SetColumnMappings(SqlBulkCopy loader)
        {
            for (int i = 0; i < colNumber; i++)
                loader.ColumnMappings.Add(i, i + 1);
        }

        private static void ExecSQL(string sql)
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                SqlCommand command = new SqlCommand(sql, connection);
                connection.Open();
                command.ExecuteNonQuery();
            }
        }

        static private void CreateTempTables(int importType)
        {
            string sql;
            switch (importType)
            {
                case 1:
                    sql = @"if object_id('dbo.TmpBoreholes') is not null drop table dbo.TmpBoreholes;
                    create table dbo.TmpBoreholes(
                    Id int not null primary key identity,
                    HoleName nvarchar(50),
                    X nvarchar(50),
                    Y nvarchar(50),
                    Z nvarchar(50),
                    Depth nvarchar(50),
                    LineName nvarchar(100)
                    )";
                    tempTable = "TmpBoreholes";
                    colNumber = 6;
                    break;
                case 2:
                    sql = @"if object_id('dbo.TmpSurvey') is not null drop table dbo.TmpSurvey;
                    create table dbo.TmpSurvey (
		            Id int not null primary key identity, 
		            HoleName nvarchar(50), 
		            Depth nvarchar(50), 
		            Dip nvarchar(50), 
		            Azimuth nvarchar(50)
                    )";
                    tempTable = "TmpSurvey";
                    colNumber = 4;
                    break;
                case 3:
                    sql = @"if object_id('dbo.TmpLithology') is not null drop table dbo.TmpLithology;
                    create table dbo.TmpLithology (
	                Id int not null primary key identity, 
	                HoleName nvarchar(50), 
	                DepthFrom nvarchar(50), 
	                DepthTo nvarchar(50), 
	                RockId nvarchar(50), 
	                Angle nvarchar(50), 
	                Core nvarchar(50),
	                ThNorm nvarchar(50),  
	                CoreNorm nvarchar(50), 
	                CoreProc nvarchar(50)
                    )";
                    tempTable = "TmpLithology";
                    colNumber = 9;
                    break;
                case 4:
                    sql = @"if object_id('dbo.TmpAssays') is not null drop table dbo.TmpAssays;
                    create table dbo.TmpAssays (
	                Id int not null primary key identity, 
	                HoleName nvarchar(50), 
	                DepthFrom nvarchar(50), 
	                DepthTo nvarchar(50),
                    NSample nvarchar(200),
                    VarId nvarchar(50),
                    Val nvarchar(100)
                    )";
                    tempTable = "TmpAssays";
                    colNumber = 6;
                    break;
                default: sql = ""; break;
            }
            ExecSQL(sql);
        }
    }
}

