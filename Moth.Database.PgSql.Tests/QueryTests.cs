using System;

namespace Moth.Database.PgSql.Tests
{
    using Xunit;
    using Extensions;

    public class QueryTests : IDisposable
    {
        public QueryTests()
        {
            ContainerTests.Register();
        }

        [Fact]
        public void DatabaseCanRunQueries()
        {
            const string QueryString = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            Record.Exception(() => Query.As(QueryString).Execute().Retrieve());
        }

        [Fact]
        public void CanCreateTable()
        {
            Flush();
            CreateTable();
            var tables =
                Query.As("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    .Execute()
                    .Retrieve();
                    

            Assert.NotEqual(0, tables.Count);
        }

        [Fact]
        public void CanInsert()
        {
            Flush();
            CreateTable();
            var insertedCount = Query.As(@"INSERT INTO ""Moth.Database.PgSql.Tests.Employee"" (""FirstName"", ""LastName"") VALUES('Employee', 'Employeeson')")
                .Execute()
                .NonQuery();

            Assert.Equal(1, insertedCount);
        }

        [Fact]
        public void CanRead()
        {
            Flush();
            CreateTable();
            var id = Query.As(@"INSERT INTO ""Moth.Database.PgSql.Tests.Employee"" (""FirstName"", ""LastName"") VALUES('Employee', 'Employeeson') RETURNING ""Id""")
                .Execute()
                .Scalar<int>();
            var employeeEnity = Query.As("SELECT * FROM employee WHERE id = @id", new Parameter("id", id)).Execute().Retrieve()[0];
            Assert.NotNull(employeeEnity);
            Assert.Equal(employeeEnity.GetValue<string>("first_name"), "Employee");
            Assert.Equal(employeeEnity.GetValue<string>("last_name"), "Employeeson");
        }

        [Fact]
        public void CanUpdate()
        {
            Flush();
            CreateTable();
            var id = Query.As(@"INSERT INTO ""Moth.Database.PgSql.Tests.Employee"" (""FirstName"", ""LastName"") VALUES('Employee', 'Employeeson') RETURNING ""Id""")
                .Execute()
                .Scalar<int>();
            Query.As(@"UPDATE ""Moth.Database.PgSql.Tests.Employee"" SET ""FirstName"" = 'Updated', ""LastName"" = 'Employee' WHERE ""Id"" = @id",
                new Parameter("id", id)).Execute().NonQuery();
            var employeeEntity = Query.As(@"SELECT * FROM ""Moth.Database.PgSql.Tests.Employee"" WHERE ""Id"" = @id", new Parameter("id", id)).Execute().Retrieve();
            Assert.Equal(employeeEntity[0].GetValue<string>("first_name"), "Updated");

        }

        public static void Flush()
        {
            Query.As("drop schema public cascade;create schema public;").Execute().NonQuery();
        }

        public static void CreateTable()
        {
            Query.As(@"CREATE TABLE ""Moth.Database.PgSql.Tests.Employee"" (""Id"" SERIAL PRIMARY KEY , ""UId"" uuid, ""DateCreated"" timestamp, ""DateUpdated"" timestamp, ""FirstName"" text, ""LastName"" text)")
                .Execute()
                .NonQuery();
        }

        public void Dispose()
        {
            Flush();
            DatabaseContainer.Clear();
        }
    }
}