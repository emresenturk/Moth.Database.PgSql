namespace Moth.Database.PgSql.Tests
{
    using System;
    using System.Configuration;
    using Configuration;
    using Xunit;

    public class ContainerTests : IDisposable
    {
        [Fact]
        public void ContainerCanRegister()
        {
            DatabaseContainer.Clear();
            Record.Exception(() => Register());
            Assert.True(DatabaseContainer.DefaultContainer.HasRegisteredItems, "Nothing registered and this is a problem.");
        }

        public void Dispose()
        {
            DatabaseContainer.Clear();
        }

        public static void Register()
        {
            foreach (ConnectionStringSettings connectionString in ConfigurationManager.ConnectionStrings)
            {
                var databaseConfig = new DatabaseConfiguration
                {
                    ConnectionString = connectionString.ConnectionString,
                    Name = connectionString.Name,
                    Provider = connectionString.ProviderName
                };

                DatabaseContainer.DefaultContainer.Register<PgSqlDatabase>(databaseConfig);
            }
        }
    }
}
