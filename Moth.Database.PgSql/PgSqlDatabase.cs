using System.Globalization;

namespace Moth.Database.PgSql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using Configuration;
    using Data;
    using Expressions;

    using Npgsql;

    public class PgSqlDatabase : Database
    {
        private readonly NpgsqlConnection connection;
        public PgSqlDatabase()
        {
            connection = new NpgsqlConnection();
        }

        public PgSqlDatabase(IDatabaseConfiguration configuration)
            : base(configuration)
        {
            connection = new NpgsqlConnection(configuration.ConnectionString);
            connection.Open();
        }

        protected override IList<Entity> RetrieveByText(Query query)
        {
            return ReadByText(query).ToList();
        }

        protected override IEnumerable<Entity> ReadByText(Query query)
        {
            using (var command = CreateCommand(query))
            {
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var properties = new Property[reader.VisibleFieldCount];
                    for (var i = 0; i < reader.VisibleFieldCount; i++)
                    {
                        var fieldName = reader.GetName(i);
                        var fieldType = reader.GetFieldType(i);
                        var fieldValue = reader.IsDBNull(i) ? null : reader[i];
                        properties[i] = new Property(fieldName, fieldType, fieldValue);
                    }

                    yield return new Entity(properties);
                }
            }
        }

        protected override int NonQueryByText(Query query)
        {
            using (var command = CreateCommand(query))
            {
                return command.ExecuteNonQuery();
            }
        }

        protected override IEnumerable<Entity> ReadByExpression(ExpressionQuery query)
        {
            var queryString = query.ToSelectQuery();
            using (var command = CreateCommand(queryString, (query.Parameters ?? new List<Parameter>()).ToArray()))
            {
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var properties = new Property[reader.VisibleFieldCount];
                    for (var i = 0; i < reader.VisibleFieldCount; i++)
                    {
                        var fieldName = reader.GetName(i);
                        var fieldType = reader.GetFieldType(i);
                        var fieldValue = reader.IsDBNull(i) ? null : reader[i];
                        properties[i] = new Property(fieldName, fieldType, fieldValue);
                    }

                    yield return new Entity(properties);
                }
            }
        }

        protected override IList<Entity> RetrieveByExpression(ExpressionQuery query)
        {
            return ReadByExpression(query).ToList();
        }

        public override Entity Create(Entity entity, TypeExpression entityType)
        {
            var tableName = entityType.Type.ToTableName();
            var columnNames = entity.PropertyNames.Where(p => p != "Id").Select(p => string.Format(@"""{0}""", p));
            var parameters = entity.PropertyNames.Where(p => p != "Id").Select((p, i) => new Parameter(string.Format("P{0}", i), entity[p])).ToArray();
            var query = string.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING *", tableName, string.Join(",", columnNames), string.Join(",", parameters.Select(p => string.Format("@{0}", p.Name))));
            using (var command = CreateCommand(query, parameters))
            {
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var properties = new Property[reader.VisibleFieldCount];
                    for (var i = 0; i < reader.VisibleFieldCount; i++)
                    {
                        var fieldName = reader.GetName(i);
                        var fieldType = reader.GetFieldType(i);
                        var fieldValue = reader.IsDBNull(i) ? null : reader[i];
                        properties[i] = new Property(fieldName, fieldType, fieldValue);
                    }

                    return new Entity(properties);
                }

                return null;
            }
        }

        public override Entity Update(Entity entity, TypeExpression entityType)
        {
            var tableName = entityType.Type.ToTableName();
            var columnNames = entity.PropertyNames.Where(p => p != "Id" && p != "UId").Select(p => string.Format(@"""{0}""", p));
            var parameters =
                entity.PropertyNames.Where(p => p != "Id" && p != "UId")
                    .Select((p, i) => new Parameter(string.Format("P{0}", i), entity[p]))
                    .ToList();
            parameters.Add(new Parameter("UId", entity["UId"]));
            var query = string.Format(@"UPDATE {0} SET {1} WHERE ""UId"" = @UId RETURNING *", tableName,
                string.Join(",", columnNames.Select((p, i) => p + "=@P" + i)));
            using (var command = CreateCommand(query, parameters.ToArray()))
            {
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var properties = new Property[reader.VisibleFieldCount];
                    for (var i = 0; i < reader.VisibleFieldCount; i++)
                    {
                        var fieldName = reader.GetName(i);
                        var fieldType = reader.GetFieldType(i);
                        var fieldValue = reader.IsDBNull(i) ? null : reader[i];
                        properties[i] = new Property(fieldName, fieldType, fieldValue);
                    }
                    return new Entity(properties);
                }

                return null;
            }
        }

        public override Entity Delete(Entity entity, TypeExpression entityType)
        {
            var tableName = entityType.Type.ToTableName();
            var query = string.Format(@"DELETE FROM {0} WHERE ""UId""=@UId RETURNING *", tableName);
            var parameters = new[] {new Parameter("UId", entity["UId"])};
            using (var command = CreateCommand(query, parameters))
            {
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var properties = new Property[reader.VisibleFieldCount];
                    for (var i = 0; i < reader.VisibleFieldCount; i++)
                    {
                        var fieldName = reader.GetName(i);
                        var fieldType = reader.GetFieldType(i);
                        var fieldValue = reader.IsDBNull(i) ? null : reader[i];
                        properties[i] = new Property(fieldName, fieldType, fieldValue);
                    }
                    return new Entity(properties);
                }

                return null;
            }
        }

        private NpgsqlCommand CreateCommand(Query query)
        {
            return CreateCommand(query.Command, query.Parameters.ToArray());
        }

        private NpgsqlCommand CreateCommand(string query, params Parameter[] parameters)
        {
            var command = new NpgsqlCommand(query, connection) { CommandType = CommandType.Text };
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    command.Parameters.AddWithValue("@" + parameter.Name.ToLower(), parameter.Value ?? DBNull.Value);
                }
            }

            return command;
        }

        public override void Dispose()
        {
            connection.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
