using System;
using System.Collections.Generic;
using System.Linq;
using Moth.Expressions;

namespace Moth.Database.PgSql
{
    static class ExpressionQueryExtension
    {
        public static string ToSelectQuery(this ExpressionQuery query)
        {
            var from = query.SubQuery != null
                ? string.Format("FROM ({0}) AS F{1}", ((ExpressionQuery)query.SubQuery).ToSelectQuery(),
                    query.QueryIndex)
                : string.Format("FROM {0}", string.Join(", ", query.Types.Select(t => ToTableName(t.Type))));
            var where = query.Filters.Any()
                ? string.Format("WHERE {0}",
                    string.Join(" AND ", query.Filters.Select(e => string.Format("({0})", e.ToSqlStatement()))))
                : string.Empty;
            var orderBy = query.Sorts.Any()
                ? string.Format("ORDER BY {0}", string.Join(",", query.Sorts.Select(s => s.ToSqlStatement())))
                : null;

            var select = query.Projections.Any()
                ? string.Format("SELECT row_number() OVER({2}) AS row_id{1}, {0}", string.Join(",", query.Projections.Select(p => p.ToSqlStatement())), query.QueryIndex, orderBy ?? "ORDER BY Id")
                : string.Format("SELECT row_number() OVER ({1}) AS row_id{0}, *", query.QueryIndex, orderBy ?? @"ORDER BY ""Id""");

            var sqlQuery = string.Format("{0} {1} {2} {3}", select, from, where, orderBy).Trim();
            if (query.Partitions.Any())
            {
                sqlQuery = string.Format("* FROM ({0}) AS P{1}", sqlQuery, query.QueryIndex);
                foreach (var partitionExpression in query.Partitions)
                {
                    var partitionMethod = partitionExpression as MethodExpression;
                    if (partitionMethod != null)
                    {
                        if (partitionMethod.Type == MethodType.First ||
                            partitionMethod.Type == MethodType.FirstOrDefault)
                        {
                            sqlQuery = string.Format(@"SELECT {0} ORDER BY ""Id"" LIMIT 1", sqlQuery);
                        }
                        else if (partitionMethod.Type == MethodType.Last ||
                                 partitionMethod.Type == MethodType.LastOrDefault)
                        {
                            sqlQuery = string.Format("SELECT {0} ORDER BY row_id{1} DESC LIMIT 1", sqlQuery,
                                query.QueryIndex);
                        }

                        else if (partitionMethod.Type == MethodType.Single ||
                                 partitionMethod.Type == MethodType.SingleOrDefault)
                        {
                            sqlQuery = string.Format("SELECT {0}", sqlQuery);
                        }

                        else if (partitionMethod.Type == MethodType.Take)
                        {
                            sqlQuery = string.Format("SELECT TOP {0} {1}", partitionMethod.Parameter.ToSqlStatement(),
                                sqlQuery);
                        }
                        else if (partitionMethod.Type == MethodType.Skip)
                        {
                            sqlQuery = string.Format("{0} WHERE row_id{1} > {2}", sqlQuery, query.QueryIndex, partitionMethod.Parameter.ToSqlStatement());
                        }
                    }
                }
            }

            return sqlQuery;
        }

        private static string ToSqlStatement(this IQueryExpression expression)
        {
            var binaryExpression = expression as BinaryExpression;
            if (binaryExpression != null)
            {
                return binaryExpression.BinaryToSqlStatement();
            }
            var constantExpression = expression as ConstantExpression;
            if (constantExpression != null)
            {
                return constantExpression.ToSqlConstantString();
            }
            var parameterExpression = expression as ParameterExpression;
            if (parameterExpression != null)
            {
                return "@" + parameterExpression.Parameter.Name;
            }
            var memberExpression = expression as MemberExpression;
            if (memberExpression != null)
            {
                var orderExpression = memberExpression as OrderExpression;
                if (orderExpression != null)
                {
                    return string.Format(@"""{0}.{1}"".""{2}"" {3}", string.Join(".", orderExpression.Namespace), orderExpression.ObjectName, orderExpression.MemberName, orderExpression.Direction == SortDirection.Ascending ? "ASC" : "DESC");
                }
                return string.Format(@"""{0}.{1}"".""{2}""", string.Join(".", memberExpression.Namespace), memberExpression.ObjectName, memberExpression.MemberName);
            }
            var methodExpression = expression as MethodExpression;
            if (methodExpression != null)
            {
                throw new NotImplementedException();
                //return methodExpression.MethodToSqlStatement();
            }

            throw new ArgumentOutOfRangeException("expression", string.Format("Expression Type: {0}", expression.GetType()));
        }

        private static string BinaryToSqlStatement(this BinaryExpression binaryExpression)
        {
            var left = binaryExpression.Left.ToSqlStatement();
            var format = binaryExpression.Operator.ToSqlBinaryFormat();
            var right = binaryExpression.Right.ToSqlStatement();
            return string.Format(format, left, right);
        }

        public static string ToTableName(this Type type)
        {
            return string.Format(@"""{0}.{1}""", type.Namespace, type.Name);
        }

        private static string ToSqlBinaryFormat(this BinaryOperator @operator)
        {
            var operators = new Dictionary<BinaryOperator, string>
            {
                {BinaryOperator.Add, "({0} + {1})"},
                {BinaryOperator.Divide, "({0} / {1})"},
                {BinaryOperator.Subtract, "({0} - {1})"},
                {BinaryOperator.Multiply, "({0} * {1})"},
                {BinaryOperator.Power, "({0} ^ {1})"},
                {BinaryOperator.Modulo, "({0} % {1})"},
                {BinaryOperator.And, "({0} & {1})"},
                {BinaryOperator.Or, "({0} | {1})"},
                {BinaryOperator.ExclusiveOr, "({0} # {1})"},
                {BinaryOperator.BitwiseOr, "({0} | {1})"},
                {BinaryOperator.AndAlso, "({0} AND {1})"},
                {BinaryOperator.OrElse, "({0} OR {1})"},
                {BinaryOperator.Equal, "({0} = {1})"},
                {BinaryOperator.NotEqual, "({0} <> {1})"},
                {BinaryOperator.GreaterThan, "({0} > {1})"},
                {BinaryOperator.GreaterThanOrEqual, "({0} >= {1})"},
                {BinaryOperator.LessThan, "({0} < {1})"},
                {BinaryOperator.LessThanOrEqual, "({0} <= {1})"}
            };

            return operators[@operator];
        }

        private static string ToSqlConstantString(this ConstantExpression expression)
        {
            return TypeIsNumber(expression.ValueType) ? expression.Value.ToString() : string.Format("'{0}'", expression.Value);
        }

        private static bool TypeIsNumber(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }
    }
}