using System;
using MySql.Data.MySqlClient;
using System.Data;

namespace Rhino.ServiceBus.Inbox
{
    internal static class Extensions
    {
        public static string TextAfter(this string self, string searchFor)
        {
            if (self == null)
                return string.Empty;

            var idx = self.IndexOf(searchFor, StringComparison.Ordinal);
            if (idx >= 0)
                return self.Substring(idx + 1);

            return string.Empty;
        }

        public static int ExecuteCommand(this MySqlConnection self, string sql, params MySqlParameter[] args)
        {
            using (MySqlCommand command = self.CreateCommand(sql, args))
                return command.ExecuteNonQuery();
        }

        public static int ExecuteNonQuery(this MySqlConnection self, MySqlTransaction tx, string sql, params MySqlParameter[] args)
        {
            using (MySqlCommand command = self.CreateCommand(tx, sql, args))
                return command.ExecuteNonQuery();
        }


        public static T ExecuteScalar<T>(this MySqlConnection self, MySqlTransaction tx, string sql, params MySqlParameter[] args)
        {
            using (MySqlCommand command = self.CreateCommand(tx, sql, args))
                return (T)command.ExecuteScalar();
        }

        public static MySqlCommand CreateCommand(this MySqlConnection self, MySqlTransaction tx, string sql, params MySqlParameter[] args)
        {
            MySqlCommand command = self.CreateCommand();
            command.Transaction = tx;
            command.CommandText = sql;
            foreach (var obj in args)
                command.Parameters.Add(obj);
            command.CommandType = CommandType.Text;
            return command;
        }

        public static MySqlCommand CreateCommand(this MySqlConnection self, string sql, params MySqlParameter[] args)
        {
            MySqlCommand command = self.CreateCommand();
            command.CommandText = sql;
            foreach (var obj in args)
                command.Parameters.Add(obj);
            command.CommandType = CommandType.Text;
            return command;
        }

        public static T SelectScalar<T>(this MySqlConnection self, string sql, params MySqlParameter[] args)
        {
            using (MySqlCommand command = self.CreateCommand(sql, args))
                return (T)command.ExecuteScalar();
        }


    }
}