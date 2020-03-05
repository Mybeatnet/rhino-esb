using Common.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Diagnostics;

namespace Rhino.ServiceBus.Inbox
{
    public class RsbInbox : IDisposable
    {
        private readonly MySqlConnection _connection;
        private readonly MySqlTransaction _transaction;
        private readonly string _table;
        private static readonly ILog _log = LogManager.GetLogger<RsbInbox>();

        public RsbInbox(MySqlConnection connection, MySqlTransaction transaction, string table)
        {
            _connection = connection;
            _transaction = transaction;
            _table = table;
        }

        public bool Insert(DateTime date, Guid messageId)
        {
            var rows = _connection.ExecuteNonQuery(_transaction,
                $@"INSERT INTO `{_table}` (DateReceived, MessageId)
                    SELECT @date, @id
                    FROM DUAL
                    WHERE NOT EXISTS (SELECT 1 FROM `{_table}` WHERE MessageId = @id)",
                new MySqlParameter("date", date),
                new MySqlParameter("id", messageId.ToString("N")));

            return rows > 0;
        }

        public void Commit()
        {
            _transaction.Commit();
        }

        public void Rollback()
        {
            _transaction.Rollback();
        }

        public void Dispose()
        {
            Run.All(() => _transaction.Dispose(), () => _connection.Dispose());
        }

        public IDisposable GetCleanupLock(out bool locked)
        {
            var ret = Convert.ToInt32(_connection.ExecuteScalar(_transaction,
                $"SELECT GET_LOCK('{_table}:Cleanup', 30)"));

            locked = (ret == 1);
            return Run.OnDispose(() =>
            {
                if (ret == 1)
                    _connection.ExecuteNonQuery(_transaction, $"SELECT RELEASE_LOCK('{_table}:Cleanup')");
            });
        }

        public bool ScheduleCleanup()
        {
            var lastRun = (DateTime?)_connection.ExecuteScalar(_transaction, $"SELECT LastCleanupDate FROM `{_table}_state` LIMIT 1");

            if (lastRun >= DateTime.Today)
                return false;

            if (lastRun.HasValue)
                _connection.ExecuteNonQuery(_transaction,
                    $"UPDATE `{_table}_state` SET LastCleanupDate = @date",
                    new MySqlParameter("date", DateTime.Today));
            else
                _connection.ExecuteNonQuery(_transaction,
                    $"INSERT INTO `{_table}_state` (LastCleanupDate) VALUES (@date)",
                    new MySqlParameter("date", DateTime.Today));
            return true;
        }

        public int Cleanup(DateTime olderThan, int maxRows)
        {
            var sw = Stopwatch.StartNew();
            var result = _connection.ExecuteNonQuery(_transaction,
                $@"DELETE FROM `{_table}` WHERE DateReceived <= @date ORDER BY DateReceived LIMIT {maxRows}",
                new MySqlParameter("date", olderThan));

            sw.Stop();
            _log.Info($"Deleted {result} rows from {_table} in {sw.Elapsed.TotalSeconds:0.000} s");

            return result;
        }
    }
}

