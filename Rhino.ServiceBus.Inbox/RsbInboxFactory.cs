using System;
using System.Configuration;
using System.Data;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

namespace Rhino.ServiceBus.Inbox
{
    public class RsbInboxFactory
    {
        private readonly string _connectionString;
        private readonly string _prefix;

        public TimeSpan CleanupAge { get; set; } = TimeSpan.FromHours(1);
        public int CleanupRows { get; set; } = 10000;
        public string Table { get; private set; }
        public bool Enabled { get; set; } = true;

        public RsbInboxFactory(string connectionStringName = "RsbInbox", string prefix = "rsbinbox")
        {
            _connectionString = ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
            _prefix = prefix;
        }

        public void Init(string queuePath)
        {
            Table = _prefix + "_" + Regex.Replace(queuePath.TextAfter("/"), @"\W+", "_");

            using (var con = Connect())
            {
                con.ExecuteCommand($@"CREATE TABLE IF NOT EXISTS `{Table}` 
                    (MessageId CHAR(32) NOT NULL PRIMARY KEY, 
                    DateReceived DATETIME)");

                var exists = con.SelectScalar<long>($@"SELECT COUNT(1) IndexIsThere 
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE table_schema=DATABASE() AND table_name='{Table}' AND index_name='IX_DateReceived';");

                if (exists == 0)
                    con.ExecuteCommand($@"CREATE INDEX IX_DateReceived ON `{Table}` (DateReceived)");

                con.ExecuteCommand($@"CREATE TABLE IF NOT EXISTS `{Table}_state`
                    (LastCleanupDate DATETIME)");
            }
        }

        private MySqlConnection Connect()
        {
            var con = new MySqlConnection(_connectionString);
            con.Open();
            return con;
        }

        public RsbInbox CreateInbox()
        {
            var con = Connect();
            var tx = con.BeginTransaction(IsolationLevel.ReadCommitted);
            return new RsbInbox(con, tx, Table);
        }
    }
}