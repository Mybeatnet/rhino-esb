using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Castle.Windsor;
using MySql.Data.MySqlClient;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Inbox;
using Xunit;

namespace Rhino.ServiceBus.RabbitMQ.Tests
{
    public class UsingInbox
    {
        public UsingInbox()
        {
            _container = new WindsorContainer();
            new RhinoServiceBusConfiguration()
                .UseRabbitMQ()
                .UseInbox()
                .UseCastleWindsor(_container)
                .UseStandaloneConfigurationFile("RabbitMQ.config")
                .Configure();
            var bus = _container.Resolve<IStartableServiceBus>();
            _container.Resolve<RabbitMQConnectionProvider>().PurgeQueue(RabbitMQAddress.From(bus.Endpoint.Uri));
            bus.Start();
            var queues = _container.Resolve<RabbitMQQueueStrategy>();
            queues.PurgeAll();
            _inboxMessageModule = _container.Resolve<RsbInboxMessageModule>();
            _rsbInboxFactory = _container.Resolve<RsbInboxFactory>();
            using (var con = _rsbInboxFactory.Connect())
            {
                con.ExecuteNonQuery($"DELETE FROM {_rsbInboxFactory.Table}");
                con.ExecuteNonQuery($"DELETE FROM {_rsbInboxFactory.Table}_state");
            }
        }

        private long _lines;

        private readonly RsbInboxMessageModule _inboxMessageModule;
        private readonly RsbInboxFactory _rsbInboxFactory;
        private readonly IWindsorContainer _container;

        protected virtual long GenerateRows(int total)
        {
            var sql = new StringBuilder();
            sql.AppendLine(
                $"INSERT INTO `{_rsbInboxFactory.Table}` (DateReceived,MessageId) VALUES ");
            for (var i = 0; i < total; i++)
                sql.AppendLine(
                    $"('{DateTime.Today.AddDays(-7.01).AddSeconds(-i):yyyy-MM-dd HH:mm:ss}','{Guid.NewGuid():N}'),");

            // remove last \r\n,
            sql.Remove(sql.Length - 3, 3);
            sql.Append(";");

            using (var con = _rsbInboxFactory.Connect())
            {                
                var sqlStr = sql.ToString();
                return con.ExecuteNonQuery(sqlStr);
            }
        }

        protected virtual long CountRows(DateTime date)
        {
            using (var con = _rsbInboxFactory.Connect())
            {
                return con.SelectScalar<long>(
                    $"SELECT COUNT(1) FROM `{_rsbInboxFactory.Table}` WHERE DateReceived < @date",
                    new MySqlParameter("date", date));                
            }
        }

        private void WaitForCompletion()
        {
            for (var i = 0; i < 100; i++)
            {
                if (CountRows(DateTime.Now.Subtract(_rsbInboxFactory.CleanupAge)) == 0)
                    return;

                Thread.Sleep(1000);
            }

            throw new Exception("Did not delete all messages within 100 seconds");
        }


        [Fact]
        public void Create_and_delete_entries()
        {
            _lines = GenerateRows(1000);

            Assert.Equal(1000, _lines);

            var sw = Stopwatch.StartNew();
            _inboxMessageModule.CleanupInbox(null);
            WaitForCompletion();
            sw.Stop();
            Console.WriteLine($"Deleted {_lines} rows in {sw.Elapsed.TotalSeconds:0.000} seconds");
        }
    }
}