using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Common.Logging;
using RabbitMQ.Client;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQConnectionProvider : IDisposable
    {
        private static readonly ConcurrentDictionary<string, ConnectionFactory> ConnectionFactories
            = new ConcurrentDictionary<string, ConnectionFactory>();

        private static readonly ConcurrentDictionary<string, IConnection> Connections
            = new ConcurrentDictionary<string, IConnection>();

        private static AsyncLocal<IDictionary<string, IModel>> _models = new AsyncLocal<IDictionary<string, IModel>>();

        private readonly RabbitMQConfiguration _config;

        private readonly ILog _log = LogManager.GetLogger<RabbitMQConnectionProvider>();

        public RabbitMQConnectionProvider(RabbitMQConfiguration config)
        {
            _config = config;
        }

        public void Dispose()
        {
            foreach (var con in Connections.Values)
                con.Dispose();

            Connections.Clear();
            ConnectionFactories.Clear();
        }

        public IModel Open(RabbitMQAddress brokerAddress, bool transactional)
        {
            if (!transactional)
            {
                var opened = OpenNew(brokerAddress);
                //opened.Disposed += (sender, e) => log.Debug("Closing " + brokerAddress);
                return opened;
            }

            return OpenTransactional(brokerAddress);
        }

        private IModel OpenTransactional(RabbitMQAddress brokerAddress)
        {
            var key = GetKey(brokerAddress);
            IDictionary<string, IModel> models = _models.Value;
            if (models != null && models.TryGetValue(key, out var model))
                return new ModelWrapper(model);

            model = OpenNew(brokerAddress);
            if (RabbitMQTransaction.Current == null) return model;

            RabbitMQTransaction.Current.Add(model);
            model.TxSelect();
            RabbitMQTransaction.Current.Enlist(x => _models = null);
            if (models == null)
                models = (_models.Value ?? (_models.Value = new Dictionary<string, IModel>()));
            models[key] = model;
            return new ModelWrapper(model);
        }

        private IModel OpenNew(RabbitMQAddress brokerAddress)
        {
            var protocol = GetProtocol();
            var factory = GetConnectionFactory(protocol, brokerAddress);
            var connection = GetConnection(protocol, brokerAddress, factory);
            var model = connection.CreateModel();
            return model;
        }

        private string GetKey(RabbitMQAddress addr)
        {
            var protocol = GetProtocol();
            var broker = addr.Broker == Environment.MachineName.ToLower()
                ? "localhost"
                : addr.Broker;

            return
                $"{protocol}:{broker}:{addr.VirtualHost}:{addr.Username}:{addr.Password}";
        }

        private ConnectionFactory GetConnectionFactory(IProtocol protocol, RabbitMQAddress brokerAddress)
        {
            var broker = brokerAddress.GetBrokerUri();

            var key = broker.ToString();

            var factory = ConnectionFactories.GetOrAdd(key, s =>
            {
                var f = new ConnectionFactory();

                f.RequestedHeartbeat = 5;

                if (brokerAddress.Broker == RabbitMQAddress.Default)
                {
                    f.VirtualHost = _config.VirtualHost;
                    f.UserName = _config.Username;
                    f.Password = _config.Password;
                }
                else
                {
                    f.Endpoint = new AmqpTcpEndpoint(broker, brokerAddress.Ssl);
                    
                    if (!string.IsNullOrEmpty(brokerAddress.VirtualHost))
                        f.VirtualHost = brokerAddress.VirtualHost;

                    if (!string.IsNullOrEmpty(brokerAddress.Username))
                        f.UserName = brokerAddress.Username;

                    if (!string.IsNullOrEmpty(brokerAddress.Password))
                        f.Password = brokerAddress.Password;
                }

                _log.Debug("Opening new Connection Factory " + brokerAddress + " using " + protocol.ApiName);
                return f;
            });

            return factory;
        }

        private IConnection GetConnection(IProtocol protocol, RabbitMQAddress brokerAddress, ConnectionFactory factory)
        {
            var broker = brokerAddress.GetBrokerUri();

            var key =
                $"{protocol}:{broker}:{brokerAddress.VirtualHost}:{brokerAddress.Username}:{brokerAddress.Password}";

            var connection = Connections.GetOrAdd(key, s =>
            {
                if (brokerAddress.Broker == RabbitMQAddress.Default)
                    return factory.CreateConnection(_config.Endpoints);
                return factory.CreateConnection();
            });

            if (!connection.IsOpen)
            {
                connection = CreateNewConnection(factory, brokerAddress);
                Connections[key] = connection;
            }

            _log.DebugFormat("Opening new Connection {0} on {1} using {2}",
                connection, brokerAddress, protocol.ApiName);

            return connection;
        }

        private IConnection CreateNewConnection(ConnectionFactory connectionFactory, RabbitMQAddress brokerAddress)
        {            
            if (brokerAddress.Broker == RabbitMQAddress.Default)
                return connectionFactory.CreateConnection(_config.Endpoints);
            return connectionFactory.CreateConnection();
        }

        private static IProtocol GetProtocol()
        {
            return Protocols.DefaultProtocol;
        }

        public void DeclareExchange(RabbitMQAddress broker, string exchange, string exchangeType,
            bool durable = false, bool autoDelete = false, IDictionary<string, object> arguments = null)
        {
            if (string.IsNullOrEmpty(exchange))
            {
                _log.Info("No Exchange Provided. Not attempting Declare");
                return;
                //var message = "No Input Exchange Provided. Cannot Declare Exchange";
                //log.Error(message);
                //throw new InvalidOperationException(message);
            }

            using (var channel = Open(broker, true))
            {
                _log.InfoFormat(
                    "Declaring Exchange {0} of Type {1} on Broker {2}",
                    exchange,
                    exchangeType,
                    broker);

                channel.ExchangeDeclare(exchange, exchangeType, durable, autoDelete, arguments);
            }
        }

        public void DeclareQueue(RabbitMQAddress broker, string queue, bool durable)
        {
            if (string.IsNullOrEmpty(queue))
            {
                _log.Info("No Queue Provided. Not attempting Declare");
                return;
                //var message = "No Input Queue Provided. Cannot Declare Queue";
                //log.Error(message);
                //throw new InvalidOperationException(message);
            }

            using (var channel = Open(broker, true))
            {
                _log.InfoFormat("Declaring Queue {0} on Broker {1}", queue, broker);
                channel.QueueDeclare(queue, durable, false, false, null);
            }
        }

        public void BindQueue(RabbitMQAddress broker, string exchange, string queue, string routingKeys)
        {
            if (string.IsNullOrEmpty(exchange))
                return;

            using (var channel = Open(broker, true))
            {
                var keys = routingKeys.Split(new[] {" "}, StringSplitOptions.RemoveEmptyEntries);
                keys = keys.Length == 0 ? new[] {queue} : keys;

                foreach (var key in keys)
                {
                    _log.InfoFormat("Binding Key {0} on Queue {1} on Exchange {2}", key, queue, exchange);
                    channel.QueueBind(queue, exchange, key);
                }
            }
        }

        public void UnbindQueue(RabbitMQAddress broker, string exchange, string queue, string routingKeys)
        {
            if (string.IsNullOrEmpty(exchange))
                return;

            using (var channel = Open(broker, true))
            {
                var keys = routingKeys.Split(new[] {" "}, StringSplitOptions.RemoveEmptyEntries);
                keys = keys.Length == 0 ? new[] {queue} : keys;

                foreach (var key in keys)
                {
                    _log.InfoFormat("Binding Key {0} on Queue {1} on Exchange {2}", key, queue, exchange);
                    channel.QueueUnbind(queue, exchange, key, new Dictionary<string, object>());
                }
            }
        }

        public void PurgeQueue(RabbitMQAddress addr)
        {
            using (var channel = Open(addr, false))
            {
                channel.QueuePurge(addr.QueueName);
            }
        }
    }
}