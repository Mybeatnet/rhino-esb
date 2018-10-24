using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Common.Logging;
using RabbitMQ.Client;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQConnectionProvider : IDisposable
    {
        private static readonly ConcurrentDictionary<string, ConnectionFactory> _connectionFactories
            = new ConcurrentDictionary<string, ConnectionFactory>();

        private static readonly ConcurrentDictionary<string, IConnection> _connections
            = new ConcurrentDictionary<string, IConnection>();
        
        [ThreadStatic]
        private static IDictionary<string, IModel> _models;
        
        private readonly ILog _log = LogManager.GetLogger<RabbitMQConnectionProvider>();

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
            IModel model;
            if (_models != null && _models.TryGetValue(key, out model))
                return new ModelWrapper(model);

            model = OpenNew(brokerAddress);
            if (RabbitMQTransaction.Current == null) return model;

            RabbitMQTransaction.Current.Add(model);
            model.TxSelect();
            RabbitMQTransaction.Current.Enlist(x => _models = null);
            if (_models == null)
                _models = new Dictionary<string, IModel>();
            _models[key] = model;
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
            var key = GetKey(brokerAddress);

            var broker = brokerAddress.Broker == Environment.MachineName.ToLower()
                ? "localhost"
                : brokerAddress.Broker;

            var factory = _connectionFactories.GetOrAdd(key, s =>
            {
                var f = new ConnectionFactory {Endpoint = new AmqpTcpEndpoint(broker)};

                if (!string.IsNullOrEmpty(brokerAddress.VirtualHost))
                    f.VirtualHost = brokerAddress.VirtualHost;

                if (!string.IsNullOrEmpty(brokerAddress.Username))
                    f.UserName = brokerAddress.Username;

                if (!string.IsNullOrEmpty(brokerAddress.Password))
                    f.Password = brokerAddress.Password;

                _log.Debug("Opening new Connection Factory " + brokerAddress + " using " + protocol.ApiName);
                return f;
            });

            return factory;
        }

        private IConnection GetConnection(IProtocol protocol, RabbitMQAddress brokerAddress, ConnectionFactory factory)
        {
            var broker = brokerAddress.Broker == Environment.MachineName.ToLower()
                ? "localhost"
                : brokerAddress.Broker;

            var key =
                $"{protocol}:{broker}:{brokerAddress.VirtualHost}:{brokerAddress.Username}:{brokerAddress.Password}";

            var connection = _connections.GetOrAdd(key, s => factory.CreateConnection());
            if (!connection.IsOpen)
            {
                connection = factory.CreateConnection();
                _connections[key] = connection;
            }

            _log.DebugFormat("Opening new Connection {0} on {1} using {2}",
                connection, brokerAddress, protocol.ApiName);

            return connection;
        }

        private static IProtocol GetProtocol()
        {
            return Protocols.DefaultProtocol;
        }

        public void DeclareExchange(RabbitMQAddress broker, string exchange, string exchangeType)
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

                channel.ExchangeDeclare(exchange, exchangeType);
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
                    channel.QueueUnbind(queue, exchange, key);                    
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

        public void Dispose()
        {
            foreach (var con in _connections.Values)
                con.Dispose();
            
            _connections.Clear();
            _connectionFactories.Clear();                
        }
    }
}