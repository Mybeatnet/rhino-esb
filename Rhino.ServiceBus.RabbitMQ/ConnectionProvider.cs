using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Common.Logging;
using RabbitMQ.Client;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class ConnectionProvider
    {
        private static readonly ConcurrentDictionary<string, ConnectionFactory> _connectionFactories
            = new ConcurrentDictionary<string, ConnectionFactory>();

        private static readonly ConcurrentDictionary<string, IConnection> _connections
            = new ConcurrentDictionary<string, IConnection>();

        [ThreadStatic] private static Dictionary<string, OpenedSession> state;

        private readonly ILog _log = LogManager.GetLogger<ConnectionProvider>();

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
            //var key = string.Format("{0}:{1}", protocolName, brokerAddress);
            return OpenNew(brokerAddress);
            //opened.Disposed += (sender, e) =>
            //                    {
            //                        log.Debug("Closing " + brokerAddress);
            //                        if (state != null)
            //                        {
            //                            var cnx = opened.Connection;
            //                            connections.TryRemove(key, out cnx);
            //                            state.Remove(brokerAddress.ToString());
            //                        }
            //                    };
            /*
		  if (Transaction.Current != null)
		  {
			Transaction.Current.EnlistVolatile(new RabbitMqEnlistment(opened), EnlistmentOptions.None);
			opened.AddRef();
		  }
		  
			return opened.AddRef();*/
        }

        private IModel OpenNew(RabbitMQAddress brokerAddress)
        {
            var protocol = GetProtocol();
            var factory = GetConnectionFactory(protocol, brokerAddress);
            var connection = GetConnection(protocol, brokerAddress, factory);
            var model = connection.CreateModel();
            return model;
        }

        private ConnectionFactory GetConnectionFactory(IProtocol protocol, RabbitMQAddress brokerAddress)
        {
            ConnectionFactory factory = null;
            var key = $"{protocol}:{brokerAddress}";

            if (!_connectionFactories.TryGetValue(key, out factory))
            {
                factory = new ConnectionFactory();
                factory.Endpoint = new AmqpTcpEndpoint(brokerAddress.Broker);

                if (!string.IsNullOrEmpty(brokerAddress.VirtualHost))
                    factory.VirtualHost = brokerAddress.VirtualHost;

                if (!string.IsNullOrEmpty(brokerAddress.Username))
                    factory.UserName = brokerAddress.Username;

                if (!string.IsNullOrEmpty(brokerAddress.Password))
                    factory.Password = brokerAddress.Password;

                factory = _connectionFactories.GetOrAdd(key, factory);
                _log.Debug("Opening new Connection Factory " + brokerAddress + " using " + protocol.ApiName);
            }

            return factory;
        }

        private IConnection GetConnection(IProtocol protocol, RabbitMQAddress brokerAddress, ConnectionFactory factory)
        {
            IConnection connection = null;
            var key = $"{protocol}:{brokerAddress}";

            if (!_connections.TryGetValue(key, out connection))
            {
                var newConnection = factory.CreateConnection();
                connection = _connections.GetOrAdd(key, newConnection);

                //if someone else beat us from another thread kill the connection just created
                if (newConnection.Equals(connection) == false)
                    newConnection.Dispose();
                else
                    _log.DebugFormat("Opening new Connection {0} on {1} using {2}",
                        connection, brokerAddress, protocol.ApiName);
            }

            return connection;
        }

        private static IProtocol GetProtocol()
        {
            return Protocols.DefaultProtocol;
        }
    }
}