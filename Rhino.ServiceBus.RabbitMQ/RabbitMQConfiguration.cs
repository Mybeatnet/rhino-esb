using System.Linq;
using RabbitMQ.Client;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQConfiguration
    {
        public string Username { get; set; }

        public string Password { get; set; }

        public string VirtualHost { get; set; }

        public AmqpTcpEndpoint[] Endpoints { get; set; }

        public static RabbitMQConfiguration From(RabbitMQConfigurationSection.RabbitMQElement config)
        {
            if (config == null)
                return new RabbitMQConfiguration
                {
                    Username = "guest",
                    Password = "guest",
                    VirtualHost = "/",
                    Endpoints = new[] {new AmqpTcpEndpoint("localhost")}
                };

            return new RabbitMQConfiguration
            {
                Username = config.Username,
                Password = config.Password,
                VirtualHost = config.VirtualHost,
                Endpoints = config.Hosts.Cast<RabbitMQConfigurationSection.RabbitMQHostElement>()
                    .Select(x => GetEndpoint(x))
                    .ToArray()
            };
        }

        private static AmqpTcpEndpoint GetEndpoint(RabbitMQConfigurationSection.RabbitMQHostElement source)
        {
            if (!source.Ssl.Enabled)
                return new AmqpTcpEndpoint(source.Host, source.Port);

            var ssl = new SslOption();
            ssl.Enabled = source.Ssl.Enabled;
            if (source.Ssl.AcceptablePolicyErrors.HasValue)
                ssl.AcceptablePolicyErrors = source.Ssl.AcceptablePolicyErrors.Value;
            ssl.CertPassphrase = source.Ssl.CertPassphrase;
            ssl.CertPath = source.Ssl.CertPath;
            if (string.IsNullOrEmpty(source.Ssl.ServerName))
                ssl.ServerName = source.Host;
            else
                ssl.ServerName = source.Ssl.ServerName;

            if (source.Ssl.Version.HasValue)
                ssl.Version = source.Ssl.Version.Value;

            return new AmqpTcpEndpoint(source.Host, source.Port, ssl);
        }
    }
}