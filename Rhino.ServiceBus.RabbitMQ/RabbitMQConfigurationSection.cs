using System.Configuration;
using System.Net.Security;
using System.Security.Authentication;
using Rhino.ServiceBus.Config;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQConfigurationSection : BusConfigurationSection
    {
        [ConfigurationProperty("rabbitmq")]
        public RabbitMQElement RabbitMQ
        {
            get { return this["rabbitmq"] as RabbitMQElement; }
        }

        public class RabbitMQElement : ConfigurationElement
        {
            [ConfigurationProperty("username", DefaultValue = "guest")]
            public string Username
            {
                get { return (string) this["username"]; }
                set { this["username"] = value; }
            }

            [ConfigurationProperty("password", DefaultValue = "guest")]
            public string Password
            {
                get { return (string) this["password"]; }
                set { this["password"] = value; }
            }

            [ConfigurationProperty("virtualHost", DefaultValue = "/")]
            public string VirtualHost
            {
                get { return (string) this["virtualHost"]; }
                set { this["virtualHost"] = value; }
            }

            [ConfigurationProperty("hosts")]
            public RabbitMQHostCollection Hosts
            {
                get { return (RabbitMQHostCollection) this["hosts"]; }
                set { this["hosts"] = value; }
            }
        }

        public class RabbitMQSSlElement : ConfigurationElement
        {
            [ConfigurationProperty("enabled", DefaultValue = false)]
            public bool Enabled
            {
                get { return (bool) this["enabled"]; }
                set { this["enabled"] = value; }
            }

            [ConfigurationProperty("acceptablePolicyErrors")]
            public SslPolicyErrors? AcceptablePolicyErrors
            {
                get { return (SslPolicyErrors?) this["acceptablePolicyErrors"]; }
                set { this["acceptablePolicyErrors"] = value; }
            }

            [ConfigurationProperty("certPassphrase")]
            public string CertPassphrase
            {
                get { return (string) this["certPassphrase"]; }
                set { this["certPassphrase"] = value; }
            }

            [ConfigurationProperty("certPath")]
            public string CertPath
            {
                get { return (string) this["certPath"]; }
                set { this["certPath"] = value; }
            }

            [ConfigurationProperty("serverName")]
            public string ServerName
            {
                get { return (string) this["serverName"]; }
                set { this["serverName"] = value; }
            }

            [ConfigurationProperty("version")]
            public SslProtocols? Version
            {
                get { return (SslProtocols?) this["version"]; }
                set { this["version"] = value; }
            }
        }

        [ConfigurationCollection(typeof(RabbitMQHostElement), CollectionType =
            ConfigurationElementCollectionType.BasicMap)]
        public class RabbitMQHostCollection : ConfigurationElementCollection
        {
            protected override ConfigurationElement CreateNewElement()
            {

                return new RabbitMQHostElement();
            }

            protected override object GetElementKey(ConfigurationElement element)
            {
                var assemblyElement = (RabbitMQHostElement) element;
                return assemblyElement.Host;
            }

            public void Add(RabbitMQHostElement assembly)
            {
                BaseAdd(assembly);
            }
        }

        public class RabbitMQHostElement : ConfigurationElement
        {
            public RabbitMQHostElement()
            {
                this["ssl"] = new RabbitMQSSlElement();
            }

            [ConfigurationProperty("host")]
            public string Host
            {
                get { return (string) this["host"]; }
                set { this["host"] = value; }
            }

            [ConfigurationProperty("port", DefaultValue = -1)]
            public int Port
            {
                get { return (int) this["port"]; }
                set { this["port"] = value; }
            }

            [ConfigurationProperty("ssl")]
            public RabbitMQSSlElement Ssl
            {
                get { return (RabbitMQSSlElement) this["ssl"]; }
                set { this["ssl"] = value; }
            }
        }
    }
}