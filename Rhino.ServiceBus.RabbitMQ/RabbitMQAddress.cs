using System;
using System.Collections.Specialized;
using System.Linq;
using System.Security.Authentication;
using Common.Logging;
using RabbitMQ.Client;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    [Serializable]
    public class RabbitMQAddress
    {
        public const string Default = "default";

        private static readonly ILog _log = LogManager.GetLogger<RabbitMQAddress>();

        private const string RoutingKeysKey = "routingKey";
        private const string RouteByTypeKey = "routeByType";

        public RabbitMQAddress(string broker, string vhost, string username, string password, string exchange,
            string queueName, string routingKeys, bool routeByType, SslOption ssl)
        {
            if (string.IsNullOrEmpty(exchange)
                && (routeByType || (string.IsNullOrEmpty(routingKeys) == false)))
                throw new InvalidOperationException(
                    "Cannot specify RouteByType or RoutingKeys and not specify an Exchange");

            if ((string.IsNullOrEmpty(queueName) == false)
                && (routeByType || (string.IsNullOrEmpty(routingKeys) == false)))
                _log.Warn("Queue specified with Routing Keys, Routing Keys take precedence");

            Broker = broker.ToLower(); //always do this for good  measure (makes comparison easier)
            VirtualHost = vhost;
            Username = username;
            Password = password;
            Exchange = exchange;
            QueueName = queueName;
            RoutingKeys = routingKeys;
            RouteByType = routeByType;
            Ssl = ssl;

            _log.DebugFormat("Broker: {0}, Exchange: {1}, QueueName: {2}, RoutingKeys: {3}, RouteByType: {4}",
                broker, exchange, queueName, routingKeys, routeByType);
        }

        public string Broker { get; }

        public string Username { get; }

        public string Password { get; }

        public string VirtualHost { get; }

        public string Exchange { get; set; }

        public string QueueName { get; set; }

        public string RoutingKeys { get; }

        public bool RouteByType { get; }

        public SslOption Ssl { get; set; }

        public static RabbitMQAddress FromString(string value)
        {
            var uri = new Uri(value);
            return From(uri);
        }

        public static RabbitMQAddress From(Uri uri)
        {
            var broker = uri.Host;

            if (uri.Port != -1)
                broker += ":" + uri.Port;
            
            var query = new NameValueCollection();

            var queryString = uri.Query;
            if (queryString.StartsWith("?"))
                queryString = queryString.Substring(1);

            queryString.Split(new[] {"&"}, StringSplitOptions.RemoveEmptyEntries)
                .ToList()
                .ForEach(
                    x =>
                    {
                        var parts = x.Split(new[] {"="}, StringSplitOptions.RemoveEmptyEntries);
                        var key = string.Empty;
                        var val = string.Empty;
                        if (parts.Length > 0)
                            key = parts[0];
                        if (parts.Length > 1)
                            val = Uri.UnescapeDataString(parts[1]);

                        query.Add(key, val);
                    });

            var pathParts = uri.LocalPath.Split('/');
            var queue = pathParts[1];
            var vhost = string.Empty;

            if (pathParts.Length > 2)
            {
                queue = pathParts[2];
                vhost = pathParts[1];
            }
            
            var exchange = uri.Fragment.StartsWith("#") ? uri.Fragment.Substring(1) : string.Empty;

            var ssl = new SslOption();
            if (uri.Scheme == "amqps")
            {
                ssl.Enabled = true;
                ssl.ServerName = uri.Host;
                SslProtocols sslVersion = SslProtocols.Default;
                if (!string.IsNullOrEmpty(query["ssl"]) && SslProtocols.TryParse(query["ssl"], out sslVersion))
                    ssl.Version = sslVersion;
            }
            
            var userInfo = uri.UserInfo.Split(new[] { ':'}, 2);
            var username = userInfo.Length > 0 ? userInfo[0] : string.Empty;
            var password = userInfo.Length > 1 ? userInfo[1] : string.Empty;
            var routingKeys = query[RoutingKeysKey] ?? string.Empty;
            var routeByTypeValue = query[RouteByTypeKey];

            var routeByType = !string.IsNullOrEmpty(routeByTypeValue) && bool.Parse(routeByTypeValue);

            if (string.IsNullOrEmpty(exchange)
                && string.IsNullOrEmpty(queue)
                && string.IsNullOrEmpty(routingKeys))
            {
                var message = "No Exchange, Queue, or RoutingKeys defined for endpoint: " + uri;
                _log.Error(message);
                throw new InvalidOperationException(message);
            }

            return new RabbitMQAddress(broker, vhost, username, password, exchange, queue, routingKeys, routeByType,
                ssl);
        }

        public string[] GetRoutingKeysAsArray()
        {
            if (string.IsNullOrEmpty(RoutingKeys))
                return new string[0];

            return RoutingKeys.Split(new[] {" "}, StringSplitOptions.RemoveEmptyEntries);
        }

        public string ToString(string route)
        {
            return
                new RabbitMQAddress(Broker, VirtualHost, Username, Password, Exchange, string.Empty, route, false, Ssl)
                    .ToString();
        }

        public RabbitMQAddress Clone()
        {
            return
                new RabbitMQAddress(Broker, VirtualHost, Username, Password, Exchange, QueueName, RoutingKeys,
                    RouteByType, Ssl);
        }

        public override string ToString()
        {
            Func<string, string, string, string> addParam =
                (source, key, value) =>
                {
                    string delim;
                    if (string.IsNullOrEmpty(source) || !source.Contains("?"))
                        delim = "?";
                    else if (source.EndsWith("?") || source.EndsWith("&"))
                        delim = string.Empty;
                    else
                        delim = "&";

                    return source
                           + delim
                           + Uri.EscapeDataString(key)
                           + "="
                           + Uri.EscapeDataString(value);
                };

            var scheme = Ssl.Enabled ? "amqps" : "amqp";
            var uri = $"{scheme}://{Broker}/{VirtualHost}/{QueueName}?";

            if (!string.IsNullOrEmpty(RoutingKeys))
                uri = addParam(uri, RoutingKeysKey, RoutingKeys);

            if (Ssl.Enabled && Ssl.Version != SslProtocols.Default)
                uri = addParam(uri, "ssl", Ssl.Version.ToString());

            if (!string.IsNullOrEmpty(Exchange))
                uri += "#" + Uri.EscapeDataString(Exchange);

            return uri;
        }

        public bool Equals(RabbitMQAddress other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(other.Broker, Broker)
                   && Equals(other.Exchange, Exchange)
                   && Equals(other.QueueName, QueueName)
                   && Equals(other.RoutingKeys, RoutingKeys)
                   && other.RouteByType.Equals(RouteByType)
                   && Ssl.Enabled == other.Ssl.Enabled;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof(RabbitMQAddress)) return false;
            return Equals((RabbitMQAddress) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var result = Broker?.GetHashCode() ?? 0;
                result = (result*397) ^ (VirtualHost?.GetHashCode() ?? 0);
                result = (result*397) ^ (Username?.GetHashCode() ?? 0);
                result = (result*397) ^ (Password?.GetHashCode() ?? 0);
                result = (result*397) ^ (Exchange?.GetHashCode() ?? 0);
                result = (result*397) ^ (QueueName?.GetHashCode() ?? 0);
                result = (result*397) ^ (RoutingKeys?.GetHashCode() ?? 0);
                result = (result*397) ^ RouteByType.GetHashCode();
                result = (result*397) ^ Ssl.Enabled.GetHashCode();
                return result;
            }
        }

        public static bool operator ==(RabbitMQAddress left, RabbitMQAddress right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(RabbitMQAddress left, RabbitMQAddress right)
        {
            return !Equals(left, right);
        }

        public Uri ToUri()
        {
            return new Uri(ToString());
        }

        public RabbitMQAddress ForSubQueue(SubQueue subQueue)
        {
            return new RabbitMQAddress(Broker, VirtualHost, Username, Password, Exchange,
                QueueName + "." + subQueue.ToString().ToLower(),
                RoutingKeys, RouteByType, Ssl);
        }

        public Uri GetBrokerUri()
        {
            var url = Ssl.Enabled ? "amqps://" : "amqp://";

            var parts = Broker.Split(':');

            url += parts[0] == Environment.MachineName.ToLower() ? "localhost" : parts[0];
            if (parts.Length > 1)
                url += ":" + parts[1];

            return new Uri(url);
        }
    }
}