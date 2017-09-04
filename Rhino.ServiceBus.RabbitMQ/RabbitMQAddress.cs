using System;
using System.Collections.Specialized;
using System.Linq;
using Common.Logging;

namespace Rhino.ServiceBus.RabbitMQ
{
    [Serializable]
    public class RabbitMQAddress
    {
        private const string VirtualHostKey = "virtualHost";
        private const string UsernameKey = "username";
        private const string PasswordKey = "password";
        private const string ExchangeKey = "exchange";
        private const string QueueKey = "queue";
        private const string RoutingKeysKey = "routingKey";
        private const string RouteByTypeKey = "routeByType";
        private static readonly ILog _log = LogManager.GetLogger<RabbitMQAddress>();

        public RabbitMQAddress(string broker, string vhost, string username, string password, string exchange,
            string queueName, string routingKeys, bool routeByType)
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

            _log.Info("Broker:" + broker);
            _log.Info("Exchange:" + exchange);
            _log.Info("QueueName:" + queueName);
            _log.Info("RoutingKeys:" + routingKeys);
            _log.Info("RouteByType:" + routeByType);
        }

        public string Broker { get; }

        public string Username { get; }

        public string Password { get; }

        public string VirtualHost { get; }

        public string Exchange { get; }

        public string QueueName { get; }

        public string RoutingKeys { get; }

        public bool RouteByType { get; }

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

            var exchange = query[ExchangeKey] ?? string.Empty;
            var queue = query[QueueKey] ?? string.Empty;
            var vhost = query[VirtualHostKey] ?? string.Empty;
            var username = query[UsernameKey] ?? string.Empty;
            var password = query[PasswordKey] ?? string.Empty;
            var routingKeys = query[RoutingKeysKey] ?? string.Empty;
            var routeByTypeValue = query[RouteByTypeKey];

            var routeByType = !string.IsNullOrEmpty(routeByTypeValue) && bool.Parse(routeByTypeValue);

            if (string.IsNullOrEmpty(exchange)
                && string.IsNullOrEmpty(queue)
                && string.IsNullOrEmpty(routingKeys))
            {
                var message = "No Exchange, Queue, or RoutingKeys defined for endpoint: " + value;
                _log.Error(message);
                throw new InvalidOperationException(message);
            }

            return new RabbitMQAddress(broker, vhost, username, password, exchange, queue, routingKeys, routeByType);
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
                new RabbitMQAddress(Broker, VirtualHost, Username, Password, Exchange, string.Empty, route, false)
                    .ToString();
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

            var uri = "rmq://" + Broker + "/?";

            if (!string.IsNullOrEmpty(VirtualHost))
                uri = addParam(uri, VirtualHostKey, VirtualHost);

            if (!string.IsNullOrEmpty(Username))
                uri = addParam(uri, UsernameKey, Username);

            if (!string.IsNullOrEmpty(Password))
                uri = addParam(uri, PasswordKey, Password);

            if (!string.IsNullOrEmpty(Exchange))
                uri = addParam(uri, ExchangeKey, Exchange);

            if (!string.IsNullOrEmpty(QueueName))
                uri = addParam(uri, QueueKey, QueueName);

            if (!string.IsNullOrEmpty(RoutingKeys))
                uri = addParam(uri, RoutingKeysKey, RoutingKeys);

            return uri;
        }

        public bool Equals(RabbitMQAddress other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(other.Broker, Broker) && Equals(other.Exchange, Exchange) &&
                   Equals(other.QueueName, QueueName) && Equals(other.RoutingKeys, RoutingKeys) &&
                   other.RouteByType.Equals(RouteByType);
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
    }
}