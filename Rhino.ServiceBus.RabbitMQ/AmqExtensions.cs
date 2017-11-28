using System;
using RabbitMQ.Client;

namespace Rhino.ServiceBus.RabbitMQ
{
    public static class AmqExtensions
    {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);

        public static DateTime ToDateTime(this AmqpTimestamp timestamp)
        {
            return UnixEpoch.AddSeconds(timestamp.UnixTime);
        }

        public static AmqpTimestamp ToAmqpTimestamp(this DateTime dateTime)
        {
            return new AmqpTimestamp((long) (dateTime - UnixEpoch).TotalSeconds);
        }
    }
}