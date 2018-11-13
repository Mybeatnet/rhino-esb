using System.IO;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.RabbitMQ
{
    public static class RabbitMQExtensions
    {
        public static object[] Deserialize(this IMessageSerializer serializer, byte[] body)
        {
            using (var ms = new MemoryStream(body))
                return serializer.Deserialize(ms);
        }

        public static TConfig UseRabbitMQ<TConfig>(this TConfig self)
            where TConfig : AbstractRhinoServiceBusConfiguration
        {                        
            self.AddAssemblyContaining<RabbitMQTransportConfigurationAware>();
            return self;
        }
    }
}