using System;
using System.Linq;
using Rhino.ServiceBus.Config;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.LoadBalancer;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQTransportConfigurationAware : IBusConfigurationAware
    {
        private readonly string[] schemes = {"rmq", "amqp", "amqps"};
        public void Configure(AbstractRhinoServiceBusConfiguration config, IBusContainerBuilder builder,
            IServiceLocator locator)
        {
            if (!(config is RhinoServiceBusConfiguration) && !(config is LoadBalancerConfiguration))
                return;

            if (!schemes.Contains(config.Endpoint.Scheme.ToLower()))
                return;

            if (!config.DisableAutoQueueCreation)
                RegisterQueueCreation(builder, locator);

            if (!(config.ConfigurationSection is RabbitMQConfigurationSection))
                throw new Exception(
                    $"ConfigurationSection is not of type RabbitMQConfigurationSection. Check that the type for rhino.esb is {typeof(RabbitMQConfigurationSection).AssemblyQualifiedName} in the config file");

            var rmqSection = (RabbitMQConfigurationSection) config.ConfigurationSection;

            var rabbitMqConfig = RabbitMQConfiguration.From(rmqSection.RabbitMQ);
            RegisterRabbitMqTransport(rabbitMqConfig, (RhinoServiceBusConfiguration) config, builder, locator);
        }

        private void RegisterQueueCreation(IBusContainerBuilder b, IServiceLocator l)
        {
            b.RegisterSingleton<IServiceBusAware>(Guid.NewGuid().ToString(),
                () => new RabbitMQQueueCreationModule(l.Resolve<RabbitMQQueueStrategy>()));
        }

        private void RegisterRabbitMqTransport(RabbitMQConfiguration rabbitMqConfig, RhinoServiceBusConfiguration c, IBusContainerBuilder b,
            IServiceLocator l)
        {
            b.RegisterSingleton(() => rabbitMqConfig);
            b.RegisterSingleton(() => new RabbitMQConnectionProvider(rabbitMqConfig));
            b.RegisterSingleton<ITransactionStrategy>(() => new RabbitMQTransactionStrategy());

            b.RegisterSingleton(() => new RabbitMQQueueStrategy(
                c.Endpoint,
                l.Resolve<RabbitMQConnectionProvider>()));

            b.RegisterSingleton<IMessageBuilder<RabbitMQMessage>>(
                () => new RabbitMQMessageBuilder(
                    l.Resolve<IMessageSerializer>(),
                    l.Resolve<IServiceLocator>()));

            b.RegisterSingleton<ISubscriptionStorage>(
                () => new RabbitMQSubscriptionStorage(c.MessageOwners, l.Resolve<IReflection>()));

            b.RegisterSingleton<ITransport>(() => new RabbitMQTransport(
                l.Resolve<IMessageSerializer>(),
                c.Endpoint,
                c.ThreadCount,
                c.ConsumeInTransaction,
                c.NumberOfRetries,
                l.Resolve<IMessageBuilder<RabbitMQMessage>>(),
                l.Resolve<ITransactionStrategy>(),
                l.Resolve<RabbitMQConnectionProvider>(),
                l.Resolve<RabbitMQQueueStrategy>()));

            b.RegisterSingleton<ISubscribeAction>(() =>
                new RabbitMQSubscribeAction(l.Resolve<RabbitMQConnectionProvider>()));

            b.RegisterSingleton<IPublishAction>(() =>
                new DefaultPublishAction(l.Resolve<ITransport>(),
                    l.Resolve<ISubscriptionStorage>(),
                    l.Resolve<IEndpointRouter>()));
        }
    }
}