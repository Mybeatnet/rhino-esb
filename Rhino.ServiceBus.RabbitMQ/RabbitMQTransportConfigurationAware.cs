using System;
using Rhino.ServiceBus.Config;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.LoadBalancer;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQTransportConfigurationAware : IBusConfigurationAware
    {
        public void Configure(AbstractRhinoServiceBusConfiguration config, IBusContainerBuilder builder,
            IServiceLocator locator)
        {
            if (!(config is RhinoServiceBusConfiguration) && !(config is LoadBalancerConfiguration))
                return;

            if (!config.Endpoint.Scheme.Equals("rmq", StringComparison.InvariantCultureIgnoreCase))
                return;

            if (!config.DisableAutoQueueCreation)
                RegisterQueueCreation(builder, locator);

            RegisterRabbitMqTransport((RhinoServiceBusConfiguration) config, builder, locator);
        }

        private void RegisterQueueCreation(IBusContainerBuilder b, IServiceLocator l)
        {
            b.RegisterSingleton<IServiceBusAware>(Guid.NewGuid().ToString(),
                () => new RabbitMQQueueCreationModule(l.Resolve<RabbitMQQueueStrategy>()));
        }

        private void RegisterRabbitMqTransport(RhinoServiceBusConfiguration c, IBusContainerBuilder b,
            IServiceLocator l)
        {
            b.RegisterSingleton(() => new RabbitMQConnectionProvider());
            b.RegisterSingleton<ITransactionStrategy>(() => new RabbitMQTransactionStrategy());

            b.RegisterSingleton(() => new RabbitMQQueueStrategy(
                c.Endpoint,
                l.Resolve<RabbitMQConnectionProvider>()));

            b.RegisterSingleton<IMessageBuilder<RabbitMQMessage>>(
                () => new RabbitMQMessageBuilder(
                    l.Resolve<IMessageSerializer>(),
                    l.Resolve<IServiceLocator>()));

            b.RegisterSingleton<ISubscriptionStorage>(() => new RabbitMQSubscriptionStorage(c.MessageOwners));

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