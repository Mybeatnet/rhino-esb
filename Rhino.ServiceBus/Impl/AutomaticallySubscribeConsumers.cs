using System;
using Rhino.ServiceBus.Config;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.Impl
{
    public class AutomaticallySubscribeConsumers : IServiceBusAware
    {
        private readonly IReflection reflection;
        private readonly IServiceLocator serviceLocator;

        public AutomaticallySubscribeConsumers(IServiceLocator serviceLocator, IReflection reflection)
        {
            this.serviceLocator = serviceLocator;
            this.reflection = reflection;
        }

        public void BusDisposed(IServiceBus bus)
        {
        }

        public void BusDisposing(IServiceBus bus)
        {
        }

        public void BusStarted(IServiceBus bus)
        {
            {
                var handlers = serviceLocator.GetAllHandlersFor(typeof(IMessageConsumer));
                foreach (var handler in handlers)
                {
                    var msgs = reflection.GetMessagesConsumed(handler.Implementation,
                        type => type == typeof(OccasionalConsumerOf<>)
                                || type == typeof(Consumer<>.SkipAutomaticSubscription));
                    foreach (var msg in msgs) bus.Subscribe(msg);
                }
            }
        }

        public void BusStarting(IServiceBus bus)
        {
        }

        public class Installer : IBusConfigurationAware
        {
            public void Configure(AbstractRhinoServiceBusConfiguration config, IBusContainerBuilder builder, IServiceLocator locator)
            {
                if (config.DisableAutoSubscribeConsumers) return;
                
                builder.RegisterSingleton<IServiceBusAware>(Guid.NewGuid().ToString(),
                    () => new AutomaticallySubscribeConsumers(locator, locator.Resolve<IReflection>()));
            }
        }
    }    
}