using System;
using Rhino.ServiceBus.Config;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.RabbitMQ;

namespace Rhino.ServiceBus.Inbox
{
    public class RsbInboxConfigurator : IBusConfigurationAware
    {
        public void Configure(AbstractRhinoServiceBusConfiguration config, IBusContainerBuilder builder, IServiceLocator locator)
        {
            if (!(config.ConfigurationSection is RabbitMQConfigurationSection))
                throw new Exception(
                    $"ConfigurationSection is not of type RabbitMQConfigurationSection. Check that the type for rhino.esb is {typeof(RabbitMQConfigurationSection).AssemblyQualifiedName} in the config file");

            var rmqSection = (RabbitMQConfigurationSection) config.ConfigurationSection;

            var inboxConfig = rmqSection.Inbox;
            if (inboxConfig == null)
            {
                builder.RegisterSingleton(() => new RsbInboxFactory());
                return;
            }

            builder.RegisterSingleton(() =>
                new RsbInboxFactory(inboxConfig.ConnectionStringName, inboxConfig.TablePrefix)
                {
                    CleanupRows = inboxConfig.CleanupRows,
                    CleanupAge = TimeSpan.Parse(inboxConfig.CleanupAge),
                    Enabled = inboxConfig.Enabled
                });
        }
    }
}