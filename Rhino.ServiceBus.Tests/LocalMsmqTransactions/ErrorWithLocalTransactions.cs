using System;
using System.Messaging;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Msmq;
using Rhino.ServiceBus.Transport;
using Xunit;

namespace Rhino.ServiceBus.Tests.LocalMsmqTransactions
{
    public class ErrorWithLocalTransactions : MsmqTestBase
    {

        private readonly IWindsorContainer container;

        public ErrorWithLocalTransactions()
        {
            container = new WindsorContainer();
            new RhinoServiceBusConfiguration()
                .UseCastleWindsor(container)
                .UseStandaloneConfigurationFile("LocalMsmqTransactions\\BusWithLocalTransactions.config")
                .Configure();
            container.Register(Component.For<ThrowingConsumer>());
        }

        [Fact]
        public void Error_subqeueue_will_contain_error_details()
        {
            using (var bus = container.Resolve<IStartableServiceBus>())
            {
                bus.Start();

                bus.Send(bus.Endpoint, DateTime.Now);

                using (var q = MsmqUtil.GetQueuePath(bus.Endpoint).Open())
                using (var errorSubQueue = q.OpenSubQueue(SubQueue.Errors, QueueAccessMode.SendAndReceive))
                {
                    var originalMessage = errorSubQueue.Receive();
                    var errorDescripotion = errorSubQueue.Receive();
                    Assert.Equal(string.Format("Error description for: {0}", originalMessage.Label), errorDescripotion.Label);
                }
            }
        }

        public class ThrowingConsumer : ConsumerOf<DateTime>
        {
            public void Consume(DateTime message)
            {
                throw new System.NotImplementedException();
            }
        }
    }
}