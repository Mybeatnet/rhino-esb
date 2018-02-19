using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Transport;
using Xunit;

namespace Rhino.ServiceBus.RabbitMQ.Tests
{
    public class UsingRabbitMQBus : IDisposable
    {
        public UsingRabbitMQBus()
        {
            StringConsumer.Value = null;
            StringConsumer.Wait = new ManualResetEvent(false);

            container = new WindsorContainer();
            new RhinoServiceBusConfiguration()
                .UseRabbitMQ()
                .UseCastleWindsor(container)
                .UseStandaloneConfigurationFile("RabbitMQ.config")
                .Configure();
            container.Register(Component.For<StringConsumer>());
            container.Register(Component.For<ThrowingIntConsumer>());
            bus = container.Resolve<IStartableServiceBus>();
            bus.Start();
            var queues = container.Resolve<RabbitMQQueueStrategy>();
            queues.PurgeAll();
        }

        public void Dispose()
        {
            container.Dispose();
        }

        private readonly IWindsorContainer container;
        private readonly IStartableServiceBus bus;

        private RabbitMQMessage GetErrorMessage()
        {
            var transport = (RabbitMQTransport) container.Resolve<ITransport>();

            for (var i = 0; i < 100; i++)
            {
                Thread.Sleep(Debugger.IsAttached ? 5000 : 500);
                using (var tx = container.Resolve<ITransactionStrategy>().Begin())
                {
                    var message = transport.ReadMessages("errors").FirstOrDefault();
                    if (message != null)
                    {
                        tx.Complete();
                        return message;
                    }
                }
            }
            throw new TimeoutException("Did not receive error message within 50 seconds");
        }

        public class StringConsumer : ConsumerOf<string>
        {
            public static string Value;
            public static ManualResetEvent Wait;

            public void Consume(string message)
            {
                Value = message;
                Wait.Set();
            }
        }


        public class ThrowingIntConsumer : ConsumerOf<int>
        {
            public void Consume(int message)
            {
                throw new InvalidOperationException("I want to be Long consumer");
            }
        }

        [Fact]
        public void Can_handle_errors_gracefully()
        {
            using (var tx = container.Resolve<ITransactionStrategy>().Begin())
            {
                bus.Send(bus.Endpoint, 5);

                tx.Complete();
            }

            var message = GetErrorMessage();

            var msgs = container.Resolve<IMessageSerializer>().Deserialize(message.Data);
            Assert.Equal(5, msgs[0]);
        }

        [Fact]
        public void Can_send_and_receive_messages()
        {
            using (var tx = container.Resolve<ITransactionStrategy>().Begin())
            {
                bus.Send(bus.Endpoint, "hello");

                tx.Complete();
            }

            Assert.True(StringConsumer.Wait.WaitOne(TimeSpan.FromSeconds(100), false));

            Assert.Equal("hello", StringConsumer.Value);
        }

        [Fact]
        public void Can_send_without_transaction()
        {
            bus.Send(bus.Endpoint, "hi there");

            Assert.True(StringConsumer.Wait.WaitOne(TimeSpan.FromSeconds(100), false));

            Assert.Equal("hi there", StringConsumer.Value);
        }
    }
}