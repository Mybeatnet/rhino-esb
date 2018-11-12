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
            StringConsumer.Wait = new AutoResetEvent(false);

            container = new WindsorContainer();
            new RhinoServiceBusConfiguration()
                .UseRabbitMQ()
                .UseCastleWindsor(container)
                .UseStandaloneConfigurationFile("RabbitMQ.config")
                .Configure();
            container.Register(Component.For<StringConsumer>());
            container.Register(Component.For<ThrowingIntConsumer>());
            container.Register(Component.For<SubscribeToMeConsumer>());
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
            public static DateTime Date;
            public static string Value;
            public static AutoResetEvent Wait;

            public void Consume(string message)
            {
                Value = message;
                Date = DateTime.Now;
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

        [Fact]
        public void Can_subscribe_to_message()
        {
            bus.Subscribe<SubscribeToMe>();
            bus.Publish(new SubscribeToMe {Data = "Test Publish"});
            if (!SubscribeToMeConsumer.Wait.WaitOne(5000))
                throw new TimeoutException("Did not receive message in 5 seconds");
            
            Assert.Equal("Test Publish", SubscribeToMeConsumer.Data);
        }
        
        [Fact]
        public void Can_send_delayed_message()
        {
            var sendTime = DateTime.Now.AddSeconds(2);
            using (var tx = container.Resolve<ITransactionStrategy>().Begin())
            {
                bus.DelaySend(bus.Endpoint, sendTime, "delayed hello");

                tx.Complete();
            }

            var received = StringConsumer.Wait.WaitOne(TimeSpan.FromSeconds(10), false);

            Assert.True(received, "Did not receive delayed message in 10 seconds");

            Assert.Equal("delayed hello", StringConsumer.Value);

            Assert.True(StringConsumer.Date >= sendTime, 
                $"Received delayed message {sendTime.Subtract(StringConsumer.Date).TotalSeconds} s before {sendTime}");
        }
    }

    public class SubscribeToMeConsumer : Consumer<SubscribeToMe>.SkipAutomaticSubscription
    {
        public static string Data { get; set; }
        public static AutoResetEvent Wait { get;  } = new AutoResetEvent(false);

        public void Consume(SubscribeToMe message)
        {
            Data = message.Data;
            Wait.Set();
        }
    }
}