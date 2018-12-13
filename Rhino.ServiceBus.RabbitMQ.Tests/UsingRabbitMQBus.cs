using System;
using System.Collections.Generic;
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
        private static readonly IList<int> _receivedInts = new List<int>();

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

        private IList<RabbitMQMessage> GetErrorMessages(int count)
        {
            var transport = (RabbitMQTransport) container.Resolve<ITransport>();

            for (var i = 0; i < 100; i++)
            {
                Thread.Sleep(Debugger.IsAttached ? 5000 : 500);
                using (var tx = container.Resolve<ITransactionStrategy>().Begin())
                {
                    var messages = transport.ReadMessages("errors").Take(count).ToList();
                    if (messages.Count == count)
                    {
                        tx.Complete();
                        return messages;
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
            private readonly IServiceBus _bus;            

            public ThrowingIntConsumer(IServiceBus bus)
            {
                _bus = bus;
            }

            public void Consume(int message)
            {
                lock(_receivedInts)
                    _receivedInts.Add(message);
                _bus.Send(_bus.Endpoint, 110);
                _bus.Send(_bus.Endpoint, 120);
                _bus.Send(_bus.Endpoint, 130);
                throw new InvalidOperationException("I want to be Long consumer");
            }
        }

        [Fact]
        public void Can_handle_errors_gracefully()
        {
            _receivedInts.Clear();

            using (var tx = container.Resolve<ITransactionStrategy>().Begin())
            {
                bus.Send(bus.Endpoint, 5);

                tx.Complete();
            }

            Thread.Sleep(500);

            var message = GetErrorMessages(1)[0];

            var msgs = container.Resolve<IMessageSerializer>().Deserialize(message.Data);
            Assert.Equal(5, msgs[0]);
            Assert.Equal("5,5,5,5,5", string.Join(",", _receivedInts));
        }

        [Fact]
        public void Can_handle_errors_under_load_gracefully()
        {
            _receivedInts.Clear();

            using (var tx = container.Resolve<ITransactionStrategy>().Begin())
            {
                for (var i = 0; i < 100; i++)
                    bus.Send(bus.Endpoint, i);

                tx.Complete();
            }

            Thread.Sleep(500);

            var messages = GetErrorMessages(100);

            var serializer = container.Resolve<IMessageSerializer>();
            var msgs = messages.Select(x => (int) serializer.Deserialize(x.Data)[0]).OrderBy(x => x).ToList();

            for (var i = 0; i < 100; i++)
            {
                Assert.Equal(msgs[i], i);
                var rec = _receivedInts.Count(x => x == i);
                Assert.True(rec == 5, $"Received {i} {rec} times");
            }

            Assert.DoesNotContain(110, _receivedInts);
            Assert.DoesNotContain(120, _receivedInts);
            Assert.DoesNotContain(130, _receivedInts);
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