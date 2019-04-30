using System;
using System.Threading;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using Castle.Windsor.Configuration.Interpreters;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Xunit;

namespace Rhino.ServiceBus.Tests.LocalMsmqTransactions
{
    public class RequestAndReplyLocalTransactions : MsmqTestBase, ConsumerOf<RequestAndReplyLocalTransactions.PongMessage>
    {
        private readonly WindsorContainer container;
        private readonly ManualResetEvent handle = new ManualResetEvent(false);
        private PongMessage message;

        public RequestAndReplyLocalTransactions()
        {
            container = new WindsorContainer(new XmlInterpreter());
            new RhinoServiceBusConfiguration()
                .UseCastleWindsor(container)
                .UseStandaloneConfigurationFile("LocalMsmqTransactions\\BusWithLocalTransactions.config")
                .Configure();
            container.Register(Component.For<PingConsumer>());
        }


        [Fact]
        public void Can_request_and_reply()
        {
            using (var bus = container.Resolve<IStartableServiceBus>())
            {
                bus.Start();

                using (bus.AddInstanceSubscription(this))
                {
                    bus.Send(bus.Endpoint, new PingMessage());

                    handle.WaitOne(TimeSpan.FromSeconds(30), false);

                    Assert.NotNull(message);
                }
            }

        }

        [Fact]        
        public void Bus_will_not_hold_reference_to_consumer()
        {            
            using (var bus = container.Resolve<IStartableServiceBus>())
            {
                bus.Start();


                WeakReference weakConsumer;

                using (WeakSubscribe(bus, out weakConsumer))
                {
                }
                GC.Collect(2, GCCollectionMode.Forced, true, true);
                GC.WaitForPendingFinalizers();
                
                Assert.False(weakConsumer.IsAlive);
            }
        }

        /// <summary>
        /// Separate method so that no local variable is held for PingConsumer preventing it from being garbage collected
        /// </summary>
        /// <param name="bus"></param>
        /// <param name="weakRef"></param>
        /// <returns></returns>
        private IDisposable WeakSubscribe(IServiceBus bus, out WeakReference weakRef)
        {
            var consumer = new PingConsumer(bus);
            weakRef = new WeakReference(consumer);
            return bus.AddInstanceSubscription(consumer);
        }

        [Fact]
        public void Can_request_and_reply_and_then_unsubscribe()
        {
            using (var bus = container.Resolve<IStartableServiceBus>())
            {
                bus.Start();

                using (bus.AddInstanceSubscription(this))
                {
                    bus.Send(bus.Endpoint, new PingMessage());

                    handle.WaitOne(TimeSpan.FromSeconds(30), false);

                    Assert.NotNull(message);
                }

                handle.Reset();

                message = null;
                container.Resolve<ITransport>().MessageArrived += m => handle.Set();
                bus.Send(bus.Endpoint, new PingMessage());

                handle.WaitOne(TimeSpan.FromSeconds(30), false);

                Assert.Null(message);
            }
        }

        public class PingMessage { }
        public class PongMessage { }

        public class PingConsumer : ConsumerOf<PingMessage>
        {
            private readonly IServiceBus bus;

            public PingConsumer(IServiceBus bus)
            {
                this.bus = bus;
            }

            public void Consume(PingMessage pong)
            {
                bus.Reply(new PongMessage());
            }
        }

        public void Consume(PongMessage pong)
        {
            message = pong;
            handle.Set();
        }
    }
}