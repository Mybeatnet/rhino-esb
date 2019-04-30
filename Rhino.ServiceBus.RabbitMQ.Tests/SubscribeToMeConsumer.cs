using System.Threading;

namespace Rhino.ServiceBus.RabbitMQ.Tests
{
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