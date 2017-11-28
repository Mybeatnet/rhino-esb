namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQConsumer
    {
        private RabbitMQAddress _address;
        private int _index;

        public RabbitMQConsumer(int index, RabbitMQAddress addr)
        {
            _index = index;
            _address = addr;
        }

        public void Start()
        {
            for (var i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(ReceiveMessage)
                {
                    Name = "Rhino Service Bus Worker Thread #" + i,
                    IsBackground = true
                };
                threads[i].Start(i);
            }

        }

    }
}