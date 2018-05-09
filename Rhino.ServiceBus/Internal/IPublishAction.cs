namespace Rhino.ServiceBus.Internal
{
    public interface IPublishAction
    {
        bool Publish(object[] messages);
    }
}