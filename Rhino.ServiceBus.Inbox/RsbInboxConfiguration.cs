using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.Inbox
{
    public static class RsbInboxConfiguration
    {
        public static TConfig UseInbox<TConfig>(this TConfig self)
            where TConfig : AbstractRhinoServiceBusConfiguration
        {
            self.InsertMessageModuleAtFirst<RsbInboxMessageModule>();
            self.AddAssemblyContaining<RsbInboxMessageModule>();
            return self;
        }
    }
}