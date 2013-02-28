using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// Factory for <see cref="ZreMailbox"/> instances.
    /// </summary>
    public sealed class ZreMailboxFactory : IZreMailboxFactory
    {
        /// <summary>
        /// The broadcast factory
        /// </summary>
        private readonly IZreBroadcastFactory _broadcastFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZreMailboxFactory"/> class.
        /// </summary>
        /// <param name="broadcastFactory">The broadcast factory.</param>
        public ZreMailboxFactory(IZreBroadcastFactory broadcastFactory = null)
        {
            _broadcastFactory = broadcastFactory ?? new ZreBroadcastFactory();
        }

        /// <summary>
        /// Creates a new mailbox.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <returns>The mailbox</returns>
        public IZreMailbox Create(Guid? uuid = null)
        {
            return new ZreMailbox(_broadcastFactory, uuid);
        }
    }
}
