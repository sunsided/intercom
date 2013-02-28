using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// Factory for <see cref="ZreMailbox"/> instances.
    /// </summary>
    public interface IZreMailboxFactory 
    {
        /// <summary>
        /// Creates a new mailbox.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <returns>The mailbox</returns>
        IZreMailbox Create(Guid? uuid = null);
    }
}