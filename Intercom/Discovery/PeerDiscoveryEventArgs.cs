using System;
using System.Net;

namespace Intercom.Discovery
{
    /// <summary>
    /// <see cref="EventArgs"/> for peer-related events
    /// </summary>
    public class PeerDiscoveryEventArgs : PeerEventArgs
    {
        /// <summary>
        /// The peer's mailbox port
        /// </summary>
        public IPEndPoint Mailbox { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="PeerDiscoveryEventArgs"/> class.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <param name="mailbox">The mailbox.</param>
        public PeerDiscoveryEventArgs(Guid uuid, IPEndPoint mailbox)
            : base(uuid)
        {
            Mailbox = mailbox;
        }
    }
}
