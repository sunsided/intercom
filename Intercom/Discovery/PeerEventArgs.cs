using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// <see cref="EventArgs"/> for peer-related events
    /// </summary>
    public class PeerEventArgs : EventArgs
    {
        /// <summary>
        /// The peer's UUID
        /// </summary>
        public Guid Uuid { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:System.EventArgs"/> class.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        public PeerEventArgs(Guid uuid)
        {
            Uuid = uuid;
        }
    }
}
