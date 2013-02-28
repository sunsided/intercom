using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// Interface for implementations of the ZeroMQ Realtime Exchange protocol as defined in 
    /// <a href="http://rfc.zeromq.org/spec:20">http://rfc.zeromq.org/spec:20</a>.
    /// </summary>
    public interface IZreMailbox : IDisposable
    {
        /// <summary>
        /// Gets a value indicating whether this <see cref="ZreMailbox"/> is started.
        /// </summary>
        /// <value>
        ///   <c>true</c> if started; otherwise, <c>false</c>.
        /// </value>
        bool Started { get; }

        /// <summary>
        /// Starts this instance.
        /// </summary>
        bool Start();

        /// <summary>
        /// Broadcasts a beacon.
        /// </summary>
        void BroadcastBeacon();

        /// <summary>
        /// Stops this instance.
        /// </summary>
        void Stop();
    }
}