using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// Interface for ZRE Breacon Broadcasters.
    /// </summary>
    public interface IZreBroadcast : IDisposable
    {
        /// <summary>
        /// Gets a value indicating whether this <see cref="IZreBroadcast"/> is started.
        /// </summary>
        /// <value>
        ///   <c>true</c> if started; otherwise, <c>false</c>.
        /// </value>
        bool Started { get; }

        /// <summary>
        /// Gets the time since the last beacon broadcast
        /// </summary>
        TimeSpan TimeSinceBeaconBroadcast { get; }

        /// <summary>
        /// Occurs when a peer was discovered.
        /// </summary>
        /// <remarks>
        /// Peer discovery does not imply a successful connection.
        /// </remarks>
        event EventHandler<PeerDiscoveryEventArgs> PeerDiscovered;

        /// <summary>
        /// Sendet ein Beacon
        /// </summary>
        void BroadcastBeacon();

        /// <summary>
        /// Starts this instance.
        /// </summary>
        /// <returns><see langword="true"/> if this instance was started; otherwise <see langword="false"/></returns>
        bool Start();

        /// <summary>
        /// Stops this instance.
        /// </summary>
        void Stop();
    }
}