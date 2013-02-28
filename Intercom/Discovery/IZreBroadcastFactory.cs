using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// Interface for factories that create <see cref="IZreBroadcast"/> instances.
    /// </summary>
    internal interface IZreBroadcastFactory 
    {
        /// <summary>
        /// Creates an <see cref="IZreBroadcast"/> instance.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <param name="broadcastPort">The broadcast port.</param>
        /// <returns>The broadcaster</returns>
        IZreBroadcast Create(Guid uuid, ushort broadcastPort);
    }
}