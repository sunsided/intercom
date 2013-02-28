using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// Factory that creates <see cref="ZreBroadcast"/> instances
    /// </summary>
    sealed class ZreBroadcastFactory : IZreBroadcastFactory
    {
        /// <summary>
        /// Creates a <see cref="ZreBroadcast"/> instance.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <param name="broadcastPort">The broadcast port.</param>
        /// <returns>The broadcaster</returns>
        public IZreBroadcast Create(Guid uuid, ushort broadcastPort)
        {
            return new ZreBroadcast(uuid, broadcastPort);
        }
    }
}
