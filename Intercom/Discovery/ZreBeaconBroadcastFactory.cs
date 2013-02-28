using System;

namespace Intercom.Discovery
{
    /// <summary>
    /// Factory that creates <see cref="ZreBeaconBroadcast"/> instances
    /// </summary>
    sealed class ZreBeaconBroadcastFactory
    {
        /// <summary>
        /// Creates a <see cref="ZreBeaconBroadcast"/> instance.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <param name="broadcastPort">The broadcast port.</param>
        /// <returns>The broadcaster</returns>
        public ZreBeaconBroadcast Create(Guid uuid, ushort broadcastPort)
        {
            return new ZreBeaconBroadcast(uuid, broadcastPort);
        }
    }
}
