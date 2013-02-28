using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Intercom.Discovery
{
    /// <summary>
    /// UDP beacon broadcast for ZeroMQ Realtime Exchange as defined in <a href="http://rfc.zeromq.org/spec:20">http://rfc.zeromq.org/spec:20</a>
    /// </summary>
    sealed class ZreBroadcast : IZreBroadcast
    {
        /// <summary>
        /// Der Vorgabeport für ZRE-Broadcasts
        /// </summary>
        private const int DefaultZreBroadcastPort = 12345;

        /// <summary>
        /// Länge der Beacon-Daten
        /// </summary>
        private const int BeaconSize = 22;

        /// <summary>
        /// Header für ZRE-Beacons der Version 1
        /// </summary>
        private static readonly byte[] BeaconVersion1Header = new[] { (byte)'Z', (byte)'R', (byte)'E', (byte)0x01 };

        /// <summary>
        /// This node's UUID
        /// </summary>
        private readonly Guid _uuid;

        /// <summary>
        /// Der Broadcast-Port, an welchen die Beacons gesendet werden sollen
        /// </summary>
        private readonly int _zreBroadcastPort;

        /// <summary>
        /// Gets a value indicating whether this <see cref="ZreMailbox"/> is started.
        /// </summary>
        /// <value>
        ///   <c>true</c> if started; otherwise, <c>false</c>.
        /// </value>
        public bool Started { get; private set; }

        /// <summary>
        /// Die Beacon-Bytes
        /// </summary>
        private byte[] _beacon;

        /// <summary>
        /// Receiver für Broadcasts
        /// </summary>
        private UdpClient _broadcastReceiver;

        /// <summary>
        /// Sender für Broadcasts
        /// </summary>
        private UdpClient _broadcastSender;

        /// <summary>
        /// The mailbox port to be broadcast
        /// </summary>
        private readonly ushort _mailboxPort;

        /// <summary>
        /// Tracks the time since the last beacon broadcast
        /// </summary>
        private Stopwatch _timeSinceBeaconBroadcast;

        /// <summary>
        /// Gets the time since the last beacon broadcast
        /// </summary>
        public TimeSpan TimeSinceBeaconBroadcast
        {
            get { return _timeSinceBeaconBroadcast != null ? _timeSinceBeaconBroadcast.Elapsed : TimeSpan.MaxValue; }
        }

        /// <summary>
        /// Occurs when a peer was discovered.
        /// </summary>
        /// <remarks>
        /// Peer discovery does not imply a successful connection.
        /// </remarks>
        public event EventHandler<PeerDiscoveryEventArgs> PeerDiscovered;

        /// <summary>
        /// The multicast IP to be used
        /// </summary>
        private readonly IPAddress _multicastIp = IPAddress.Parse("224.0.0.0"); // TODO: Allow multiple multicast IPs OR a single broadcast IP

        /// <summary>
        /// Initializes a new instance of the <see cref="ZreBroadcast"/> class.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <param name="mailboxPort">The mailbox port.</param>
        /// <param name="zreBroadcastPort">The ZRE broadcast port.</param>
        public ZreBroadcast(Guid uuid, ushort mailboxPort, int zreBroadcastPort = DefaultZreBroadcastPort)
        {
            _uuid = uuid;
            _mailboxPort = mailboxPort;
            _zreBroadcastPort = zreBroadcastPort;
        }

        /// <summary>
        /// Starts this instance.
        /// </summary>
        public bool Start()
        {
            if (Started) return true;

            // Prepare the time tracking
            _timeSinceBeaconBroadcast = new Stopwatch();

            // Beacon generieren
            _beacon = CreateVersion1Beacon(_uuid, _mailboxPort);

            // UPD receiver erzeugen
            var receiveIp = _multicastIp;
            var receiveEndpoint = new IPEndPoint(IPAddress.Any, _zreBroadcastPort);
            _broadcastReceiver = new UdpClient();
            _broadcastReceiver.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, 1);
            _broadcastReceiver.JoinMulticastGroup(receiveIp); // TODO: Allow multiple multicast IPs (224.0.0.0 .. 239.255.255.255)
            _broadcastReceiver.Client.Bind(receiveEndpoint);
            StartReceiveBroadcast(_broadcastReceiver);

            // UDP sender erzeugen
            _broadcastSender = new UdpClient();

            return true;
        }


        /// <summary>
        /// Sendet ein Beacon
        /// </summary>
        public void BroadcastBeacon()
        {
            var sender = _broadcastSender;
            if (sender == null) return;

            // Beacon beziehen
            var beacon = _beacon;
            if (beacon == null) return;

            // Daten senden
            var multicastIp = _multicastIp;
            var multicastEndpoint = new IPEndPoint(multicastIp, _zreBroadcastPort);
            var state = new UdpState(multicastEndpoint, sender);
            sender.BeginSend(beacon, beacon.Length, multicastEndpoint, EndBroadcastBeacon, state);
        }

        /// <summary>
        /// Beendet eine asynchrone Sendeoperation
        /// </summary>
        /// <param name="ar"></param>
        private void EndBroadcastBeacon(IAsyncResult ar)
        {
            try
            {
                var state = ar.AsyncState as UdpState;
                Debug.Assert(state != null);

                // Sender ermitteln
                var sender = state.Client;
                if (sender.Client == null) return;
                if (!sender.Client.IsBound) return;

                // Senden beenden
                sender.EndSend(ar);

                // Notify
                var tracker = _timeSinceBeaconBroadcast;
                if (tracker != null) tracker.Restart();
            }
            catch (ObjectDisposedException) { }
            catch (SocketException) { }
        }

        /// <summary>
        /// Beginnt das Horchen auf UDP-Broadcasts
        /// </summary>
        private void StartReceiveBroadcast(UdpClient client)
        {
            var ip = new IPEndPoint(IPAddress.Any, _zreBroadcastPort);
            var state = new UdpState(ip, client);

            // Mit dem Warten beginnen
            client.BeginReceive(EndReceiveBroadcast, state);
        }

        /// <summary>
        /// Broadcast wurde über UDP empfangen
        /// </summary>
        /// <param name="ar"></param>
        private void EndReceiveBroadcast(IAsyncResult ar)
        {
            try
            {
                var state = ar.AsyncState as UdpState;
                Debug.Assert(state != null);

                // Receiver ermitteln
                var receiver = state.Client;
                if (receiver.Client == null) return;
                if (!receiver.Client.IsBound) return;

                // Endpunkt ermitteln
                IPEndPoint endpoint = state.EndPoint;
                Debug.Assert(endpoint != null);

                // Daten lesen
                var payload = receiver.EndReceive(ar, ref endpoint);

                // Port freigeben
                StartReceiveBroadcast(receiver);

                // Daten verarbeiten
                ProcessBroadcastPayload(endpoint, payload);
            }
            catch (ObjectDisposedException) { }
            catch (SocketException) { }
        }

        /// <summary>
        /// Verarbeitet die Daten des UDP-Broadcasts
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="payload">Die Daten</param>
        private void ProcessBroadcastPayload(IPEndPoint endpoint, byte[] payload)
        {
            if (payload == null) return;
            if (payload.Length != BeaconSize) return;

            // Header überprüfen
            var sentHeader = BitConverter.ToInt32(payload, 0);
            var version1Header = BitConverter.ToInt32(BeaconVersion1Header, 0);
            if (sentHeader != version1Header) return;

            // Get the interface used to talk to that client; discard message if no interface was found
            var interfaceMatch = DetectInterfaceForEndpoint(endpoint);
            if (interfaceMatch == null) return;

            // Parse the uuid and discard the message if we caught our own echo
            var uuid = payload.ParseGuid(4);
            if (uuid == _uuid) return;

            // Extract the mailbox port
            var port = (ushort)IPAddress.NetworkToHostOrder(BitConverter.ToInt16(payload, 20));

            // Mailbox registrieren
            var mailboxEndpoint = new IPEndPoint(endpoint.Address, port);
            OnPeerDiscovered(new PeerDiscoveryEventArgs(uuid, mailboxEndpoint, interfaceMatch));
        }

        /// <summary>
        /// Detects the correct interface for the given endpoint
        /// </summary>
        /// <param name="endpoint">The endpoint</param>
        /// <returns>The matching interface's IP address or <see langword="null"/> if no match was found</returns>
        private static IPAddress DetectInterfaceForEndpoint(IPEndPoint endpoint)
        {
            // TODO: Currently IPv6 is not supported
            if (endpoint.AddressFamily != AddressFamily.InterNetwork) return null;

            // Query the interfaces
            var unicastAddresses = NetworkInterface.GetAllNetworkInterfaces()
                .Select(ni => ni.GetIPProperties())
                .SelectMany(prop => prop.UnicastAddresses)
                .Where(uni => uni.Address.AddressFamily == endpoint.AddressFamily);

            foreach (var unicastAddress in unicastAddresses)
            {
                // early exit if message is from the same interface
                if (unicastAddress.Address.Equals(endpoint.Address)) return unicastAddress.Address;

                // get the address bytes
                byte[] maskBytes = unicastAddress.IPv4Mask.GetAddressBytes();
                var ifIpBytes = unicastAddress.Address.GetAddressBytes();
                var ipBytes = endpoint.Address.GetAddressBytes();

                // extract endian-correct addresses
                var ifIp = (uint)IPAddress.NetworkToHostOrder((int)BitConverter.ToUInt32(ifIpBytes, 0)); // TODO: verify on big-endian machine
                var mask = (uint)IPAddress.NetworkToHostOrder((int)BitConverter.ToUInt32(maskBytes, 0));
                var address = (uint)IPAddress.NetworkToHostOrder((int)BitConverter.ToUInt32(ipBytes, 0));

                // test the ip range
                uint rangeEnd = ifIp & mask;
                uint rangeStart = ifIp | ~mask;
                if (rangeEnd <= address && rangeStart >= address)
                {
                    return unicastAddress.Address;
                }
            }
            return null;
        }

        /// <summary>
        /// Raises the <see cref="PeerDiscovered"/> event.
        /// </summary>
        /// <param name="e">The <see cref="Intercom.Discovery.PeerEventArgs"/> instance containing the event data.</param>
        private void OnPeerDiscovered(PeerDiscoveryEventArgs e)
        {
            var handler = PeerDiscovered;
            if (handler != null)
            {
                handler(this, e);
            }
        }

        /// <summary>
        /// Erzeugt ein ZRE v1-Beacon
        /// </summary>
        /// <param name="uuid">Die UUID des Knotens</param>
        /// <param name="port">Der ZRE-Port</param>
        /// <returns>Die Beacon-Bytes</returns>
        private static byte[] CreateVersion1Beacon(Guid uuid, ushort port)
        {
            // Payload erzeugen
            var header = BeaconVersion1Header;
            Debug.Assert(BeaconVersion1Header.Length == 4);

            // UUID
            var uuidBytes = uuid.ToByteArray();
            Debug.Assert(uuidBytes.Length == 16);

            // Version
            var routerPort = IPAddress.HostToNetworkOrder((short)port);
            var routerPortBytes = BitConverter.GetBytes(routerPort);
            Debug.Assert(routerPortBytes.Length == 2);

            // Beacon erzeugen
            using (MemoryStream stream = new MemoryStream())
            {
                stream.Write(header, 0, header.Length);
                stream.Write(uuidBytes, 0, uuidBytes.Length);
                stream.Write(routerPortBytes, 0, routerPortBytes.Length);
                var beacon = stream.ToArray();
                Debug.Assert(beacon.Length == BeaconSize);
                return beacon;
            }
        }

        /// <summary>
        /// Stops this instance.
        /// </summary>
        public void Stop()
        {
            if (!Started) return;
            StopInternal();
        }

        /// <summary>
        /// Stops this instance.
        /// </summary>
        private void StopInternal()
        {
            try
            {
                // Release broadcast sender
                if (_broadcastSender != null)
                {
                    _broadcastSender.Close();
                    _broadcastSender = null;
                }

                // Release broadcast receiver
                if (_broadcastReceiver != null)
                {
                    _broadcastReceiver.Close();
                    _broadcastReceiver = null;
                }

                // Release the Kraken
                _beacon = null;
                _timeSinceBeaconBroadcast = null;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("An error occured during disposal: {0}", e.Message);
                throw;
            }
            finally
            {
                // Oink
                Started = false;
            }
        }

        #region Dispose pattern

        /// <summary>
        /// Gets a value indicating whether this <see cref="ZreMailbox"/> is disposed.
        /// </summary>
        /// <value>
        ///   <c>true</c> if disposed; otherwise, <c>false</c>.
        /// </value>
        public bool Disposed { get; private set; }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        private void Dispose(bool disposing)
        {
            if (Disposed) return;

            if (disposing)
            {
                StopInternal();
            }

            Disposed = true;
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        #region Internal state variables

        /// <summary>
        /// UDP-State für asynchrone Verarbeitung
        /// </summary>
        private sealed class UdpState
        {
            /// <summary>
            /// Der Endpunkt
            /// </summary>
            public readonly IPEndPoint EndPoint;

            /// <summary>
            /// Der Client
            /// </summary>
            public readonly UdpClient Client;

            /// <summary>
            /// Initializes a new instance of the <see cref="T:System.Object"/> class.
            /// </summary>
            public UdpState(IPEndPoint endPoint, UdpClient client)
            {
                EndPoint = endPoint;
                Client = client;
            }
        }

        #endregion
    }
}
