using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using ZeroMQ;
using SocketType = ZeroMQ.SocketType;

namespace Intercom.Discovery
{
    /// <summary>
    /// Implementierung des ZeroMQ Realtime Exchange-Protokolls, definiert unter <a href="http://rfc.zeromq.org/spec:20">http://rfc.zeromq.org/spec:20</a>
    /// </summary>
    sealed class ZeroMqRealtimeExchange : IDisposable
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
        /// Die ID des Knoten
        /// </summary>
        private readonly Guid _uuid;

        /// <summary>
        /// Der Broadcast-Port, an welchen die Beacons gesendet werden sollen
        /// </summary>
        private readonly int _zreBroadcastPort;

        /// <summary>
        /// Der Kontext
        /// </summary>
        private ZmqContext _context;

        /// <summary>
        /// Der Router-Socket
        /// </summary>
        private ZmqSocket _mailbox;

        /// <summary>
        /// Receiver für Broadcasts
        /// </summary>
        private UdpClient _broadcastReceiver;

        /// <summary>
        /// Sender für Broadcasts
        /// </summary>
        private UdpClient _broadcastSender;

        /// <summary>
        /// Der Port des Routers
        /// </summary>
        /// <remarks>
        /// Nur gültig, wenn <see cref="_mailbox"/> != <see langword="null"/>.
        /// </remarks>
        private ushort _mailboxPort;

        /// <summary>
        /// Gets a value indicating whether this <see cref="ZeroMqRealtimeExchange"/> is started.
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
        /// Initializes a new instance of the <see cref="ZeroMqRealtimeExchange"/> class.
        /// </summary>
        /// <param name="uuid">The UUID.</param>
        /// <param name="zreBroadcastPort">The ZRE broadcast port.</param>
        public ZeroMqRealtimeExchange(Guid? uuid = null, int zreBroadcastPort = DefaultZreBroadcastPort)
        {
            _uuid = uuid ?? Guid.NewGuid();
            _zreBroadcastPort = zreBroadcastPort;
        }

        /// <summary>
        /// Starts this instance.
        /// </summary>
        public bool Start()
        {
            if (Started) return true;

            // Kontext erzeugen
            _context = ZmqContext.Create();

            // Router erzeugen und binden
            _mailbox = _context.CreateSocket(SocketType.ROUTER);
            _mailbox.Bind("tcp://*:*");
            
            // Port ermitteln
            var port = _mailbox.LastEndpoint.Split(':').Last();
            if (!UInt16.TryParse(port, NumberStyles.Integer, CultureInfo.InvariantCulture, out _mailboxPort))
            {
                Trace.TraceError("Could not parse ZRE router port '{0}'", port);
                StopInternal();
                return false;
            }

            // Beacon generieren
            _beacon = CreateVersion1Beacon(_uuid, _mailboxPort);

            // UPD receiver erzeugen
            IPEndPoint receiveIp = new IPEndPoint(IPAddress.Any, _zreBroadcastPort);
            _broadcastReceiver = new UdpClient(receiveIp);
            StartReceiveBroadcast(_broadcastReceiver);

            // UDP sender erzeugen
            IPEndPoint broadcastIp = new IPEndPoint(IPAddress.Broadcast, _zreBroadcastPort);
            _broadcastSender = new UdpClient(broadcastIp);

            // Oink
            Started = true;
            return true;
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
        public void StopInternal()
        {
            // Beacon freigeben
            _beacon = null;

            // Sender freigeben
            if (_broadcastSender != null)
            {
                _broadcastSender.Close();
                _broadcastSender = null;
            }

            // Receiver freigeben
            if (_broadcastReceiver != null)
            {
                _broadcastReceiver.Close();
                _broadcastReceiver = null;
            }

            // Router freigeben
            if (_mailbox != null)
            {
                _mailbox.Dispose();
                _mailbox = null;
            }

            // Kontext freigeben
            if (_context != null)
            {
                _context.Dispose();
                _context = null;
            }

            // Oink
            Started = false;
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
            var state = ar.AsyncState as UdpState;
            Debug.Assert(state != null);

            // Receiver ermitteln
            var receiver = state.Client;
            Debug.Assert(receiver != null);
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

            // UUID parsen
            int a = BitConverter.ToInt32(payload, 4);
            short b = BitConverter.ToInt16(payload, 8);
            short c = BitConverter.ToInt16(payload, 10);
            byte d = payload[12],
                e = payload[13], 
                f = payload[14], 
                g = payload[15], 
                h = payload[16], 
                i = payload[17], 
                j = payload[18], 
                k = payload[19];
            var uuid = new Guid(a, b, c, d, e, f, g, h, i, j, k);
            if (uuid == _uuid) return;
            
            // Port beziehen
            ushort port = (ushort)IPAddress.NetworkToHostOrder(BitConverter.ToInt16(payload, 20));

            // Mailbox registrieren
            RegisterPeer(endpoint, uuid, port);
        }

        /// <summary>
        /// Registers the mailbox.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="uuid">The UUID.</param>
        /// <param name="remoteMailboxPort">The remote mailbox port.</param>
        private void RegisterPeer(IPEndPoint endpoint, Guid uuid, ushort remoteMailboxPort)
        {
            Debug.WriteLine("Registering service {0} at mailbox {1} on {2}.", uuid, remoteMailboxPort, endpoint.Address);
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
            IPEndPoint broadcastIp = new IPEndPoint(IPAddress.Broadcast, _zreBroadcastPort);
            UdpState state = new UdpState(broadcastIp, sender);
            sender.BeginSend(beacon, beacon.Length, broadcastIp, EndBroadcastBeacon, state);
        }

        /// <summary>
        /// Beendet eine asynchrone Sendeoperation
        /// </summary>
        /// <param name="ar"></param>
        private void EndBroadcastBeacon(IAsyncResult ar)
        {
            var state = ar.AsyncState as UdpState;
            Debug.Assert(state != null);

            // Sender ermitteln
            var sender = state.Client;
            Debug.Assert(sender != null);
            if (!sender.Client.IsBound) return;

            // Senden beenden
            sender.EndSend(ar);
        }

        /// <summary>
        /// Gets a value indicating whether this <see cref="ZeroMqRealtimeExchange"/> is disposed.
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
            if (!Disposed)
            {
                if (disposing)
                {
                    StopInternal();
                }

                Disposed = true;
                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            Dispose(true);
        }

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
    }
}
