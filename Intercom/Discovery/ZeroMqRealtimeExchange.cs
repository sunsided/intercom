using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;
using SocketFlags = ZeroMQ.SocketFlags;
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
        /// The dictionary of peers
        /// </summary>
        private ConcurrentDictionary<Guid, Node> _peers;

        /// <summary>
        /// The cancellation token source
        /// </summary>
        private CancellationTokenSource _cancellationTokenSource;

        /// <summary>
        /// Occurs when a peer was discovered.
        /// </summary>
        /// <remarks>
        /// Peer discovery does not imply a successful connection.
        /// </remarks>
        public event EventHandler<PeerEventArgs> PeerDiscovered;

        /// <summary>
        /// Occurs when a peer was disconnected.
        /// </summary>
        public event EventHandler<PeerEventArgs> PeerDisconnected;

        /// <summary>
        /// The task used to poll the mailbox
        /// </summary>
        private Task _mailboxPollTask;

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

            // Prepare peer asynchronous operation
            _cancellationTokenSource = new CancellationTokenSource();
            _peers = new ConcurrentDictionary<Guid, Node>();

            // Create the context
            _context = ZmqContext.Create();

            // Create the mailbox router and connect it
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

            // Start mailbox processing task
            _mailboxPollTask = Task.Factory.StartNew(PollMailbox, new ConsumerTaskState(_mailbox, _cancellationTokenSource.Token));

            // Beacon generieren
            _beacon = CreateVersion1Beacon(_uuid, _mailboxPort);

            // UPD receiver erzeugen
            var receiveIp = new IPEndPoint(IPAddress.Any, _zreBroadcastPort);
            _broadcastReceiver = new UdpClient(receiveIp);
            StartReceiveBroadcast(_broadcastReceiver);

            // UDP sender erzeugen
            _broadcastSender = new UdpClient();

            // Oink
            Started = true;
            return true;
        }

        /// <summary>
        /// Polls the mailbox
        /// </summary>
        private void PollMailbox(object state)
        {
            var cts = (ConsumerTaskState)state;
            var cancellationToken = cts.CancellationToken;
            var socket = cts.RouterSocket;

            TimeSpan timeout = TimeSpan.FromSeconds(1);
            while (!cancellationToken.IsCancellationRequested)
            {
                try 
                {
                    Frame frame = socket.ReceiveFrame(timeout);
                    if (frame.ReceiveStatus == ReceiveStatus.TryAgain) continue;

                    // TODO: While more frames, loop
                }
                catch (ObjectDisposedException) { }
                catch (SocketException) { }
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
        public void StopInternal()
        {
            try
            {
                // Stoppity stop
                _cancellationTokenSource.Cancel();
                _mailboxPollTask.Wait(TimeSpan.FromSeconds(5));

                // Release the Kraken
                _beacon = null;

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

                // Release mailbox router
                if (_mailbox != null)
                {
                    _mailbox.Close();
                    _mailbox.Dispose();
                    _mailbox = null;
                }

                // Release peers
                var peers = _peers;
                if (peers != null)
                {
                    foreach (var kvp in peers)
                    {
                        DisconnectPeer(kvp.Key);
                    }
                    peers.Clear();
                    _peers = null;
                }

                // Lose context
                if (_context != null)
                {
                    _context.Terminate();
                    _context.Dispose();
                    _context = null;
                }
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
            var port = (ushort)IPAddress.NetworkToHostOrder(BitConverter.ToInt16(payload, 20));

            // Mailbox registrieren
            var maildboxEndpoint = new IPEndPoint(endpoint.Address, port);
            RegisterPeer(maildboxEndpoint, uuid);
        }

        /// <summary>
        /// Registers the mailbox.
        /// </summary>
        /// <param name="endpoint">The endpoint address.</param>
        /// <param name="uuid">The UUID.</param>
        private void RegisterPeer(IPEndPoint endpoint, Guid uuid)
        {
            try
            {
                var endpointAddress = String.Format("tcp://{0}:{1}", endpoint.Address, endpoint.Port);
                Debug.WriteLine("Registering service {0} at mailbox {1}.", uuid, endpointAddress);

                // Create the dealer
                var context = _context;
                if (context == null) return;
                var dealer = context.CreateSocket(SocketType.DEALER);
                dealer.Identity = uuid.ToByteArray();
                // TODO: dealer.ReceiveHighWatermark = x
                // TODO: dealer.SendHighWatermark = x
                // TODO: dealer.SendTimeout = x
                // TODO: dealer.ReceiveTimeout = x

                // Check the peers dictionary
                var peers = _peers;
                if (peers == null) return;

                // Register and connect the dealer
                var node = new Node(endpointAddress, dealer);
                _peers.TryAdd(uuid, node);
                dealer.Connect(endpointAddress);
            }
            catch (Exception e)
            {
                Trace.TraceError("An error occured during peer registration: {0}", e.Message);
                return;
            }

            // Send event
            OnPeerDiscovered(new PeerEventArgs(uuid));

            // Send dummy data ...
            // TODO: Send Frames instead?
            SendMessage(uuid, new byte[] {1, 2, 3, 4});
        }

        /// <summary>
        /// Disconnects a peer
        /// </summary>
        /// <param name="uuid">The peer's GUID</param>
        public void DisconnectPeer(Guid uuid)
        {
            try
            {
                var peers = _peers;
                if (peers == null) return;

                Node node;
                if (!peers.TryRemove(uuid, out node)) return;

                var endpoint = node.Endpoint;
                var socket = node.DealerSocket;

                // disconnect
                socket.Disconnect(endpoint);
                socket.Close();
                socket.Dispose();

                // send event
                OnPeerDisconnected(new PeerEventArgs(uuid));
            }
            catch (ObjectDisposedException) {}
        }

        /// <summary>
        /// Sends a message to a peer
        /// </summary>
        /// <param name="uuid">The peer's UUID</param>
        /// <param name="payload">The datagram to send</param>
        /// <returns><see langword="true"/> in case of success; <see langword="false"/> otherwise</returns>
        public bool SendMessage(Guid uuid, byte[] payload)
        {
            Node node;
            if (!_peers.TryGetValue(uuid, out node)) return false;

            // create the datagram
            var frame = new Frame(payload);

            // send the datagram
            var socket = node.DealerSocket;
            SendStatus status = socket.SendFrame(frame, TimeSpan.FromSeconds(0.1)); 
            // TODO: check result
            // TODO: What about DontWait?

            return true;
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
            }
            catch (ObjectDisposedException) { }
            catch (SocketException) { }
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

        /// <summary>
        /// Raises the <see cref="PeerDiscovered"/> event.
        /// </summary>
        /// <param name="e">The <see cref="Intercom.Discovery.PeerEventArgs"/> instance containing the event data.</param>
        private void OnPeerDiscovered(PeerEventArgs e)
        {
            EventHandler<PeerEventArgs> handler = PeerDiscovered;
            if (handler != null)
            {
                handler(this, e);
            }
        }

        /// <summary>
        /// Raises the <see cref="PeerDisconnected"/> event.
        /// </summary>
        /// <param name="e">The <see cref="Intercom.Discovery.PeerEventArgs"/> instance containing the event data.</param>
        private void OnPeerDisconnected(PeerEventArgs e)
        {
            EventHandler<PeerEventArgs> handler = PeerDisconnected;
            if (handler != null)
            {
                handler(this, e);
            }
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

        /// <summary>
        /// A remote node
        /// </summary>
        private struct Node
        {
            /// <summary>
            /// The remote Endpoint
            /// </summary>
            public readonly string Endpoint;
            
            /// <summary>
            /// The dealer socket
            /// </summary>
            public readonly ZmqSocket DealerSocket;

            /// <summary>
            /// Initializes a new instance of the <see cref="T:System.Object"/> class.
            /// </summary>
            public Node(string endpoint, ZmqSocket dealerSocket)
            {
                Endpoint = endpoint;
                DealerSocket = dealerSocket;
            }
        }

        /// <summary>
        /// State for the consumer task
        /// </summary>
        private struct ConsumerTaskState
        {
            /// <summary>
            /// The router socket
            /// </summary>
            public ZmqSocket RouterSocket;

            /// <summary>
            /// The cancellation token
            /// </summary>
            public CancellationToken CancellationToken;

            /// <summary>
            /// Initializes a new instance of the <see cref="ConsumerTaskState"/> struct.
            /// </summary>
            /// <param name="routerSocket">The router socket.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            public ConsumerTaskState(ZmqSocket routerSocket, CancellationToken cancellationToken)
            {
                RouterSocket = routerSocket;
                CancellationToken = cancellationToken;
            }
        }
    }
}
