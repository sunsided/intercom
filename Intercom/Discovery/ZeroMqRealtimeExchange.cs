using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
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
        /// The multicast IP to be used
        /// </summary>
        private readonly IPAddress _multicastIp = IPAddress.Parse("224.0.0.0");

        /// <summary>
        /// Task zur Mailbox-Verarbeitung
        /// </summary>
        private Task _mailboxProcessTask;

        /// <summary>
        /// The mailbox inbound queue
        /// </summary>
        private ConcurrentQueue<ZmqMessage> _mailboxMessageQueue;

        /// <summary>
        /// Signals that data is available in the inbound queue
        /// </summary>
        private AutoResetEvent _dataAvailable;

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
            _mailboxMessageQueue = new ConcurrentQueue<ZmqMessage>();
            _dataAvailable = new AutoResetEvent(false);

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
            _mailboxPollTask = Task.Factory.StartNew(MailboxPollTask, new ConsumerTaskState(_mailbox, _cancellationTokenSource.Token, _dataAvailable, _mailboxMessageQueue), TaskCreationOptions.LongRunning);
            _mailboxProcessTask = Task.Factory.StartNew(MailboxProcessTask, new ConsumerTaskState(null, _cancellationTokenSource.Token, _dataAvailable, _mailboxMessageQueue), TaskCreationOptions.LongRunning);

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

            // Oink
            Started = true;
            return true;
        }

        /// <summary>
        /// Polls the mailbox
        /// </summary>
        private static void MailboxPollTask(object state)
        {
            var cts = (ConsumerTaskState)state;
            var cancellationToken = cts.CancellationToken;
            var socket = cts.RouterSocket;
            var signal = cts.Signal;
            var queue = cts.Queue;

            if (String.IsNullOrWhiteSpace(Thread.CurrentThread.Name))
            {
                Thread.CurrentThread.Name = "ZRE Mailbox Receiver";
            }

            TimeSpan timeout = TimeSpan.FromSeconds(1);
            while (!cancellationToken.IsCancellationRequested)
            {
                try 
                {
                    ZmqMessage message = socket.ReceiveMessage(timeout);
                    if (socket.ReceiveStatus == ReceiveStatus.TryAgain)
                    {
                        continue;
                    }

                    // TODO: Handle interrupted (partial) messages.

                    // Enqueue message and wake up consumer
                    Debug.Assert(message != null, "ZeroMQ message was null after ReceiveMessage()");
                    queue.Enqueue(message);
                    signal.Set();
                }
                catch (ObjectDisposedException) { }
                catch (SocketException) { }
            }
        }

        /// <summary>
        /// Processes mailbox events
        /// </summary>
        /// <param name="state"></param>
        private static void MailboxProcessTask(object state)
        {
            var cts = (ConsumerTaskState)state;
            var cancellationToken = cts.CancellationToken;
            var signal = cts.Signal;
            var queue = cts.Queue;

            if (String.IsNullOrWhiteSpace(Thread.CurrentThread.Name))
            {
                Thread.CurrentThread.Name = "ZRE Mailbox Message Processing";
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                ZmqMessage message;
                while (queue.TryDequeue(out message))
                {
                    // process message
                }

                // sleep
                signal.WaitOne();
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
            var uuid = ParseGuidFromBytes(payload, 4);
            if (uuid == _uuid) return;
            
            // Port beziehen
            var port = (ushort)IPAddress.NetworkToHostOrder(BitConverter.ToInt16(payload, 20));

            // Mailbox registrieren
            var maildboxEndpoint = new IPEndPoint(endpoint.Address, port);
            RegisterPeer(maildboxEndpoint, uuid);
        }

        /// <summary>
        /// Parses an UUID from a byte sequence
        /// </summary>
        /// <param name="payload">The payload.</param>
        /// <param name="startIndex">The start index.</param>
        /// <returns></returns>
        private static Guid ParseGuidFromBytes(byte[] payload, int startIndex)
        {
            if (payload == null) throw new ArgumentNullException("payload");
            if (payload.Length - startIndex < 16) throw new ArgumentException("Payload must be at least 16 bytes long");

            int a = BitConverter.ToInt32(payload, startIndex);
            short b = BitConverter.ToInt16(payload, startIndex + 4 );
            short c = BitConverter.ToInt16(payload, startIndex + 6);
            byte d = payload[startIndex + 8 ],
                 e = payload[startIndex + 9],
                 f = payload[startIndex + 10],
                 g = payload[startIndex + 11],
                 h = payload[startIndex + 12],
                 i = payload[startIndex + 13],
                 j = payload[startIndex + 14],
                 k = payload[startIndex + 15];
            return new Guid(a, b, c, d, e, f, g, h, i, j, k);
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
            var message = new ZmqMessage();
            message.Append(new Frame(payload));

            // send the datagram
            var socket = node.DealerSocket;
            SendStatus status = socket.SendMessage(message);
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
        /// Stops this instance.
        /// </summary>
        public void StopInternal()
        {
            try
            {
                // Signal stop and wake up consumer
                if (_cancellationTokenSource != null)
                {
                    _cancellationTokenSource.Cancel();
                }
                if (_dataAvailable != null)
                {
                    _dataAvailable.Set();
                }

                // Release poll task
                if (_mailboxPollTask != null)
                {
                    _mailboxPollTask.Wait(TimeSpan.FromSeconds(5));
                    _mailboxPollTask.Dispose();
                    _mailboxPollTask = null;
                }

                // Release poll task
                if (_mailboxProcessTask != null)
                {
                    _mailboxProcessTask.Wait(TimeSpan.FromSeconds(5));
                    _mailboxProcessTask.Dispose();
                    _mailboxProcessTask = null;
                }

                // Release signal
                if (_dataAvailable != null)
                {
                    _dataAvailable.Dispose();
                    _dataAvailable = null;
                }

                // Release token source
                if (_cancellationTokenSource != null)
                {
                    _cancellationTokenSource.Dispose();
                    _cancellationTokenSource = null;
                }

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
                {
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
                }

                // Release inbound queue
                if (_mailboxMessageQueue != null)
                {
                    ZmqMessage discarded;
                    while (_mailboxMessageQueue.TryDequeue(out discarded)) {}
                    _mailboxMessageQueue = null;
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

        /// <summary>
        /// A remote node
        /// </summary>
        private sealed class Node
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
            /// The group dictionary
            /// </summary>
            private readonly ConcurrentDictionary<string, int> _groups = new ConcurrentDictionary<string, int>();

            /// <summary>
            /// Information about when the peer was last seen on the network
            /// </summary>
            private readonly Stopwatch _lastSeen = Stopwatch.StartNew();

            /// <summary>
            /// Gets the duration since the peer was last seen on the network.
            /// </summary>
            /// <seealso cref="MarkAlive"/>
            public TimeSpan TimeSinceLastSeen
            {
                get { return _lastSeen.Elapsed; }
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="T:System.Object"/> class.
            /// </summary>
            public Node(string endpoint, ZmqSocket dealerSocket)
            {
                Endpoint = endpoint;
                DealerSocket = dealerSocket;
            }

            /// <summary>
            /// Marks this peer as alive.
            /// </summary>
            /// <seealso cref="TimeSinceLastSeen"/>
            public void MarkAlive()
            {
                _lastSeen.Restart();
            }

            /// <summary>
            /// Gets the number of groups the peer is in.
            /// </summary>
            public int GroupCount
            {
                get { return _groups.Count; }
            }

            /// <summary>
            /// Enumerates the groups the peer is in.
            /// </summary>
            public IEnumerable<string> Groups
            {
                get { return _groups.Select(@group => @group.Key); }
            }

            /// <summary>
            /// Join a group
            /// </summary>
            /// <param name="group">The group to join</param>
            /// <param name="statusSequence">The status sequence</param>
            public void JoinGroup(string group, int statusSequence)
            {
                int sequence = _groups.AddOrUpdate(group, s => statusSequence, (s, i) => i + 1);
                if (statusSequence != sequence + 1)
                {
                    Trace.TraceWarning("Group status sequence diverging after JOIN.");
                }
            }

            /// <summary>
            /// Leave a group
            /// </summary>
            /// <param name="group">The group to leave</param>
            /// <param name="statusSequence">The status sequence</param>
            public void LeaveGroup(string group, int statusSequence)
            {
                int sequence;
                if (_groups.TryRemove(group, out sequence))
                {
                    if (statusSequence != sequence + 1)
                    {
                        Trace.TraceWarning("Group status sequence diverging after LEAVE.");
                    }
                }
            }
        }

        /// <summary>
        /// State for the consumer task
        /// </summary>
        private struct ConsumerTaskState
        {
            /// <summary>
            /// The message queue
            /// </summary>
            public readonly ConcurrentQueue<ZmqMessage> Queue;

            /// <summary>
            /// The router socket
            /// </summary>
            public readonly ZmqSocket RouterSocket;

            /// <summary>
            /// The cancellation token
            /// </summary>
            public readonly CancellationToken CancellationToken;

            /// <summary>
            /// The signal to notify new inbound data
            /// </summary>
            public readonly AutoResetEvent Signal;

            /// <summary>
            /// Initializes a new instance of the <see cref="ConsumerTaskState"/> struct.
            /// </summary>
            /// <param name="routerSocket">The router socket.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <param name="signal">The signal.</param>
            /// <param name="queue">The queue.</param>
            public ConsumerTaskState(ZmqSocket routerSocket, CancellationToken cancellationToken, AutoResetEvent signal, ConcurrentQueue<ZmqMessage> queue)
            {
                Queue = queue;
                RouterSocket = routerSocket;
                CancellationToken = cancellationToken;
                Signal = signal;
            }
        }

        #endregion Internal state variables
    }
}
