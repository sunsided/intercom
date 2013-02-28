using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
    /// Implementation of the ZeroMQ Realtime Exchange protocol as defined in <a href="http://rfc.zeromq.org/spec:20">http://rfc.zeromq.org/spec:20</a>
    /// </summary>
    sealed class ZreMailbox : IZreMailbox
    {
        /// <summary>
        /// Factory used to create beacon broadcast instances
        /// </summary>
        private readonly IZreBroadcastFactory _broadcastFactory;

        /// <summary>
        /// This node's UUID
        /// </summary>
        private readonly Guid _uuid;

        /// <summary>
        /// Der Kontext
        /// </summary>
        private ZmqContext _context;

        /// <summary>
        /// Der Router-Socket
        /// </summary>
        private ZmqSocket _mailbox;

        /// <summary>
        /// Der Port des Routers
        /// </summary>
        /// <remarks>
        /// Nur gültig, wenn <see cref="_mailbox"/> != <see langword="null"/>.
        /// </remarks>
        private ushort _mailboxPort;

        /// <summary>
        /// Gets a value indicating whether this <see cref="ZreMailbox"/> is started.
        /// </summary>
        /// <value>
        ///   <c>true</c> if started; otherwise, <c>false</c>.
        /// </value>
        public bool Started { get; private set; }

        /// <summary>
        /// The dictionary of peers
        /// </summary>
        private ConcurrentDictionary<Guid, Node> _peers;

        /// <summary>
        /// The cancellation token source
        /// </summary>
        private CancellationTokenSource _cancellationTokenSource;

        /// <summary>
        /// Occurs when a peer was disconnected.
        /// </summary>
        public event EventHandler<PeerEventArgs> PeerDisconnected;

        /// <summary>
        /// The broadcaster
        /// </summary>
        private IZreBroadcast _broadcast;

        /// <summary>
        /// The task used to poll the mailbox
        /// </summary>
        private Task _mailboxPollTask;

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
        /// Initializes a new instance of the <see cref="ZreMailbox"/> class.
        /// </summary>
        /// <param name="broadcastFactory">The broadcast factory.</param>
        /// <param name="uuid">The UUID.</param>
        public ZreMailbox(IZreBroadcastFactory broadcastFactory, Guid? uuid = null)
        {
            _broadcastFactory = broadcastFactory;
            _uuid = uuid ?? Guid.NewGuid();
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

            // Broadcast erzeugen
            Trace.Assert(_broadcastFactory != null);
            _broadcast = _broadcastFactory.Create(_uuid, _mailboxPort);
            _broadcast.PeerDiscovered += OnBroadcastPeerDiscovered;
            _broadcast.Start();

            // Oink
            Started = true;
            return true;
        }

        /// <summary>
        /// Called when a peer was discovered by broadcast.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="peerDiscoveryEventArgs">The <see cref="Intercom.Discovery.PeerDiscoveryEventArgs"/> instance containing the event data.</param>
        private void OnBroadcastPeerDiscovered(object sender, PeerDiscoveryEventArgs peerDiscoveryEventArgs)
        {
            var endpoint = peerDiscoveryEventArgs.Mailbox;
            var uuid = peerDiscoveryEventArgs.Uuid;
            
            RegisterPeer(endpoint, uuid);
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
                    // TODO: Assert message is two frames long (first added by ROUTER), second containing command
                }

                // sleep
                signal.WaitOne();
            }
        }

        /// <summary>
        /// Broadcasts a beacon.
        /// </summary>
        public void BroadcastBeacon()
        {
            var broadcast = _broadcast;
            if (broadcast != null)
            {
                broadcast.BroadcastBeacon();
            }
        }
        
        /// <summary>
        /// Registers the mailbox.
        /// </summary>
        /// <param name="endpoint">The endpoint address.</param>
        /// <param name="uuid">The UUID.</param>
        private void RegisterPeer(IPEndPoint endpoint, Guid uuid)
        {
            Node node = null;
            bool success = false;

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
                 node = new Node(uuid, endpointAddress, dealer);
                if (!_peers.TryAdd(uuid, node))
                {
                    Trace.TraceWarning("Could not register peer because an entry for the same peer already existed; skipping.");
                    return;
                }
                dealer.Connect(endpointAddress);
                success = true;
            }
            catch (Exception e)
            {
                Trace.TraceError("An error occured during peer registration: {0}", e.Message);
                success = false;
            }
            finally
            {
                // clean up the node (the dealer!) in case of an error
                if (!success && node != null)
                {
                    node.Disconnect();
                }
            }

            // Send HELLO
            if (success)
            {
                // Send dummy data ...
                SendMessage(uuid, new byte[] { 1, 2, 3, 4 });
            }
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

                // disconnect
                node.Disconnect();

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
            SendStatus status = node.SendFrame(frame);
            // TODO: check result
            // TODO: What about DontWait?

            return true;
        }

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
                // stop the broadcast
                if (_broadcast != null)
                {
                    _broadcast.PeerDiscovered -= OnBroadcastPeerDiscovered;
                    _broadcast.Stop();
                    _broadcast = null;
                }

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
        /// A remote node
        /// </summary>
        private sealed class Node : IDisposable
        {
            /// <summary>
            /// Gets or sets a value indicating whether this <see cref="Node"/> is disposed.
            /// </summary>
            /// <value>
            ///   <c>true</c> if disposed; otherwise, <c>false</c>.
            /// </value>
            public bool Disposed { get; private set; }

            /// <summary>
            /// The peer's uuid
            /// </summary>
            public Guid Uuid
            {
                get { return _uuid; }
            }

            /// <summary>
            /// Gets a value indicating whether this instance is connected.
            /// </summary>
            /// <value>
            /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
            /// </value>
            public bool IsConnected { get { return _lastSeen.IsRunning; } }

            /// <summary>
            /// The remote Endpoint
            /// </summary>
            private readonly string _endpoint;
            
            /// <summary>
            /// The dealer socket
            /// </summary>
            private readonly ZmqSocket _dealerSocket;

            /// <summary>
            /// The group dictionary
            /// </summary>
            private readonly ConcurrentDictionary<string, int> _groups = new ConcurrentDictionary<string, int>();

            /// <summary>
            /// Information about when the peer was last seen on the network
            /// </summary>
            private readonly Stopwatch _lastSeen = Stopwatch.StartNew();

            /// <summary>
            /// The node's uuid
            /// </summary>
            private readonly Guid _uuid;

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
            public Node(Guid uuid, string endpoint, ZmqSocket dealerSocket)
            {
                _uuid = uuid;
                _endpoint = endpoint;
                _dealerSocket = dealerSocket;
            }

            /// <summary>
            /// Sends a frame to the peer.
            /// </summary>
            /// <param name="frame">The frame.</param>
            /// <param name="timeout">The timeout.</param>
            /// <returns></returns>
            public SendStatus SendFrame(Frame frame, TimeSpan? timeout = null)
            {
                try
                {
                    return IsConnected
                               ? _dealerSocket.SendFrame(frame, timeout ?? TimeSpan.MaxValue)
                               : SendStatus.None;
                }
                catch(ObjectDisposedException)
                {
                    return SendStatus.None;
                }
            }

            /// <summary>
            /// Disconnects this instance.
            /// </summary>
            public void Disconnect()
            {
                if (_lastSeen.IsRunning)
                {
                    _lastSeen.Stop();

                    _dealerSocket.Disconnect(_endpoint);
                    _dealerSocket.Close();
                    _dealerSocket.Dispose();
                }
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

            /// <summary>
            /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
            /// </summary>
            /// <filterpriority>2</filterpriority>
            void IDisposable.Dispose()
            {
                if (Disposed) return;
                Disconnect();
                Disposed = true;
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
