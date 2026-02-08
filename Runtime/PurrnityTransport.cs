// NetSim Implementation compilation boilerplate
// All references to UNITY_MP_TOOLS_NETSIM_IMPLEMENTATION_ENABLED should be defined in the same way,
// as any discrepancies are likely to result in build failures
#if UNITY_EDITOR || (DEVELOPMENT_BUILD && !UNITY_MP_TOOLS_NETSIM_DISABLED_IN_DEVELOP) || (!DEVELOPMENT_BUILD && UNITY_MP_TOOLS_NETSIM_ENABLED_IN_RELEASE)
#define UNITY_MP_TOOLS_NETSIM_IMPLEMENTATION_ENABLED
#endif

using System;
using System.Collections.Generic;
using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;
using PurrNet.Transports;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using Unity.Networking.Transport.Relay;
using Unity.Networking.Transport.TLS;
using Unity.Networking.Transport.Utilities;
using UnityEngine;

using TransportError = Unity.Networking.Transport.Error.StatusCode;
using TransportEvent = Unity.Networking.Transport.NetworkEvent.Type;

namespace PurrNet.Purrnity
{
    /// <summary>
    /// The Netcode for GameObjects NetworkTransport for PurrnityTransport.
    /// Note: This is highly recommended to use over UNet.
    /// </summary>
    [AddComponentMenu("PurrNet/Purrnity Transport")]
    public class PurrnityTransport : GenericTransport, ITransport, INetworkStreamDriverConstructor
    {
        /// <summary>
        /// Enum type stating the type of protocol
        /// </summary>
        public enum ProtocolType
        {
            /// <summary>
            /// Unity Transport Protocol
            /// </summary>
            UnityTransport,
            /// <summary>
            /// Unity Transport Protocol over Relay
            /// </summary>
            RelayUnityTransport,
        }

        // --- ITransport events ---
        public event OnConnected onConnected;
        public event OnDisconnected onDisconnected;
        public event OnDataReceived onDataReceived;
        public event OnDataSent onDataSent;
        public event OnConnectionState onConnectionState;

        // --- ITransport properties ---
        public ConnectionState listenerState { get; private set; } = ConnectionState.Disconnected;
        public ConnectionState clientState { get; private set; } = ConnectionState.Disconnected;

        private readonly List<Connection> _connections = new List<Connection>();
        private readonly Dictionary<int, ulong> _connToClientId = new Dictionary<int, ulong>();
        public IReadOnlyList<Connection> connections => _connections;

        public override bool isSupported => true;
        public override ITransport transport => this;

        /// <summary>
        /// The default maximum (receive) packet queue size
        /// </summary>
        public const int InitialMaxPacketQueueSize = 128;

        /// <summary>
        /// The default maximum payload size
        /// </summary>
        public const int InitialMaxPayloadSize = 6 * 1024;

        /// <summary>
        /// The default maximum send queue size
        /// </summary>
        [Obsolete("MaxSendQueueSize is now determined dynamically (can still be set programmatically using the MaxSendQueueSize property). This initial value is not used anymore.", false)]
        public const int InitialMaxSendQueueSize = 16 * InitialMaxPayloadSize;

        // Maximum reliable throughput, assuming the full reliable window can be sent on every
        // frame at 60 FPS. This will be a large over-estimation in any realistic scenario.
        private const int k_MaxReliableThroughput = (NetworkParameterConstants.MTU * 64 * 60) / 1000; // bytes per millisecond

        private static ConnectionAddressData s_DefaultConnectionAddressData = new ConnectionAddressData { Address = "127.0.0.1", Port = 7777, ServerListenAddress = string.Empty };

#pragma warning disable IDE1006 // Naming Styles
        /// <summary>
        /// An instance of a <see cref="INetworkStreamDriverConstructor"/> implementation. If null,
        /// the default driver constructor will be used. Setting it to a non-null value allows
        /// controlling how the internal <see cref="NetworkDriver"/> instance is created. See the
        /// interface's documentation for details.
        /// </summary>
        public static INetworkStreamDriverConstructor s_DriverConstructor;
#pragma warning restore IDE1006 // Naming Styles

        /// <summary>
        /// If a custom <see cref="INetworkStreamDriverConstructor"/> implementation is in use (see
        /// <see cref="s_DriverConstructor"/>), this returns it. Otherwise it returns the current
        /// <see cref="PurrnityTransport"/> instance, which acts as the default constructor.
        /// </summary>
        public INetworkStreamDriverConstructor DriverConstructor => s_DriverConstructor ?? this;

        [Tooltip("Which protocol should be selected (Relay/Non-Relay).")]
        [SerializeField]
        private ProtocolType m_ProtocolType;

        [Tooltip("Per default the client/server will communicate over UDP. Set to true to communicate with WebSocket.")]
        [SerializeField]
        private bool m_UseWebSockets = false;

        /// <summary>Whether to use WebSockets as the protocol of communication. Default is UDP.</summary>
        public bool UseWebSockets
        {
            get => m_UseWebSockets;
            set => m_UseWebSockets = value;
        }

        [Tooltip("Per default the client/server communication will not be encrypted. Select true to enable DTLS for UDP and TLS for Websocket.")]
        [SerializeField]
        private bool m_UseEncryption = false;

        /// <summary>
        /// Whether to use encryption (default is false). Note that unless using Unity Relay, encryption requires
        /// providing certificate information with <see cref="SetClientSecrets"/> and <see cref="SetServerSecrets"/>.
        /// </summary>
        public bool UseEncryption
        {
            get => m_UseEncryption;
            set => m_UseEncryption = value;
        }

        [Tooltip("The maximum amount of packets that can be in the internal send/receive queues. Basically this is how many packets can be sent/received in a single update/frame.")]
        [SerializeField]
        private int m_MaxPacketQueueSize = InitialMaxPacketQueueSize;

        /// <summary>The maximum amount of packets that can be in the internal send/receive queues.</summary>
        /// <remarks>Basically this is how many packets can be sent/received in a single update/frame.</remarks>
        public int MaxPacketQueueSize
        {
            get => m_MaxPacketQueueSize;
            set => m_MaxPacketQueueSize = value;
        }

        [Tooltip("The maximum size of an unreliable payload that can be handled by the transport.")]
        [SerializeField]
        private int m_MaxPayloadSize = InitialMaxPayloadSize;

        /// <summary>The maximum size of an unreliable payload that can be handled by the transport.</summary>
        public int MaxPayloadSize
        {
            get => m_MaxPayloadSize;
            set => m_MaxPayloadSize = value;
        }

        private int m_MaxSendQueueSize = 0;

        /// <summary>The maximum size in bytes of the transport send queue.</summary>
        /// <remarks>
        /// The send queue accumulates messages for batching and stores messages when other internal
        /// send queues are full. Note that there should not be any need to set this value manually
        /// since the send queue size is dynamically sized based on need.
        ///
        /// This value should only be set if you have particular requirements (e.g. if you want to
        /// limit the memory usage of the send queues). Note however that setting this value too low
        /// can easily lead to disconnections under heavy traffic.
        /// </remarks>
        public int MaxSendQueueSize
        {
            get => m_MaxSendQueueSize;
            set => m_MaxSendQueueSize = value;
        }

        [Tooltip("Timeout in milliseconds after which a heartbeat is sent if there is no activity.")]
        [SerializeField]
        private int m_HeartbeatTimeoutMS = NetworkParameterConstants.HeartbeatTimeoutMS;

        /// <summary>Timeout in milliseconds after which a heartbeat is sent if there is no activity.</summary>
        public int HeartbeatTimeoutMS
        {
            get => m_HeartbeatTimeoutMS;
            set => m_HeartbeatTimeoutMS = value;
        }

        [Tooltip("Timeout in milliseconds indicating how long we will wait until we send a new connection attempt.")]
        [SerializeField]
        private int m_ConnectTimeoutMS = NetworkParameterConstants.ConnectTimeoutMS;

        /// <summary>
        /// Timeout in milliseconds indicating how long we will wait until we send a new connection attempt.
        /// </summary>
        public int ConnectTimeoutMS
        {
            get => m_ConnectTimeoutMS;
            set => m_ConnectTimeoutMS = value;
        }

        [Tooltip("The maximum amount of connection attempts we will try before disconnecting.")]
        [SerializeField]
        private int m_MaxConnectAttempts = NetworkParameterConstants.MaxConnectAttempts;

        /// <summary>The maximum amount of connection attempts we will try before disconnecting.</summary>
        public int MaxConnectAttempts
        {
            get => m_MaxConnectAttempts;
            set => m_MaxConnectAttempts = value;
        }

        [Tooltip("Inactivity timeout after which a connection will be disconnected. The connection needs to receive data from the connected endpoint within this timeout. Note that with heartbeats enabled, simply not sending any data will not be enough to trigger this timeout (since heartbeats count as connection events).")]
        [SerializeField]
        private int m_DisconnectTimeoutMS = NetworkParameterConstants.DisconnectTimeoutMS;

        /// <summary>Inactivity timeout after which a connection will be disconnected.</summary>
        /// <remarks>
        /// The connection needs to receive data from the connected endpoint within this timeout.
        /// Note that with heartbeats enabled, simply not sending any data will not be enough to
        /// trigger this timeout (since heartbeats count as connection events).
        /// </remarks>
        public int DisconnectTimeoutMS
        {
            get => m_DisconnectTimeoutMS;
            set => m_DisconnectTimeoutMS = value;
        }

        /// <summary>
        /// Structure to store the address to connect to
        /// </summary>
        [Serializable]
        public struct ConnectionAddressData
        {
            /// <summary>
            /// IP address of the server (address to which clients will connect to).
            /// </summary>
            [Tooltip("IP address of the server (address to which clients will connect to).")]
            [SerializeField]
            public string Address;

            /// <summary>
            /// UDP port of the server.
            /// </summary>
            [Tooltip("UDP port of the server.")]
            [SerializeField]
            public ushort Port;

            /// <summary>
            /// IP address the server will listen on. If not provided, will use localhost.
            /// </summary>
            [Tooltip("IP address the server will listen on. If not provided, will use localhost.")]
            [SerializeField]
            public string ServerListenAddress;

            internal static NetworkEndpoint ParseNetworkEndpoint(string ip, ushort port)
            {
                NetworkEndpoint endpoint = default;
                if (!NetworkEndpoint.TryParse(ip, port, out endpoint, NetworkFamily.Ipv4))
                {
                    NetworkEndpoint.TryParse(ip, port, out endpoint, NetworkFamily.Ipv6);
                }
                return endpoint;
            }

            /// <summary>
            /// The port the client will bind to. If 0 (the default), an ephemeral port will be used.
            /// </summary>
            [SerializeField]
            public ushort ClientBindPort;

            /// <summary>
            /// Endpoint (IP address and port) clients will connect to.
            /// </summary>
            /// <remarks>
            /// If a DNS hostname was set as the address, this will return an invalid endpoint. This
            /// is still handled correctly by NGO, but for this reason usage of this property is
            /// discouraged.
            /// </remarks>
            [Obsolete("Use NetworkEndpoint.Parse on the Address field instead.")]
            public NetworkEndpoint ServerEndPoint => ParseNetworkEndpoint(Address, Port);

            /// <summary>
            /// Endpoint (IP address and port) server will listen/bind on.
            /// </summary>
            public NetworkEndpoint ListenEndPoint
            {
                get
                {
                    NetworkEndpoint endpoint = default;
                    if (string.IsNullOrEmpty(ServerListenAddress))
                    {
                        endpoint = IsIpv6 ? NetworkEndpoint.LoopbackIpv6 : NetworkEndpoint.LoopbackIpv4;
                        endpoint = endpoint.WithPort(Port);
                    }
                    else
                    {
                        endpoint = ParseNetworkEndpoint(ServerListenAddress, Port);
                        if (endpoint == default)
                        {
                            Debug.LogError($"Invalid listen endpoint: {ServerListenAddress}:{Port}. Note that the listen endpoint MUST be an IP address (not a hostname).");
                        }
                    }
                    return endpoint;
                }
            }

            /// <summary>
            /// Returns true if the end point address is of type <see cref="NetworkFamily.Ipv6"/> or
            /// if it is a hostname (because in current versions of the engine, hostname resolution
            /// prioritizes IPv6 addresses).
            /// </summary>
            public bool IsIpv6 => !string.IsNullOrEmpty(Address) && !NetworkEndpoint.TryParse(Address, Port, out NetworkEndpoint _, NetworkFamily.Ipv4);
        }


        /// <summary>
        /// The connection (address) data for this <see cref="PurrnityTransport"/> instance.
        /// This is where you can change IP Address, Port, or server's listen address.
        /// <see cref="ConnectionAddressData"/>
        /// </summary>
        public ConnectionAddressData ConnectionData = s_DefaultConnectionAddressData;

        /// <summary>
        /// Parameters for the Network Simulator
        /// </summary>
        [Serializable]
        public struct SimulatorParameters
        {
            /// <summary>
            /// Delay to add to every send and received packet (in milliseconds). Only applies in the editor and in development builds. The value is ignored in production builds.
            /// </summary>
            [Tooltip("Delay to add to every send and received packet (in milliseconds). Only applies in the editor and in development builds. The value is ignored in production builds.")]
            [SerializeField]
            public int PacketDelayMS;

            /// <summary>
            /// Jitter (random variation) to add/substract to the packet delay (in milliseconds). Only applies in the editor and in development builds. The value is ignored in production builds.
            /// </summary>
            [Tooltip("Jitter (random variation) to add/substract to the packet delay (in milliseconds). Only applies in the editor and in development builds. The value is ignored in production builds.")]
            [SerializeField]
            public int PacketJitterMS;

            /// <summary>
            /// Percentage of sent and received packets to drop. Only applies in the editor and in the editor and in developments builds.
            /// </summary>
            [Tooltip("Percentage of sent and received packets to drop. Only applies in the editor and in the editor and in developments builds.")]
            [SerializeField]
            public int PacketDropRate;
        }

        /// <summary>
        /// Can be used to simulate poor network conditions such as:
        /// - packet delay/latency
        /// - packet jitter (variances in latency, see: https://en.wikipedia.org/wiki/Jitter)
        /// - packet drop rate (packet loss)
        /// </summary>

        [Obsolete("DebugSimulator is no longer supported and has no effect. Use Network Simulator from the Multiplayer Tools package.", false)]
        [HideInInspector]
        public SimulatorParameters DebugSimulator = new SimulatorParameters
        {
            PacketDelayMS = 0,
            PacketJitterMS = 0,
            PacketDropRate = 0
        };

        internal uint? DebugSimulatorRandomSeed { get; set; } = null;

        private struct PacketLossCache
        {
            public int PacketsReceived;
            public int PacketsDropped;
            public float PacketLoss;
        };

#if UNITY_6000_2_OR_NEWER
        internal static event Action<EntityId, NetworkDriver> OnDriverInitialized;
        internal static event Action<EntityId> OnDisposingDriver;
#endif
        internal static event Action<int, NetworkDriver> TransportInitialized;
        internal static event Action<int> TransportDisposed;

        /// <summary>
        /// Provides access to the <see cref="NetworkDriver"/> for this instance.
        /// </summary>
        protected NetworkDriver m_Driver;

        /// <summary>
        /// Gets a reference to the <see cref="NetworkDriver"/>.
        /// </summary>
        /// <returns>ref <see cref="NetworkDriver"/></returns>
        public ref NetworkDriver GetNetworkDriver()
        {
            return ref m_Driver;
        }

        /// <summary>
        /// Gets the local sytem's <see cref="NetworkEndpoint"/> that is assigned for the current network session.
        /// </summary>
        /// <remarks>
        /// If the driver is not created it will return an invalid <see cref="NetworkEndpoint"/>.
        /// </remarks>
        /// <returns><see cref="NetworkEndpoint"/></returns>
        public NetworkEndpoint GetLocalEndpoint()
        {
            if (m_Driver.IsCreated)
            {
                return m_Driver.GetLocalEndpoint();
            }
            return new NetworkEndpoint();
        }

        private PacketLossCache m_PacketLossCache = new PacketLossCache();

        private ulong m_ServerClientId;

        private NetworkPipeline m_UnreliableFragmentedPipeline;
        private NetworkPipeline m_UnreliableSequencedFragmentedPipeline;
        private NetworkPipeline m_ReliableSequencedPipeline;

        /// <summary>
        /// The current ProtocolType used by the transport
        /// </summary>
        public ProtocolType Protocol => m_ProtocolType;

        private RelayServerData m_RelayServerData;

        /// <summary>
        /// SendQueue dictionary is used to batch events instead of sending them immediately.
        /// </summary>
        private readonly Dictionary<SendTarget, BatchedSendQueue> m_SendQueue = new Dictionary<SendTarget, BatchedSendQueue>();

        // Since reliable messages may be spread out over multiple transport payloads, it's possible
        // to receive only parts of a message in an update. We thus keep the reliable receive queues
        // around to avoid losing partial messages.
        private readonly Dictionary<ulong, BatchedReceiveQueue> m_ReliableReceiveQueues = new Dictionary<ulong, BatchedReceiveQueue>();

        private void InitDriver()
        {
            DriverConstructor.CreateDriver(
                this,
                out m_Driver,
                out m_UnreliableFragmentedPipeline,
                out m_UnreliableSequencedFragmentedPipeline,
                out m_ReliableSequencedPipeline);
#if UNITY_6000_2_OR_NEWER
            var entityId = GetEntityId();
            OnDriverInitialized?.Invoke(entityId, m_Driver);
            TransportInitialized?.Invoke(entityId.GetHashCode(), m_Driver);
#else
            TransportInitialized?.Invoke(GetInstanceID(), m_Driver);
#endif
        }

        private void DisposeInternals()
        {
            if (m_Driver.IsCreated)
            {
                m_Driver.Dispose();
            }

            foreach (var queue in m_SendQueue.Values)
            {
                queue.Dispose();
            }

            m_SendQueue.Clear();

#if UNITY_6000_2_OR_NEWER
            var entityId = GetEntityId();
            OnDisposingDriver?.Invoke(entityId);
            TransportDisposed?.Invoke(entityId.GetHashCode());
#else
            TransportDisposed?.Invoke(GetInstanceID());
#endif
        }

        /// <summary>
        /// Get the <see cref="NetworkSettings"/> that will be used to create the underlying
        /// <see cref="NetworkDriver"/> based on the current configuration of the transport. This
        /// method is meant to be used with custom <see cref="INetworkStreamDriverConstructor"/>
        /// implementations, where it can be used to modify the default parameters instead of
        /// attempting to recreate them from scratch, which could be error-prone.
        /// </summary>
        /// <remarks>
        /// The returned object is allocated using the <see cref="Allocator.Temp"/> allocator.
        /// Do not hold a reference to it for longer than the current frame.
        /// </remarks>
        /// <returns>The default network settings.</returns>
        /// <exception cref="Exception">
        /// If the transport has an invalid configuration that prevents creating appropriate
        /// settings for the driver. For example, if encryption is enabled but secrets haven't
        /// been set, or if Relay is selected but the Relay server data hasn't been set.
        /// </exception>
        public NetworkSettings GetDefaultNetworkSettings()
        {
            var settings = new NetworkSettings();

            // Basic driver configuration settings.
            settings.WithNetworkConfigParameters(
                maxConnectAttempts: m_MaxConnectAttempts,
                connectTimeoutMS: m_ConnectTimeoutMS,
                disconnectTimeoutMS: m_DisconnectTimeoutMS,
                sendQueueCapacity: m_MaxPacketQueueSize,
                receiveQueueCapacity: m_MaxPacketQueueSize,
                heartbeatTimeoutMS: m_HeartbeatTimeoutMS);

            // Configure Relay if in use.
            if (m_ProtocolType == ProtocolType.RelayUnityTransport)
            {
                if (m_RelayServerData.Equals(default(RelayServerData)))
                {
                    throw new Exception("You must call SetRelayServerData() before calling StartClient() or StartServer().");
                }
                else
                {
                    settings.WithRelayParameters(ref m_RelayServerData, m_HeartbeatTimeoutMS);
                }
            }

#if UNITY_MP_TOOLS_NETSIM_IMPLEMENTATION_ENABLED
            // Latency, jitter and packet loss will be set by the network simulator in the tools
            // package. We just need to initialize the settings since otherwise these features will
            // not be enabled at all in the driver.
            // Assuming a maximum average latency of 50 ms, and that we're somehow able to flush an entire reliable window every tick,
            // then at 60 ticks per second we need to be able to store 60 * 0.05 * 64 = 192 packets per connection in the simulator
            // pipeline stage. Double that since we handle both directions and round it up, and
            // that's how we get 400 here.
            settings.WithSimulatorStageParameters(maxPacketCount: 400, randomSeed: DebugSimulatorRandomSeed ?? (uint)System.Diagnostics.Stopwatch.GetTimestamp());
            settings.WithNetworkSimulatorParameters();
#endif

            // If the user sends a message of exactly m_MaxPayloadSize in length, we need to
            // account for the overhead of its length when we store it in the send queue.
            var fragmentationCapacity = m_MaxPayloadSize + BatchedSendQueue.PerMessageOverhead;
            settings.WithFragmentationStageParameters(payloadCapacity: fragmentationCapacity);

            // Bump the reliable window size to its maximum size of 64. Since NGO makes heavy use of
            // reliable delivery, we're better off with the increased window size compared to the
            // extra 4 bytes of header that this costs us.
            //
            // We also increase the maximum resend timeout since the default one in UTP is very
            // aggressive (optimized for latency and low bandwidth). With NGO, it's too low and
            // we sometimes notice a lot of useless resends, especially if using Relay.
            var maxResendTime = m_ProtocolType == ProtocolType.RelayUnityTransport ? 750 : 500;
            settings.WithReliableStageParameters(windowSize: 64, maximumResendTime: maxResendTime);

            // Set up encryption if in use. This only needs to be done when not using Relay. If
            // using Relay, then UTP configures encryption on its own based solely on the protocol
            // configured in the Relay server data.
            if (m_UseEncryption && m_ProtocolType == ProtocolType.UnityTransport)
            {
                if (m_Driver.IsCreated && m_Driver.Listening)
                {
                    if (string.IsNullOrEmpty(m_ServerCertificate) || string.IsNullOrEmpty(m_ServerPrivateKey))
                    {
                        throw new Exception("In order to use encryption, you must call SetServerSecrets() before calling StartServer().");
                    }
                    else
                    {
                        settings.WithSecureServerParameters(m_ServerCertificate, m_ServerPrivateKey);
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(m_ServerCommonName))
                    {
                        throw new Exception("In order to use encryption, you must call SetClientSecrets() before calling StartClient().");
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_ClientCaCertificate))
                        {
                            settings.WithSecureClientParameters(m_ServerCommonName);
                        }
                        else
                        {
                            settings.WithSecureClientParameters(m_ClientCaCertificate, m_ServerCommonName);
                        }
                    }
                }
            }

            return settings;
        }

        /// <summary>
        /// Get the pipeline configurations that will be used based on the current configuration of
        /// the transport. Useful for custom <see cref="INetworkStreamDriverConstructor"/>
        /// implementations, since it allows extending the existing configuration while maintaining
        /// the existing functionality of the default pipelines that's not otherwise available
        /// publicly (like integration with the network profiler).
        /// </summary>
        /// <remarks>
        /// All returned values are allocated with the <see cref="Allocator.Temp"/> allocator. Do not
        /// hold references to them longer than the current frame. There is no need to dispose of the
        /// returned lists.
        /// </remarks>
        /// <param name="unreliableFragmentedPipelineStages">
        /// Pipeline stages that will be used for the unreliable and fragmented pipeline.
        /// </param>
        /// <param name="unreliableSequencedFragmentedPipelineStages">
        /// Pipeline stages that will be used for the unreliable, sequenced, and fragmented pipeline.
        /// </param>
        /// <param name="reliableSequencedPipelineStages">
        /// Pipeline stages that will be used for the reliable and sequenced pipeline stage.
        /// </param>
        public void GetDefaultPipelineConfigurations(
            out NativeArray<NetworkPipelineStageId> unreliableFragmentedPipelineStages,
            out NativeArray<NetworkPipelineStageId> unreliableSequencedFragmentedPipelineStages,
            out NativeArray<NetworkPipelineStageId> reliableSequencedPipelineStages)
        {
            var unreliableFragmented = new NetworkPipelineStageId[]
            {
                NetworkPipelineStageId.Get<FragmentationPipelineStage>(),
#if UNITY_MP_TOOLS_NETSIM_IMPLEMENTATION_ENABLED
                NetworkPipelineStageId.Get<SimulatorPipelineStage>(),
#endif
            };

            var unreliableSequencedFragmented = new NetworkPipelineStageId[]
            {
                NetworkPipelineStageId.Get<FragmentationPipelineStage>(),
                NetworkPipelineStageId.Get<UnreliableSequencedPipelineStage>(),
#if UNITY_MP_TOOLS_NETSIM_IMPLEMENTATION_ENABLED
                NetworkPipelineStageId.Get<SimulatorPipelineStage>(),
#endif
            };

            var reliableSequenced = new NetworkPipelineStageId[]
            {
                NetworkPipelineStageId.Get<ReliableSequencedPipelineStage>(),
#if UNITY_MP_TOOLS_NETSIM_IMPLEMENTATION_ENABLED
                NetworkPipelineStageId.Get<SimulatorPipelineStage>(),
#endif
            };

            unreliableFragmentedPipelineStages = new(unreliableFragmented, Allocator.Temp);
            unreliableSequencedFragmentedPipelineStages = new(unreliableSequencedFragmented, Allocator.Temp);
            reliableSequencedPipelineStages = new(reliableSequenced, Allocator.Temp);
        }

        private NetworkPipeline SelectSendPipeline(Channel channel)
        {
            switch (channel)
            {
                case Channel.Unreliable:
                    return m_UnreliableFragmentedPipeline;

                case Channel.UnreliableSequenced:
                    return m_UnreliableSequencedFragmentedPipeline;

                case Channel.ReliableUnordered:
                case Channel.ReliableOrdered:
                    return m_ReliableSequencedPipeline;

                default:
                    Debug.LogError($"Unknown {nameof(Channel)} value: {channel}");
                    return NetworkPipeline.Null;
            }
        }

        private bool ClientBindAndConnect()
        {
            var serverEndpoint = default(NetworkEndpoint);

            if (m_ProtocolType == ProtocolType.RelayUnityTransport)
            {
                serverEndpoint = m_RelayServerData.Endpoint;
            }
            else
            {
                // This will result in an invalid endpoint if the address is a hostname.
                // This is handled later in the Connect method if hostname resolution is available
                // (although we still check for hostname validity to error out early if it's not),
                // but if not then we need to error out here.
                serverEndpoint = ConnectionAddressData.ParseNetworkEndpoint(ConnectionData.Address, ConnectionData.Port);

                if (serverEndpoint.Family == NetworkFamily.Invalid)
                {
#if HOSTNAME_RESOLUTION_AVAILABLE
                    if (Uri.CheckHostName(ConnectionData.Address) != UriHostNameType.Dns)
                    {
                        Debug.LogError($"Provided connection address \"{ConnectionData.Address}\" is not a valid hostname.");
                        return false;
                    }
#else
                    Debug.LogError($"Invalid server address: {ConnectionData.Address}:{ConnectionData.Port}.");
                    return false;
#endif
                }
            }

            InitDriver();

            // Don't bind yet if connecting to a hostname, since we don't know if it will resolve to IPv4 or IPv6.
            if (serverEndpoint.Family != NetworkFamily.Invalid && ConnectionData.ClientBindPort != 0)
            {
                var bindEndpoint = serverEndpoint.Family == NetworkFamily.Ipv6
                    ? NetworkEndpoint.AnyIpv6.WithPort(ConnectionData.ClientBindPort)
                    : NetworkEndpoint.AnyIpv4.WithPort(ConnectionData.ClientBindPort);
                if (m_Driver.Bind(bindEndpoint) != 0)
                {
                    Debug.LogError($"Couldn't create socket. Possibly another process is using port {ConnectionData.ClientBindPort}.");
                    return false;
                }
            }

            Connect(serverEndpoint);

            return true;
        }

        /// <summary>
        /// Virtual method that is invoked during <see cref="StartClient"/>.
        /// </summary>
        /// <param name="serverEndpoint">The <see cref="NetworkEndpoint"/> that the client is connecting to.</param>
        /// <returns>A <see cref="NetworkConnection"/> representing the connection to the server, or an invalid connection if the connection attempt fails.</returns>
        protected virtual NetworkConnection Connect(NetworkEndpoint serverEndpoint)
        {
#if HOSTNAME_RESOLUTION_AVAILABLE
            // If the server endpoint is invalid, it means whatever the user entered in the address
            // field was not an IP address, and must be presumed to be a hostname.
            if (serverEndpoint.Family == NetworkFamily.Invalid)
            {
                return m_Driver.Connect(ConnectionData.Address, ConnectionData.Port);
            }
#endif
            return m_Driver.Connect(serverEndpoint);
        }

        private bool ServerBindAndListen(NetworkEndpoint endPoint)
        {
            if (endPoint.Family == NetworkFamily.Invalid)
            {
                return false;
            }

            InitDriver();

            int result = m_Driver.Bind(endPoint);
            if (result != 0)
            {
                Debug.LogError("Server failed to bind. This is usually caused by another process being bound to the same port.");
                return false;
            }

            result = m_Driver.Listen();
            if (result != 0)
            {
                Debug.LogError("Server failed to listen.");
                return false;
            }

            return true;
        }

        private void SetProtocol(ProtocolType inProtocol)
        {
            m_ProtocolType = inProtocol;
        }

        /// <summary>Set the relay server data for the server.</summary>
        /// <param name="ipv4Address">IP address or hostname of the relay server.</param>
        /// <param name="port">UDP port of the relay server.</param>
        /// <param name="allocationIdBytes">Allocation ID as a byte array.</param>
        /// <param name="keyBytes">Allocation key as a byte array.</param>
        /// <param name="connectionDataBytes">Connection data as a byte array.</param>
        /// <param name="hostConnectionDataBytes">The HostConnectionData as a byte array.</param>
        /// <param name="isSecure">Whether the connection is secure (uses DTLS).</param>
        public void SetRelayServerData(string ipv4Address, ushort port, byte[] allocationIdBytes, byte[] keyBytes, byte[] connectionDataBytes, byte[] hostConnectionDataBytes = null, bool isSecure = false)
        {
            var hostConnectionData = hostConnectionDataBytes ?? connectionDataBytes;
            m_RelayServerData = new RelayServerData(ipv4Address, port, allocationIdBytes, connectionDataBytes, hostConnectionData, keyBytes, isSecure);
            SetProtocol(ProtocolType.RelayUnityTransport);
        }

        /// <summary>Set the relay server data (using the lower-level Unity Transport data structure).</summary>
        /// <param name="serverData">Data for the Relay server to use.</param>
        public void SetRelayServerData(RelayServerData serverData)
        {
            m_RelayServerData = serverData;
            SetProtocol(ProtocolType.RelayUnityTransport);
        }

        /// <summary>Set the relay server data for the host.</summary>
        /// <param name="ipAddress">IP address or hostname of the relay server.</param>
        /// <param name="port">UDP port of the relay server.</param>
        /// <param name="allocationId">Allocation ID as a byte array.</param>
        /// <param name="key">Allocation key as a byte array.</param>
        /// <param name="connectionData">Connection data as a byte array.</param>
        /// <param name="isSecure">Whether the connection is secure (uses DTLS).</param>
        public void SetHostRelayData(string ipAddress, ushort port, byte[] allocationId, byte[] key, byte[] connectionData, bool isSecure = false)
        {
            SetRelayServerData(ipAddress, port, allocationId, key, connectionData, null, isSecure);
        }

        /// <summary>Set the relay server data for the host.</summary>
        /// <param name="ipAddress">IP address or hostname of the relay server.</param>
        /// <param name="port">UDP port of the relay server.</param>
        /// <param name="allocationId">Allocation ID as a byte array.</param>
        /// <param name="key">Allocation key as a byte array.</param>
        /// <param name="connectionData">Connection data as a byte array.</param>
        /// <param name="hostConnectionData">Host's connection data as a byte array.</param>
        /// <param name="isSecure">Whether the connection is secure (uses DTLS).</param>
        public void SetClientRelayData(string ipAddress, ushort port, byte[] allocationId, byte[] key, byte[] connectionData, byte[] hostConnectionData, bool isSecure = false)
        {
            SetRelayServerData(ipAddress, port, allocationId, key, connectionData, hostConnectionData, isSecure);
        }

        // Command line options
        private const string k_OverridePortArg = "-port";
        private const string k_OverrideIpAddressArg = "-ip";

        private static string GetCommandLineArg(string argName)
        {
            var args = Environment.GetCommandLineArgs();
            for (int i = 0; i < args.Length - 1; i++)
            {
                if (args[i] == argName)
                    return args[i + 1];
            }
            return null;
        }

        private bool ParseCommandLineOptionsPort(out ushort port)
        {
#if UNITY_SERVER && UNITY_DEDICATED_SERVER_ARGUMENTS_PRESENT
            if (UnityEngine.DedicatedServer.Arguments.Port != null)
            {
                port = (ushort)UnityEngine.DedicatedServer.Arguments.Port;
                return true;
            }
#else
            if (GetCommandLineArg(k_OverridePortArg) is string argValue)
            {
                port = (ushort)Convert.ChangeType(argValue, typeof(ushort));
                return true;
            }
#endif
            port = default;
            return false;
        }

        private bool ParseCommandLineOptionsAddress(out string ipValue)
        {
            if (GetCommandLineArg(k_OverrideIpAddressArg) is string argValue)
            {
                ipValue = argValue;
                return true;
            }
            ipValue = default;
            return false;
        }

        /// <summary>
        /// Sets IP and Port information. This will be ignored if using the Unity Relay and you should call <see cref="SetRelayServerData"/>
        /// </summary>
        /// <param name="ipv4Address">The remote IP address (despite the name, can be an IPv6 address or a domain name).</param>
        /// <param name="port">The remote port to connect to.</param>
        /// <param name="listenAddress">The address the server is going to listen on.</param>
        public void SetConnectionData(string ipv4Address, ushort port, string listenAddress = null)
        {
            SetConnectionData(false, ipv4Address, port, listenAddress);
        }

        /// <summary>
        /// Sets IP and Port information. This will be ignored if using the Unity Relay and you should call <see cref="SetRelayServerData"/>
        /// </summary>
        /// <param name="ipv4Address">The remote IP address (despite the name, can be an IPv6 address or a domain name).</param>
        /// <param name="port">The remote port to connect to.</param>
        /// <param name="listenAddress">The address the server is going to listen on.</param>
        /// <param name="forceOverrideCommandLineArgs">When true, -port and -ip command line arguments will be ignored.</param>
        public void SetConnectionData(bool forceOverrideCommandLineArgs, string ipv4Address, ushort port, string listenAddress = null)
        {
            m_HasForcedConnectionData = forceOverrideCommandLineArgs;
            if (!forceOverrideCommandLineArgs && ParseCommandLineOptionsPort(out var commandLinePort))
            {
                port = commandLinePort;
            }

            if (!forceOverrideCommandLineArgs && ParseCommandLineOptionsAddress(out var commandLineIp))
            {
                ipv4Address = commandLineIp;
            }

            ConnectionData = new ConnectionAddressData
            {
                Address = ipv4Address,
                Port = port,
                ServerListenAddress = listenAddress ?? ipv4Address,
                ClientBindPort = ConnectionData.ClientBindPort
            };

            SetProtocol(ProtocolType.UnityTransport);
        }

        /// <summary>
        /// Sets IP and Port information. This will be ignored if using the Unity Relay and you should call <see cref="SetRelayServerData"/>
        /// </summary>
        /// <param name="endPoint">The remote endpoint the client should connect to.</param>
        /// <param name="listenEndPoint">The endpoint the server should listen on.</param>
        public void SetConnectionData(NetworkEndpoint endPoint, NetworkEndpoint listenEndPoint = default)
        {
            string serverAddress = endPoint.Address.Split(':')[0];

            string listenAddress = string.Empty;
            if (listenEndPoint != default)
            {
                listenAddress = listenEndPoint.Address.Split(':')[0];
                if (endPoint.Port != listenEndPoint.Port)
                {
                    Debug.LogError($"Port mismatch between server and listen endpoints ({endPoint.Port} vs {listenEndPoint.Port}).");
                }
            }

            SetConnectionData(serverAddress, endPoint.Port, listenAddress);
        }

        /// <summary>Set the parameters for the debug simulator.</summary>
        /// <param name="packetDelay">Packet delay in milliseconds.</param>
        /// <param name="packetJitter">Packet jitter in milliseconds.</param>
        /// <param name="dropRate">Packet drop percentage.</param>
        [Obsolete("SetDebugSimulatorParameters is no longer supported and has no effect. Use Network Simulator from the Multiplayer Tools package.", false)]
        public void SetDebugSimulatorParameters(int packetDelay, int packetJitter, int dropRate)
        {
            if (m_Driver.IsCreated)
            {
                Debug.LogError("SetDebugSimulatorParameters() must be called before StartClient() or StartServer().");
                return;
            }

            DebugSimulator = new SimulatorParameters
            {
                PacketDelayMS = packetDelay,
                PacketJitterMS = packetJitter,
                PacketDropRate = dropRate
            };
        }

        [BurstCompile]
        private struct SendBatchedMessagesJob : IJob
        {
            public NetworkDriver.Concurrent Driver;
            public SendTarget Target;
            public BatchedSendQueue Queue;
            public NetworkPipeline ReliablePipeline;
            public int MTU;

            public void Execute()
            {
                var clientId = Target.ClientId;
                var connection = ParseClientId(clientId);
                var pipeline = Target.NetworkPipeline;

                while (!Queue.IsEmpty)
                {
                    var result = Driver.BeginSend(pipeline, connection, out var writer);
                    if (result != (int)TransportError.Success)
                    {
                        Debug.LogError($"Send error on connection {clientId}: {ErrorUtilities.ErrorToFixedString(result)}");
                        return;
                    }

                    // We don't attempt to send entire payloads over the reliable pipeline. Instead we
                    // fragment it manually. This is safe and easy to do since the reliable pipeline
                    // basically implements a stream, so as long as we separate the different messages
                    // in the stream (the send queue does that automatically) we are sure they'll be
                    // reassembled properly at the other end. This allows us to lift the limit of ~44KB
                    // on reliable payloads (because of the reliable window size).
                    var written = pipeline == ReliablePipeline ? Queue.FillWriterWithBytes(ref writer, MTU) : Queue.FillWriterWithMessages(ref writer, MTU);

                    result = Driver.EndSend(writer);
                    if (result == written)
                    {
                        // Batched message was sent successfully. Remove it from the queue.
                        Queue.Consume(written);
                    }
                    else
                    {
                        // Some error occured. If it's just the UTP queue being full, then don't log
                        // anything since that's okay (the unsent message(s) are still in the queue
                        // and we'll retry sending them later). Otherwise log the error and remove the
                        // message from the queue (we don't want to resend it again since we'll likely
                        // just get the same error again).
                        if (result != (int)TransportError.NetworkSendQueueFull)
                        {
                            Debug.LogError($"Send error on connection {clientId}: {ErrorUtilities.ErrorToFixedString(result)}");
                            Queue.Consume(written);
                        }

                        return;
                    }
                }
            }
        }

        // Send as many batched messages from the queue as possible.
        private void SendBatchedMessages(SendTarget sendTarget, BatchedSendQueue queue)
        {
            if (!m_Driver.IsCreated)
            {
                return;
            }

            var mtu = NetworkParameterConstants.MTU;

            new SendBatchedMessagesJob
            {
                Driver = m_Driver.ToConcurrent(),
                Target = sendTarget,
                Queue = queue,
                ReliablePipeline = m_ReliableSequencedPipeline,
                MTU = mtu,
            }.Run();
        }

        private bool AcceptConnection()
        {
            var connection = m_Driver.Accept();

            if (connection == default)
            {
                return false;
            }

            var clientId = ParseClientId(connection);
            var conn = new Connection(connection.GetHashCode());
            _connToClientId[conn.connectionId] = clientId;
            _connections.Add(conn);
            onConnected?.Invoke(conn, true);

            return true;

        }

        private void ReceiveMessages(ulong clientId, NetworkPipeline pipeline, DataStreamReader dataReader)
        {
            BatchedReceiveQueue queue;
            if (pipeline == m_ReliableSequencedPipeline)
            {
                if (m_ReliableReceiveQueues.TryGetValue(clientId, out queue))
                {
                    queue.PushReader(dataReader);
                }
                else
                {
                    queue = new BatchedReceiveQueue(dataReader);
                    m_ReliableReceiveQueues[clientId] = queue;
                }
            }
            else
            {
                queue = new BatchedReceiveQueue(dataReader);
            }

            while (!queue.IsEmpty)
            {
                var message = queue.PopMessage();
                if (message == default)
                {
                    // Only happens if there's only a partial message in the queue (rare).
                    break;
                }

                var isServer = m_Driver.Listening;
                var conn = isServer ? FindConnectionByClientId(clientId) : new Connection(0);
                var byteData = new ByteData(message.Array, message.Offset, message.Count);
                onDataReceived?.Invoke(conn, byteData, isServer);
            }
        }

        private bool ProcessEvent()
        {
            var eventType = m_Driver.PopEvent(out var networkConnection, out var reader, out var pipeline);
            var clientId = ParseClientId(networkConnection);

            switch (eventType)
            {
                case TransportEvent.Connect:
                    {
                        m_ServerClientId = clientId;
                        clientState = ConnectionState.Connected;
                        onConnectionState?.Invoke(clientState, false);
                        var conn = new Connection(0);
                        onConnected?.Invoke(conn, false);
                        return true;
                    }
                case TransportEvent.Disconnect:
                    {
                        if (!m_Driver.Listening && m_ServerClientId == default)
                        {
                            Debug.LogError("Failed to connect to server.");
                        }

                        var isServer = m_Driver.Listening;
                        var utpReason = (Unity.Networking.Transport.Error.DisconnectReason)reader.ReadByte();
                        var reason = MapDisconnectReason(utpReason);

                        m_ServerClientId = default;
                        m_ReliableReceiveQueues.Remove(clientId);
                        ClearSendQueuesForClientId(clientId);

                        if (isServer)
                        {
                            var conn = FindConnectionByClientId(clientId);
                            _connToClientId.Remove(conn.connectionId);
                            _connections.Remove(conn);
                            onDisconnected?.Invoke(conn, reason, true);
                        }
                        else
                        {
                            onDisconnected?.Invoke(new Connection(0), reason, false);
                        }

                        return true;
                    }
                case TransportEvent.Data:
                    {
                        ReceiveMessages(clientId, pipeline, reader);
                        return true;
                    }
            }

            return false;
        }

        /// <summary>
        /// Handles accepting new connections and processing transport events.
        /// </summary>
        private void OnEarlyUpdate()
        {
            if (m_Driver.IsCreated)
            {
                if (m_ProtocolType == ProtocolType.RelayUnityTransport && m_Driver.GetRelayConnectionStatus() == RelayConnectionStatus.AllocationInvalid)
                {
                    Debug.LogError("Transport failure! Relay allocation needs to be recreated, and NetworkManager restarted. " +
                        "Use NetworkManager.OnTransportFailure to be notified of such events programmatically.");

                    Debug.LogError("Relay allocation invalid. Transport failure.");
                    return;
                }

                m_Driver.ScheduleUpdate().Complete();

                // Process any new connections
                while (AcceptConnection() && m_Driver.IsCreated)
                {
                    ;
                }

                // Process any transport events (i.e. connect, disconnect, data, etc)
                while (ProcessEvent() && m_Driver.IsCreated)
                {
                    ;
                }
            }
        }

        /// <summary>
        /// Handles sending any queued batched messages.
        /// </summary>
        private void OnPostLateUpdate()
        {
            if (m_Driver.IsCreated)
            {
                foreach (var kvp in m_SendQueue)
                {
                    SendBatchedMessages(kvp.Key, kvp.Value);
                }

                // Schedule a flush send as the last transport action for the
                // current frame.
                m_Driver.ScheduleFlushSend(default).Complete();
            }
        }

        private void OnDestroy()
        {
            DisposeInternals();
        }

        private int ExtractRtt(NetworkConnection networkConnection)
        {
            if (m_Driver.GetConnectionState(networkConnection) != NetworkConnection.State.Connected)
            {
                return 0;
            }

            m_Driver.GetPipelineBuffers(m_ReliableSequencedPipeline,
                NetworkPipelineStageId.Get<ReliableSequencedPipelineStage>(),
                networkConnection,
                out _,
                out _,
                out var sharedBuffer);

            unsafe
            {
                var sharedContext = (ReliableUtility.SharedContext*)sharedBuffer.GetUnsafePtr();

                return sharedContext->RttInfo.LastRtt;
            }
        }

        private float ExtractPacketLoss(NetworkConnection networkConnection)
        {
            if (m_Driver.GetConnectionState(networkConnection) != NetworkConnection.State.Connected)
            {
                return 0f;
            }

            m_Driver.GetPipelineBuffers(m_ReliableSequencedPipeline,
                NetworkPipelineStageId.Get<ReliableSequencedPipelineStage>(),
                networkConnection,
                out _,
                out _,
                out var sharedBuffer);

            unsafe
            {
                var sharedContext = (ReliableUtility.SharedContext*)sharedBuffer.GetUnsafePtr();

                var packetReceivedDelta = (float)(sharedContext->stats.PacketsReceived - m_PacketLossCache.PacketsReceived);
                var packetDroppedDelta = (float)(sharedContext->stats.PacketsDropped - m_PacketLossCache.PacketsDropped);

                // There can be multiple update happening in a single frame where no packets have transitioned
                // In those situation we want to return the last packet loss value instead of 0 to avoid invalid swings
                if (packetDroppedDelta == 0 && packetReceivedDelta == 0)
                {
                    return m_PacketLossCache.PacketLoss;
                }

                m_PacketLossCache.PacketsReceived = sharedContext->stats.PacketsReceived;
                m_PacketLossCache.PacketsDropped = sharedContext->stats.PacketsDropped;

                m_PacketLossCache.PacketLoss = packetReceivedDelta > 0 ? packetDroppedDelta / packetReceivedDelta : 0;

                return m_PacketLossCache.PacketLoss;
            }
        }

        private static unsafe ulong ParseClientId(NetworkConnection utpConnectionId)
        {
            return *(ulong*)&utpConnectionId;
        }

        private static unsafe NetworkConnection ParseClientId(ulong netcodeConnectionId)
        {
            return *(NetworkConnection*)&netcodeConnectionId;
        }

        private Connection FindConnectionByClientId(ulong clientId)
        {
            var connId = ParseClientId(clientId).GetHashCode();
            for (int i = 0; i < _connections.Count; i++)
            {
                if (_connections[i].connectionId == connId)
                    return _connections[i];
            }
            return default;
        }

        private static Transports.DisconnectReason MapDisconnectReason(Unity.Networking.Transport.Error.DisconnectReason utpReason)
        {
            switch (utpReason)
            {
                case Unity.Networking.Transport.Error.DisconnectReason.Timeout:
                case Unity.Networking.Transport.Error.DisconnectReason.MaxConnectionAttempts:
                    return Transports.DisconnectReason.Timeout;
                case Unity.Networking.Transport.Error.DisconnectReason.ClosedByRemote:
                    return Transports.DisconnectReason.ServerRequest;
                case Unity.Networking.Transport.Error.DisconnectReason.Default:
                default:
                    return Transports.DisconnectReason.ClientRequest;
            }
        }

        private void ClearSendQueuesForClientId(ulong clientId)
        {
            // NativeList and manual foreach avoids any allocations.
            using var keys = new NativeList<SendTarget>(16, Allocator.Temp);
            foreach (var key in m_SendQueue.Keys)
            {
                if (key.ClientId == clientId)
                {
                    keys.Add(key);
                }
            }

            foreach (var target in keys)
            {
                m_SendQueue[target].Dispose();
                m_SendQueue.Remove(target);
            }
        }

        private void FlushSendQueuesForClientId(ulong clientId)
        {
            foreach (var kvp in m_SendQueue)
            {
                if (kvp.Key.ClientId == clientId)
                {
                    SendBatchedMessages(kvp.Key, kvp.Value);
                }
            }
        }

        /// <summary>
        /// Disconnects the local client from the remote
        /// </summary>
        private void DisconnectLocalClient()
        {
            if (m_ServerClientId != default)
            {
                var clientId = m_ServerClientId;
                FlushSendQueuesForClientId(clientId);

                if (m_Driver.Disconnect(ParseClientId(clientId)) == 0)
                {
                    m_ServerClientId = default;

                    m_ReliableReceiveQueues.Remove(clientId);
                    ClearSendQueuesForClientId(clientId);

                    onDisconnected?.Invoke(new Connection(0), Transports.DisconnectReason.ClientRequest, false);
                }
            }
        }

        /// <summary>
        /// Disconnects a remote client from the server
        /// </summary>
        /// <param name="clientId">The client to disconnect</param>
        private void DisconnectRemoteClient(ulong clientId)
        {
#if DEBUG
            if (!m_Driver.IsCreated)
            {
                Debug.LogWarning($"{nameof(DisconnectRemoteClient)} should only be called on a listening server!");
                return;
            }
#endif

            if (m_Driver.IsCreated && m_Driver.Listening)
            {
                FlushSendQueuesForClientId(clientId);

                m_ReliableReceiveQueues.Remove(clientId);
                ClearSendQueuesForClientId(clientId);

                var connection = ParseClientId(clientId);
                if (m_Driver.GetConnectionState(connection) != NetworkConnection.State.Disconnected)
                {
                    m_Driver.Disconnect(connection);
                }
            }
        }

        /// <summary>
        /// Gets the current RTT for a specific client
        /// </summary>
        /// <param name="clientId">The client RTT to get</param>
        /// <returns>The RTT</returns>
        public ulong GetCurrentRtt(ulong clientId)
        {
            return (ulong)ExtractRtt(ParseClientId(clientId));
        }

        /// <summary>
        /// Provides the <see cref="NetworkEndpoint"/> for the NGO client identifier specified.
        /// </summary>
        /// <remarks>
        /// - This is only really useful for direct connections.
        /// - Relay connections and clients connected using a distributed authority network topology will not provide the client's actual endpoint information.
        /// - For LAN topologies this should work as long as it is a direct connection and not a relay connection.
        /// </remarks>
        /// <param name="clientId">NGO client identifier to get endpoint information about.</param>
        /// <returns><see cref="NetworkEndpoint"/></returns>
        public NetworkEndpoint GetEndpoint(ulong clientId)
        {
            if (m_Driver.IsCreated)
            {
                var networkConnection = ParseClientId(clientId);
                if (m_Driver.GetConnectionState(networkConnection) == NetworkConnection.State.Connected)
                {
                    return m_Driver.GetRemoteEndpoint(networkConnection);
                }
            }

            return new NetworkEndpoint();
        }

        /// <summary>
        /// Send a payload to the specified clientId, data and networkDelivery.
        /// </summary>
        /// <param name="clientId">The clientId to send to</param>
        /// <param name="payload">The data to send</param>
        /// <param name="networkDelivery">The delivery type (QoS) to send data with</param>
        private void Send(ulong clientId, ArraySegment<byte> payload, Channel channel)
        {
            var connection = ParseClientId(clientId);
            if (!m_Driver.IsCreated || m_Driver.GetConnectionState(connection) != NetworkConnection.State.Connected)
            {
                return;
            }

            var pipeline = SelectSendPipeline(channel);
            if (pipeline != m_ReliableSequencedPipeline && payload.Count > m_MaxPayloadSize)
            {
                Debug.LogError($"Unreliable payload of size {payload.Count} larger than configured 'Max Payload Size' ({m_MaxPayloadSize}).");
                return;
            }

            var sendTarget = new SendTarget(clientId, pipeline);
            if (!m_SendQueue.TryGetValue(sendTarget, out var queue))
            {
                // The maximum size of a send queue is determined according to the disconnection
                // timeout. The idea being that if the send queue contains enough reliable data that
                // sending it all out would take longer than the disconnection timeout, then there's
                // no point storing even more in the queue (it would be like having a ping higher
                // than the disconnection timeout, which is far into the realm of unplayability).
                //
                // The throughput used to determine what consists the maximum send queue size is
                // the maximum theoritical throughput of the reliable pipeline assuming we only send
                // on each update at 60 FPS, which turns out to be around 2.688 MB/s.
                //
                // Note that we only care about reliable throughput for send queues because that's
                // the only case where a full send queue causes a connection loss. Full unreliable
                // send queues are dealt with by flushing it out to the network or simply dropping
                // new messages if that fails.
                var maxCapacity = m_MaxSendQueueSize;
                if (maxCapacity <= 0)
                {
                    // Setting m_DisconnectTimeoutMS to zero will disable the timeout entirely
                    // Set the capacity as if the disconnect timeout is the largest possible value
                    if (m_DisconnectTimeoutMS == 0)
                    {
                        maxCapacity = BatchedSendQueue.MaximumMaximumCapacity;
                    }
                    else
                    {
                        // Avoids overflow when m_DisconnectTimeoutMS is set to a very high value
                        var fullCalculation = Math.BigMul(m_DisconnectTimeoutMS, k_MaxReliableThroughput);
                        maxCapacity = (int)Math.Min(fullCalculation, BatchedSendQueue.MaximumMaximumCapacity);
                    }
                }

                queue = new BatchedSendQueue(Math.Max(maxCapacity, m_MaxPayloadSize));
                m_SendQueue.Add(sendTarget, queue);
            }

            if (!queue.PushMessage(payload))
            {
                if (pipeline == m_ReliableSequencedPipeline)
                {
                    // If the message is sent reliably, then we're over capacity and we can't
                    // provide any reliability guarantees anymore. Disconnect the client since at
                    // this point they're bound to become desynchronized.
                    Debug.LogError($"Couldn't add payload of size {payload.Count} to reliable send queue. " +
                        $"Closing connection {clientId} as reliability guarantees can't be maintained.");

                    if (clientId == m_ServerClientId)
                    {
                        DisconnectLocalClient();
                    }
                    else
                    {
                        DisconnectRemoteClient(clientId);

                        var conn = FindConnectionByClientId(clientId);
                        _connToClientId.Remove(conn.connectionId);
                        _connections.Remove(conn);
                        onDisconnected?.Invoke(conn, Transports.DisconnectReason.ServerRequest, true);
                    }
                }
                else
                {
                    // If the message is sent unreliably, we can always just flush everything out
                    // to make space in the send queue. This is an expensive operation, but a user
                    // would need to send A LOT of unreliable traffic in one update to get here.

                    m_Driver.ScheduleFlushSend(default).Complete();
                    SendBatchedMessages(sendTarget, queue);

                    // Don't check for failure. If it still doesn't work, there's nothing we can do
                    // at this point and the message is lost (it was sent unreliable anyway).
                    queue.PushMessage(payload);
                }
            }
        }

        /// <summary>
        /// Connects client to the server
        /// Note:
        /// When this method returns false it could mean:
        /// - You are trying to start a client that is already started
        /// - It failed during the initial port binding when attempting to begin to connect
        /// </summary>
        /// <returns>true if the client was started and false if it failed to start the client</returns>
        private bool StartClientTransport()
        {
            if (m_Driver.IsCreated)
            {
                return false;
            }

            var succeeded = ClientBindAndConnect();
            if (!succeeded && m_Driver.IsCreated)
            {
                m_Driver.Dispose();
            }
            return succeeded;
        }

        /// <summary>
        /// Starts to listening for incoming clients
        /// Note:
        /// When this method returns false it could mean:
        /// - You are trying to start a client that is already started
        /// - It failed during the initial port binding when attempting to begin to connect
        /// </summary>
        /// <returns>true if the server was started and false if it failed to start the server</returns>
        private bool StartServerTransport()
        {
            if (m_Driver.IsCreated)
            {
                return false;
            }

            var listenEndpoint = m_ProtocolType == ProtocolType.UnityTransport
                ? ConnectionData.ListenEndPoint
                : NetworkEndpoint.AnyIpv4;

            var succeeded = ServerBindAndListen(listenEndpoint);
            if (!succeeded && m_Driver.IsCreated)
            {
                m_Driver.Dispose();
            }

            return succeeded;
        }

        /// <summary>
        /// This is set in <see cref="SetConnectionData(string, ushort, string, bool)"/>
        /// </summary>
        private bool m_HasForcedConnectionData;

        private void Initialize()
        {
            //If the port doesn't have a forced value and is set by a command line option, override it.
            if (!m_HasForcedConnectionData && ParseCommandLineOptionsAddress(out var portAsString))
            {
                Debug.Log($"The port is set by a command line option. Using following connection data: {ConnectionData.Address}:{portAsString}");
                if (ushort.TryParse(portAsString, out ushort port))
                {
                    ConnectionData.Port = port;
                }
                else
                {
                    Debug.LogError($"The port ({portAsString}) is not a valid unsigned short value!");
                }
            }
        }

        /// <summary>
        /// Shuts down the transport
        /// </summary>
        private void Shutdown()
        {
            if (m_Driver.IsCreated)
            {
                while (ProcessEvent() && m_Driver.IsCreated)
                {
                    ;
                }

                // Flush all send queues to the network. NGO can be configured to flush its message
                // queue on shutdown. But this only calls the Send() method, which doesn't actually
                // get anything to the network.
                foreach (var kvp in m_SendQueue)
                {
                    SendBatchedMessages(kvp.Key, kvp.Value);
                }

                // The above flush only puts the message in UTP internal buffers, need an update to
                // actually get the messages on the wire. (Normally a flush send would be sufficient,
                // but there might be disconnect messages and those require an update call.)
                m_Driver.ScheduleUpdate().Complete();
            }

            DisposeInternals();

            m_ReliableReceiveQueues.Clear();

            // We must reset this to zero because UTP actually re-uses clientIds if there is a clean disconnect
            m_ServerClientId = default;

            _connToClientId.Clear();
            _connections.Clear();
        }

        private string m_ServerPrivateKey;
        private string m_ServerCertificate;

        private string m_ServerCommonName;
        private string m_ClientCaCertificate;

        /// <summary>Set the server parameters for encryption.</summary>
        /// <remarks>
        /// The public certificate and private key are expected to be in the PEM format, including
        /// the begin/end markers like <c>-----BEGIN CERTIFICATE-----</c>.
        /// </remarks>
        /// <param name="serverCertificate">Public certificate for the server (PEM format).</param>
        /// <param name="serverPrivateKey">Private key for the server (PEM format).</param>
        public void SetServerSecrets(string serverCertificate, string serverPrivateKey)
        {
            m_ServerPrivateKey = serverPrivateKey;
            m_ServerCertificate = serverCertificate;
        }

        /// <summary>Set the client parameters for encryption.</summary>
        /// <remarks>
        /// <para>
        /// If the CA certificate is not provided, validation will be done against the OS/browser
        /// certificate store. This is what you'd want if using certificates from a known provider.
        /// For self-signed certificates, the CA certificate needs to be provided.
        /// </para>
        /// <para>
        /// The CA certificate (if provided) is expected to be in the PEM format, including the
        /// begin/end markers like <c>-----BEGIN CERTIFICATE-----</c>.
        /// </para>
        /// </remarks>
        /// <param name="serverCommonName">Common name of the server (typically hostname).</param>
        /// <param name="caCertificate">CA certificate used to validate the server's authenticity.</param>
        public void SetClientSecrets(string serverCommonName, string caCertificate = null)
        {
            m_ServerCommonName = serverCommonName;
            m_ClientCaCertificate = caCertificate;
        }

        /// <inheritdoc cref="INetworkStreamDriverConstructor.CreateDriver"/>
        public void CreateDriver(
            PurrnityTransport transport,
            out NetworkDriver driver,
            out NetworkPipeline unreliableFragmentedPipeline,
            out NetworkPipeline unreliableSequencedFragmentedPipeline,
            out NetworkPipeline reliableSequencedPipeline)
        {
#if UNITY_WEBGL && !UNITY_EDITOR
            if (m_Driver.Listening && m_ProtocolType != ProtocolType.RelayUnityTransport)
            {
                throw new Exception("WebGL as a server is not supported by Unity Transport, outside the Editor.");
            }
#endif

#if UNITY_SERVER
            if (m_ProtocolType == ProtocolType.RelayUnityTransport)
            {
                if (m_UseWebSockets)
                {
                    Debug.LogError("Transport is configured to use Websockets, but websockets are not available on server builds. Ensure that the \"Use WebSockets\" checkbox is checked under \"Unity Transport\" component.");
                }

                if (m_RelayServerData.IsWebSocket != 0)
                {
                    Debug.LogError("Relay server data indicates usage of WebSockets, but websockets are not available on server builds. Be sure to use \"dtls\" or \"udp\" as the connection type when creating the server data");
                }
            }
#endif

            if (m_ProtocolType == ProtocolType.RelayUnityTransport)
            {
                if (m_UseWebSockets && m_RelayServerData.IsWebSocket == 0)
                {
                    Debug.LogError("Transport is configured to use WebSockets, but Relay server data isn't. Be sure to use \"wss\" as the connection type when creating the server data (instead of \"dtls\" or \"udp\").");
                }

                if (!m_UseWebSockets && m_RelayServerData.IsWebSocket != 0)
                {
                    Debug.LogError("Relay server data indicates usage of WebSockets, but \"Use WebSockets\" checkbox isn't checked under \"Unity Transport\" component.");
                }
            }

            if (m_UseWebSockets)
            {
                driver = NetworkDriver.Create(new WebSocketNetworkInterface(), GetDefaultNetworkSettings());
            }
            else
            {
#if UNITY_WEBGL && !UNITY_EDITOR
                Debug.LogWarning($"WebSockets were used even though they're not selected in NetworkManager. You should check {nameof(UseWebSockets)}', on the Unity Transport component, to silence this warning.");
                driver = NetworkDriver.Create(new WebSocketNetworkInterface(), GetDefaultNetworkSettings());
#else
                driver = NetworkDriver.Create(new UDPNetworkInterface(), GetDefaultNetworkSettings());
#endif
            }

            GetDefaultPipelineConfigurations(
                out var unreliableFragmentedPipelineStages,
                out var unreliableSequencedFragmentedPipelineStages,
                out var reliableSequencedPipelineStages);

            unreliableFragmentedPipeline = driver.CreatePipeline(unreliableFragmentedPipelineStages);
            unreliableSequencedFragmentedPipeline = driver.CreatePipeline(unreliableSequencedFragmentedPipelineStages);
            reliableSequencedPipeline = driver.CreatePipeline(reliableSequencedPipelineStages);
        }

        // -------------- ITransport Methods ---------------------------------------------------------------------------

        public void Listen(ushort port)
        {
            ConnectionData.Port = port;
            Initialize();
            listenerState = ConnectionState.Connecting;
            onConnectionState?.Invoke(listenerState, true);

            if (StartServerTransport())
            {
                listenerState = ConnectionState.Connected;
                onConnectionState?.Invoke(listenerState, true);
            }
            else
            {
                listenerState = ConnectionState.Disconnected;
                onConnectionState?.Invoke(listenerState, true);
            }
        }

        public void StopListening()
        {
            listenerState = ConnectionState.Disconnecting;
            onConnectionState?.Invoke(listenerState, true);
            Shutdown();
            listenerState = ConnectionState.Disconnected;
            onConnectionState?.Invoke(listenerState, true);
        }

        public void Connect(string ip, ushort port)
        {
            SetConnectionData(ip, port);
            Initialize();
            clientState = ConnectionState.Connecting;
            onConnectionState?.Invoke(clientState, false);

            if (!StartClientTransport())
            {
                clientState = ConnectionState.Disconnected;
                onConnectionState?.Invoke(clientState, false);
            }
        }

        public void Disconnect()
        {
            clientState = ConnectionState.Disconnecting;
            onConnectionState?.Invoke(clientState, false);
            DisconnectLocalClient();
            Shutdown();
            clientState = ConnectionState.Disconnected;
            onConnectionState?.Invoke(clientState, false);
        }

        public void SendToClient(Connection target, ByteData data, Channel method = Channel.ReliableOrdered)
        {
            if (listenerState != ConnectionState.Connected) return;
            var clientId = FindClientIdByConnection(target);
            Send(clientId, data.segment, method);
            RaiseDataSent(target, data, true);
        }

        public void SendToServer(ByteData data, Channel method = Channel.ReliableOrdered)
        {
            if (clientState != ConnectionState.Connected) return;
            Send(m_ServerClientId, data.segment, method);
            RaiseDataSent(default, data, false);
        }

        public void CloseConnection(Connection conn)
        {
            var clientId = FindClientIdByConnection(conn);
            DisconnectRemoteClient(clientId);
            _connToClientId.Remove(conn.connectionId);
            _connections.Remove(conn);
            onDisconnected?.Invoke(conn, Transports.DisconnectReason.ServerRequest, true);
        }

        public void ReceiveMessages(float delta)
        {
            OnEarlyUpdate();
        }

        public void SendMessages(float delta)
        {
            OnPostLateUpdate();
        }

        public int GetMTU(Connection target, Channel channel, bool asServer)
        {
            return m_MaxPayloadSize;
        }

        public void RaiseDataReceived(Connection conn, ByteData data, bool asServer)
        {
            onDataReceived?.Invoke(conn, data, asServer);
        }

        public void RaiseDataSent(Connection conn, ByteData data, bool asServer)
        {
            onDataSent?.Invoke(conn, data, asServer);
        }

        protected override void StartServerInternal()
        {
            Listen(ConnectionData.Port);
        }

        protected override void StartClientInternal()
        {
            Initialize();
            clientState = ConnectionState.Connecting;
            onConnectionState?.Invoke(clientState, false);

            if (!StartClientTransport())
            {
                clientState = ConnectionState.Disconnected;
                onConnectionState?.Invoke(clientState, false);
            }
        }

        private ulong FindClientIdByConnection(Connection conn)
        {
            return _connToClientId.TryGetValue(conn.connectionId, out var clientId) ? clientId : default;
        }

        // -------------- Utility Types -------------------------------------------------------------------------------

        /// <summary>
        /// Cached information about reliability mode with a certain client
        /// </summary>
        private struct SendTarget : IEquatable<SendTarget>
        {
            public readonly ulong ClientId;
            public readonly NetworkPipeline NetworkPipeline;

            public SendTarget(ulong clientId, NetworkPipeline networkPipeline)
            {
                ClientId = clientId;
                NetworkPipeline = networkPipeline;
            }

            public bool Equals(SendTarget other)
            {
                return ClientId == other.ClientId && NetworkPipeline.Equals(other.NetworkPipeline);
            }

            public override bool Equals(object obj)
            {
                return obj is SendTarget other && Equals(other);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (ClientId.GetHashCode() * 397) ^ NetworkPipeline.GetHashCode();
                }
            }
        }
    }

    /// <summary>
    /// Utility class to convert Unity Transport error codes to human-readable error messages.
    /// </summary>
    public static class ErrorUtilities
    {
        /// <summary>
        /// Convert a Unity Transport error code to human-readable error message.
        /// </summary>
        /// <param name="error">Unity Transport error code.</param>
        /// <param name="connectionId">ID of connection on which error occurred (unused).</param>
        /// <returns>Human-readable error message.</returns>
        public static string ErrorToString(TransportError error, ulong connectionId)
        {
            return ErrorToFixedString((int)error).ToString();
        }

        internal static FixedString128Bytes ErrorToFixedString(int error)
        {
            switch ((TransportError)error)
            {
                case TransportError.NetworkVersionMismatch:
                case TransportError.NetworkStateMismatch:
                    return "invalid connection state (likely stale/closed connection)";
                case TransportError.NetworkPacketOverflow:
                    return "packet is too large for the transport (likely need to increase MTU)";
                case TransportError.NetworkSendQueueFull:
                    return "send queue full (need to increase 'Max Packet Queue Size' parameter)";
                default:
                    return FixedString.Format("unexpected error code {0}", error);
            }
        }
    }

}
