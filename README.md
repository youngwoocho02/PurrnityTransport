# [Purrnity Transport](https://github.com/youngwoocho02/PurrnityTransport)

A transport adapter that bridges Unity's low-level [`Unity Transport Package`](https://docs-multiplayer.unity3d.com/transport/current/about/) (UTP) with the [`PurrNet`](https://purrnet.dev) networking framework. This allows PurrNet to use Unity's `NetworkDriver` for UDP, WebSocket, and Unity Relay connections.

> **Note:** This package is a near **1:1 adaptation** of the [`Unity Transport`](https://docs-multiplayer.unity3d.com/transport/current/about/) for [`Netcode for GameObjects`](https://docs-multiplayer.unity3d.com/netcode/current/about/). The core transport logic (NetworkDriver, pipelines, batching, relay, encryption) is preserved as-is, with only the interface layer adapted to work with PurrNet's `ITransport`.

## Features

*   **Direct UDP/WebSocket connections** via Unity Transport's `NetworkDriver`
*   **Unity Relay support** for NAT traversal and player privacy
*   **Encryption support** (DTLS for UDP, TLS for WebSocket)
*   **Batched message sending/receiving** for optimal bandwidth usage
*   **Configurable network parameters** (timeouts, packet queue sizes, payload sizes)
*   **Custom driver constructor** support via `INetworkStreamDriverConstructor`
*   **Command line overrides** for port and IP address (`-port`, `-ip`)

## Prerequisites

*   **[`Unity 6.0`](https://docs.unity3d.com/6000.0/Documentation/Manual/index.html)** or newer
*   **[`PurrNet`](https://purrnet.dev)**: PurrNet networking framework
*   **[`Unity Transport`](https://docs-multiplayer.unity3d.com/transport/current/about/)** (`com.unity.transport` version 2.0 or newer)

## Installation

1.  **Install Unity Transport**: Using the Unity Package Manager, install the [`Unity Transport`](https://docs-multiplayer.unity3d.com/transport/current/about/) package from the Unity Registry.
2.  **Install PurrNet**: Add the PurrNet package to your project.
3.  **Install this package**: Add the package via Git URL in the Package Manager:
    ```
    https://github.com/youngwoocho02/PurrnityTransport.git
    ```

## Setup

1.  Add a `PurrnityTransport` component to your `NetworkManager` GameObject.
2.  Assign it as the transport in PurrNet's `NetworkManager`.
3.  Configure connection settings (address, port) in the Inspector or via code.

## Usage

**Basic Server/Client**
```csharp
var transport = GetComponent<PurrnityTransport>();

// Server
transport.Listen(7777);

// Client
transport.Connect("127.0.0.1", 7777);
```

**Direct Connection with Custom Address**
```csharp
var transport = GetComponent<PurrnityTransport>();
transport.SetConnectionData("192.168.1.100", 7777, "0.0.0.0");
```

**Unity Relay**
```csharp
var transport = GetComponent<PurrnityTransport>();

// Host/Server
transport.SetRelayServerData(relayServerData);

// Client
transport.SetRelayServerData(relayClientData);
```

**Encryption**
```csharp
var transport = GetComponent<PurrnityTransport>();
transport.UseEncryption = true;

// Server
transport.SetServerSecrets(certificate, privateKey);

// Client
transport.SetClientSecrets(serverCommonName, caCertificate);
```

## Inspector Properties

| Property | Description |
|----------|-------------|
| Protocol Type | `UnityTransport` (direct) or `RelayUnityTransport` (relay) |
| Use WebSockets | Use WebSocket instead of UDP |
| Use Encryption | Enable DTLS/TLS encryption |
| Max Packet Queue Size | Max packets in send/receive queues per frame |
| Max Payload Size | Max unreliable payload size |
| Heartbeat Timeout MS | Heartbeat interval when idle |
| Connect Timeout MS | Timeout between connection attempts |
| Max Connect Attempts | Max connection attempts before disconnect |
| Disconnect Timeout MS | Inactivity timeout for disconnection |

## License

This project is distributed under the MIT License. See the `LICENSE` file for more information.

## Related Resources

*   [PurrNet Documentation](https://purrnet.dev)
*   [Unity Transport Documentation](https://docs-multiplayer.unity3d.com/transport/current/about/)
*   [Unity Relay Documentation](https://docs.unity.com/ugs/manual/relay/manual/get-started)
