using Unity.Networking.Transport;

namespace PurrNet.Purrnity
{
    /// <summary>
    /// <para>
    /// This interface allows one to override the creation of the <see cref="NetworkDriver"/> object
    /// that will be used under the hood by <see cref="PurrnityTransport"/>. This can be useful when
    /// implementing a custom <see cref="INetworkInterface"/> or to add custom pipeline stages to
    /// the default pipelines.
    /// </para>
    /// <para>
    /// To use a custom driver constructor, set <see cref="PurrnityTransport.s_DriverConstructor"/> to
    /// an instance of an implementation of this interface. This must be done before calling
    /// StartClient or StartServer.
    /// </para>
    /// </summary>
    public interface INetworkStreamDriverConstructor
    {
        /// <summary>
        /// Creates the <see cref="NetworkDriver"/> instance to be used by the transport.
        /// </summary>
        /// <param name="transport">The transport for which the driver is created.</param>
        /// <param name="driver">The newly-created <see cref="NetworkDriver"/>.</param>
        /// <param name="unreliableFragmentedPipeline">
        /// The driver's pipeline on which to send unreliable traffic. This pipeline must also
        /// support fragmentation (payloads larger than the MTU).
        /// </param>
        /// <param name="unreliableSequencedFragmentedPipeline">
        /// The driver's pipeline on which to send unreliable but sequenced traffic. Traffic sent
        /// on this pipeline must be delivered in the right order, although packet loss is okay.
        /// This pipeline must also support fragmentation (payloads larger than the MTU).
        /// </param>
        /// <param name="reliableSequencedPipeline">
        /// The driver's pipeline on which to send reliable traffic. This pipeline must ensure that
        /// all of its traffic is delivered, and in the correct order too. There is no need for that
        /// pipeline to support fragmentation.
        /// </param>
        public void CreateDriver(
            PurrnityTransport transport,
            out NetworkDriver driver,
            out NetworkPipeline unreliableFragmentedPipeline,
            out NetworkPipeline unreliableSequencedFragmentedPipeline,
            out NetworkPipeline reliableSequencedPipeline);
    }
}
