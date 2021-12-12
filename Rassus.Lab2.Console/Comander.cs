/*
 * There will be different topics and the argumes will be defined as:
 * "arg1^arg2^arg2"
 * Register:
 *  <dev-id>
 * PortNumber:
 *  <dev-id>^<port>
 * NewPeer:
 *  <dev-id>^<port>
 * RmPeer:
 *  <dev-id>
 * Ping:
 *  <dev-id>
 */
/* prog.exe peer
 * prog.exe peers <n> <killAfterMilliSeconds>
 */
using Confluent.Kafka;
public class Comander {
    public Queue<string> UnassignedDevices { get; } = new Queue<string>( );
    public Dictionary<string, DevStatus> AssignedDevices { get; } = new Dictionary<string, DevStatus>();
    public Queue<int> UnassignedPorts { get; } = new Queue<int>( );

    private readonly ConsumerConfig _configConsume;
    private readonly ProducerConfig _config;
    private readonly IConsumer<Null, string> _consumer;
    private readonly IProducer<Null, string> _producer;
    private Thread _consumeThread;
    private bool active = true;
    private Queue<ConsumeResult<Null, string>> Q = new Queue<ConsumeResult<Null, string>>();

    public Comander( ) {
        UnassignedPorts = new Queue<int>( Enumerable.Range( 10, 100 ).Select( i => 10000 + i ) );
        AssignedDevices = new Dictionary<string, DevStatus>( );
        UnassignedDevices = new Queue<string>( );

        _configConsume = new Confluent.Kafka.ConsumerConfig {
            BootstrapServers = Help.KafkaUri,
            GroupId = $"1{DateTime.Now.Ticks}",
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        _config = new ProducerConfig {
            BootstrapServers = Help.KafkaUri,
            ClientId = Help.LocalIp
        };
        _consumer = new ConsumerBuilder<Null, string>( _configConsume ).Build( );
        _producer = new ProducerBuilder<Null, string>( _config ).Build( );
    }
    public void KConsumer( ) {
        _consumeThread = new Thread( ( ) => {
            _consumer.Subscribe( new[] { nameof( Messages.Register ), nameof( Messages.Ping ) } );
            while ( active ) {
                try {
                    Q.Enqueue( _consumer.Consume( ) );
                } catch ( Exception ex ) {
                    L.Log( ex );
                }
            }
        } );
        _consumeThread.Start( );
    }
    public async Task KHandlers( ) {
        while ( active ) {
            if ( !Q.Any( ) ) {
                await Task.Delay( 1000 );
                continue;
            }
            var con = Q.Dequeue();
            var res = con.Topic switch {
                nameof( Messages.Register ) => Register( con.Message.Value ),
                nameof( Messages.Ping ) => Ping( con.Message.Value ),
                _ => throw new NotImplementedException( con.Topic )
            };
        }
    }
    public async Task StartCommanding( ) {
        KConsumer( );
        await KHandlers( );
    }
    public bool Register( string devId ) {
        L.Log( $"Register: {devId}" );
        var port = this.UnassignedPorts.Dequeue();
        this.AssignedDevices.Add( devId, new DevStatus( DateTime.Now, port ) );
        Task.Delay( 1000 ).Wait( );
        _producer.Produce( nameof( Messages.PortNumber ), new Message<Null, string> { Value = $"{devId}^{port}" } );
        _producer.Produce( nameof( Messages.NewPeer ), new Message<Null, string> { Value = $"{devId}^{port}" } );
        Ping( devId );
        return true;
    }
    public bool Ping( string devId ) {
        L.Log( $"Ping: {devId}" );
        if ( AssignedDevices.ContainsKey( devId ) )
            AssignedDevices[devId] = AssignedDevices[devId] with { LastPing = DateTime.Now };
        var toRemove = AssignedDevices.Where(i => (DateTime.Now - i.Value.LastPing).TotalSeconds > 5.0 ).ToList();
        if ( toRemove.Any( ) ) {
            
            foreach ( var r in toRemove ) {
                L.Log( $"Removing: {r.Key}" );
                AssignedDevices.Remove( r.Key );
                UnassignedPorts.Enqueue( r.Value.Port );
                _producer.Produce( nameof( Messages.RmPeer ), new Message<Null, string> { Value = r.Key } );
            }
        }
        return true;
    }
}
