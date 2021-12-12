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
public class Peer {
    private readonly IConsumer<Null, string> _consumer;
    private readonly IProducer<Null, string> _producer;
    private readonly string _id;
    private Thread _consumeThread;
    public bool active = true;

    public Dictionary<string, int> DevicePorts { get; }
    public List<Measurement> Measurements { get; set; } = new List<Measurement>( );
    public System.Net.Sockets.UdpClient Client { get; private set; }
    public Queue<ConsumeResult<Null, string>> Q { get; } = new Queue<ConsumeResult<Null, string>>( );
    public Peer( ) {
        DevicePorts = new Dictionary<string, int>( );
        _id = Help.NameGenerator( Random.Shared.Next( ) );
        var configConsume = new Confluent.Kafka.ConsumerConfig {
            BootstrapServers = Help.KafkaUri,
            GroupId = _id,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        var config = new ProducerConfig {
            BootstrapServers = Help.KafkaUri,
            ClientId = _id
        };
        _consumer = new ConsumerBuilder<Null, string>( configConsume ).Build( );
        _producer = new ProducerBuilder<Null, string>( config ).Build( );

        L.Log( $"My name is {_id}" );
    }
    public async Task KConsumer( ) {
        _consumeThread = new Thread( ( ) => {
            _consumer.Subscribe( new[] { nameof( Messages.PortNumber ), nameof( Messages.NewPeer ), nameof( Messages.RmPeer ) } );
            _producer.Produce( nameof( Messages.Register ), new Message<Null, string> { Value = _id } );
            L.Log( "Started consume loop" );
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
                nameof(Messages.NewPeer) => NewPeer(con.Message.Value),
                nameof(Messages.RmPeer) => RmPeer(con.Message.Value),
                nameof(Messages.PortNumber) => SetupUdp(con.Message.Value),
                _ => throw new NotImplementedException(con.Topic)
            };
        }
    }
    private async Task KPing( ) {
        while ( active ) {
            try {
                if ( Client is not null )
                    _producer.Produce( nameof( Messages.Ping ), new Message<Null, string> { Value = _id } );
            } catch ( Exception ex ) {
                L.Log( ex );
            }
            await Task.Delay( 1000 );
        }
    }
    public async Task Start( ) {
        await KConsumer( );
        await Task.WhenAll( new[] { StartListeningUdp( ), StartSendingUdp( ), KHandlers( ), KPing( ) } );
    }

    public async Task StartListeningUdp( ) {
        while ( active ) {
            try {
                if ( Client is not null ) {
                    var r = await Client.ReceiveAsync( );
                    L.Log( "Udp got something" );
                    var newM = r.Buffer.ToMeasurment( );
                    if ( !DevicePorts.ContainsKey( newM.DevId ) )
                        DevicePorts.Add( newM.DevId, r.RemoteEndPoint.Port );
                    Measurements.Add( newM );
                    L.Log( newM );
                }
            } catch ( Exception ex ) {
                L.Log( ex );
            }
            await Task.Delay( 100 );
        }
    }
    public async Task StartSendingUdp( ) {
        while ( active ) {
            if ( Client is not null ) {
                var m = new Measurement( _id, 100 ).ToByteArray( ).AsMemory( );
                foreach ( var d in DevicePorts ) {
                    L.Log( $"Sending message to {d.Key} on port {d.Value}" );
                    await Client.SendAsync( m, "127.0.0.1", d.Value );
                }
            }
            await Task.Delay( 1000 );
        }
    }
    private object SetupUdp( string value ) {
        L.Log( $"Udp: {value}" );
        var split = value.Split('^');
        var (id, port) = (split[0], int.Parse( split[1] ));
        if ( id == _id )
            Client = new System.Net.Sockets.UdpClient( port );
        return 1;
    }
    private object RmPeer( string value ) {
        L.Log( $"Remve peer {value}" );
        DevicePorts.Remove( value );
        return 1;
    }
    private object NewPeer( string value ) {
        var (did, port) = (value.Split( "^" )[0], int.Parse( value.Split( "^" )[1] ));
        if ( did == _id )
            return 1;
        L.Log( $"New peer {value}" );
        DevicePorts.Add( did, port );
        return 1;
    }
}
