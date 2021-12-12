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
using System.Net;
using System.Net.Sockets;


var pid = System.Diagnostics.Process.GetCurrentProcess( ).Id;
System.IO.File.AppendAllLines( "pids.sh", new[] { $"kill {pid}; rm logs/{pid}.log;" } );

L.Log( "----------------" );

var task = args[1] switch {
    //"peers" => StartManyPeers( int.Parse( args[2] ), int.Parse( args[3] ) ),
    "peer" => new Peer( ).Start( ),
    "comander" => new Comander( ).StartCommanding( ),
    _ => throw new NotImplementedException($"|{args[1]}|")
};
await task;

Task StartManyPeers( int n, int killAfter ) {
    var name = System.Environment.GetCommandLineArgs( ).First();
    var tasks = Enumerable.Range( 0, n ).Select( i => {
        var pi = $"{name} peer peer";
        var p = System.Diagnostics.Process.Start( System.Environment.ProcessPath, pi );
        p.Start( );
        return p;
    }).ToList().Select(i => i.WaitForExitAsync());
    return Task.WhenAny(
        Task.Delay( killAfter ),
        Task.WhenAll( tasks ) );
}

enum Messages {
    Register,
    PortNumber,
    NewPeer,
    RmPeer,
    Ping
};
public record DevStatus( DateTime LastPing, int Port );
public record Measurement( string DevId, int Measuerment );
public static class Help {
    public static readonly string KafkaUri = "127.0.0.1:9092";
    public static readonly string LocalIp = "127.0.0.1";
    public static byte[] ToByteArray( this Measurement measurement ) {
        var str = $"{measurement.DevId}^{measurement.Measuerment}";
        return System.Text.Encoding.UTF8.GetBytes( str );
    }
    public static Measurement ToMeasurment( this byte[] measurement ) {
        var str = System.Text.Encoding.UTF8.GetString( measurement );
        var a = str.Split('^');
        return new Measurement( a[0], int.Parse( a[1] ) );
    }
    public static string NameGenerator( int n ) {
        var adjective = new []{"Great", "Minor", "Major", "Random", "Impulsive", "Acausal", "Causal", "Main", "Global", "Local", "Dumb", "Inpractical", "Practical", "Analog", "Digital", "Quantum" };
        var purpuse = new []{"Measuring", "Destroying", "Storing", "Producing", "Reading", "Drinking", "Eating", "Studying", "Programming", "Cooking" };
        var noun = new [] {"Measuring unit", "Device", "Machine", "Human", "Animal", "Plant", "Computer", "Drinker", "Phone", "Sensor", "Arduino", "Search engine"};
        var totalNumber = adjective.Length * adjective.Length * purpuse.Length * noun.Length;
        n = n % totalNumber;
        var ret = adjective[n % adjective.Length] + " ";
        n /= adjective.Length;
        ret += adjective[n % adjective.Length] + ' ';
        n /= adjective.Length;
        ret += purpuse[n % purpuse.Length] + ' ';
        n /= purpuse.Length;
        ret += noun[n % noun.Length];
        return ret;
    }
}

public static class L {
    public static void Log( object obj ) {
        var pid = System.Diagnostics.Process.GetCurrentProcess( ).Id;
        var fileName = System.IO.Path.Combine("logs", $"{pid}.log");
        //if (!System.IO.File.Exists(fileName)) {
        //    var using = System.IO.File.Create(fileName);
        //}
        System.IO.File.AppendAllText( fileName, $"{obj}\n" );
    }
}