using Tracking.Core.Geo;

namespace Tracking.Simulator;

/// <summary>
/// A device wandering around an area: constant speed, a heading that drifts a little each step,
/// and a turn back towards the centre whenever it strays outside the area.
/// </summary>
public sealed class SimulatedDevice
{
    private const double MaxHeadingDriftDegrees = 15;

    private readonly GeoPoint _center;
    private readonly double _areaRadiusMeters;
    private readonly Random _random;

    public SimulatedDevice(string id, GeoPoint center, double areaRadiusMeters, double speedMetersPerSecond, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);
        Id = id;
        _center = center;
        _areaRadiusMeters = areaRadiusMeters;
        _random = random;
        SpeedMetersPerSecond = speedMetersPerSecond;

        // Uniform over the disc: radius scales with the square root of a uniform sample.
        var startDistance = areaRadiusMeters * Math.Sqrt(random.NextDouble());
        Position = GeoMath.Destination(center, random.NextDouble() * 360, startDistance);
        HeadingDegrees = random.NextDouble() * 360;
    }

    public string Id { get; }

    public GeoPoint Position { get; private set; }

    public double HeadingDegrees { get; private set; }

    public double SpeedMetersPerSecond { get; }

    public GeoPoint Advance(TimeSpan elapsed)
    {
        if (GeoMath.HaversineMeters(_center, Position) > _areaRadiusMeters)
        {
            HeadingDegrees = GeoMath.InitialBearingDegrees(Position, _center);
        }
        else
        {
            var drift = ((_random.NextDouble() * 2) - 1) * MaxHeadingDriftDegrees;
            HeadingDegrees = (HeadingDegrees + drift + 360) % 360;
        }

        Position = GeoMath.Destination(Position, HeadingDegrees, SpeedMetersPerSecond * elapsed.TotalSeconds);
        return Position;
    }
}
