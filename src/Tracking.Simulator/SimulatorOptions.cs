using System.ComponentModel.DataAnnotations;

namespace Tracking.Simulator;

public sealed class SimulatorOptions
{
    public const string SectionName = "Simulator";

    /// <summary>gRPC address of the ingest service.</summary>
    [Required]
    public Uri IngestUrl { get; set; } = new("http://localhost:5100");

    /// <summary>When set, demo geofences around the simulation area are created through the REST API first.</summary>
    public Uri? ApiUrl { get; set; }

    [Range(1, 100_000)]
    public int Devices { get; set; } = 10;

    [Required]
    public string DevicePrefix { get; set; } = "sim";

    [Range(typeof(TimeSpan), "00:00:00.010", "01:00:00")]
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>How long to run; zero runs until the process is stopped.</summary>
    [Range(typeof(TimeSpan), "00:00:00", "30.00:00:00")]
    public TimeSpan Duration { get; set; } = TimeSpan.Zero;

    [Range(-90, 90)]
    public double CenterLatitude { get; set; } = 52.3731;

    [Range(-180, 180)]
    public double CenterLongitude { get; set; } = 4.8926;

    /// <summary>Devices start inside, and turn back towards the centre when they leave, this radius.</summary>
    [Range(10, 1_000_000)]
    public double AreaRadiusMeters { get; set; } = 3_000;

    [Range(0, 100)]
    public double MinSpeedMetersPerSecond { get; set; } = 3;

    [Range(0, 100)]
    public double MaxSpeedMetersPerSecond { get; set; } = 15;

    /// <summary>Random seed, so a run can be reproduced; unset means a different run each time.</summary>
    public int? Seed { get; set; }
}
