using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Tracking.Analytics.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialAnalyticsSchema : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "device_daily_distance",
                columns: table => new
                {
                    DeviceId = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                    Day = table.Column<DateOnly>(type: "date", nullable: false),
                    DistanceMeters = table.Column<double>(type: "double precision", nullable: false),
                    Positions = table.Column<int>(type: "integer", nullable: false),
                    FirstRecordedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    LastRecordedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_device_daily_distance", x => new { x.DeviceId, x.Day });
                });

            migrationBuilder.CreateTable(
                name: "dwell_events",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    DeviceId = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                    GeofenceId = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                    GeofenceName = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    EnteredAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ExitedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    DurationSeconds = table.Column<double>(type: "double precision", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_dwell_events", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "geofence_visits",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    DeviceId = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                    GeofenceId = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                    GeofenceName = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    EnteredAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ExitedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_geofence_visits", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "processed_events",
                columns: table => new
                {
                    EventId = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    ProcessedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_processed_events", x => x.EventId);
                });

            migrationBuilder.CreateIndex(
                name: "IX_dwell_events_DeviceId_GeofenceId_EnteredAt",
                table: "dwell_events",
                columns: new[] { "DeviceId", "GeofenceId", "EnteredAt" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_dwell_events_ExitedAt",
                table: "dwell_events",
                column: "ExitedAt");

            migrationBuilder.CreateIndex(
                name: "IX_geofence_visits_DeviceId_GeofenceId_EnteredAt",
                table: "geofence_visits",
                columns: new[] { "DeviceId", "GeofenceId", "EnteredAt" });

            migrationBuilder.CreateIndex(
                name: "IX_geofence_visits_DeviceId_GeofenceId_ExitedAt",
                table: "geofence_visits",
                columns: new[] { "DeviceId", "GeofenceId", "ExitedAt" });

            migrationBuilder.CreateIndex(
                name: "IX_processed_events_ProcessedAt",
                table: "processed_events",
                column: "ProcessedAt");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "device_daily_distance");

            migrationBuilder.DropTable(
                name: "dwell_events");

            migrationBuilder.DropTable(
                name: "geofence_visits");

            migrationBuilder.DropTable(
                name: "processed_events");
        }
    }
}
