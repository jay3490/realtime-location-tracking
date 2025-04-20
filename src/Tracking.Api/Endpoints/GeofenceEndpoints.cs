using System.Text.Json;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Tracking.Core;
using Tracking.Core.Abstractions;
using Tracking.Core.Geofencing;

namespace Tracking.Api.Endpoints;

public static class GeofenceEndpoints
{
    public static RouteGroupBuilder MapGeofenceEndpoints(this IEndpointRouteBuilder routes)
    {
        var group = routes.MapGroup("/api/geofences").WithTags("Geofences");
        group.MapGet("/", ListAsync);
        group.MapGet("/{id}", GetAsync).WithName("GetGeofence");
        group.MapPost("/", CreateAsync);
        group.MapPut("/{id}", UpsertAsync);
        group.MapDelete("/{id}", DeleteAsync);
        return group;
    }

    private static async Task<Ok<IReadOnlyList<Geofence>>> ListAsync(IGeofenceStore store, CancellationToken cancellationToken) =>
        TypedResults.Ok(await store.GetAllAsync(cancellationToken));

    private static async Task<Results<Ok<Geofence>, NotFound>> GetAsync(string id, IGeofenceStore store, CancellationToken cancellationToken)
    {
        var geofence = Geofence.IsValidId(id) ? await store.GetAsync(id, cancellationToken) : null;
        return geofence is null ? TypedResults.NotFound() : TypedResults.Ok(geofence);
    }

    /// <summary>Creates a geofence; the id is generated when the body omits it.</summary>
    private static async Task<Results<CreatedAtRoute<Geofence>, Conflict<ProblemDetails>, ValidationProblem>> CreateAsync(
        HttpRequest request,
        IGeofenceStore store,
        CancellationToken cancellationToken)
    {
        var (geofence, problem) = await ReadAsync(request, cancellationToken);
        if (problem is not null)
        {
            return problem;
        }

        if (string.IsNullOrEmpty(geofence!.Id))
        {
            geofence = geofence with { Id = $"gf-{Guid.NewGuid():N}"[..19] };
        }

        if (Validate(geofence) is { } invalid)
        {
            return invalid;
        }

        if (!await store.TryCreateAsync(geofence, cancellationToken))
        {
            return TypedResults.Conflict(new ProblemDetails
            {
                Title = "Geofence already exists",
                Detail = $"A geofence with id '{geofence.Id}' already exists; use PUT to replace it.",
                Status = StatusCodes.Status409Conflict,
            });
        }

        return TypedResults.CreatedAtRoute(geofence, "GetGeofence", new { id = geofence.Id });
    }

    /// <summary>Creates or replaces the geofence with the id in the route.</summary>
    private static async Task<Results<Ok<Geofence>, CreatedAtRoute<Geofence>, ValidationProblem>> UpsertAsync(
        string id,
        HttpRequest request,
        IGeofenceStore store,
        CancellationToken cancellationToken)
    {
        var (geofence, problem) = await ReadAsync(request, cancellationToken);
        if (problem is not null)
        {
            return problem;
        }

        if (!string.IsNullOrEmpty(geofence!.Id) && geofence.Id != id)
        {
            return TypedResults.ValidationProblem(new Dictionary<string, string[]> { ["id"] = ["must match the id in the URL or be omitted"] });
        }

        geofence = geofence with { Id = id };
        if (Validate(geofence) is { } invalid)
        {
            return invalid;
        }

        var created = await store.UpsertAsync(geofence, cancellationToken);
        return created
            ? TypedResults.CreatedAtRoute(geofence, "GetGeofence", new { id })
            : TypedResults.Ok(geofence);
    }

    private static async Task<Results<NoContent, NotFound>> DeleteAsync(string id, IGeofenceStore store, CancellationToken cancellationToken) =>
        Geofence.IsValidId(id) && await store.DeleteAsync(id, cancellationToken)
            ? TypedResults.NoContent()
            : TypedResults.NotFound();

    /// <summary>
    /// Reads the polymorphic body by hand so that a missing or unknown "type" is reported as a
    /// validation problem rather than surfacing as a serializer exception.
    /// </summary>
    private static async Task<(Geofence? Geofence, ValidationProblem? Problem)> ReadAsync(HttpRequest request, CancellationToken cancellationToken)
    {
        try
        {
            var geofence = await JsonSerializer.DeserializeAsync<Geofence>(request.Body, TrackingJson.Options, cancellationToken);
            return geofence is null
                ? (null, BodyProblem("a geofence object is required"))
                : (geofence, null);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            return (null, BodyProblem("must be a JSON geofence with \"type\" set to \"circle\" or \"polygon\""));
        }
    }

    private static ValidationProblem? Validate(Geofence geofence)
    {
        var errors = geofence.Validate();
        return errors.Count == 0
            ? null
            : TypedResults.ValidationProblem(new Dictionary<string, string[]> { ["geofence"] = [.. errors] });
    }

    private static ValidationProblem BodyProblem(string message) =>
        TypedResults.ValidationProblem(new Dictionary<string, string[]> { ["body"] = [message] });
}
