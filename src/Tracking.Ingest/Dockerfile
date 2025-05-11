# syntax=docker/dockerfile:1
# Build from the repository root: docker build -f src/Tracking.Ingest/Dockerfile .

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /source

# Restore first, from project files only, so the package layer is reused until a dependency changes.
COPY global.json Directory.Build.props Directory.Packages.props .editorconfig ./
COPY src/Tracking.Core/Tracking.Core.csproj src/Tracking.Core/
COPY src/Tracking.Contracts/Tracking.Contracts.csproj src/Tracking.Contracts/
COPY src/Tracking.Infrastructure/Tracking.Infrastructure.csproj src/Tracking.Infrastructure/
COPY src/Tracking.Ingest/Tracking.Ingest.csproj src/Tracking.Ingest/
RUN dotnet restore src/Tracking.Ingest/Tracking.Ingest.csproj

COPY src/ src/
RUN dotnet publish src/Tracking.Ingest/Tracking.Ingest.csproj --configuration Release --no-restore --output /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
# gRPC (HTTP/2 only) on 8080, health probes (HTTP/1.1) on 8081.
# The base image sets ASPNETCORE_HTTP_PORTS=8080; clear it so the explicit endpoints below are the only ones.
ENV ASPNETCORE_HTTP_PORTS=
ENV Kestrel__Endpoints__Grpc__Url=http://+:8080 \
    Kestrel__Endpoints__Health__Url=http://+:8081
EXPOSE 8080 8081
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "Tracking.Ingest.dll"]
