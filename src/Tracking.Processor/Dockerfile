# syntax=docker/dockerfile:1
# Build from the repository root: docker build -f src/Tracking.Processor/Dockerfile .

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /source

# Restore first, from project files only, so the package layer is reused until a dependency changes.
COPY global.json Directory.Build.props Directory.Packages.props .editorconfig ./
COPY src/Tracking.Core/Tracking.Core.csproj src/Tracking.Core/
COPY src/Tracking.Infrastructure/Tracking.Infrastructure.csproj src/Tracking.Infrastructure/
COPY src/Tracking.Kafka/Tracking.Kafka.csproj src/Tracking.Kafka/
COPY src/Tracking.Processor/Tracking.Processor.csproj src/Tracking.Processor/
RUN dotnet restore src/Tracking.Processor/Tracking.Processor.csproj

COPY src/ src/
RUN dotnet publish src/Tracking.Processor/Tracking.Processor.csproj --configuration Release --no-restore --output /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
# HTTP is used only for health probes and metrics.
# The base image sets ASPNETCORE_HTTP_PORTS=8080; clear it so the explicit endpoints below are the only ones.
ENV ASPNETCORE_HTTP_PORTS=
ENV Urls=http://+:8080
EXPOSE 8080
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "Tracking.Processor.dll"]
