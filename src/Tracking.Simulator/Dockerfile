# syntax=docker/dockerfile:1
# Build from the repository root: docker build -f src/Tracking.Simulator/Dockerfile .

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /source

# Restore first, from project files only, so the package layer is reused until a dependency changes.
COPY global.json Directory.Build.props Directory.Packages.props .editorconfig ./
COPY src/Tracking.Core/Tracking.Core.csproj src/Tracking.Core/
COPY src/Tracking.Contracts/Tracking.Contracts.csproj src/Tracking.Contracts/
COPY src/Tracking.Simulator/Tracking.Simulator.csproj src/Tracking.Simulator/
RUN dotnet restore src/Tracking.Simulator/Tracking.Simulator.csproj

COPY src/ src/
RUN dotnet publish src/Tracking.Simulator/Tracking.Simulator.csproj --configuration Release --no-restore --output /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "Tracking.Simulator.dll"]
