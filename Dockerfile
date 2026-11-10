FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG PROJECT
WORKDIR /src
COPY . .
RUN dotnet publish $PROJECT/$PROJECT.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0
ARG PROJECT
ENV APP_DLL=$PROJECT.dll
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["sh", "-c", "dotnet $APP_DLL"]
