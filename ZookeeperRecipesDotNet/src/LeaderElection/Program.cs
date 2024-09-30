using Serilog;
using LeaderElection;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog(logger);

// Register ElectionHandler as a hosted service
builder.Services.AddHostedService<ElectionHandler>();

var app = builder.Build();

app.MapGet("/leader", (IElectionHandler electionHandler) =>
{
    var isLeader = electionHandler.IsLeader();
    return Results.Json(new { isLeader });
});

app.Run();