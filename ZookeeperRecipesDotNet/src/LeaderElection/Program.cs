using Serilog;
using LeaderElection;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog(logger);

builder.Services.AddSingleton<LeaderElection.LeaderElection>();
builder.Services.AddSingleton<IElectionHandler, ElectionHandler>();
builder.Services.AddHostedService<ElectionHandler>();

var app = builder.Build();

app.MapGet("/leader", (IElectionHandler electionHandler) =>
{
    var isLeader = electionHandler.IsLeader();
    return Results.Json(new { isLeader });
});

app.Run();