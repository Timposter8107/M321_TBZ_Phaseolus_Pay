using System.Diagnostics;
using System.Text.Json;
using OpenTelemetry.Trace;
using PayBeancoins.HealthChecks;
using PayBeancoins.Messaging;
using PayBeancoins.Models;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

// otel-tracing plan Step 2/Q4: surfaces the active trace/span ID in log scopes
// (Console:IncludeScopes in appsettings.json renders them) so a trace can be
// cross-referenced against kubectl logs / Loki without a UI click-through.
builder.Logging.Configure(options =>
{
    options.ActivityTrackingOptions = ActivityTrackingOptions.TraceId
        | ActivityTrackingOptions.SpanId
        | ActivityTrackingOptions.ParentId;
});

// otel-tracing plan Step 2: AddSource("PayBeancoins") picks up both the manual
// consume-side span (EventConsumerHostedService.cs, linked to OrderBeans'
// publish span via the propagated traceparent header) and the publish-side
// spans for payment.succeeded/payment.failed (EventPublisher.cs, same pattern
// as OrderBeans'). OTEL_SERVICE_NAME/OTEL_EXPORTER_OTLP_ENDPOINT are set as env
// vars in deploy/deployment.yaml, read automatically by AddOtlpExporter.
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddSource("PayBeancoins")
        .AddOtlpExporter());

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.SectionName));
builder.Services.AddSingleton<EventPublisher>();

var paymentsSucceeded = Metrics.CreateCounter("payments_succeeded_total", "Total number of successful payments.");
var paymentsFailed = Metrics.CreateCounter("payments_failed_total", "Total number of failed payments.");

builder.Services.AddSingleton<IReadOnlyList<ConsumerBinding>>(sp =>
{
    var publisher = sp.GetRequiredService<EventPublisher>();
    var logger = sp.GetRequiredService<ILogger<Program>>();
    return new List<ConsumerBinding>
    {
        new("pay.incoming-orders", "order.created", envelope => HandleOrderCreatedAsync(envelope, publisher, paymentsSucceeded, paymentsFailed, logger)),
    };
});
builder.Services.AddHostedService<EventConsumerHostedService>();

builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy(), tags: ["live"])
    .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready"]);

var app = builder.Build();

app.UseHttpMetrics();
app.MapMetrics();

app.MapHealthChecks("/healthz/live", new() { Predicate = c => c.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new() { Predicate = c => c.Tags.Contains("ready") });

app.Run();

// Deterministic payment mock (architecture plan Q8): any item named "Rotten Beans"
// (case-insensitive) fails the payment; everything else succeeds.
static Task HandleOrderCreatedAsync(EventEnvelope envelope, EventPublisher publisher, Counter paymentsSucceeded, Counter paymentsFailed, ILogger<Program> logger)
{
    var payload = envelope.Payload.Deserialize<OrderCreatedPayload>()!;
    var isRotten = payload.Items.Any(i => string.Equals(i.Name, "Rotten Beans", StringComparison.OrdinalIgnoreCase));

    if (isRotten)
    {
        var failedPayload = new PaymentFailedPayload("Simulated failure: Rotten Beans");
        var failedEnvelope = envelope with
        {
            EventId = Guid.NewGuid(),
            EventType = "payment.failed",
            OccurredAt = DateTimeOffset.UtcNow,
            Payload = JsonSerializer.SerializeToElement(failedPayload),
        };
        publisher.Publish("payment.failed", failedEnvelope);
        paymentsFailed.Inc();
        logger.LogInformation("Payment failed for order {OrderId}", envelope.OrderId);
    }
    else
    {
        var succeededPayload = new PaymentSucceededPayload(payload.TotalAmount, payload.Items);
        var succeededEnvelope = envelope with
        {
            EventId = Guid.NewGuid(),
            EventType = "payment.succeeded",
            OccurredAt = DateTimeOffset.UtcNow,
            Payload = JsonSerializer.SerializeToElement(succeededPayload),
        };
        publisher.Publish("payment.succeeded", succeededEnvelope);
        paymentsSucceeded.Inc();
        logger.LogInformation("Payment succeeded for order {OrderId}", envelope.OrderId);
    }

    return Task.CompletedTask;
}
