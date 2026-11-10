namespace Common;

public record RepairOrder(Guid Id, string WellId, string[] WorkTypes, DateTime CreatedAt);

public record WorkPart(Guid OrderId, string WellId, string WorkType, int TotalParts);

public record Estimate(Guid OrderId, string WellId, string WorkType, decimal Cost, int Days, int TotalParts, string Executor);

public record Summary(Guid OrderId, string WellId, List<Estimate> Estimates, decimal TotalCost, int TotalDays);
