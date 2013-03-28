namespace RavenDBPerformance
{
    using System;

    public interface ISagaEntity
    {
        Guid Id { get; set; }
    }
}