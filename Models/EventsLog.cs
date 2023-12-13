using System;
using System.Collections.Generic;

namespace CheckAndResume.Models
{
    public partial class EventsLog
    {
        public long EventId { get; set; }
        public string EventTypeId { get; set; } = null!;
        public string EventLevelId { get; set; } = null!;
        public int EventSystemId { get; set; }
        public int EventModuleId { get; set; }
        public string EventObjectId { get; set; } = null!;
        public DateTime EventDatetimeUtc { get; set; }
        public DateTime EventDatetime { get; set; }
        public short EventOffset { get; set; }
        public string EventCode { get; set; } = null!;
        public string EventMessage { get; set; } = null!;
        public string EventInfo { get; set; } = null!;
        public int PartitionId { get; set; }
    }
}
