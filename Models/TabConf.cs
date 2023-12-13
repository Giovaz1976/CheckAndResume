using System;
using System.Collections.Generic;

namespace CheckAndResume.Models
{
    public partial class TabConf
    {
        public int IdConf { get; set; }
        public string ConnReadConf { get; set; } = null!;
        public string ConfFrom { get; set; } = null!;
        public string ToConf { get; set; } = null!;
        public string SmtpConf { get; set; } = null!;
        public long PortConf { get; set; }
        public string UsernameConf { get; set; } = null!;
        public string PasswordConf { get; set; } = null!;
        public int ReadIntervalConf { get; set; }
        public int ResumeIntervalResume { get; set; }
        public DateTime CreationTimeConf { get; set; }
        public DateTime UpdateTimeConf { get; set; }
        public string ConnInsertConf { get; set; } = null!;
        public string ConnResumeConf { get; set; } = null!;
        public int? ReadIntervalResume { get; set; }
        public string? ApiRoot { get; set; }
    }
}
