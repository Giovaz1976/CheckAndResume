using System;
using System.Collections.Generic;

namespace CheckAndResume.Models
{
    public partial class TabNotification
    {
        public int IdNot { get; set; }
        public DateTime NotCreated { get; set; }
        public string NotFrom { get; set; } = null!;
        public string NotTo { get; set; } = null!;
        public int NotType { get; set; }
        public int NotState { get; set; }
        public int? NotResponse { get; set; }
        public DateTime NotUpdated { get; set; }
        public string NotSubject { get; set; } = null!;
        public string NotContent { get; set; } = null!;
        public byte[]? NotFls { get; set; }
        public long? EventId { get; set; }

        public virtual CatNotResponse? NotResponseNavigation { get; set; }
        public virtual CatNotState NotStateNavigation { get; set; } = null!;
        public virtual CatNotype NotTypeNavigation { get; set; } = null!;
    }
}
