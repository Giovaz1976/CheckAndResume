using System;
using System.Collections.Generic;

namespace CheckAndResume.Models
{
    public partial class CatNotResponse
    {
        public CatNotResponse()
        {
            TabNotifications = new HashSet<TabNotification>();
        }

        public int IdResp { get; set; }
        public int NotResponse { get; set; }
        public string? ResponseDesc { get; set; }

        public virtual ICollection<TabNotification> TabNotifications { get; set; }
    }
}
