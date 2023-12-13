using System;
using System.Collections.Generic;

namespace CheckAndResume.Models
{
    public partial class CatNotState
    {
        public CatNotState()
        {
            TabNotifications = new HashSet<TabNotification>();
        }

        public int IdStat { get; set; }
        public int NotState { get; set; }
        public string StatDescription { get; set; } = null!;

        public virtual ICollection<TabNotification> TabNotifications { get; set; }
    }
}
