using System;
using System.Collections.Generic;

namespace CheckAndResume.Models
{
    public partial class CatNotype
    {
        public CatNotype()
        {
            TabNotifications = new HashSet<TabNotification>();
        }

        public int IdType { get; set; }
        public int NotType { get; set; }
        public string NotTypeDesc { get; set; } = null!;

        public virtual ICollection<TabNotification> TabNotifications { get; set; }
    }
}
