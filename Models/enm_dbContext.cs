using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CheckAndResume.Models
{
    public partial class enm_dbContext : DbContext
    {
        public enm_dbContext()
        {
        }

        public enm_dbContext(DbContextOptions<enm_dbContext> options)
            : base(options)
        {
        }

        public virtual DbSet<CatNotResponse> CatNotResponses { get; set; } = null!;
        public virtual DbSet<CatNotState> CatNotStates { get; set; } = null!;
        public virtual DbSet<CatNotype> CatNotypes { get; set; } = null!;
        public virtual DbSet<EventsLog> EventsLogs { get; set; } = null!;
        public virtual DbSet<TabConf> TabConfs { get; set; } = null!;
        public virtual DbSet<TabNotification> TabNotifications { get; set; } = null!;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();
                              
                string? connectionString = configuration.GetConnectionString("enm_dbConnection");

                optionsBuilder.UseNpgsql(connectionString);
            
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CatNotResponse>(entity =>
            {
                entity.HasKey(e => e.NotResponse)
                    .HasName("cat_not_response_pk");

                entity.ToTable("cat_not_response", "enm");

                entity.Property(e => e.NotResponse)
                    .ValueGeneratedNever()
                    .HasColumnName("not_response");

                entity.Property(e => e.IdResp)
                    .ValueGeneratedOnAdd()
                    .HasColumnName("id_resp");

                entity.Property(e => e.ResponseDesc)
                    .HasMaxLength(100)
                    .HasColumnName("response_desc");
            });

            modelBuilder.Entity<CatNotState>(entity =>
            {
                entity.HasKey(e => e.NotState)
                    .HasName("cat_not_state_pk");

                entity.ToTable("cat_not_state", "enm");

                entity.Property(e => e.NotState)
                    .ValueGeneratedNever()
                    .HasColumnName("not_state");

                entity.Property(e => e.IdStat).HasColumnName("id_stat");

                entity.Property(e => e.StatDescription)
                    .HasMaxLength(100)
                    .HasColumnName("stat_description");
            });

            modelBuilder.Entity<CatNotype>(entity =>
            {
                entity.HasKey(e => e.NotType)
                    .HasName("cat_notypes_pk");

                entity.ToTable("cat_notypes", "enm");

                entity.Property(e => e.NotType)
                    .ValueGeneratedNever()
                    .HasColumnName("not_type");

                entity.Property(e => e.IdType)
                    .ValueGeneratedOnAdd()
                    .HasColumnName("id_type");

                entity.Property(e => e.NotTypeDesc)
                    .HasMaxLength(100)
                    .HasColumnName("not_type_desc");
            });

            modelBuilder.Entity<EventsLog>(entity =>
            {
                entity.HasKey(e => new { e.EventId, e.PartitionId })
                    .HasName("events_log_pkey");

                entity.ToTable("events_log", "enm");

                entity.Property(e => e.EventId).HasColumnName("event_id");

                entity.Property(e => e.PartitionId).HasColumnName("partition_id");

                entity.Property(e => e.EventCode)
                    .HasMaxLength(5)
                    .HasColumnName("event_code");

                entity.Property(e => e.EventDatetime)
                    .HasColumnType("timestamp without time zone")
                    .HasColumnName("event_datetime");

                entity.Property(e => e.EventDatetimeUtc)
                    .HasColumnType("timestamp without time zone")
                    .HasColumnName("event_datetime_utc");

                entity.Property(e => e.EventInfo).HasColumnName("event_info");

                entity.Property(e => e.EventLevelId)
                    .HasMaxLength(3)
                    .HasColumnName("event_level_id");

                entity.Property(e => e.EventMessage).HasColumnName("event_message");

                entity.Property(e => e.EventModuleId).HasColumnName("event_module_id");

                entity.Property(e => e.EventObjectId).HasColumnName("event_object_id");

                entity.Property(e => e.EventOffset).HasColumnName("event_offset");

                entity.Property(e => e.EventSystemId).HasColumnName("event_system_id");

                entity.Property(e => e.EventTypeId)
                    .HasMaxLength(3)
                    .HasColumnName("event_type_id");
            });

            modelBuilder.Entity<TabConf>(entity =>
            {
                entity.HasNoKey();

                entity.ToTable("tab_conf", "enm");

                entity.Property(e => e.ApiRoot).HasColumnName("api_root");

                entity.Property(e => e.ConfFrom)
                    .HasMaxLength(128)
                    .HasColumnName("conf_from");

                entity.Property(e => e.ConnInsertConf).HasColumnName("conn_insert_conf");

                entity.Property(e => e.ConnReadConf).HasColumnName("conn_read_conf");

                entity.Property(e => e.ConnResumeConf).HasColumnName("conn_resume_conf");

                entity.Property(e => e.CreationTimeConf)
                    .HasColumnType("timestamp without time zone")
                    .HasColumnName("creation_time_conf")
                    .HasDefaultValueSql("now()");

                entity.Property(e => e.IdConf)
                    .ValueGeneratedOnAdd()
                    .HasColumnName("id_conf");

                entity.Property(e => e.PasswordConf).HasColumnName("password_conf");

                entity.Property(e => e.PortConf).HasColumnName("port_conf");

                entity.Property(e => e.ReadIntervalConf).HasColumnName("read_interval_conf");

                entity.Property(e => e.ReadIntervalResume).HasColumnName("read_interval_resume");

                entity.Property(e => e.ResumeIntervalResume).HasColumnName("resume_interval_resume");

                entity.Property(e => e.SmtpConf).HasColumnName("smtp_conf");

                entity.Property(e => e.ToConf)
                    .HasMaxLength(128)
                    .HasColumnName("to_conf");

                entity.Property(e => e.UpdateTimeConf)
                    .HasColumnType("timestamp without time zone")
                    .HasColumnName("update_time_conf")
                    .HasDefaultValueSql("now()");

                entity.Property(e => e.UsernameConf).HasColumnName("username_conf");
            });

            modelBuilder.Entity<TabNotification>(entity =>
            {
                entity.HasKey(e => e.IdNot)
                    .HasName("tab_notifications_pkey");

                entity.ToTable("tab_notifications", "enm");

                entity.Property(e => e.IdNot).HasColumnName("id_not");

                entity.Property(e => e.EventId).HasColumnName("event_id");

                entity.Property(e => e.NotContent).HasColumnName("not_content");

                entity.Property(e => e.NotCreated)
                    .HasColumnType("timestamp without time zone")
                    .HasColumnName("not_created");

                entity.Property(e => e.NotFls).HasColumnName("not_fls");

                entity.Property(e => e.NotFrom)
                    .HasMaxLength(100)
                    .HasColumnName("not_from");

                entity.Property(e => e.NotResponse).HasColumnName("not_response");

                entity.Property(e => e.NotState).HasColumnName("not_state");

                entity.Property(e => e.NotSubject)
                    .HasMaxLength(100)
                    .HasColumnName("not_subject");

                entity.Property(e => e.NotTo)
                    .HasMaxLength(100)
                    .HasColumnName("not_to");

                entity.Property(e => e.NotType).HasColumnName("not_type");

                entity.Property(e => e.NotUpdated)
                    .HasColumnType("timestamp without time zone")
                    .HasColumnName("not_updated");

                entity.HasOne(d => d.NotResponseNavigation)
                    .WithMany(p => p.TabNotifications)
                    .HasForeignKey(d => d.NotResponse)
                    .HasConstraintName("tab_notifications_fk");

                entity.HasOne(d => d.NotStateNavigation)
                    .WithMany(p => p.TabNotifications)
                    .HasForeignKey(d => d.NotState)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("tab_notifications_state_fk");

                entity.HasOne(d => d.NotTypeNavigation)
                    .WithMany(p => p.TabNotifications)
                    .HasForeignKey(d => d.NotType)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("tab_notifications_nottype_fk");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
