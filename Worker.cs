using Newtonsoft.Json;
using Npgsql;
using System.Text;

namespace CheckAndResume
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {


            while (!stoppingToken.IsCancellationRequested)
            {
                //string sourceConnectionString = "Server=localhost;Port=5436;Database=cl.qfree.zen_0.0.9_202308;user id=qfree;Password=123456;";
                string sourceConnectionString = "Server=localhost;Database=enm_db;user id=postgres;Password=nolose;";
              


                using NpgsqlConnection sourceConnection = new NpgsqlConnection(sourceConnectionString);
                await sourceConnection.OpenAsync();

              

               

                //using NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM events.events_log where event_code  <> '99401' and event_code in ('XX000');", sourceConnection);
                using NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM events.events_log where event_code in ('99401','XX000','P0001','99999','99480','99409','99403','99400','42P01','42883','42846','42809','42804','42803','42704','42703','42702','42601','3F000','2D000','23505','22P02','22023','22012','22007','22004','22003','22001','21000','0A000') AND event_datetime AT TIME ZONE 'CST7CDT' BETWEEN CURRENT_TIMESTAMP - INTERVAL '10 minutes' AND CURRENT_TIMESTAMP;", sourceConnection);
                //using NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM events.events_log where event_code  <> '99401' and event_code in ('XX000','P0001','99999','99480','99409','99403','99400','42P01','42883','42846','42809','42804','42803','42704','42703','42702','42601','3F000','2D000','23505','22P02','22023','22012','22007','22004','22003','22001','21000','0A000') AND event_datetime AT TIME ZONE 'CST7CDT' BETWEEN CURRENT_TIMESTAMP - INTERVAL '10 minutes' AND CURRENT_TIMESTAMP;", sourceConnection);
                using NpgsqlDataReader reader = await command.ExecuteReaderAsync();


                //using NpgsqlTransaction transaction = destinationConnection.BeginTransaction();

                try
                {

                    while (await reader.ReadAsync())
                    {
                        // Assuming destination table has the same structure as the source table


                        var rowData = new Dictionary<string, object>
                        {

                       {"param1" , reader["event_id"] },
                       {"param2" , reader["event_type_id"] },
                       {"param3" , reader["event_level_id"] },
                       {"param4" , reader["event_system_id"] },
                       {"param5" , reader["event_module_id"] },
                       {"param6" , reader["event_object_id"] },
                       {"param7" , reader["event_datetime_utc"] },
                       {"param8" , reader["event_datetime"] },
                       {"param9" , reader["event_offset"] },
                       {"param10" , reader["event_code"] },
                       {"param11" , reader["event_message"] },
                       {"param12" , reader["event_info"] },
                       {"param13" , reader["partition_id"] },



                        };
                        rowData.Add("NotFrom", "emanotmod@gmail.com");
                        rowData.Add("NotTo", "emanotmod@gmail.com");
                        rowData.Add("NotType", 2);
                        rowData.Add("NotState", 1);
                        rowData.Add("NotResponse", 200);
                        rowData.Add("NotSubject", reader["event_message"]);
                        rowData.Add("NotContent", reader["event_info"]);
                        rowData.Add("EventId", reader["event_id"]);




                        // Add parameters for all columns

                        await insertCommandIN.ExecuteNonQueryAsync();

                        var payload = JsonConvert.SerializeObject(rowData);

                        //transaction.Commit();


                        using (var httpClient = new HttpClient())
                        {
                            var apiUrl = "https://localhost:5001/EmailSender/api/saveMail/";

                            var content = new StringContent(payload, Encoding.UTF8, "application/json");
                            //var content = new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json");

                            var response = await httpClient.PostAsync(apiUrl, content);

                            if (response.IsSuccessStatusCode)
                            {
                                _logger.LogInformation("API POST request succeeded for a row.");
                            }
                            else
                            {
                                _logger.LogError("API POST request failed with status code: {statusCode} for a row.", response.StatusCode);
                            }
                        }
                    }



                }
                catch (Exception ex)
                {
                    //transaction.Rollback();
                    Console.WriteLine("Error: " + ex.Message);
                }

                sourceConnection.Close();
               



                _logger.LogInformation("Table copy operation completed: {time}", DateTimeOffset.Now);
                await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
                //await Task.Delay(15000, stoppingToken);

            }
        }

    }
}