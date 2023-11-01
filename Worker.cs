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


            while (true)
            {
                //string sourceConnectionString = "Server=localhost;Port=5436;Database=cl.qfree.zen_0.0.9_202308;user id=qfree;Password=123456;";
                //string sourceConnectionString = "Server=localhost;Database=enm_db;user id=postgres;Password=nolose;";
                //using (var sourceConnectionString = new NpgsqlConnection("Server=localhost;Database=enm_db;user id=postgres;Password=nolose;"));
                var sourceConnectionString = new NpgsqlConnection("Server=localhost;Database=enm_db;user id=postgres;Password=nolose;");






                await sourceConnectionString.OpenAsync();

              

               

               
                using NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM enm.tab_notifications where not_state = 3;", sourceConnectionString);
                
                using NpgsqlDataReader reader = await command.ExecuteReaderAsync();


                //using NpgsqlTransaction transaction = destinationConnection.BeginTransaction();

                try
                {

                    while (await reader.ReadAsync())
                    {
                        // Assuming destination table has the same structure as the source table
                        //int notState = (int)reader["not_state"];

                        var rowData = new Dictionary<string, object>
                        {

                       {"param1" , reader["id_not"] },
                       {"param2" , reader["not_created"] },
                       {"param3" , reader["not_from"] },
                       {"param4" , reader["not_to"] },
                       {"param5" , reader["not_type"] },
                       {"param6" , reader["not_state"] },
                       {"param7" , reader["not_response"] },
                       {"param8" , reader["not_updated"] },
                       {"param9" , reader["not_subject"] },
                       {"param10" , reader["not_content"] },
                       {"param11" , reader["not_fls"] },
                       {"param12" , reader["event_id"] }
                       



                        };
                        rowData.Add("NotFrom", "emanotmod@gmail.com");
                        rowData.Add("NotTo", "emanotmod@gmail.com");
                        rowData.Add("NotType", 2);
                        rowData.Add("NotState", 1);
                        rowData.Add("NotResponse", 200);
                        rowData.Add("NotSubject", reader["not_subject"]);
                        rowData.Add("NotContent", reader["not_content"]);
                        rowData.Add("EventId", reader["event_id"]);





                        // Add parameters for all columns

                        

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

                sourceConnectionString.Close();
               



                _logger.LogInformation("Pending emails send completed: {time}", DateTimeOffset.Now);
                await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
                //await Task.Delay(15000, stoppingToken);

            }
        }

    }
}