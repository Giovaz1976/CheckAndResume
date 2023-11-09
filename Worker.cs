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
               
                var sourceConnectionString = new NpgsqlConnection("Server=localhost;Database=enm_db;user id=postgres;Password=nolose;");



                await sourceConnectionString.OpenAsync();             

               

               
                using NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM enm.tab_notifications where not_state = 3;", sourceConnectionString);
                
                using NpgsqlDataReader reader = await command.ExecuteReaderAsync();

              

                try
                {

                    while (await reader.ReadAsync())
                    {
                       

                        var rowData = new Dictionary<string, object>
                        {

                        

                            {"IdNot" , reader["id_not"] },
                            {"NotCreated" , reader["not_created"] },
                            {"NotFrom" , reader["not_from"] },
                            {"NotTo" , reader["not_to"] },
                            {"NotType" , reader["not_type"] },
                            {"NotState" , reader["not_state"] },
                            {"NotResponse" , reader["not_response"] },
                            {"NotUpdated" , reader["not_updated"] },
                            {"NotSubject" , reader["not_subject"] },
                            {"NotContent" , reader["not_content"] },
                            {"NotFls" , reader["not_fls"] },
                            {"EventId" , reader["event_id"] },




                        };


                    



                        var payload = JsonConvert.SerializeObject(rowData);

                       


                        using (var httpClient = new HttpClient())
                        {
                            
                            var apiUrl = "https://localhost:5000/EmailSender/";

                            var content = new StringContent(payload, Encoding.UTF8, "application/json");
                            
                            var response = await httpClient.PutAsync(apiUrl, content);

                            if (response.IsSuccessStatusCode)
                            {

                                _logger.LogInformation("API POST request succeeded for a row.");

                              

                                _logger.LogInformation("State Changed Successfully");

                                                                                                                        

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
                   Console.WriteLine("Error: " + ex.Message);
                }

                sourceConnectionString.Close();
               



                _logger.LogInformation("Pending emails send completed: {time}", DateTimeOffset.Now);
                await Task.Delay(TimeSpan.FromMinutes(30), stoppingToken);
               

            }
        }

    }
}