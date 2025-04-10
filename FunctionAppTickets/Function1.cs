using System;
using System.Data;
using System.Text.Json;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FunctionAppTickets
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(Function1))]
        public async Task Run([QueueTrigger("tickethub", Connection = "AzureWebJobsStorage")] QueueMessage message)
        {
            _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
            var ticket = JsonSerializer.Deserialize<Ticket>(message.MessageText, options);

            if(ticket == null)
            {
                _logger.LogError("Ticket is null - failed to deserialize ticket.");
                return;
            }

            //fetch connection string from environment variable
            string? connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("SQL connection string is not set in the environment variables.");
            }

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync(); 

                var query = @"
                    INSERT INTO Tickets (
                        concertId, email, name, phone, quantity,
                        creditCard, expiration, securityCode,
                        address, city, province, postalCode, country
                    ) VALUES (
                        @concertId, @email, @name, @phone, @quantity,
                        @creditCard, @expiration, @securityCode,
                        @address, @city, @province, @postalCode, @country
                    )";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    //cmd parms here
                    cmd.Parameters.Add("@concertId", SqlDbType.Int).Value = ticket.concertId;
                    cmd.Parameters.Add("@email", SqlDbType.NVarChar, 255).Value = ticket.email;
                    cmd.Parameters.Add("@name", SqlDbType.NVarChar, 50).Value = ticket.name;
                    cmd.Parameters.Add("@phone", SqlDbType.NVarChar, 20).Value = ticket.phone;
                    cmd.Parameters.Add("@quantity", SqlDbType.Int).Value = ticket.quantity;

                    cmd.Parameters.Add("@creditCard", SqlDbType.NVarChar, 20).Value = ticket.creditCard;
                    cmd.Parameters.Add("@expiration", SqlDbType.NVarChar, 5).Value = ticket.expiration;
                    cmd.Parameters.Add("@securityCode", SqlDbType.NVarChar, 4).Value = ticket.securityCode;

                    cmd.Parameters.Add("@address", SqlDbType.NVarChar, 50).Value = ticket.address;
                    cmd.Parameters.Add("@city", SqlDbType.NVarChar, 50).Value = ticket.city;
                    cmd.Parameters.Add("@province", SqlDbType.NVarChar, 25).Value = ticket.province;
                    cmd.Parameters.Add("@postalCode", SqlDbType.NVarChar, 10).Value = ticket.postalCode;
                    cmd.Parameters.Add("@country", SqlDbType.NVarChar, 50).Value = ticket.country;

                    await cmd.ExecuteNonQueryAsync();
                    
                }
            }
        }
    }
}
