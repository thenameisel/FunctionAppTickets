using System.ComponentModel.DataAnnotations;

namespace FunctionAppTickets
{
    public class Ticket
    {

        public int purchaseId { get; set; }

        [Required]
        [Range(1, 200, ErrorMessage = "Valid concert ID required")]
        public int concertId { get; set; }

        [Required]
        [EmailAddress(ErrorMessage = "Valid email required")]
        public string email { get; set; }

        [Required]
        [StringLength(50, MinimumLength = 2, ErrorMessage = "Name must be less than 50 characters")]
        public string name { get; set; }

        [Required]
        [Phone(ErrorMessage = "Valid phone required")]
        public string phone { get; set; }

        [Required]
        [Range(1, 25, ErrorMessage = "Valid quantity required")]
        public int quantity { get; set; }

        [Required]
        [CreditCard(ErrorMessage = "Valid credit card required")]
        public string creditCard { get; set; }

        [Required]
        [RegularExpression(@"^(0[1-9]|1[0-2])\/\d{2}$", ErrorMessage = "Valid expiration required")]
        public string expiration { get; set; }

        [Required]
        [RegularExpression(@"^[0-9]{3,4}$", ErrorMessage = "Security code must be 3 or 4 digits")]
        public string securityCode { get; set; }

        [Required]
        [StringLength(50, MinimumLength = 3, ErrorMessage = "Address must be less than 50 characters")]
        public string address { get; set; }

        [Required]
        [StringLength(50, MinimumLength = 3, ErrorMessage = "City must be less than 50 characters")]
        public string city { get; set; }

        [Required]
        [StringLength(25, MinimumLength = 2, ErrorMessage = "Province must be less than 25 characters")]
        public string province { get; set; }

        [Required]
        [RegularExpression(@"^(?i)[a-z]\d[a-z][ -]?\d[a-z]\d(?-i)$", ErrorMessage = "Valid postal code required")]
        public string postalCode { get; set; }

        [Required]
        [RegularExpression(@"^(?i)canada(?-i)$", ErrorMessage = "Sorry, we only sell to Canada!")]
        public string country { get; set; }

        public DateTime CreatedDate { get; set; }

    }
}