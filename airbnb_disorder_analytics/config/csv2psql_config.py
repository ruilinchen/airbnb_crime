'''
csv2psql_config.py
with information to match csv columns (of raw data) to database columns by table

ruilin chen
08/09/2020
'''

__all__ = ['ColumnInfo']


class ColumnInfo:
    tables = ['property', 'daily_booking', 'monthly_match', 'review', 'reviewer']
    property = {
        'table': 'property',
        'primary_keys': ['property_id'],
        'db': ['property_id', 'property_title', 'property_type',
               'listing_type', 'created_on', 'last_scraped_on',
               'country', 'latitude', 'longitude', 'state',
               'city', 'zipcode', 'neighborhood', 'msa',
               'average_daily_rate', 'annual_revenue',
               'occupancy_rate', 'number_of_bookings',
               'count_reservation_days', 'count_available_days',
               'count_blocked_days',
               'response_rate', 'airbnb_superhost',
               'security_deposit', 'cleaning_fee', 'published_nightly_rate',
               'published_monthly_rate', 'published_weekly_rate', 'number_of_reviews',
               'overall_rating', 'airbnb_host_id', 'airbnb_listing_url'
               ],  # a list of all the column headings to be included in the insertion entry
        'df': ['Property ID', 'Listing Title', 'Property Type',
               'Listing Type', 'Created Date', 'Last Scraped Date',
               'Country', 'Latitude', 'Longitude',
               'State', 'City', 'Zipcode', 'Neighborhood',
               'Metropolitan Statistical Area', 'Average Daily Rate (USD)',
               'Annual Revenue LTM (USD)', 'Occupancy Rate LTM',
               'Number of Bookings LTM', 'Count Reservation Days LTM',
               'Count Available Days LTM', 'Count Blocked Days LTM',
               'Response Rate', 'Airbnb Superhost',
               'Security Deposit (USD)', 'Cleaning Fee (USD)',
               'Published Nightly Rate (USD)', 'Published Monthly Rate (USD)',
               'Published Weekly Rate (USD)', 'Number of Reviews', 'Overall Rating',
               'Airbnb Host ID', 'Airbnb Listing URL'
               ]  # a list of pandas column names arranged in the same sequence as the database columns list
    }

    daily_booking = {
        'table': 'daily_booking',
        'primary_keys': ['property_id', 'date'],
        'db': ['property_id', 'date', 'status',
               'booked_date', 'price'
               ],  # a list of all the column headings to be included in the insertion entry
        'df': ['Property ID', 'Date', 'Status', 'Booked Date', 'Price (USD)'
               ]  # a list of pandas column names arranged in the same sequence as the database columns list
    }
    monthly_match = {
        'table': 'monthly_match',
        'primary_keys': ['property_id', 'reporting_month'],
        'db': ['property_id', 'reporting_month', 'occupancy_rate',
               'revenue', 'number_of_reservations', 'reservation_days',
               'available_days', 'blocked_days', 'active'
               ],  # a list of all the column headings to be included in the insertion entry
        'df': ['Property ID', 'Reporting Month', 'Occupancy Rate', 'Revenue (USD)',
               'Number of Reservations', 'Reservation Days', 'Available Days',
               'Blocked Days', 'Active'
               ]  # a list of pandas column names arranged in the same sequence as the database columns list
    }
    review = {
        'table': 'review',
        'primary_keys': ['property_id', 'reviewer_id'],
        'db': ['property_id', 'review_date', 'review_text', 'reviewer_id'
               ],  # a list of all the column headings to be included in the insertion entry
        'df': ['Property ID', 'Review Date', 'Review Text', 'User ID'
               ]  # a list of pandas column names arranged in the same sequence as the database columns list
    }
    reviewer = {
        'table': 'reviewer',
        'primary_keys': ['reviewer_id'],
        'db': ['reviewer_id', 'member_since', 'first_name',
               'country', 'state', 'city', 'description',
               'school', 'work', 'profile_url', 'profile_image_url'
               ],  # a list of all the column headings to be included in the insertion entry
        'df': ['User ID', 'Member Since', 'First Name',
               'Country', 'State', 'City',
               'Description', 'School', 'Work',
               'Profile Image URL', 'Profile URL'
               ]  # a list of pandas column names arranged in the same sequence as the database columns list
    }
