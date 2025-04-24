import re

# Dictionary to map full state names to abbreviations and vice versa
us_state_abbrev = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL',
    'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI',
    'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT',
    'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND',
    'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN',
    'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
}

# Reverse the dictionary to map abbreviations back to full state names
abbrev_us_state = {v: k for k, v in us_state_abbrev.items()}

def get_location_details(location):
    location = location.strip()
    
    # Handle remote locations
    if location.lower() == "remote":
        return "", "United States (Remote)", ""
    
    # Check if the location matches a known state abbreviation
    match_abbrev = re.match(r"^(.*?),\s*([A-Z]{2})$", location)
    match_full = re.match(r"^(.*?),\s*(.*?)(,|$)", location)
    
    if match_abbrev:
        city = match_abbrev.group(1).strip()
        state_code = match_abbrev.group(2).strip()
        state = abbrev_us_state.get(state_code, "United States (Remote)")
    elif match_full:
        city = match_full.group(1).strip()
        state = match_full.group(2).strip()
        state_code = us_state_abbrev.get(state, "")
    else:
        city = ""
        state = "United States (Remote)"
        state_code = ""
    
    return city, state, state_code
