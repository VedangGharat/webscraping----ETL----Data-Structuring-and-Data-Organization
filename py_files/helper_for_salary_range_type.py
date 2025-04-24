import re 


def get_salary_info(salary_text):
    """
    Determines if the salary is hourly or yearly and extracts the salary range.
    
    Args:
        salary_text (str): The salary text extracted from the HTML.
        
    Returns:
        tuple: (salary_type, min_salary, max_salary) where salary_type is 'hourly' or 'yearly', 
               and min_salary, max_salary are the extracted salary values or None.
    """
    salary_type = 'unknown'
    min_salary = None
    max_salary = None

    # Function to clean salary strings
    def clean_salary_string(salary_str):
        # Remove currency symbols and commas, convert to float
        return float(salary_str.replace('$', '').replace(',', ''))

    # Check for hourly salary
    hourly_match = re.search(r'\$([\d,]+(?:\.\d+)?)\/(hour|hr|hourly)', salary_text, re.IGNORECASE)
    if hourly_match:
        salary_type = 'hourly'
        min_salary = clean_salary_string(hourly_match.group(1))
        # Extract maximum salary if a range is present
        max_match = re.search(r'(\$[\d,]+(?:\.\d+)?)\/(hour|hr|hourly)', salary_text.split('-')[-1].strip(), re.IGNORECASE)
        if max_match:
            max_salary = clean_salary_string(max_match.group(1))
        return salary_type, min_salary, max_salary

    # Check for yearly salary
    yearly_match = re.search(r'\$([\d,]+(?:\.\d+)?)\/(year|yr|yearly)', salary_text, re.IGNORECASE)
    if yearly_match:
        salary_type = 'yearly'
        min_salary = clean_salary_string(yearly_match.group(1))
        # Extract maximum salary if a range is present
        max_match = re.search(r'(\$[\d,]+(?:\.\d+)?)\/(year|yr|yearly)', salary_text.split('-')[-1].strip(), re.IGNORECASE)
        if max_match:
            max_salary = clean_salary_string(max_match.group(1))
        return salary_type, min_salary, max_salary

    return salary_type, min_salary, max_salary