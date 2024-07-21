import requests
from config import CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN

def get_access_token():
    url = 'https://api.amazon.com/auth/o2/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': REFRESH_TOKEN,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    }
    
    # Print the payload for debugging
    print("Payload:", payload)
    
    response = requests.post(url, headers=headers, data=payload)
    
    # Print the full response for debugging
    print("Response status code:", response.status_code)
    print("Response content:", response.content)
    
    response_data = response.json()
    
    if 'access_token' in response_data:
        return response_data['access_token']
    else:
        raise Exception("Failed to obtain access token: " + response_data.get('error_description', 'Unknown error'))

if __name__ == "__main__":
    try:
        access_token = get_access_token()
        print(f"Access Token: {access_token}")
    except Exception as e:
        print(f"Error: {e}")
