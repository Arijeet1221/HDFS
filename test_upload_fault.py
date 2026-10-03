import requests

# Upload test file for fault tolerance testing
url = "http://127.0.0.1:5000/api/upload"
file_path = "test_fault.bin"

try:
    with open(file_path, 'rb') as f:
        files = {'file': (file_path, f)}
        data = {'filename': 'test_fault.bin'}
        
        response = requests.post(url, files=files, data=data)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        if response.status_code == 200 and response.json().get('status') == 'success':
            print("✅ Upload successful for fault tolerance test")
        else:
            print("❌ Upload failed")
            
except Exception as e:
    print(f"Error: {e}")
