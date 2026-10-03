import requests
import os

# Test file upload to HDFS
url = "http://127.0.0.1:5000/api/upload"
file_path = "test_large_file.bin"

if not os.path.exists(file_path):
    print(f"Error: {file_path} not found")
    exit(1)

try:
    with open(file_path, 'rb') as f:
        files = {'file': (file_path, f)}
        data = {'filename': 'test_large_file.bin'}
        
        response = requests.post(url, files=files, data=data)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        if response.status_code == 200 and response.json().get('status') == 'success':
            print("✅ Upload successful!")
        else:
            print("❌ Upload failed")
            
except Exception as e:
    print(f"Error: {e}")
