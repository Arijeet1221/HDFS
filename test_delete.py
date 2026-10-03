import requests

# Test file deletion from HDFS
url = "http://127.0.0.1:5000/api/files/test_large_file.bin"

try:
    response = requests.delete(url)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    if response.status_code == 200 and response.json().get('status') == 'success':
        print("✅ Delete successful!")
    else:
        print("❌ Delete failed")
        
except Exception as e:
    print(f"Error: {e}")
