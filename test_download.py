import requests
import hashlib
import os

# Test file download from HDFS
url = "http://127.0.0.1:5000/api/download/test_large_file.bin"
output_path = "test_large_file_downloaded.bin"

try:
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ Download successful!")
        print(f"Downloaded size: {len(response.content)} bytes")
        
        # Calculate checksums
        with open("test_large_file.bin", 'rb') as f:
            original_hash = hashlib.sha256(f.read()).hexdigest()
        
        with open(output_path, 'rb') as f:
            downloaded_hash = hashlib.sha256(f.read()).hexdigest()
        
        print(f"Original SHA256: {original_hash}")
        print(f"Downloaded SHA256: {downloaded_hash}")
        
        if original_hash == downloaded_hash:
            print("✅ Checksum verification PASSED - files are identical")
        else:
            print("❌ Checksum verification FAILED - files differ")
    else:
        print(f"❌ Download failed: {response.text}")
        
except Exception as e:
    print(f"Error: {e}")
