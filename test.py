
import s3fs

# ✅ Cloudflare R2 S3 credentials
fs = s3fs.S3FileSystem(
    anon=False,
    key="5c8ea9c516abfc78987bc98c70d2868a",
    secret="0cf64f9f0b64f6008cf5efe1529c6772daa7d7d0822f5db42a7c6a1e41b3cadf",
    client_kwargs={
        "endpoint_url": "https://cbabd13f6c54798a9ec05df5b8070a6e.r2.cloudflarestorage.com"
    },
)

# ✅ List all files inside candles folder
path = "desiquant/data/candles/NIFTY/2024-01-11"

print(f"Listing all files under: {path}\n")

files = fs.ls(path, detail=True)  # detail=True gives size & timestamps
for f in files:
    print(f"{f['Key']}  ({f['Size']/1024:.1f} KB)")

print(f"\n✅ Total files found: {len(files)}")
