import requests

resp = requests.post(
    "https://engine.metri.one/metri.MetriService/Query",
    headers={
        "Content-Type": "application/grpc-web+proto",
        "X-Grpc-Web": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    },
    data=b"\x00\x00\x00\x00\x00"
)
print("Status:", resp.status_code)
print("Body:", resp.text[:100])
