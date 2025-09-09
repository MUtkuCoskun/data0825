import sys, json, pathlib
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def main():
    if len(sys.argv) != 2:
        print("Kullanım: python scripts/generate_google_token.py /path/to/client_secret.json")
        sys.exit(1)
    path = pathlib.Path(sys.argv[1])
    conf = json.loads(path.read_text(encoding="utf-8"))
    flow = InstalledAppFlow.from_client_config(conf, SCOPES)
    creds = flow.run_local_server(port=0)
    print("\n=== GOOGLE_OAUTH_TOKEN (JSON) ===\n")
    print(creds.to_json())

if __name__ == "__main__":
    main()
