from google_auth_oauthlib.flow import InstalledAppFlow

# Dois escopos de uma vez só
SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/drive.readonly'
]

flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
creds = flow.run_local_server(port=5000)  # abre browser para login/consentimento

# salva o token.json com os dois escopos
with open('token.json', 'w') as token:
    token.write(creds.to_json())

print("token.json (Gmail+Drive) gerado com sucesso.")
