# src/test_medium.py
import requests

# Bandit Test: B113
# Severity: MEDIUM
# Reason: HTTP requests without a timeout can cause your
# program to hang indefinitely if the remote server doesn't respond.
def fetch_data(url):
    response = requests.get(url)  # This will trigger B113 (MEDIUM)
    return response.text