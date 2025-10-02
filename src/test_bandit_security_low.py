# src/test_low.py

# Bandit Test: B101
# Severity: LOW
# Reason: Using `assert` statements is unsafe in production code
# because they can be globally disabled with the -O (optimize) flag.
def check_user(user):
    assert user != ""  # This will trigger B101 (LOW)