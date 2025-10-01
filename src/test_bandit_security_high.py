# src/test_high.py
import subprocess

# Bandit Test: B602
# Severity: HIGH
# Reason: Running subprocesses with shell=True allows shell injection
# attacks, meaning an attacker could execute arbitrary commands.
def run_command(cmd):
    subprocess.Popen(cmd, shell=True)  # This will trigger B602 (HIGH)