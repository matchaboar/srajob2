"""
Debug the voltage_park fixture escaping issue.
"""
import json
from pathlib import Path

fixture_path = Path('tests/job_scrape_application/workflows/fixtures/dbos_schedule/voltage_park_listing.json')
raw_text = fixture_path.read_text()

# Parse the fixture
fixture = json.loads(raw_text)
first_response = fixture['response'][0]

print("=== First response item ===")
print(f"Length: {len(first_response)}")
print(f"Starts with: {repr(first_response[:50])}")
print(f"Ends with: {repr(first_response[-50:])}")

# The key issue: inside the string, we have:
# {"content":{"commonmark":"```\n{\"data\":...
# When json.loads parses this, it sees:
# - { start object
# - "content" key
# - : colon
# - { start nested object
# - "commonmark" key
# - : colon
# - " start string value
# - ``` three backticks
# - \n which is the escape sequence for newline (valid)
# - { literal brace
# - \" which should be an escaped quote...

# But wait! After loading the fixture, the string has:
# \" which is backslash + quote (two chars)
# In JSON, inside a string, \" is an escaped quote
# So the parser should see: backslash-quote = literal quote

# Let me trace through exactly what the parser sees
print("\n=== Tracing JSON parsing ===")

# Simulate what json.loads sees
s = first_response
print(f"String to parse (first 100 chars): {repr(s[:100])}")

# Find the first problematic character
# Look for escape sequences
i = 0
escape_count = 0
in_string = False
string_start = -1
max_show = 10
shown = 0
while i < len(s):
    c = s[i]
    if not in_string:
        if c == '"':
            in_string = True
            string_start = i
    else:
        if c == '\\':
            # Escape sequence
            if i + 1 < len(s):
                next_char = s[i + 1]
                if next_char in '"\\bfnrt/':
                    i += 1  # Skip the escaped char
                elif next_char == 'u':
                    i += 5  # Skip \uXXXX
                else:
                    # Invalid escape!
                    if shown < max_show:
                        print(f"Invalid escape at {i}: \\{next_char}")
                        print(f"  Context: {repr(s[max(0,i-10):i+10])}")
                        shown += 1
                    escape_count += 1
                    i += 1
        elif c == '"':
            in_string = False
    i += 1

print(f"\nFound {escape_count} invalid escapes")

if escape_count > 0:
    print("\n=== The problem is invalid escape sequences ===")
    print("The fixture contains backslash followed by characters that aren't valid JSON escapes")
    print("For example: \\_ or \\N or similar")
