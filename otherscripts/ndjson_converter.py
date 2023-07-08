import json
# Load the JSON file
with open('restaurants.json', 'r') as file:
    data = json.load(file)
# Convert the data to NDJSON format
ndjson = ''
for item in data:
    print(item)
    ndjson += json.dumps(item) + '\n'
# Save the NDJSON data to a file
with open('restaurants.ndjson', 'w') as file:
    file.write(ndjson)