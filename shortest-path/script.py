import os

# Set the path to your folder
folder_path = "C:\\Users\\mugce\\OneDrive\\Desktop\\Spring 25\\Data Mining\\Final Project\\data-mining-graph-algorithms\\shortest-path\\input\\"
output_file = 'graph.txt'

with open(output_file, 'w', encoding='utf-8') as out_file:
    for filename in os.listdir(folder_path):
        if not os.path.isfile(os.path.join(folder_path, filename)):
            continue  # Skip subdirectories or non-files

        # Handle filenames that are safe (no commas in values themselves)
        base_name = os.path.splitext(filename)[0]  # Remove file extension
        parts = base_name.split(',')

        if len(parts) != 4:
            print(f"Skipping malformed filename: {filename}")
            continue

        out_file.write(','.join(parts) + '\n')

print(f"Bundled entries written to {output_file}")
