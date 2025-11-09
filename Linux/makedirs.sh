#!/bin/bash


for i in $(seq 1 100); do
  # Define directory and file names
  dir_name="test$i"
  file_name="file$i"


  if mkdir "$dir_name"; then
    echo "Created directory $dir_name" [cite: 20]
  else
    echo "Error creating directory $dir_name or it already exists."
    continue # Skip to the next iteration if directory creation fails
  fi


  if echo "This is file$i" > "$dir_name/$file_name"; then
    echo "Created file $dir_name/$file_name" [cite: 21]
  else
    echo "Error creating file $dir_name/$file_name"
  fi
done

echo "All directories and files created successfully."
